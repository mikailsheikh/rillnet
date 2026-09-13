#pragma once

#include <rillnet/frame.hpp>
#include <rillnet/frame_codec.hpp>
#include <rillnet/status_code.hpp>
#include <rillnet/transport.hpp>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/experimental/channel.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/error_code.hpp>
#include <boost/system/system_error.hpp>

#include <cstddef>
#include <limits>
#include <string>
#include <utility>

namespace rillnet {

// The outcome of a single WriteQueue::enqueue call.
struct WriteResult {
    StatusCode status = StatusCode::ok;
    std::string message;

    [[nodiscard]] bool ok() const noexcept { return status == StatusCode::ok; }

    [[nodiscard]] static WriteResult success() noexcept { return WriteResult{}; }

    [[nodiscard]] static WriteResult failure(StatusCode status, std::string message)
    {
        return WriteResult{status, std::move(message)};
    }
};

struct WriteQueueLimits {
    std::size_t max_frames = (std::numeric_limits<std::size_t>::max)();
    std::size_t max_bytes = (std::numeric_limits<std::size_t>::max)();
};

// Provides one coordinated write path per connection. Several operations may call enqueue()
// concurrently (as coroutines interleaved on the same executor); frames are handed to the
// underlying Transport strictly in the order they were enqueued, and the queue guarantees that no
// two writes overlap on the socket.
//
// A WriteQueue must be driven by a single run() coroutine, spawned once per connection for the
// lifetime of the queue. enqueue() only ever appends to the internal FIFO and never touches the
// transport directly; run() is the sole writer.
//
// The queue has no size limit by default: enqueue() never suspends the caller waiting for
// capacity.
class WriteQueue {
  public:
    using WriteObserver = std::function<void(const Frame &, std::size_t)>;
    // Effectively unbounded: large enough that enqueue() never blocks on capacity in practice.
    static constexpr std::size_t unbounded_capacity = (std::numeric_limits<std::size_t>::max)();

    explicit WriteQueue(boost::asio::any_io_executor executor, Transport &transport,
                        WriteQueueLimits limits = {}, WriteObserver write_observer = {})
        : transport_(transport), limits_(limits), channel_(executor, limits.max_frames),
          space_channel_(std::move(executor), space_signal_capacity(limits)),
          write_observer_(std::move(write_observer))
    {
    }

    explicit WriteQueue(boost::asio::any_io_executor executor, Transport &transport,
                        std::size_t max_frames)
        : WriteQueue(std::move(executor), transport,
                     WriteQueueLimits{max_frames, unbounded_capacity})
    {
    }

    WriteQueue(const WriteQueue &) = delete;
    WriteQueue &operator=(const WriteQueue &) = delete;

    // Enqueues a frame for writing, preserving FIFO order relative to every other enqueue() call.
    // Returns once the frame has been accepted into the queue, which is not the same as having
    // been written to the transport yet. Waits for bounded capacity to become available and
    // reports resource_limit_exceeded if the queue is closed or the frame is too large.
    [[nodiscard]] WriteResult try_enqueue(Frame frame) { return try_enqueue_frame(frame); }

    boost::asio::awaitable<WriteResult> enqueue(Frame frame)
    {
        const auto bytes = queued_bytes(frame);
        if (bytes > limits_.max_bytes || limits_.max_frames == 0) {
            co_return WriteResult::failure(StatusCode::resource_limit_exceeded,
                                           "write queue capacity exceeded");
        }

        while (true) {
            auto result = try_enqueue_frame(frame);
            if (result.ok()) {
                co_return result;
            }
            if (closed_) {
                co_return result;
            }

            boost::system::error_code error;
            co_await space_channel_.async_receive(
                boost::asio::redirect_error(boost::asio::use_awaitable, error));
            if (error) {
                co_return WriteResult::failure(StatusCode::resource_limit_exceeded,
                                               error.message());
            }
        }
    }

    // Runs the write loop: repeatedly takes the next queued frame in FIFO order and writes it to
    // the transport to completion before starting the next, so writes never overlap on the socket.
    // Returns normally once close() has been called and every queued frame has drained. Returns a
    // failure if a transport write fails; the queue is left closed in that case and any frames
    // still queued are discarded.
    boost::asio::awaitable<WriteResult> run()
    {
        while (true) {
            boost::system::error_code error;
            auto frame = co_await channel_.async_receive(
                boost::asio::redirect_error(boost::asio::use_awaitable, error));

            if (error) {
                // The channel was closed and every buffered frame has already been delivered.
                co_return WriteResult::success();
            }

            queued_bytes_ -= queued_bytes(frame);
            (void)space_channel_.try_send(boost::system::error_code{});

            try {
                const auto encoded = encode_frame(frame);
                co_await transport_.write(encoded);
                if (write_observer_) {
                    write_observer_(frame, encoded.size());
                }
            } catch (const boost::system::system_error &write_error) {
                close();
                transport_.close();
                co_return WriteResult::failure(StatusCode::transport_error, write_error.what());
            }
        }
    }

    // Stops accepting new frames. Frames already queued are still delivered to run(), which then
    // returns once they have drained. Safe to call repeatedly and from within run() itself.
    void close() noexcept
    {
        closed_ = true;
        channel_.close();
        space_channel_.close();
    }

  private:
    [[nodiscard]] WriteResult try_enqueue_frame(Frame &frame)
    {
        const auto bytes = queued_bytes(frame);
        if (!can_fit(bytes)) {
            return WriteResult::failure(StatusCode::resource_limit_exceeded,
                                        "write queue capacity exceeded");
        }
        if (!channel_.try_send(boost::system::error_code{}, std::move(frame))) {
            return WriteResult::failure(StatusCode::resource_limit_exceeded,
                                        "write queue is closed or full");
        }
        queued_bytes_ += bytes;
        return WriteResult::success();
    }

    [[nodiscard]] static std::size_t queued_bytes(const Frame &frame) noexcept
    {
        return frame_header_size + frame.payload.size();
    }

    [[nodiscard]] bool can_fit(std::size_t bytes) const noexcept
    {
        return !closed_ && limits_.max_frames != 0 && bytes <= limits_.max_bytes &&
               queued_bytes_ <= limits_.max_bytes - bytes;
    }

    [[nodiscard]] static std::size_t space_signal_capacity(const WriteQueueLimits &limits) noexcept
    {
        if (limits.max_frames == unbounded_capacity || limits.max_frames == 0) {
            return 1;
        }
        return limits.max_frames;
    }

    Transport &transport_;
    WriteQueueLimits limits_;
    std::size_t queued_bytes_ = 0;
    bool closed_ = false;
    boost::asio::experimental::channel<void(boost::system::error_code, Frame)> channel_;
    boost::asio::experimental::channel<void(boost::system::error_code)> space_channel_;
    WriteObserver write_observer_;
};

} // namespace rillnet
