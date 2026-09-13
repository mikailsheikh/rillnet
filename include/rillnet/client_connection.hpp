#pragma once

#include <rillnet/codec.hpp>
#include <rillnet/frame.hpp>
#include <rillnet/frame_decoder.hpp>
#include <rillnet/frame_flags.hpp>
#include <rillnet/identifiers.hpp>
#include <rillnet/message_codec.hpp>
#include <rillnet/message_registry.hpp>
#include <rillnet/operation.hpp>
#include <rillnet/status_code.hpp>
#include <rillnet/stream_id_allocator.hpp>
#include <rillnet/transport.hpp>
#include <rillnet/write_queue.hpp>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/experimental/channel.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/error_code.hpp>
#include <boost/system/system_error.hpp>

#include <array>
#include <chrono>
#include <cstddef>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <unordered_map>
#include <utility>

namespace rillnet {

// Owns one client-side connection: a Transport, the single write path required by every
// operation sharing it, and the read loop that decodes incoming frames and routes each response
// to the request awaiting it. run() must be spawned once (e.g. via co_spawn) for the lifetime of
// the connection; request() can then be called concurrently from any number of coroutines running
// on the same executor.
template <typename CodecType = PodCodec> class ClientConnection {
  private:
    using ResponseChannel =
        boost::asio::experimental::channel<void(boost::system::error_code, Frame)>;

    struct PendingRequest {
        PendingRequest(boost::asio::any_io_executor executor, StreamId stream)
            : channel(executor, 1), timer(executor), operation(stream)
        {
            (void)operation.activate();
        }

        ResponseChannel channel;
        boost::asio::steady_timer timer;
        Operation operation;
        bool removed = false;
        bool stream_released = false;
    };

  public:
    template <typename Response> class RequestOperation {
      public:
        RequestOperation() = default;

        ~RequestOperation()
        {
            if (connection_ != nullptr && pending_) {
                connection_->discard_pending(pending_);
            }
        }

        RequestOperation(RequestOperation &&other) noexcept
            : connection_(std::exchange(other.connection_, nullptr)),
              pending_(std::move(other.pending_))
        {
        }

        RequestOperation &operator=(RequestOperation &&other) noexcept
        {
            if (this != &other) {
                if (connection_ != nullptr && pending_) {
                    connection_->discard_pending(pending_);
                }
                connection_ = std::exchange(other.connection_, nullptr);
                pending_ = std::move(other.pending_);
            }
            return *this;
        }

        RequestOperation(const RequestOperation &) = delete;
        RequestOperation &operator=(const RequestOperation &) = delete;

        [[nodiscard]] StreamId stream() const noexcept
        {
            if (pending_) {
                return pending_->operation.stream();
            }
            return StreamId{};
        }

        [[nodiscard]] bool cancel(std::string message = "operation cancelled")
        {
            if (connection_ == nullptr || !pending_) {
                return false;
            }
            return connection_->cancel_pending(pending_, std::move(message));
        }

        boost::asio::awaitable<DecodeResult<Response>> async_wait()
        {
            if (connection_ == nullptr || !pending_) {
                co_return DecodeResult<Response>::failure(StatusCode::operation_error,
                                                          "operation handle is empty");
            }

            boost::system::error_code error;
            auto frame = co_await pending_->channel.async_receive(
                boost::asio::redirect_error(boost::asio::use_awaitable, error));
            const auto pending = pending_;
            connection_->cleanup_pending(pending, true);

            if (error) {
                const auto &result = pending->operation.result();
                if (result.has_value()) {
                    co_return DecodeResult<Response>::failure(result->status(), result->message());
                }
                co_return DecodeResult<Response>::failure(
                    StatusCode::connection_closed, "connection closed while awaiting a response");
            }

            co_return decode_message<Response>(connection_->registry_, frame);
        }

      private:
        friend class ClientConnection;

        RequestOperation(ClientConnection *connection, std::shared_ptr<PendingRequest> pending)
            : connection_(connection), pending_(std::move(pending))
        {
        }

        ClientConnection *connection_ = nullptr;
        std::shared_ptr<PendingRequest> pending_;
    };

    template <typename Response> struct StartRequestResult {
        std::optional<RequestOperation<Response>> operation;
        StatusCode status = StatusCode::ok;
        std::string message;

        [[nodiscard]] bool ok() const noexcept { return status == StatusCode::ok; }

        [[nodiscard]] static StartRequestResult success(RequestOperation<Response> operation)
        {
            return StartRequestResult{std::move(operation), StatusCode::ok, {}};
        }

        [[nodiscard]] static StartRequestResult failure(StatusCode status, std::string message)
        {
            return StartRequestResult{std::nullopt, status, std::move(message)};
        }
    };

    ClientConnection(boost::asio::any_io_executor executor, std::unique_ptr<Transport> transport,
                     const MessageRegistry<CodecType> &registry,
                     WriteQueueLimits write_queue_limits = {},
                     std::uint64_t maximum_stream_id = std::numeric_limits<std::uint64_t>::max())
        : executor_(std::move(executor)), transport_(std::move(transport)), registry_(registry),
          write_queue_(executor_, *transport_, write_queue_limits),
          stream_ids_(StreamInitiator::client, maximum_stream_id)
    {
    }

    ClientConnection(const ClientConnection &) = delete;
    ClientConnection &operator=(const ClientConnection &) = delete;

    // Drives the connection's write queue and read loop until the transport is closed or fails.
    // Every request registered with this connection is failed with connection_closed once run()
    // returns.
    boost::asio::awaitable<void> run()
    {
        using namespace boost::asio::experimental::awaitable_operators;
        co_await (write_queue_.run() && read_loop());
    }

    // Sends a Request as a new stream and awaits the matching Response:
    //   1. allocate a stream;
    //   2. encode the request;
    //   3. send it;
    //   4. register an outstanding operation;
    //   5. await the corresponding response;
    //   6. release operation state.
    template <typename Request, typename Response>
    boost::asio::awaitable<DecodeResult<Response>> request(const Request &value)
    {
        auto started = co_await start_request<Request, Response>(value);
        if (!started.ok()) {
            co_return DecodeResult<Response>::failure(started.status, started.message);
        }

        auto operation = std::move(*started.operation);
        co_return co_await operation.async_wait();
    }

    template <typename Request, typename Response, typename Rep, typename Period>
    boost::asio::awaitable<DecodeResult<Response>>
    request(const Request &value, std::chrono::duration<Rep, Period> timeout)
    {
        auto started = co_await start_request<Request, Response>(value, timeout);
        if (!started.ok()) {
            co_return DecodeResult<Response>::failure(started.status, started.message);
        }

        auto operation = std::move(*started.operation);
        co_return co_await operation.async_wait();
    }

    template <typename Request, typename Response>
    boost::asio::awaitable<DecodeResult<Response>> request(const Request &value,
                                                           Operation::Deadline deadline)
    {
        auto started = co_await start_request<Request, Response>(value, deadline);
        if (!started.ok()) {
            co_return DecodeResult<Response>::failure(started.status, started.message);
        }

        auto operation = std::move(*started.operation);
        co_return co_await operation.async_wait();
    }

    template <typename Request, typename Response>
    boost::asio::awaitable<StartRequestResult<Response>> start_request(const Request &value)
    {
        const auto allocation = stream_ids_.allocate();
        if (!allocation.ok()) {
            co_return StartRequestResult<Response>::failure(allocation.status(),
                                                            "stream identifiers exhausted");
        }
        const StreamId stream = *allocation.stream();

        const auto encoded = encode_message(registry_, value, stream);
        if (!encoded.ok()) {
            co_return StartRequestResult<Response>::failure(encoded.status, encoded.message);
        }

        auto pending = std::make_shared<PendingRequest>(executor_, stream);
        pending_.emplace(stream, pending);

        const auto sent = co_await write_queue_.enqueue(std::move(*encoded.frame));
        if (!sent.ok()) {
            (void)pending->operation.fail(sent.status, sent.message);
            pending->channel.close();
            cleanup_pending(pending, true);
            co_return StartRequestResult<Response>::failure(sent.status, sent.message);
        }

        co_return StartRequestResult<Response>::success(
            RequestOperation<Response>(this, std::move(pending)));
    }

    template <typename Request, typename Response, typename Rep, typename Period>
    boost::asio::awaitable<StartRequestResult<Response>>
    start_request(const Request &value, std::chrono::duration<Rep, Period> timeout)
    {
        const auto deadline = Operation::Clock::now() +
                              std::chrono::duration_cast<Operation::Clock::duration>(timeout);
        co_return co_await start_request<Request, Response>(value, deadline);
    }

    template <typename Request, typename Response>
    boost::asio::awaitable<StartRequestResult<Response>> start_request(const Request &value,
                                                                       Operation::Deadline deadline)
    {
        auto started = co_await start_request<Request, Response>(value);
        if (!started.ok()) {
            co_return started;
        }

        auto pending = started.operation->pending_;
        (void)pending->operation.set_deadline(deadline);
        pending->timer.expires_at(deadline);
        pending->timer.async_wait([this, pending](const boost::system::error_code &error) {
            if (!error) {
                timeout_pending(pending);
            }
        });
        co_return started;
    }

  private:
    boost::asio::awaitable<void> read_loop()
    {
        std::array<std::byte, 4096> buffer{};
        while (transport_->is_open()) {
            std::size_t bytes_read = 0;
            try {
                bytes_read = co_await transport_->read(buffer);
            } catch (const boost::system::system_error &) {
                break;
            }
            if (bytes_read == 0) {
                // A zero-length read means the peer closed the connection, mirroring socket EOF.
                break;
            }

            for (auto &frame : decoder_.push(std::span(buffer).first(bytes_read))) {
                dispatch(std::move(frame));
            }
        }

        while (!pending_.empty()) {
            auto pending = pending_.begin()->second;
            (void)pending->operation.fail(StatusCode::connection_closed,
                                          "connection closed while awaiting a response");
            pending->channel.close();
            cleanup_pending(pending, false);
        }
        write_queue_.close();
    }

    void dispatch(Frame frame)
    {
        if (frame.header.type != FrameType::response) {
            return;
        }

        const auto found = pending_.find(frame.header.stream);
        if (found == pending_.end()) {
            return; // response for an unknown or already-completed stream is silently dropped
        }
        if (has_flag(frame.header.flags, FrameFlags::cancel)) {
            (void)cancel_pending(found->second, "operation cancelled by peer");
            return;
        }
        const auto pending = found->second;
        if (!pending->operation.complete("response received")) {
            return;
        }
        pending->timer.cancel();
        pending->channel.try_send(boost::system::error_code{}, std::move(frame));
        cleanup_pending(pending, true);
    }

    void timeout_pending(const std::shared_ptr<PendingRequest> &pending)
    {
        if (!pending->operation.timeout("operation deadline exceeded")) {
            return;
        }

        send_cancellation(pending->operation.stream());
        pending->channel.close();
        cleanup_pending(pending, false);
    }

    [[nodiscard]] bool cancel_pending(const std::shared_ptr<PendingRequest> &pending,
                                      std::string message)
    {
        if (!pending->operation.cancel(std::move(message))) {
            return false;
        }

        send_cancellation(pending->operation.stream());
        pending->channel.close();
        cleanup_pending(pending, false);
        return true;
    }

    void discard_pending(const std::shared_ptr<PendingRequest> &pending)
    {
        if (!pending->operation.is_terminal()) {
            (void)pending->operation.cancel("operation handle released");
            send_cancellation(pending->operation.stream());
            pending->channel.close();
        }
        cleanup_pending(pending, true);
    }

    void cleanup_pending(const std::shared_ptr<PendingRequest> &pending,
                         bool release_stream) noexcept
    {
        if (!pending->removed) {
            pending->removed = true;
            pending->timer.cancel();
            const auto stream = pending->operation.stream();
            const auto found = pending_.find(stream);
            if (found != pending_.end() && found->second == pending) {
                pending_.erase(found);
            }
        }
        if (release_stream && !pending->stream_released) {
            pending->stream_released = true;
            stream_ids_.release(pending->operation.stream());
        }
    }

    void send_cancellation(StreamId stream)
    {
        Frame frame;
        frame.header.type = FrameType::request;
        frame.header.flags = FrameFlags::cancel | FrameFlags::end_of_stream;
        frame.header.stream = stream;
        frame.header.payload_size = 0;

        (void)write_queue_.try_enqueue(std::move(frame));
    }

    boost::asio::any_io_executor executor_;
    std::unique_ptr<Transport> transport_;
    const MessageRegistry<CodecType> &registry_;
    WriteQueue write_queue_;
    StreamIdAllocator stream_ids_;
    FrameDecoder decoder_;
    std::unordered_map<StreamId, std::shared_ptr<PendingRequest>> pending_;
};

} // namespace rillnet
