#pragma once

#include <rillnet/frame.hpp>
#include <rillnet/frame_codec.hpp>
#include <rillnet/frame_decoder.hpp>
#include <rillnet/transport.hpp>

#include <boost/asio/experimental/channel.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/error_code.hpp>
#include <boost/system/system_error.hpp>

#include <algorithm>
#include <array>
#include <cstddef>
#include <deque>
#include <memory>
#include <optional>
#include <span>
#include <utility>
#include <vector>

namespace rillnet::testing {

// A Transport backed by an in-memory byte buffer rather than a socket, so protocol logic can be
// exercised deterministically without real networking (see docs/implementation-plan.md's
// "In-Memory Protocol Tests" strategy). Bytes handed to the constructor are delivered to read()
// in order; once they are exhausted, read() reports end-of-stream by returning 0, mirroring a
// closed socket.
class InMemoryTransport final : public rillnet::Transport {
  public:
    explicit InMemoryTransport(std::vector<std::byte> incoming) : incoming_(std::move(incoming)) {}

    boost::asio::awaitable<std::size_t> read(std::span<std::byte> buffer) override
    {
        if (!is_open()) {
            co_return 0;
        }

        const auto bytes_to_read = std::min(buffer.size(), incoming_.size() - read_offset_);
        std::copy_n(incoming_.begin() + static_cast<std::ptrdiff_t>(read_offset_), bytes_to_read,
                    buffer.begin());
        read_offset_ += bytes_to_read;
        co_return bytes_to_read;
    }

    boost::asio::awaitable<void> write(std::span<const std::byte> buffer) override
    {
        if (is_open()) {
            outgoing_.insert(outgoing_.end(), buffer.begin(), buffer.end());
        }
        co_return;
    }

    void close() noexcept override { open_ = false; }

    [[nodiscard]] bool is_open() const noexcept override { return open_; }

    [[nodiscard]] const std::vector<std::byte> &outgoing() const noexcept { return outgoing_; }

  private:
    std::vector<std::byte> incoming_;
    std::vector<std::byte> outgoing_;
    std::size_t read_offset_ = 0;
    bool open_ = true;
};

class DuplexTransport final : public rillnet::Transport {
  private:
    using Channel =
        boost::asio::experimental::channel<void(boost::system::error_code, std::vector<std::byte>)>;

    struct Endpoint {
        explicit Endpoint(boost::asio::any_io_executor executor) : channel(std::move(executor), 64)
        {
        }

        Channel channel;
    };

  public:
    static std::pair<std::unique_ptr<DuplexTransport>, std::unique_ptr<DuplexTransport>>
    make_pair(boost::asio::any_io_executor executor)
    {
        auto first_endpoint = std::make_shared<Endpoint>(executor);
        auto second_endpoint = std::make_shared<Endpoint>(std::move(executor));
        return {
            std::unique_ptr<DuplexTransport>(new DuplexTransport(first_endpoint, second_endpoint)),
            std::unique_ptr<DuplexTransport>(new DuplexTransport(second_endpoint, first_endpoint))};
    }

    boost::asio::awaitable<std::size_t> read(std::span<std::byte> buffer) override
    {
        if (!is_open()) {
            co_return 0;
        }

        if (pending_offset_ == pending_.size()) {
            boost::system::error_code error;
            pending_ = co_await incoming_->channel.async_receive(
                boost::asio::redirect_error(boost::asio::use_awaitable, error));
            pending_offset_ = 0;
            if (error) {
                co_return 0;
            }
        }

        const auto bytes_to_read = std::min(buffer.size(), pending_.size() - pending_offset_);
        std::copy_n(pending_.begin() + static_cast<std::ptrdiff_t>(pending_offset_), bytes_to_read,
                    buffer.begin());
        pending_offset_ += bytes_to_read;
        co_return bytes_to_read;
    }

    boost::asio::awaitable<void> write(std::span<const std::byte> buffer) override
    {
        std::vector<std::byte> bytes(buffer.begin(), buffer.end());
        boost::system::error_code error;
        co_await outgoing_->channel.async_send(
            boost::system::error_code{}, std::move(bytes),
            boost::asio::redirect_error(boost::asio::use_awaitable, error));
        if (error) {
            throw boost::system::system_error(error);
        }
    }

    void close() noexcept override
    {
        open_ = false;
        incoming_->channel.close();
        outgoing_->channel.close();
    }

    [[nodiscard]] bool is_open() const noexcept override { return open_; }

  private:
    DuplexTransport(std::shared_ptr<Endpoint> incoming, std::shared_ptr<Endpoint> outgoing)
        : incoming_(std::move(incoming)), outgoing_(std::move(outgoing))
    {
    }

    std::shared_ptr<Endpoint> incoming_;
    std::shared_ptr<Endpoint> outgoing_;
    std::vector<std::byte> pending_;
    std::size_t pending_offset_ = 0;
    bool open_ = true;
};

// Drives the far end of a DuplexTransport pair at frame granularity, so a test can script the
// exact order in which frames cross a connection: every send() is observable by the peer before
// the test decides what to send next. receive() reports end-of-stream as std::nullopt.
class FramePeer {
  public:
    explicit FramePeer(std::unique_ptr<rillnet::Transport> transport)
        : transport_(std::move(transport))
    {
    }

    boost::asio::awaitable<std::optional<rillnet::Frame>> receive()
    {
        while (frames_.empty()) {
            std::array<std::byte, 4096> buffer{};
            std::size_t bytes_read = 0;
            try {
                bytes_read = co_await transport_->read(buffer);
            } catch (const boost::system::system_error &) {
                co_return std::nullopt;
            }
            if (bytes_read == 0) {
                co_return std::nullopt;
            }
            for (auto &frame : decoder_.push(std::span(buffer).first(bytes_read))) {
                frames_.push_back(std::move(frame));
            }
        }

        auto frame = std::move(frames_.front());
        frames_.pop_front();
        co_return frame;
    }

    // Returns false once the far end has gone away, so a scripted peer can stop rather than
    // propagate a transport exception out of the test.
    boost::asio::awaitable<bool> send(const rillnet::Frame &frame)
    {
        try {
            co_await transport_->write(rillnet::encode_frame(frame));
        } catch (const boost::system::system_error &) {
            co_return false;
        }
        co_return true;
    }

    void close() noexcept { transport_->close(); }

  private:
    std::unique_ptr<rillnet::Transport> transport_;
    rillnet::FrameDecoder decoder_;
    std::deque<rillnet::Frame> frames_;
};

} // namespace rillnet::testing
