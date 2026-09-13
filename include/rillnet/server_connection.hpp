#pragma once

#include <rillnet/codec.hpp>
#include <rillnet/frame.hpp>
#include <rillnet/frame_decoder.hpp>
#include <rillnet/identifiers.hpp>
#include <rillnet/message_codec.hpp>
#include <rillnet/message_registry.hpp>
#include <rillnet/operation.hpp>
#include <rillnet/transport.hpp>
#include <rillnet/write_queue.hpp>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/system/system_error.hpp>

#include <array>
#include <cstddef>
#include <functional>
#include <memory>
#include <span>
#include <unordered_map>
#include <utility>

namespace rillnet {

class SessionContext {
  public:
    [[nodiscard]] StreamId stream_id() const noexcept { return stream_id_; }
    [[nodiscard]] bool is_cancelled() const noexcept
    {
        return operation_ != nullptr && operation_->cancellation_requested();
    }

    void throw_if_cancelled() const
    {
        if (is_cancelled()) {
            throw CancellationError();
        }
    }

  private:
    template <typename> friend class ServerConnection;

    SessionContext(StreamId stream_id, std::shared_ptr<Operation> operation) noexcept
        : stream_id_(stream_id), operation_(std::move(operation))
    {
    }

    StreamId stream_id_;
    std::shared_ptr<Operation> operation_;
};

// Owns one server-side connection and dispatches request frames to registered typed handlers.
// Each handler is spawned independently, so a suspended handler never stalls the read loop or
// other requests. Responses are serialized through the connection's single WriteQueue.
template <typename CodecType = PodCodec> class ServerConnection {
  public:
    ServerConnection(boost::asio::any_io_executor executor, std::unique_ptr<Transport> transport,
                     const MessageRegistry<CodecType> &registry,
                     WriteQueueLimits write_queue_limits = {})
        : executor_(std::move(executor)), transport_(std::move(transport)), registry_(registry),
          write_queue_(executor_, *transport_, write_queue_limits)
    {
    }

    ServerConnection(const ServerConnection &) = delete;
    ServerConnection &operator=(const ServerConnection &) = delete;

    // Registers a handler for Request. Handler must be invocable as
    // `awaitable<Response>(SessionContext&, Request)` and replaces any handler already registered
    // for Request's wire message type.
    template <typename Request, typename Handler> void handle(Handler handler)
    {
        const auto message_type = registry_.template message_type<Request>();
        if (!message_type.has_value()) {
            return;
        }

        handlers_.insert_or_assign(
            *message_type,
            [this, handler = std::move(handler)](
                Frame frame, std::shared_ptr<Operation> operation) -> boost::asio::awaitable<void> {
                const auto request = decode_message<Request>(registry_, frame);
                if (!request.ok()) {
                    co_return;
                }

                SessionContext context(frame.header.stream, operation);
                const auto response = co_await handler(context, *request.value);
                const auto encoded =
                    encode_message(registry_, response, frame.header.stream, FrameType::response);
                if (encoded.ok() && operation->complete("response ready")) {
                    co_await write_queue_.enqueue(std::move(*encoded.frame));
                }
            });
    }

    // Drives the connection's write queue and read loop until the transport is closed or fails.
    boost::asio::awaitable<void> run()
    {
        using namespace boost::asio::experimental::awaitable_operators;
        co_await (write_queue_.run() && read_loop());
    }

  private:
    using RequestHandler =
        std::function<boost::asio::awaitable<void>(Frame, std::shared_ptr<Operation>)>;

    boost::asio::awaitable<void> read_loop()
    {
        std::array<std::byte, 4096> buffer{};
        bool connection_lost = false;
        while (transport_->is_open()) {
            std::size_t bytes_read = 0;
            try {
                bytes_read = co_await transport_->read(buffer);
            } catch (const boost::system::system_error &) {
                connection_lost = true;
                break;
            }
            if (bytes_read == 0) {
                connection_lost = !transport_->is_open();
                break;
            }

            for (auto &frame : decoder_.push(std::span(buffer).first(bytes_read))) {
                dispatch(std::move(frame));
            }
        }
        if (connection_lost) {
            fail_operations_on_connection_close();
        }
        reading_ = false;
        close_queue_when_idle();
    }

    void fail_operations_on_connection_close()
    {
        for (const auto &[stream, operation] : operations_) {
            (void)stream;
            (void)operation->request_cancellation();
            (void)operation->fail(StatusCode::connection_closed,
                                  "connection closed while processing a request");
        }
    }

    void dispatch(Frame frame)
    {
        if (frame.header.type != FrameType::request) {
            return;
        }

        if (has_flag(frame.header.flags, FrameFlags::cancel)) {
            const auto found = operations_.find(frame.header.stream);
            if (found != operations_.end()) {
                (void)found->second->cancel("operation cancelled by peer");
            }
            return;
        }

        const auto message_type = peek_message_type(frame.payload);
        if (!message_type.has_value()) {
            return;
        }
        const auto found = handlers_.find(*message_type);
        if (found == handlers_.end()) {
            return;
        }

        auto operation = std::make_shared<Operation>(frame.header.stream);
        (void)operation->activate();
        const auto [inserted, did_insert] = operations_.emplace(frame.header.stream, operation);
        if (!did_insert) {
            return;
        }

        ++active_handlers_;
        boost::asio::co_spawn(executor_,
                              run_handler(found->second, std::move(frame), inserted->second),
                              boost::asio::detached);
    }

    boost::asio::awaitable<void> run_handler(RequestHandler handler, Frame frame,
                                             std::shared_ptr<Operation> operation)
    {
        try {
            co_await handler(std::move(frame), std::move(operation));
        } catch (...) {
            // Handler exceptions are isolated to their operation; error frames follow in Epic 4.6.
        }
        operations_.erase(frame.header.stream);
        --active_handlers_;
        close_queue_when_idle();
    }

    void close_queue_when_idle()
    {
        if (!reading_ && active_handlers_ == 0) {
            write_queue_.close();
        }
    }

    boost::asio::any_io_executor executor_;
    std::unique_ptr<Transport> transport_;
    const MessageRegistry<CodecType> &registry_;
    WriteQueue write_queue_;
    FrameDecoder decoder_;
    std::unordered_map<MessageType, RequestHandler> handlers_;
    std::unordered_map<StreamId, std::shared_ptr<Operation>> operations_;
    std::size_t active_handlers_ = 0;
    bool reading_ = true;
};

} // namespace rillnet