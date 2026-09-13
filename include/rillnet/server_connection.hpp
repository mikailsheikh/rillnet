#pragma once

#include <rillnet/codec.hpp>
#include <rillnet/diagnostics.hpp>
#include <rillnet/frame.hpp>
#include <rillnet/frame_decoder.hpp>
#include <rillnet/identifiers.hpp>
#include <rillnet/lifecycle.hpp>
#include <rillnet/message_codec.hpp>
#include <rillnet/message_registry.hpp>
#include <rillnet/operation.hpp>
#include <rillnet/protocol_violation.hpp>
#include <rillnet/transport.hpp>
#include <rillnet/write_queue.hpp>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/awaitable_operators.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/system/system_error.hpp>

#include <array>
#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <span>
#include <unordered_map>
#include <unordered_set>
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
                     WriteQueueLimits write_queue_limits = {}, DiagnosticsHooks diagnostics = {})
        : executor_(std::move(executor)), transport_(std::move(transport)), registry_(registry),
          write_queue_(executor_, *transport_, write_queue_limits,
                       [diagnostics](const Frame &frame, std::size_t bytes) mutable {
                           notify_diagnostics(diagnostics, {DiagnosticsEventType::bytes_transferred,
                                                            {},
                                                            StatusCode::ok,
                                                            bytes,
                                                            false,
                                                            {}});
                           notify_diagnostics(diagnostics,
                                              {DiagnosticsEventType::message_transferred,
                                               frame.header.stream,
                                               StatusCode::ok,
                                               0,
                                               false,
                                               {}});
                       }),
          diagnostics_(std::move(diagnostics))
    {
    }

    ServerConnection(const ServerConnection &) = delete;
    ServerConnection &operator=(const ServerConnection &) = delete;

    [[nodiscard]] ConnectionState state() const noexcept { return lifecycle_.state(); }

    // Stops accepting new requests, allows active handlers to finish, and closes the transport
    // after the drain period expires.
    template <typename Rep, typename Period>
    boost::asio::awaitable<void> shutdown(std::chrono::duration<Rep, Period> drain_period)
    {
        begin_shutdown();
        const auto deadline = Operation::Clock::now() +
                              std::chrono::duration_cast<Operation::Clock::duration>(drain_period);
        boost::asio::steady_timer timer(executor_);
        while (active_handlers_ != 0 && Operation::Clock::now() < deadline) {
            timer.expires_after(std::chrono::milliseconds(1));
            boost::system::error_code error;
            co_await timer.async_wait(
                boost::asio::redirect_error(boost::asio::use_awaitable, error));
            if (error) {
                break;
            }
        }

        if (active_handlers_ != 0) {
            fail_operations_on_connection_close();
        }
        transport_->close();
        lifecycle_.transition(ConnectionState::closed);
        notify_closed();
        co_return;
    }

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
                    (void)operation->fail(request.status, request.message);
                    send_operation_error(frame.header.stream, request.status);
                    co_return;
                }

                SessionContext context(frame.header.stream, operation);
                const auto response = co_await handler(context, *request.value);
                auto encoded =
                    encode_message(registry_, response, frame.header.stream, FrameType::response);
                if (encoded.ok() && operation->complete("response ready")) {
                    encoded.frame->header.flags |= FrameFlags::end_of_stream;
                    co_await write_queue_.enqueue(std::move(*encoded.frame));
                }
            });
    }

    // Drives the connection's write queue and read loop until the transport is closed or fails.
    boost::asio::awaitable<void> run()
    {
        if (lifecycle_.state() == ConnectionState::created) {
            (void)lifecycle_.transition(ConnectionState::accepting);
            (void)lifecycle_.transition(ConnectionState::active);
            notify_diagnostics(diagnostics_, {DiagnosticsEventType::connection_opened});
        }
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
            notify_diagnostics(diagnostics_, {DiagnosticsEventType::bytes_transferred,
                                              {},
                                              StatusCode::ok,
                                              bytes_read,
                                              true,
                                              {}});

            for (auto &frame : decoder_.push(std::span(buffer).first(bytes_read))) {
                notify_diagnostics(diagnostics_, {DiagnosticsEventType::message_transferred,
                                                  frame.header.stream,
                                                  StatusCode::ok,
                                                  0,
                                                  true,
                                                  {}});
                dispatch(std::move(frame));
                if (!transport_->is_open()) {
                    break;
                }
            }
            if (decoder_.error().has_value()) {
                const auto error = *decoder_.error();
                const auto status = status_for_frame_validation(error);
                notify_diagnostics(diagnostics_, {DiagnosticsEventType::protocol_error,
                                                  {},
                                                  status,
                                                  0,
                                                  true,
                                                  std::string(frame_validation_message(error))});
                transport_->close();
                break;
            }
        }
        if (connection_lost) {
            fail_operations_on_connection_close();
        }
        reading_ = false;
        close_queue_when_idle();
        lifecycle_.transition(ConnectionState::closed);
        notify_closed();
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
        if (lifecycle_.state() == ConnectionState::draining ||
            lifecycle_.state() == ConnectionState::closing ||
            lifecycle_.state() == ConnectionState::closed) {
            send_operation_error(frame.header.stream, StatusCode::connection_closed);
            mark_terminal(frame);
            return;
        }
        if (frame.header.type != FrameType::request) {
            fail_connection(StatusCode::malformed_frame, "server received a non-request frame");
            return;
        }

        if (terminal_streams_.contains(frame.header.stream)) {
            fail_connection(StatusCode::malformed_frame, "duplicate terminal frame");
            return;
        }

        if (has_flag(frame.header.flags, FrameFlags::cancel)) {
            const auto found = operations_.find(frame.header.stream);
            if (found != operations_.end()) {
                (void)found->second->cancel("operation cancelled by peer");
                mark_terminal(frame);
            } else {
                fail_connection(StatusCode::unknown_stream, "cancellation for an unknown stream");
            }
            return;
        }

        const auto message_type = peek_message_type(frame.payload);
        if (!message_type.has_value()) {
            send_operation_error(frame.header.stream, StatusCode::decode_error);
            mark_terminal(frame);
            return;
        }
        const auto found = handlers_.find(*message_type);
        if (found == handlers_.end()) {
            send_operation_error(frame.header.stream, StatusCode::unknown_message_type);
            mark_terminal(frame);
            return;
        }

        auto operation = std::make_shared<Operation>(frame.header.stream);
        operation->set_completion_handler(
            [diagnostics = diagnostics_,
             stream = frame.header.stream](const OperationResult &result) {
                const auto type = result.status() == StatusCode::cancelled
                                      ? DiagnosticsEventType::cancellation
                                  : result.status() == StatusCode::timeout_error
                                      ? DiagnosticsEventType::timeout
                                      : DiagnosticsEventType::request_completed;
                notify_diagnostics(diagnostics,
                                   {type, stream, result.status(), 0, false, result.message()});
            });
        (void)operation->activate();
        const auto [inserted, did_insert] = operations_.emplace(frame.header.stream, operation);
        if (!did_insert) {
            transport_->close();
            return;
        }
        notify_diagnostics(diagnostics_,
                           {DiagnosticsEventType::request_started, frame.header.stream});

        ++active_handlers_;
        boost::asio::co_spawn(executor_,
                              run_handler(found->second, std::move(frame), inserted->second),
                              boost::asio::detached);
    }

    void send_operation_error(StreamId stream, StatusCode status)
    {
        (void)write_queue_.try_enqueue(make_error_frame(stream, status));
    }

    void mark_terminal(const Frame &frame)
    {
        if (has_flag(frame.header.flags, FrameFlags::end_of_stream)) {
            terminal_streams_.insert(frame.header.stream);
        }
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

    void fail_connection(StatusCode status, std::string message)
    {
        if (status_category(status) == StatusCategory::protocol) {
            notify_diagnostics(
                diagnostics_, {DiagnosticsEventType::protocol_error, {}, status, 0, true, message});
        }
        transport_->close();
    }

    void notify_closed()
    {
        if (closed_notified_) {
            return;
        }
        closed_notified_ = true;
        notify_diagnostics(diagnostics_, {DiagnosticsEventType::connection_closed,
                                          {},
                                          StatusCode::connection_closed,
                                          0,
                                          false,
                                          "connection closed"});
    }

    void begin_shutdown() noexcept
    {
        if (lifecycle_.state() == ConnectionState::created) {
            (void)lifecycle_.transition(ConnectionState::accepting);
            (void)lifecycle_.transition(ConnectionState::active);
        }
        if (lifecycle_.state() == ConnectionState::active) {
            (void)lifecycle_.transition(ConnectionState::draining);
        }
    }

    boost::asio::any_io_executor executor_;
    std::unique_ptr<Transport> transport_;
    const MessageRegistry<CodecType> &registry_;
    WriteQueue write_queue_;
    FrameDecoder decoder_;
    std::unordered_map<MessageType, RequestHandler> handlers_;
    std::unordered_map<StreamId, std::shared_ptr<Operation>> operations_;
    std::unordered_set<StreamId> terminal_streams_;
    std::size_t active_handlers_ = 0;
    bool reading_ = true;
    ConnectionLifecycle lifecycle_;
    DiagnosticsHooks diagnostics_;
    bool closed_notified_ = false;
};

} // namespace rillnet