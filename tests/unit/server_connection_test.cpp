#include <rillnet/frame_codec.hpp>
#include <rillnet/message_codec.hpp>
#include <rillnet/message_registry.hpp>
#include <rillnet/server_connection.hpp>

#include "in_memory_transport.hpp"

#include <rillnet/diagnostics.hpp>
#include <rillnet/protocol_violation.hpp>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/experimental/channel.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <future>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

namespace {

using rillnet::decode_message;
using rillnet::DiagnosticsEvent;
using rillnet::DiagnosticsEventType;
using rillnet::DiagnosticsHooks;
using rillnet::encode_frame;
using rillnet::encode_message;
using rillnet::Frame;
using rillnet::FrameDecoder;
using rillnet::FrameFlags;
using rillnet::FrameType;
using rillnet::has_flag;
using rillnet::MessageRegistry;
using rillnet::ServerConnection;
using rillnet::SessionContext;
using rillnet::StatusCode;
using rillnet::StreamId;
using rillnet::testing::DuplexTransport;
using rillnet::testing::FramePeer;
using rillnet::testing::InMemoryTransport;

struct StartSimulation {
    std::uint32_t id = 0;
};

struct SimulationStarted {
    std::uint32_t id = 0;
};

MessageRegistry<> make_registry()
{
    MessageRegistry<> registry;
    EXPECT_TRUE(registry.register_message<StartSimulation>(100).ok());
    EXPECT_TRUE(registry.register_message<SimulationStarted>(101).ok());
    return registry;
}

std::vector<std::byte> encode_request_bytes(const MessageRegistry<> &registry)
{
    std::vector<std::byte> bytes;
    for (const auto &[stream, id] : {std::pair{StreamId{1}, 7U}, std::pair{StreamId{3}, 9U}}) {
        const auto encoded = encode_message(registry, StartSimulation{id}, stream);
        EXPECT_TRUE(encoded.ok());
        const auto frame_bytes =
            encode_frame(*encoded.frame); // NOLINT(bugprone-unchecked-optional-access)
        bytes.insert(bytes.end(), frame_bytes.begin(), frame_bytes.end());
    }
    return bytes;
}

std::vector<std::byte> encode_cancel_bytes(StreamId stream)
{
    Frame cancel;
    cancel.header.type = FrameType::request;
    cancel.header.flags = FrameFlags::cancel | FrameFlags::end_of_stream;
    cancel.header.stream = stream;
    return encode_frame(cancel);
}

Frame make_request_frame(const MessageRegistry<> &registry, StreamId stream, std::uint32_t id)
{
    auto encoded = encode_message(registry, StartSimulation{id}, stream);
    EXPECT_TRUE(encoded.ok());
    return std::move(*encoded.frame); // NOLINT(bugprone-unchecked-optional-access)
}

Frame make_cancel_frame(StreamId stream)
{
    Frame cancel;
    cancel.header.type = FrameType::request;
    cancel.header.flags = FrameFlags::cancel | FrameFlags::end_of_stream;
    cancel.header.stream = stream;
    return cancel;
}

void append_frame(std::vector<std::byte> &bytes, const Frame &frame)
{
    const auto encoded = encode_frame(frame);
    bytes.insert(bytes.end(), encoded.begin(), encoded.end());
}

void join(std::future<void> &future)
{
    ASSERT_EQ(future.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    future.get();
}

// Closes both ends and drains once more after the scenario should already be finished, so a
// regression fails an assertion instead of hanging the suite on a future that never completes.
void run_until_idle(boost::asio::io_context &context, rillnet::Transport &server_transport,
                    FramePeer &peer)
{
    context.run_for(std::chrono::seconds(5));
    server_transport.close();
    peer.close();
    context.restart();
    context.run_for(std::chrono::milliseconds(100));
}

class DisconnectingTransport final : public rillnet::Transport {
  public:
    explicit DisconnectingTransport(std::vector<std::byte> incoming)
        : incoming_(std::move(incoming))
    {
    }

    boost::asio::awaitable<std::size_t> read(std::span<std::byte> buffer) override
    {
        if (delivered_) {
            open_ = false;
            co_return 0;
        }

        std::copy(incoming_.begin(), incoming_.end(), buffer.begin());
        delivered_ = true;
        co_return incoming_.size();
    }

    boost::asio::awaitable<void> write(std::span<const std::byte> buffer) override
    {
        outgoing_.insert(outgoing_.end(), buffer.begin(), buffer.end());
        co_return;
    }

    void close() noexcept override { open_ = false; }

    [[nodiscard]] bool is_open() const noexcept override { return open_; }
    [[nodiscard]] const std::vector<std::byte> &outgoing() const noexcept { return outgoing_; }

  private:
    std::vector<std::byte> incoming_;
    std::vector<std::byte> outgoing_;
    bool delivered_ = false;
    bool open_ = true;
};

TEST(ServerConnectionTest, DispatchesHandlersWithoutBlockingTheReadLoop)
{
    boost::asio::io_context context;
    const auto registry = make_registry();
    auto transport = std::make_unique<InMemoryTransport>(encode_request_bytes(registry));
    auto *transport_ptr = transport.get();
    ServerConnection connection(context.get_executor(), std::move(transport), registry);

    connection.handle<StartSimulation>(
        [&context](SessionContext &session,
                   StartSimulation request) -> boost::asio::awaitable<SimulationStarted> {
            if (request.id == 7) {
                boost::asio::steady_timer timer(context);
                timer.expires_after(std::chrono::milliseconds(1));
                co_await timer.async_wait(boost::asio::use_awaitable);
            }
            co_return SimulationStarted{request.id +
                                        static_cast<std::uint32_t>(session.stream_id().value())};
        });

    auto future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);
    context.run();
    future.get();

    FrameDecoder decoder;
    const auto responses = decoder.push(transport_ptr->outgoing());
    ASSERT_EQ(responses.size(), 2U);
    EXPECT_EQ(responses[0].header.type, FrameType::response);
    EXPECT_EQ(responses[0].header.stream, StreamId{3});
    EXPECT_EQ(responses[1].header.type, FrameType::response);
    EXPECT_EQ(responses[1].header.stream, StreamId{1});

    const auto first_response = decode_message<SimulationStarted>(registry, responses[0]);
    const auto second_response = decode_message<SimulationStarted>(registry, responses[1]);
    ASSERT_TRUE(first_response.ok());
    ASSERT_TRUE(second_response.ok());
    EXPECT_EQ(first_response.value->id, 12U); // NOLINT(bugprone-unchecked-optional-access)
    EXPECT_EQ(second_response.value->id, 8U); // NOLINT(bugprone-unchecked-optional-access)
}

TEST(ServerConnectionTest, IsolatesHandlerExceptionsFromOtherOperations)
{
    boost::asio::io_context context;
    const auto registry = make_registry();
    auto transport = std::make_unique<InMemoryTransport>(encode_request_bytes(registry));
    auto *transport_ptr = transport.get();
    ServerConnection connection(context.get_executor(), std::move(transport), registry);

    connection.handle<StartSimulation>(
        [](SessionContext &, StartSimulation request) -> boost::asio::awaitable<SimulationStarted> {
            if (request.id == 7) {
                throw std::runtime_error("handler failure");
            }
            co_return SimulationStarted{request.id};
        });

    auto future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);
    context.run();
    future.get();

    FrameDecoder decoder;
    const auto responses = decoder.push(transport_ptr->outgoing());
    ASSERT_EQ(responses.size(), 1U);
    EXPECT_EQ(responses[0].header.type, FrameType::response);
    EXPECT_EQ(responses[0].header.stream, StreamId{3});
    const auto response = decode_message<SimulationStarted>(registry, responses[0]);
    ASSERT_TRUE(response.ok());
    EXPECT_EQ(response.value->id, 9U); // NOLINT(bugprone-unchecked-optional-access)
}

TEST(ServerConnectionTest, SupportsMultiThreadedIoWithSerializedConnectionState)
{
    boost::asio::io_context context;
    auto connection_executor = boost::asio::make_strand(context);
    const auto registry = make_registry();
    auto transport = std::make_unique<InMemoryTransport>(encode_request_bytes(registry));
    auto *transport_ptr = transport.get();
    ServerConnection connection(connection_executor, std::move(transport), registry);

    connection.handle<StartSimulation>(
        [](SessionContext &, StartSimulation request) -> boost::asio::awaitable<SimulationStarted> {
            co_return SimulationStarted{request.id};
        });

    auto future = boost::asio::co_spawn(
        connection_executor, [&]() { return connection.run(); }, boost::asio::use_future);
    std::vector<std::thread> workers;
    for (int index = 0; index < 4; ++index) {
        workers.emplace_back([&context]() { context.run(); });
    }
    for (auto &worker : workers) {
        worker.join();
    }
    future.get();

    FrameDecoder decoder;
    const auto responses = decoder.push(transport_ptr->outgoing());
    ASSERT_EQ(responses.size(), 2U);
    for (const auto &response : responses) {
        EXPECT_EQ(response.header.type, FrameType::response);
    }
}

TEST(ServerConnectionTest, ExposesRemoteCancellationToOnlyTheMatchingHandler)
{
    boost::asio::io_context context;
    const auto registry = make_registry();
    auto incoming = encode_request_bytes(registry);
    const auto cancellation = encode_cancel_bytes(StreamId{1});
    incoming.insert(incoming.end(), cancellation.begin(), cancellation.end());
    auto transport = std::make_unique<InMemoryTransport>(std::move(incoming));
    auto *transport_ptr = transport.get();
    ServerConnection connection(context.get_executor(), std::move(transport), registry);
    std::size_t cancelled_handlers = 0;

    connection.handle<StartSimulation>(
        [&cancelled_handlers](SessionContext &session, StartSimulation request)
            -> boost::asio::awaitable<SimulationStarted> {
            co_await boost::asio::post(boost::asio::use_awaitable);
            if (session.is_cancelled()) {
                ++cancelled_handlers;
                session.throw_if_cancelled();
            }
            co_return SimulationStarted{request.id};
        });

    auto future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);
    context.run();
    future.get();

    EXPECT_EQ(cancelled_handlers, 1U);
    FrameDecoder decoder;
    const auto responses = decoder.push(transport_ptr->outgoing());
    ASSERT_EQ(responses.size(), 1U);
    EXPECT_EQ(responses[0].header.stream, StreamId{3});
    EXPECT_EQ(responses[0].header.type, FrameType::response);
    EXPECT_FALSE(has_flag(responses[0].header.flags, FrameFlags::cancel));
}

TEST(ServerConnectionTest, CancelsActiveHandlerWhenTheTransportDisconnects)
{
    boost::asio::io_context context;
    const auto registry = make_registry();
    auto transport = std::make_unique<DisconnectingTransport>(encode_request_bytes(registry));
    auto *transport_ptr = transport.get();
    ServerConnection connection(context.get_executor(), std::move(transport), registry);
    bool handler_saw_disconnect = false;

    connection.handle<StartSimulation>(
        [&handler_saw_disconnect](SessionContext &session,
                                  StartSimulation) -> boost::asio::awaitable<SimulationStarted> {
            co_await boost::asio::post(boost::asio::use_awaitable);
            handler_saw_disconnect = session.is_cancelled();
            co_return SimulationStarted{};
        });

    auto future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);
    context.run();

    future.get();
    EXPECT_TRUE(handler_saw_disconnect);
    EXPECT_TRUE(transport_ptr->outgoing().empty());
}

TEST(ServerConnectionTest, SendsStreamErrorForMalformedRequestPayload)
{
    boost::asio::io_context context;
    const auto registry = make_registry();
    Frame malformed;
    malformed.header.type = FrameType::request;
    malformed.header.stream = StreamId{1};
    malformed.payload = {std::byte{0x01}};
    malformed.header.payload_size = static_cast<std::uint32_t>(malformed.payload.size());
    auto transport = std::make_unique<InMemoryTransport>(encode_frame(malformed));
    auto *transport_ptr = transport.get();
    ServerConnection connection(context.get_executor(), std::move(transport), registry);

    auto future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);
    context.run();
    future.get();

    FrameDecoder decoder;
    const auto responses = decoder.push(transport_ptr->outgoing());
    ASSERT_EQ(responses.size(), 1U);
    EXPECT_EQ(responses[0].header.stream, StreamId{1});
    EXPECT_TRUE(has_flag(responses[0].header.flags, FrameFlags::error));
    EXPECT_TRUE(has_flag(responses[0].header.flags, FrameFlags::end_of_stream));
    const auto status = rillnet::decode_error_status(responses[0]);
    ASSERT_TRUE(status.has_value());
    EXPECT_EQ(*status, StatusCode::decode_error);
}

TEST(ServerConnectionTest, GracefulShutdownClosesAnIdleConnection)
{
    boost::asio::io_context context;
    const auto registry = make_registry();
    auto transports = rillnet::testing::DuplexTransport::make_pair(context.get_executor());
    ServerConnection connection(context.get_executor(), std::move(transports.first), registry);

    auto shutdown_future = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<void> {
            co_await connection.shutdown(std::chrono::milliseconds{1});
            EXPECT_EQ(connection.state(), rillnet::ConnectionState::closed);
        },
        boost::asio::use_future);
    auto run_future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);

    context.run();

    shutdown_future.get();
    run_future.get();
}

struct HandlerOutcome {
    bool finished = false;
    bool saw_cancellation = false;
    bool transport_open = true;
    std::size_t written_frames = 0;
};

// Delivers one request followed by `trailing_bytes`, and suspends the handler long enough for the
// read loop to finish before the handler inspects its cancellation state.
HandlerOutcome handler_outcome_for(std::vector<std::byte> trailing_bytes)
{
    boost::asio::io_context context;
    const auto registry = make_registry();

    std::vector<std::byte> incoming;
    append_frame(incoming, make_request_frame(registry, StreamId{1}, 7));
    incoming.insert(incoming.end(), trailing_bytes.begin(), trailing_bytes.end());

    auto transport = std::make_unique<InMemoryTransport>(std::move(incoming));
    auto *transport_observer = transport.get();
    ServerConnection connection(context.get_executor(), std::move(transport), registry);
    HandlerOutcome outcome;

    connection.handle<StartSimulation>(
        [&outcome](SessionContext &session,
                   StartSimulation request) -> boost::asio::awaitable<SimulationStarted> {
            co_await boost::asio::post(boost::asio::use_awaitable);
            outcome.saw_cancellation = session.is_cancelled();
            outcome.finished = true;
            co_return SimulationStarted{request.id};
        });

    auto run_future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);

    context.run_for(std::chrono::seconds(5));
    join(run_future);

    outcome.transport_open = transport_observer->is_open();
    FrameDecoder decoder;
    outcome.written_frames = decoder.push(transport_observer->outgoing()).size();
    return outcome;
}

TEST(ServerConnectionTest, LeavesAnActiveHandlerUnawareOfAProtocolViolationThatClosedTheConnection)
{
    Frame response_frame; // a server may only ever receive requests
    response_frame.header.type = FrameType::response;
    response_frame.header.stream = StreamId{3};
    std::vector<std::byte> violation;
    append_frame(violation, response_frame);

    const auto outcome = handler_outcome_for(std::move(violation));

    EXPECT_FALSE(outcome.transport_open);
    EXPECT_TRUE(outcome.finished);
    EXPECT_FALSE(outcome.saw_cancellation);
    EXPECT_EQ(outcome.written_frames, 0U);
}

TEST(ServerConnectionTest, CancelsActiveHandlersWhenAProtocolViolationClosesTheConnection)
{
    Frame response_frame;
    response_frame.header.type = FrameType::response;
    response_frame.header.stream = StreamId{3};
    std::vector<std::byte> violation;
    append_frame(violation, response_frame);

    const auto outcome = handler_outcome_for(std::move(violation));

    EXPECT_TRUE(outcome.saw_cancellation);
}

TEST(ServerConnectionTest, LeavesAnActiveHandlerUnawareOfAMalformedFrameThatClosedTheConnection)
{
    Frame malformed;
    malformed.header.stream = StreamId{3};
    malformed.header.flags = static_cast<FrameFlags>(1U << 7U); // no such flag exists
    std::vector<std::byte> violation;
    append_frame(violation, malformed);

    const auto outcome = handler_outcome_for(std::move(violation));

    EXPECT_FALSE(outcome.transport_open);
    EXPECT_TRUE(outcome.finished);
    EXPECT_FALSE(outcome.saw_cancellation);
}

// A peer that stops sending without the transport closing itself leaves every active operation
// alone, so the handler finishes and its response is still written. Whether an active handler is
// cancelled at end-of-stream therefore depends on the Transport implementation: TcpTransport
// closes itself on EOF (see CancelsActiveHandlerWhenTheTransportDisconnects), an in-memory
// transport does not.
TEST(ServerConnectionTest, FinishesAnActiveHandlerWhenThePeerStopsSendingWithoutClosing)
{
    const auto outcome = handler_outcome_for({});

    EXPECT_TRUE(outcome.transport_open);
    EXPECT_TRUE(outcome.finished);
    EXPECT_FALSE(outcome.saw_cancellation);
    EXPECT_EQ(outcome.written_frames, 1U);
}

struct LateCancellationOutcome {
    bool first_response = false;
    bool second_response = false;
    std::size_t handled_requests = 0;
};

// Answers a request, then cancels it: the cancellation and the response crossed on the wire, so
// the server sees a cancellation for an operation it has already completed and forgotten.
LateCancellationOutcome outcome_when_a_cancellation_follows_its_response()
{
    boost::asio::io_context context;
    const auto registry = make_registry();
    auto transports = DuplexTransport::make_pair(context.get_executor());
    auto *server_transport = transports.first.get();
    ServerConnection connection(context.get_executor(), std::move(transports.first), registry);
    FramePeer peer(std::move(transports.second));
    LateCancellationOutcome outcome;

    connection.handle<StartSimulation>(
        [&outcome](SessionContext &,
                   StartSimulation request) -> boost::asio::awaitable<SimulationStarted> {
            ++outcome.handled_requests;
            co_return SimulationStarted{request.id};
        });

    auto peer_future = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<void> {
            EXPECT_TRUE(co_await peer.send(make_request_frame(registry, StreamId{1}, 7)));
            const auto first = co_await peer.receive();
            outcome.first_response = first.has_value();

            (void)co_await peer.send(make_cancel_frame(StreamId{1}));
            (void)co_await peer.send(make_request_frame(registry, StreamId{3}, 9));
            const auto second = co_await peer.receive();
            outcome.second_response = second.has_value();
            peer.close();
        },
        boost::asio::use_future);
    auto run_future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);

    run_until_idle(context, *server_transport, peer);
    join(peer_future);
    join(run_future);
    return outcome;
}

TEST(ServerConnectionTest, TearsDownTheConnectionForACancellationThatCrossesItsResponse)
{
    const auto outcome = outcome_when_a_cancellation_follows_its_response();

    EXPECT_TRUE(outcome.first_response);
    EXPECT_FALSE(outcome.second_response);
    EXPECT_EQ(outcome.handled_requests, 1U);
}

TEST(ServerConnectionTest, KeepsServingAfterACancellationThatCrossesItsResponse)
{
    const auto outcome = outcome_when_a_cancellation_follows_its_response();

    EXPECT_TRUE(outcome.second_response);
    EXPECT_EQ(outcome.handled_requests, 2U);
}

struct DrainCancellationOutcome {
    std::optional<Frame> reply;
    bool handler_saw_cancellation = false;
    bool reached_closed_state = false;
};

// Cancels an operation whose handler is still running while the connection is draining.
DrainCancellationOutcome outcome_when_a_cancellation_arrives_while_draining()
{
    using Signal = boost::asio::experimental::channel<void(boost::system::error_code)>;

    boost::asio::io_context context;
    const auto registry = make_registry();
    auto transports = DuplexTransport::make_pair(context.get_executor());
    auto *server_transport = transports.first.get();
    ServerConnection connection(context.get_executor(), std::move(transports.first), registry);
    FramePeer peer(std::move(transports.second));
    Signal started(context.get_executor(), 1);
    Signal release(context.get_executor(), 1);
    DrainCancellationOutcome outcome;

    connection.handle<StartSimulation>(
        [&](SessionContext &session,
            StartSimulation request) -> boost::asio::awaitable<SimulationStarted> {
            EXPECT_TRUE(started.try_send(boost::system::error_code{}));
            boost::system::error_code error;
            co_await release.async_receive(
                boost::asio::redirect_error(boost::asio::use_awaitable, error));
            outcome.handler_saw_cancellation = session.is_cancelled();
            co_return SimulationStarted{request.id};
        });

    auto shutdown_future = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<void> {
            boost::system::error_code error;
            co_await started.async_receive(
                boost::asio::redirect_error(boost::asio::use_awaitable, error));
            co_await connection.shutdown(std::chrono::seconds{30});
            outcome.reached_closed_state = connection.state() == rillnet::ConnectionState::closed;
        },
        boost::asio::use_future);
    auto peer_future = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<void> {
            EXPECT_TRUE(co_await peer.send(make_request_frame(registry, StreamId{1}, 7)));
            while (connection.state() != rillnet::ConnectionState::draining) {
                co_await boost::asio::post(boost::asio::use_awaitable);
            }

            (void)co_await peer.send(make_cancel_frame(StreamId{1}));
            outcome.reply = co_await peer.receive();
            EXPECT_TRUE(release.try_send(boost::system::error_code{}));
        },
        boost::asio::use_future);
    auto run_future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);

    run_until_idle(context, *server_transport, peer);
    join(peer_future);
    join(shutdown_future);
    join(run_future);
    return outcome;
}

TEST(ServerConnectionTest, IgnoresACancellationThatArrivesWhileDraining)
{
    const auto outcome = outcome_when_a_cancellation_arrives_while_draining();

    ASSERT_TRUE(outcome.reply.has_value());
    EXPECT_EQ(outcome.reply->header.stream, StreamId{1});
    EXPECT_TRUE(has_flag(outcome.reply->header.flags, FrameFlags::error));
    EXPECT_FALSE(rillnet::decode_error_status(*outcome.reply).has_value());
    EXPECT_FALSE(outcome.handler_saw_cancellation);
    EXPECT_TRUE(outcome.reached_closed_state);
}

TEST(ServerConnectionTest, CancelsARunningHandlerWhenACancellationArrivesWhileDraining)
{
    const auto outcome = outcome_when_a_cancellation_arrives_while_draining();

    EXPECT_TRUE(outcome.handler_saw_cancellation);
}

TEST(ServerConnectionTest, GracefulShutdownWaitsForAnInFlightHandlerAndDeliversItsResponse)
{
    using Signal = boost::asio::experimental::channel<void(boost::system::error_code)>;

    boost::asio::io_context context;
    const auto registry = make_registry();
    auto transports = DuplexTransport::make_pair(context.get_executor());
    auto *server_transport = transports.first.get();
    ServerConnection connection(context.get_executor(), std::move(transports.first), registry);
    FramePeer peer(std::move(transports.second));
    Signal started(context.get_executor(), 1);
    Signal release(context.get_executor(), 1);
    std::optional<Frame> response;
    auto drain_duration = std::chrono::steady_clock::duration::max();

    connection.handle<StartSimulation>(
        [&](SessionContext &,
            StartSimulation request) -> boost::asio::awaitable<SimulationStarted> {
            EXPECT_TRUE(started.try_send(boost::system::error_code{}));
            boost::system::error_code error;
            co_await release.async_receive(
                boost::asio::redirect_error(boost::asio::use_awaitable, error));
            co_return SimulationStarted{request.id};
        });

    auto shutdown_future = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<void> {
            boost::system::error_code error;
            co_await started.async_receive(
                boost::asio::redirect_error(boost::asio::use_awaitable, error));
            const auto begin = std::chrono::steady_clock::now();
            co_await connection.shutdown(std::chrono::seconds{30});
            drain_duration = std::chrono::steady_clock::now() - begin;
            EXPECT_EQ(connection.state(), rillnet::ConnectionState::closed);
        },
        boost::asio::use_future);
    auto peer_future = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<void> {
            EXPECT_TRUE(co_await peer.send(make_request_frame(registry, StreamId{1}, 7)));
            while (connection.state() != rillnet::ConnectionState::draining) {
                co_await boost::asio::post(boost::asio::use_awaitable);
            }

            EXPECT_TRUE(release.try_send(boost::system::error_code{}));
            response = co_await peer.receive();
        },
        boost::asio::use_future);
    auto run_future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);

    run_until_idle(context, *server_transport, peer);
    join(peer_future);
    join(shutdown_future);
    join(run_future);

    ASSERT_TRUE(response.has_value());
    EXPECT_EQ(response->header.stream, StreamId{1});
    EXPECT_TRUE(has_flag(response->header.flags, FrameFlags::end_of_stream));
    EXPECT_FALSE(has_flag(response->header.flags, FrameFlags::error));
    const auto decoded = decode_message<SimulationStarted>(registry, *response);
    ASSERT_TRUE(decoded.ok());
    ASSERT_TRUE(decoded.value.has_value());
    EXPECT_EQ(decoded.value->id, 7U); // NOLINT(bugprone-unchecked-optional-access)
    EXPECT_LT(drain_duration, std::chrono::seconds{25});
}

struct DuplicateStreamOutcome {
    bool transport_open = true;
    std::size_t handled_requests = 0;
    std::size_t protocol_errors = 0;
    std::size_t written_frames = 0;
};

DuplicateStreamOutcome outcome_for_a_second_request_on_an_active_stream()
{
    boost::asio::io_context context;
    const auto registry = make_registry();

    std::vector<std::byte> incoming;
    append_frame(incoming, make_request_frame(registry, StreamId{1}, 7));
    append_frame(incoming, make_request_frame(registry, StreamId{1}, 8));

    DuplicateStreamOutcome outcome;
    const DiagnosticsHooks hooks{[&outcome](const DiagnosticsEvent &event) {
        if (event.type == DiagnosticsEventType::protocol_error) {
            ++outcome.protocol_errors;
        }
    }};
    auto transport = std::make_unique<InMemoryTransport>(std::move(incoming));
    auto *transport_observer = transport.get();
    ServerConnection connection(context.get_executor(), std::move(transport), registry, {}, hooks);

    connection.handle<StartSimulation>(
        [&outcome](SessionContext &,
                   StartSimulation request) -> boost::asio::awaitable<SimulationStarted> {
            ++outcome.handled_requests;
            co_await boost::asio::post(boost::asio::use_awaitable);
            co_return SimulationStarted{request.id};
        });

    auto run_future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);

    context.run_for(std::chrono::seconds(5));
    join(run_future);

    outcome.transport_open = transport_observer->is_open();
    FrameDecoder decoder;
    outcome.written_frames = decoder.push(transport_observer->outgoing()).size();
    return outcome;
}

TEST(ServerConnectionTest, ClosesTheConnectionSilentlyForASecondRequestOnAnActiveStream)
{
    const auto outcome = outcome_for_a_second_request_on_an_active_stream();

    EXPECT_FALSE(outcome.transport_open);
    EXPECT_EQ(outcome.handled_requests, 1U);
    EXPECT_EQ(outcome.written_frames, 0U);
    EXPECT_EQ(outcome.protocol_errors, 0U);
}

TEST(ServerConnectionTest, ReportsAProtocolErrorForASecondRequestOnAnActiveStream)
{
    const auto outcome = outcome_for_a_second_request_on_an_active_stream();

    EXPECT_EQ(outcome.protocol_errors, 1U);
}

TEST(ServerConnectionTest, ReportsDiagnosticsForTheWholeLifeOfARequest)
{
    boost::asio::io_context context;
    const auto registry = make_registry();
    std::vector<DiagnosticsEvent> events;
    const DiagnosticsHooks hooks{
        [&events](const DiagnosticsEvent &event) { events.push_back(event); }};

    std::vector<std::byte> incoming;
    append_frame(incoming, make_request_frame(registry, StreamId{1}, 7));
    ServerConnection connection(context.get_executor(),
                                std::make_unique<InMemoryTransport>(std::move(incoming)), registry,
                                {}, hooks);

    connection.handle<StartSimulation>(
        [](SessionContext &, StartSimulation request) -> boost::asio::awaitable<SimulationStarted> {
            co_return SimulationStarted{request.id};
        });

    auto run_future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);

    context.run_for(std::chrono::seconds(5));
    join(run_future);

    const auto count = [&events](DiagnosticsEventType type) {
        return std::count_if(events.begin(), events.end(),
                             [type](const DiagnosticsEvent &event) { return event.type == type; });
    };
    const auto transferred = [&events](DiagnosticsEventType type, bool inbound) {
        return std::any_of(events.begin(), events.end(),
                           [type, inbound](const DiagnosticsEvent &event) {
                               return event.type == type && event.inbound == inbound;
                           });
    };

    EXPECT_EQ(count(DiagnosticsEventType::connection_opened), 1);
    EXPECT_EQ(count(DiagnosticsEventType::request_started), 1);
    EXPECT_EQ(count(DiagnosticsEventType::request_completed), 1);
    EXPECT_EQ(count(DiagnosticsEventType::connection_closed), 1);
    EXPECT_EQ(count(DiagnosticsEventType::cancellation), 0);
    EXPECT_EQ(count(DiagnosticsEventType::protocol_error), 0);
    EXPECT_TRUE(transferred(DiagnosticsEventType::bytes_transferred, true));
    EXPECT_TRUE(transferred(DiagnosticsEventType::bytes_transferred, false));
    EXPECT_TRUE(transferred(DiagnosticsEventType::message_transferred, true));
    EXPECT_TRUE(transferred(DiagnosticsEventType::message_transferred, false));
    for (const auto &event : events) {
        if (event.type == DiagnosticsEventType::request_started ||
            event.type == DiagnosticsEventType::request_completed) {
            EXPECT_EQ(event.stream, StreamId{1});
        }
    }
}

} // namespace