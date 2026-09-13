#include <rillnet/client_connection.hpp>
#include <rillnet/server_connection.hpp>

#include "in_memory_transport.hpp"

#include <rillnet/diagnostics.hpp>
#include <rillnet/frame.hpp>
#include <rillnet/frame_codec.hpp>
#include <rillnet/frame_decoder.hpp>
#include <rillnet/frame_flags.hpp>
#include <rillnet/message_codec.hpp>
#include <rillnet/message_registry.hpp>
#include <rillnet/protocol_violation.hpp>
#include <rillnet/status_code.hpp>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/experimental/channel.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_future.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <future>
#include <initializer_list>
#include <limits>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

namespace {

using rillnet::ClientConnection;
using rillnet::DecodeResult;
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
using rillnet::make_error_frame;
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

// A client's first allocated stream id is always 1 (see StreamIdAllocator), so a canned response
// can be built for it up front.
std::vector<std::byte> encode_response_bytes(const MessageRegistry<> &registry,
                                             SimulationStarted response)
{
    const auto encoded = encode_message(registry, response, StreamId{1}, FrameType::response);
    EXPECT_TRUE(encoded.ok());
    return encode_frame(*encoded.frame); // NOLINT(bugprone-unchecked-optional-access)
}

std::vector<std::byte>
encode_response_bytes(const MessageRegistry<> &registry,
                      std::initializer_list<std::pair<StreamId, SimulationStarted>> responses)
{
    std::vector<std::byte> bytes;
    for (const auto &[stream, response] : responses) {
        const auto encoded = encode_message(registry, response, stream, FrameType::response);
        EXPECT_TRUE(encoded.ok());
        const auto frame_bytes =
            encode_frame(*encoded.frame); // NOLINT(bugprone-unchecked-optional-access)
        bytes.insert(bytes.end(), frame_bytes.begin(), frame_bytes.end());
    }
    return bytes;
}

// A response frame as ServerConnection writes it: a response carrying the terminal end_of_stream
// flag for its stream.
Frame make_terminal_response(const MessageRegistry<> &registry, StreamId stream,
                             SimulationStarted response)
{
    auto encoded = encode_message(registry, response, stream, FrameType::response);
    EXPECT_TRUE(encoded.ok());
    Frame frame = std::move(*encoded.frame); // NOLINT(bugprone-unchecked-optional-access)
    frame.header.flags |= FrameFlags::end_of_stream;
    return frame;
}

TEST(ClientConnectionTest, CorrelatesResponsesWhenTheyCompleteOutOfOrder)
{
    boost::asio::io_context context;
    const auto registry = make_registry();
    auto transport = std::make_unique<InMemoryTransport>(encode_response_bytes(
        registry, {{StreamId{3}, {30}}, {StreamId{1}, {10}}, {StreamId{5}, {50}}}));
    ClientConnection connection(context.get_executor(), std::move(transport), registry);

    auto first_request = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<DecodeResult<SimulationStarted>> {
            co_return co_await connection.request<StartSimulation, SimulationStarted>({1});
        },
        boost::asio::use_future);
    auto second_request = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<DecodeResult<SimulationStarted>> {
            co_return co_await connection.request<StartSimulation, SimulationStarted>({2});
        },
        boost::asio::use_future);
    auto third_request = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<DecodeResult<SimulationStarted>> {
            co_return co_await connection.request<StartSimulation, SimulationStarted>({3});
        },
        boost::asio::use_future);
    auto run_future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);

    context.run();

    const auto first_result = first_request.get();
    const auto second_result = second_request.get();
    const auto third_result = third_request.get();
    run_future.get();

    ASSERT_TRUE(first_result.ok());
    ASSERT_TRUE(second_result.ok());
    ASSERT_TRUE(third_result.ok());
    ASSERT_TRUE(first_result.value.has_value());
    ASSERT_TRUE(second_result.value.has_value());
    ASSERT_TRUE(third_result.value.has_value());
    EXPECT_EQ(first_result.value->id, 10U);  // NOLINT(bugprone-unchecked-optional-access)
    EXPECT_EQ(second_result.value->id, 30U); // NOLINT(bugprone-unchecked-optional-access)
    EXPECT_EQ(third_result.value->id, 50U);  // NOLINT(bugprone-unchecked-optional-access)
}

TEST(ClientConnectionTest, CompletesHundredsOfConcurrentOperations)
{
    constexpr std::uint32_t operation_count = 256;
    boost::asio::io_context context;
    const auto registry = make_registry();
    std::vector<std::byte> incoming;
    for (std::uint32_t index = 0; index < operation_count; ++index) {
        const auto stream = StreamId{1 + (index * 2)};
        const auto encoded =
            encode_message(registry, SimulationStarted{index}, stream, FrameType::response);
        EXPECT_TRUE(encoded.ok());
        const auto frame_bytes =
            encode_frame(*encoded.frame); // NOLINT(bugprone-unchecked-optional-access)
        incoming.insert(incoming.end(), frame_bytes.begin(), frame_bytes.end());
    }
    ClientConnection connection(context.get_executor(),
                                std::make_unique<InMemoryTransport>(std::move(incoming)), registry);

    std::vector<std::future<DecodeResult<SimulationStarted>>> requests;
    requests.reserve(operation_count);
    for (std::uint32_t index = 0; index < operation_count; ++index) {
        requests.push_back(boost::asio::co_spawn(
            context,
            [&connection, index]() -> boost::asio::awaitable<DecodeResult<SimulationStarted>> {
                co_return co_await connection.request<StartSimulation, SimulationStarted>({index});
            },
            boost::asio::use_future));
    }
    auto run_future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);

    context.run();

    for (std::uint32_t index = 0; index < operation_count; ++index) {
        const auto result = requests[index].get();
        ASSERT_TRUE(result.ok());
        ASSERT_TRUE(result.value.has_value());
        EXPECT_EQ(result.value->id, index); // NOLINT(bugprone-unchecked-optional-access)
    }
    run_future.get();
}

TEST(ClientConnectionTest, SupportsMultiThreadedIoWithSerializedConnectionState)
{
    constexpr std::uint32_t operation_count = 128;
    boost::asio::io_context context;
    auto connection_executor = boost::asio::make_strand(context);
    const auto registry = make_registry();
    std::vector<std::byte> incoming;
    for (std::uint32_t index = 0; index < operation_count; ++index) {
        const auto encoded = encode_message(registry, SimulationStarted{index},
                                            StreamId{1 + (index * 2)}, FrameType::response);
        EXPECT_TRUE(encoded.ok());
        const auto frame_bytes =
            encode_frame(*encoded.frame); // NOLINT(bugprone-unchecked-optional-access)
        incoming.insert(incoming.end(), frame_bytes.begin(), frame_bytes.end());
    }
    ClientConnection connection(connection_executor,
                                std::make_unique<InMemoryTransport>(std::move(incoming)), registry);

    std::vector<std::future<DecodeResult<SimulationStarted>>> requests;
    requests.reserve(operation_count);
    for (std::uint32_t index = 0; index < operation_count; ++index) {
        requests.push_back(boost::asio::co_spawn(
            connection_executor,
            [&connection, index]() -> boost::asio::awaitable<DecodeResult<SimulationStarted>> {
                co_return co_await connection.request<StartSimulation, SimulationStarted>({index});
            },
            boost::asio::use_future));
    }
    auto run_future = boost::asio::co_spawn(
        connection_executor, [&]() { return connection.run(); }, boost::asio::use_future);

    std::vector<std::thread> workers;
    for (int index = 0; index < 4; ++index) {
        workers.emplace_back([&context]() { context.run(); });
    }
    for (auto &worker : workers) {
        worker.join();
    }

    for (std::uint32_t index = 0; index < operation_count; ++index) {
        const auto result = requests[index].get();
        ASSERT_TRUE(result.ok());
        ASSERT_TRUE(result.value.has_value());
        EXPECT_EQ(result.value->id, index); // NOLINT(bugprone-unchecked-optional-access)
    }
    run_future.get();
}

TEST(ClientConnectionTest, FailsOneOperationWithoutAffectingOtherOperations)
{
    boost::asio::io_context context;
    const auto registry = make_registry();
    const auto failed_response =
        encode_message(registry, StartSimulation{11}, StreamId{1}, FrameType::response);
    EXPECT_TRUE(failed_response.ok());
    auto incoming =
        encode_frame(*failed_response.frame); // NOLINT(bugprone-unchecked-optional-access)
    const auto successful_responses =
        encode_response_bytes(registry, {{StreamId{3}, {22}}, {StreamId{5}, {33}}});
    incoming.insert(incoming.end(), successful_responses.begin(), successful_responses.end());
    ClientConnection connection(context.get_executor(),
                                std::make_unique<InMemoryTransport>(std::move(incoming)), registry);

    auto first_request = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<DecodeResult<SimulationStarted>> {
            co_return co_await connection.request<StartSimulation, SimulationStarted>({1});
        },
        boost::asio::use_future);
    auto second_request = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<DecodeResult<SimulationStarted>> {
            co_return co_await connection.request<StartSimulation, SimulationStarted>({2});
        },
        boost::asio::use_future);
    auto third_request = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<DecodeResult<SimulationStarted>> {
            co_return co_await connection.request<StartSimulation, SimulationStarted>({3});
        },
        boost::asio::use_future);
    auto run_future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);

    context.run();

    const auto first_result = first_request.get();
    const auto second_result = second_request.get();
    const auto third_result = third_request.get();
    run_future.get();
    EXPECT_FALSE(first_result.ok());
    EXPECT_EQ(first_result.status, StatusCode::unknown_message_type);
    ASSERT_TRUE(second_result.ok());
    ASSERT_TRUE(second_result.value.has_value());
    ASSERT_TRUE(third_result.ok());
    ASSERT_TRUE(third_result.value.has_value());
    EXPECT_EQ(second_result.value->id, 22U); // NOLINT(bugprone-unchecked-optional-access)
    EXPECT_EQ(third_result.value->id, 33U);  // NOLINT(bugprone-unchecked-optional-access)
}

TEST(ClientConnectionTest, ClosesConnectionForARequestFrameOnTheClient)
{
    boost::asio::io_context context;
    const auto registry = make_registry();
    const auto encoded = encode_message(registry, StartSimulation{99}, StreamId{1});
    EXPECT_TRUE(encoded.ok());
    auto transport = std::make_unique<InMemoryTransport>(
        encode_frame(*encoded.frame)); // NOLINT(bugprone-unchecked-optional-access)
    ClientConnection connection(context.get_executor(), std::move(transport), registry);

    auto request_future = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<DecodeResult<SimulationStarted>> {
            co_return co_await connection.request<StartSimulation, SimulationStarted>({7});
        },
        boost::asio::use_future);
    auto run_future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);

    context.run();

    run_future.get();
    const auto result = request_future.get();
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.status, StatusCode::malformed_frame);
}

TEST(ClientConnectionTest, RequestSendsAFrameAndDecodesTheMatchingResponse)
{
    boost::asio::io_context context;
    const auto registry = make_registry();
    auto transport = std::make_unique<InMemoryTransport>(encode_response_bytes(registry, {42}));
    ClientConnection connection(context.get_executor(), std::move(transport), registry);

    auto request_future = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<DecodeResult<SimulationStarted>> {
            co_return co_await connection.request<StartSimulation, SimulationStarted>({7});
        },
        boost::asio::use_future);
    auto run_future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);

    context.run();

    const auto result = request_future.get();
    run_future.get();
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.value.has_value());
    EXPECT_EQ(result.value->id, 42U); // NOLINT(bugprone-unchecked-optional-access)
}

TEST(ClientConnectionTest, RequestFailsImmediatelyForAnUnregisteredMessageType)
{
    boost::asio::io_context context;
    const MessageRegistry<> registry; // StartSimulation intentionally not registered
    ClientConnection connection(context.get_executor(),
                                std::make_unique<InMemoryTransport>(std::vector<std::byte>{}),
                                registry);

    auto request_future = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<DecodeResult<SimulationStarted>> {
            co_return co_await connection.request<StartSimulation, SimulationStarted>({7});
        },
        boost::asio::use_future);

    context.run();

    const auto result = request_future.get();
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.status, StatusCode::unknown_message_type);
}

TEST(ClientConnectionTest, ReusesStreamIdentifierAfterTerminalResultIsConsumed)
{
    boost::asio::io_context context;
    const auto registry = make_registry();
    ClientConnection connection(context.get_executor(),
                                std::make_unique<InMemoryTransport>(std::vector<std::byte>{}),
                                registry, {}, 1);

    auto future = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<void> {
            auto first = co_await connection.start_request<StartSimulation, SimulationStarted>({1});
            EXPECT_TRUE(first.ok());
            if (!first.ok()) {
                co_return;
            }
            auto operation = std::move(*first.operation);
            EXPECT_TRUE(operation.cancel());
            const auto result = co_await operation.async_wait();
            EXPECT_EQ(result.status, StatusCode::cancelled);

            auto second =
                co_await connection.start_request<StartSimulation, SimulationStarted>({2});
            EXPECT_TRUE(second.ok());
            if (!second.ok()) {
                co_return;
            }
            EXPECT_EQ(second.operation->stream(), StreamId{1});
        },
        boost::asio::use_future);

    context.run();
    future.get();
}

TEST(ClientConnectionTest, RequestFailsWithConnectionClosedWhenTheTransportClosesFirst)
{
    boost::asio::io_context context;
    const auto registry = make_registry();
    ClientConnection connection(context.get_executor(),
                                std::make_unique<InMemoryTransport>(std::vector<std::byte>{}),
                                registry);

    // Spawned before run(), so the request has already registered itself as pending by the time
    // the read loop observes end-of-stream and fails every outstanding request.
    auto request_future = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<DecodeResult<SimulationStarted>> {
            co_return co_await connection.request<StartSimulation, SimulationStarted>({7});
        },
        boost::asio::use_future);
    auto run_future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);

    context.run();

    run_future.get();
    const auto result = request_future.get();
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.status, StatusCode::connection_closed);
}

TEST(ClientConnectionTest, FailsAllOutstandingOperationsWhenTheTransportCloses)
{
    constexpr std::uint32_t operation_count = 128;
    boost::asio::io_context context;
    const auto registry = make_registry();
    ClientConnection connection(context.get_executor(),
                                std::make_unique<InMemoryTransport>(std::vector<std::byte>{}),
                                registry);

    std::vector<std::future<DecodeResult<SimulationStarted>>> requests;
    requests.reserve(operation_count);
    for (std::uint32_t index = 0; index < operation_count; ++index) {
        requests.push_back(boost::asio::co_spawn(
            context,
            [&connection, index]() -> boost::asio::awaitable<DecodeResult<SimulationStarted>> {
                co_return co_await connection.request<StartSimulation, SimulationStarted>({index});
            },
            boost::asio::use_future));
    }
    auto run_future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);

    context.run();

    for (auto &request : requests) {
        const auto result = request.get();
        EXPECT_FALSE(result.ok());
        EXPECT_EQ(result.status, StatusCode::connection_closed);
    }
    run_future.get();
}

TEST(ClientConnectionTest, ClosesConnectionForAResponseOnAnUnknownStream)
{
    boost::asio::io_context context;
    const auto registry = make_registry();
    auto incoming = encode_response_bytes(
        registry, {{StreamId{999}, {99}}, {StreamId{1}, {42}}, {StreamId{1}, {43}}});
    ClientConnection connection(context.get_executor(),
                                std::make_unique<InMemoryTransport>(std::move(incoming)), registry);

    auto request_future = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<DecodeResult<SimulationStarted>> {
            co_return co_await connection.request<StartSimulation, SimulationStarted>({7});
        },
        boost::asio::use_future);
    auto run_future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);

    context.run();

    const auto result = request_future.get();
    run_future.get();
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.status, StatusCode::unknown_stream);
}

TEST(ClientConnectionTest, ClosesConnectionForMalformedFrameAndFailsAllOperations)
{
    boost::asio::io_context context;
    const auto registry = make_registry();
    Frame malformed;
    malformed.header.stream = StreamId{1};
    malformed.header.flags = static_cast<FrameFlags>(1U << 7U);
    const auto incoming = encode_frame(malformed);
    ClientConnection connection(context.get_executor(),
                                std::make_unique<InMemoryTransport>(incoming), registry);

    auto first = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<DecodeResult<SimulationStarted>> {
            co_return co_await connection.request<StartSimulation, SimulationStarted>({1});
        },
        boost::asio::use_future);
    auto second = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<DecodeResult<SimulationStarted>> {
            co_return co_await connection.request<StartSimulation, SimulationStarted>({2});
        },
        boost::asio::use_future);
    auto run_future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);

    context.run();

    EXPECT_EQ(first.get().status, StatusCode::malformed_frame);
    EXPECT_EQ(second.get().status, StatusCode::malformed_frame);
    run_future.get();
}

TEST(ClientConnectionTest, CancelsOneOperationWithoutClosingTheConnection)
{
    boost::asio::io_context context;
    const auto registry = make_registry();
    auto transport =
        std::make_unique<InMemoryTransport>(encode_response_bytes(registry, {{StreamId{3}, {22}}}));
    const auto *transport_observer = transport.get();
    ClientConnection connection(context.get_executor(), std::move(transport), registry);

    auto cancelled_request = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<DecodeResult<SimulationStarted>> {
            auto started =
                co_await connection.start_request<StartSimulation, SimulationStarted>({1});
            if (!started.ok()) {
                co_return DecodeResult<SimulationStarted>::failure(started.status, started.message);
            }

            auto operation = std::move(*started.operation);
            EXPECT_EQ(operation.stream(), StreamId{1});
            EXPECT_TRUE(operation.cancel());
            co_return co_await operation.async_wait();
        },
        boost::asio::use_future);
    auto successful_request = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<DecodeResult<SimulationStarted>> {
            co_return co_await connection.request<StartSimulation, SimulationStarted>({2});
        },
        boost::asio::use_future);
    auto run_future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);

    context.run();

    const auto cancelled_result = cancelled_request.get();
    const auto successful_result = successful_request.get();
    run_future.get();

    EXPECT_FALSE(cancelled_result.ok());
    EXPECT_EQ(cancelled_result.status, StatusCode::cancelled);
    EXPECT_FALSE(cancelled_result.value.has_value());
    ASSERT_TRUE(successful_result.ok());
    ASSERT_TRUE(successful_result.value.has_value());
    EXPECT_EQ(successful_result.value->id, 22U); // NOLINT(bugprone-unchecked-optional-access)

    FrameDecoder outgoing_decoder;
    auto outgoing_frames = outgoing_decoder.push(transport_observer->outgoing());
    ASSERT_EQ(outgoing_frames.size(), 3U);
    EXPECT_EQ(outgoing_frames[1].header.stream, StreamId{1});
    EXPECT_EQ(outgoing_frames[1].header.type, FrameType::request);
    EXPECT_TRUE(has_flag(outgoing_frames[1].header.flags, FrameFlags::cancel));
}

TEST(ClientConnectionTest, ResponseWinsWhenItArrivesBeforeCancellation)
{
    boost::asio::io_context context;
    const auto registry = make_registry();
    ClientConnection connection(
        context.get_executor(),
        std::make_unique<InMemoryTransport>(encode_response_bytes(registry, {42})), registry);

    auto request_future = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<DecodeResult<SimulationStarted>> {
            auto started =
                co_await connection.start_request<StartSimulation, SimulationStarted>({7});
            if (!started.ok()) {
                co_return DecodeResult<SimulationStarted>::failure(started.status, started.message);
            }

            auto operation = std::move(*started.operation);
            co_await boost::asio::post(boost::asio::use_awaitable);
            EXPECT_FALSE(operation.cancel());
            co_return co_await operation.async_wait();
        },
        boost::asio::use_future);
    auto run_future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);

    context.run();

    const auto result = request_future.get();
    run_future.get();
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.value.has_value());
    EXPECT_EQ(result.value->id, 42U); // NOLINT(bugprone-unchecked-optional-access)
}

TEST(ClientConnectionTest, TimesOutARequest)
{
    boost::asio::io_context context;
    const auto registry = make_registry();
    auto transports = DuplexTransport::make_pair(context.get_executor());
    auto client_transport = std::move(transports.first);
    auto *client_transport_ptr = client_transport.get();
    ClientConnection connection(context.get_executor(), std::move(client_transport), registry);

    auto request_future = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<DecodeResult<SimulationStarted>> {
            auto result = co_await connection.request<StartSimulation, SimulationStarted>(
                {7}, std::chrono::milliseconds{1});
            client_transport_ptr->close();
            co_return result;
        },
        boost::asio::use_future);
    auto run_future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);

    context.run();

    const auto result = request_future.get();
    run_future.get();
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.status, StatusCode::timeout_error);
}

TEST(ClientConnectionTest, ResponseWinsWhenItArrivesBeforeADeadline)
{
    boost::asio::io_context context;
    const auto registry = make_registry();
    auto transport = std::make_unique<InMemoryTransport>(encode_response_bytes(registry, {42}));
    const auto *transport_observer = transport.get();
    ClientConnection connection(context.get_executor(), std::move(transport), registry);

    auto request_future = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<DecodeResult<SimulationStarted>> {
            co_return co_await connection.request<StartSimulation, SimulationStarted>(
                {7}, std::chrono::seconds{1});
        },
        boost::asio::use_future);
    auto run_future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);

    context.run();

    const auto result = request_future.get();
    run_future.get();
    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.value.has_value());
    EXPECT_EQ(result.value->id, 42U); // NOLINT(bugprone-unchecked-optional-access)

    FrameDecoder decoder;
    EXPECT_EQ(decoder.push(transport_observer->outgoing()).size(), 1U);
}

TEST(ClientConnectionTest, GracefulShutdownRejectsRequestsAndHonorsDrainDeadline)
{
    boost::asio::io_context context;
    const auto registry = make_registry();
    auto transports = DuplexTransport::make_pair(context.get_executor());
    ClientConnection connection(context.get_executor(), std::move(transports.first), registry);

    auto shutdown_future = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<void> {
            auto started =
                co_await connection.start_request<StartSimulation, SimulationStarted>({7});
            EXPECT_TRUE(started.ok());
            if (!started.ok()) {
                co_return;
            }

            auto operation = std::move(*started.operation);
            co_await connection.shutdown(std::chrono::milliseconds{1});
            EXPECT_EQ(connection.state(), rillnet::ConnectionState::closed);

            const auto completed = co_await operation.async_wait();
            EXPECT_FALSE(completed.ok());
            EXPECT_EQ(completed.status, StatusCode::connection_closed);

            const auto rejected =
                co_await connection.request<StartSimulation, SimulationStarted>({8});
            EXPECT_FALSE(rejected.ok());
            EXPECT_EQ(rejected.status, StatusCode::connection_closed);
        },
        boost::asio::use_future);
    auto run_future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);

    context.run();

    shutdown_future.get();
    run_future.get();
}

TEST(ClientConnectionTest, CancelsOneOfTwoConcurrentRequestsThroughTheServer)
{
    boost::asio::io_context context;
    const auto registry = make_registry();
    auto transports = DuplexTransport::make_pair(context.get_executor());
    auto client_transport = std::move(transports.first);
    auto server_transport = std::move(transports.second);
    auto *client_transport_ptr = client_transport.get();
    auto *server_transport_ptr = server_transport.get();
    ClientConnection client(context.get_executor(), std::move(client_transport), registry);
    ServerConnection server(context.get_executor(), std::move(server_transport), registry);
    auto watchdog = std::make_shared<boost::asio::steady_timer>(context);
    watchdog->expires_after(std::chrono::seconds(1));
    std::size_t cancellation_observed = 0;
    std::size_t handled_requests = 0;

    server.handle<StartSimulation>(
        [&context, &cancellation_observed,
         &handled_requests](SessionContext &session,
                            StartSimulation request) -> boost::asio::awaitable<SimulationStarted> {
            ++handled_requests;
            if (request.id == 1) {
                boost::asio::steady_timer timer(context);
                timer.expires_after(std::chrono::milliseconds(10));
                co_await timer.async_wait(boost::asio::use_awaitable);
                if (session.is_cancelled()) {
                    ++cancellation_observed;
                    session.throw_if_cancelled();
                }
            }
            co_return SimulationStarted{request.id};
        });

    auto client_future = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<void> {
            auto first = co_await client.start_request<StartSimulation, SimulationStarted>({1});
            auto second = co_await client.start_request<StartSimulation, SimulationStarted>({2});
            EXPECT_TRUE(first.ok());
            EXPECT_TRUE(second.ok());
            if (!first.ok() || !second.ok()) {
                client_transport_ptr->close();
                co_return;
            }

            auto cancelled = std::move(*first.operation);
            auto successful = std::move(*second.operation);
            EXPECT_TRUE(cancelled.cancel());
            const auto cancelled_result = co_await cancelled.async_wait();
            const auto successful_result = co_await successful.async_wait();
            EXPECT_FALSE(cancelled_result.ok());
            EXPECT_EQ(cancelled_result.status, StatusCode::cancelled);
            EXPECT_TRUE(successful_result.ok());
            EXPECT_TRUE(successful_result.value.has_value());
            if (successful_result.ok() && successful_result.value.has_value()) {
                EXPECT_EQ(successful_result.value->id, 2U);
            }
            watchdog->cancel();
            client_transport_ptr->close();
            server_transport_ptr->close();
        },
        boost::asio::use_future);
    boost::asio::co_spawn(
        context,
        [watchdog, client_transport_ptr, server_transport_ptr]() -> boost::asio::awaitable<void> {
            boost::system::error_code error;
            co_await watchdog->async_wait(
                boost::asio::redirect_error(boost::asio::use_awaitable, error));
            if (!error) {
                client_transport_ptr->close();
                server_transport_ptr->close();
            }
        },
        boost::asio::detached);
    auto server_future =
        boost::asio::co_spawn(context, [&]() { return server.run(); }, boost::asio::use_future);
    auto client_run_future =
        boost::asio::co_spawn(context, [&]() { return client.run(); }, boost::asio::use_future);

    context.run_for(std::chrono::seconds(2));
    client_transport_ptr->close();
    server_transport_ptr->close();
    context.restart();
    context.run_for(std::chrono::milliseconds(100));

    EXPECT_EQ(client_future.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    EXPECT_EQ(server_future.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    if (client_future.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
        client_future.get();
    }
    if (server_future.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
        server_future.get();
    }
    EXPECT_EQ(client_run_future.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    if (client_run_future.wait_for(std::chrono::seconds(0)) == std::future_status::ready) {
        client_run_future.get();
    }
    EXPECT_EQ(handled_requests, 2U);
    EXPECT_EQ(cancellation_observed, 1U);
}

void join(std::future<void> &future)
{
    ASSERT_EQ(future.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    future.get();
}

// Closes both ends and drains once more after the scenario should already be finished, so a
// regression fails an assertion instead of hanging the suite on a future that never completes.
void run_until_idle(boost::asio::io_context &context, rillnet::Transport &client_transport,
                    FramePeer &peer)
{
    context.run_for(std::chrono::seconds(5));
    client_transport.close();
    peer.close();
    context.restart();
    context.run_for(std::chrono::milliseconds(100));
}

// Completes one request, then issues a second one that reuses the identifier released by the
// first, answering both with a terminal response exactly as ServerConnection does.
DecodeResult<SimulationStarted> second_request_on_recycled_stream()
{
    boost::asio::io_context context;
    const auto registry = make_registry();
    auto transports = DuplexTransport::make_pair(context.get_executor());
    auto *client_transport = transports.first.get();
    ClientConnection connection(context.get_executor(), std::move(transports.first), registry);
    FramePeer peer(std::move(transports.second));
    auto second =
        DecodeResult<SimulationStarted>::failure(StatusCode::operation_error, "scenario not run");

    auto client_future = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<void> {
            const auto first = co_await connection.request<StartSimulation, SimulationStarted>({1});
            EXPECT_TRUE(first.ok());
            second = co_await connection.request<StartSimulation, SimulationStarted>({2});
            client_transport->close();
        },
        boost::asio::use_future);
    auto peer_future = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<void> {
            for (std::uint32_t index = 0; index < 2; ++index) {
                const auto request = co_await peer.receive();
                if (!request.has_value()) {
                    co_return;
                }
                // Both requests travel on stream 1: the identifier is released as soon as the
                // first terminal response is consumed.
                EXPECT_EQ(request->header.stream, StreamId{1});
                EXPECT_TRUE(co_await peer.send(
                    make_terminal_response(registry, request->header.stream, {10 + index})));
            }
        },
        boost::asio::use_future);
    auto run_future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);

    run_until_idle(context, *client_transport, peer);
    join(client_future);
    join(peer_future);
    join(run_future);
    return second;
}

// BUG: dispatch() records every stream that carried a terminal frame in terminal_streams_ and
// never forgets it, while StreamIdAllocator hands the same identifier out again once the
// operation releases it. The first legitimate response on a recycled stream is therefore treated
// as a duplicate terminal frame and the entire connection is torn down.
TEST(ClientConnectionTest, TearsDownTheConnectionForAResponseOnARecycledStreamIdentifier)
{
    const auto second = second_request_on_recycled_stream();

    EXPECT_FALSE(second.ok());
    EXPECT_EQ(second.status, StatusCode::malformed_frame);
}

TEST(ClientConnectionTest, AcceptsAResponseOnARecycledStreamIdentifier)
{
    const auto second = second_request_on_recycled_stream();

    ASSERT_TRUE(second.ok());
    ASSERT_TRUE(second.value.has_value());
    EXPECT_EQ(second.value->id, 11U); // NOLINT(bugprone-unchecked-optional-access)
}

// Cancels one of two in-flight requests and then delivers a response for the cancelled stream, as
// happens whenever a cancellation and a response cross on the wire.
DecodeResult<SimulationStarted> surviving_request_when_a_response_crosses_a_cancellation()
{
    boost::asio::io_context context;
    const auto registry = make_registry();
    auto transports = DuplexTransport::make_pair(context.get_executor());
    auto *client_transport = transports.first.get();
    ClientConnection connection(context.get_executor(), std::move(transports.first), registry);
    FramePeer peer(std::move(transports.second));
    auto surviving =
        DecodeResult<SimulationStarted>::failure(StatusCode::operation_error, "scenario not run");

    auto client_future = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<void> {
            auto first = co_await connection.start_request<StartSimulation, SimulationStarted>({1});
            auto second =
                co_await connection.start_request<StartSimulation, SimulationStarted>({2});
            EXPECT_TRUE(first.ok());
            EXPECT_TRUE(second.ok());
            if (!first.ok() || !second.ok()) {
                client_transport->close();
                co_return;
            }

            auto cancelled = std::move(*first.operation);
            auto survivor = std::move(*second.operation);
            EXPECT_TRUE(cancelled.cancel());
            const auto cancelled_result = co_await cancelled.async_wait();
            EXPECT_EQ(cancelled_result.status, StatusCode::cancelled);
            surviving = co_await survivor.async_wait();
            client_transport->close();
        },
        boost::asio::use_future);
    auto peer_future = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<void> {
            const auto first_request = co_await peer.receive();
            const auto second_request = co_await peer.receive();
            const auto cancellation = co_await peer.receive();
            EXPECT_TRUE(first_request.has_value());
            EXPECT_TRUE(second_request.has_value());
            EXPECT_TRUE(cancellation.has_value());
            if (!first_request.has_value() || !second_request.has_value() ||
                !cancellation.has_value()) {
                co_return;
            }
            EXPECT_TRUE(has_flag(cancellation->header.flags, FrameFlags::cancel));
            EXPECT_EQ(cancellation->header.stream, first_request->header.stream);

            // The second send is only delivered while the connection survives the first one.
            EXPECT_TRUE(co_await peer.send(
                make_terminal_response(registry, first_request->header.stream, {11})));
            (void)co_await peer.send(
                make_terminal_response(registry, second_request->header.stream, {33}));
        },
        boost::asio::use_future);
    auto run_future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);

    run_until_idle(context, *client_transport, peer);
    join(client_future);
    join(peer_future);
    join(run_future);
    return surviving;
}

// BUG: a response that crosses a cancellation on the wire arrives for a stream the client has
// already removed from pending_, and dispatch() answers an unknown stream by failing the whole
// connection. One unavoidable cancellation race therefore destroys every unrelated operation
// sharing the connection.
TEST(ClientConnectionTest, TearsDownTheConnectionWhenAResponseCrossesACancellation)
{
    const auto surviving = surviving_request_when_a_response_crosses_a_cancellation();

    EXPECT_FALSE(surviving.ok());
    EXPECT_EQ(surviving.status, StatusCode::unknown_stream);
}

TEST(ClientConnectionTest, KeepsUnrelatedOperationsWhenAResponseCrossesACancellation)
{
    const auto surviving = surviving_request_when_a_response_crosses_a_cancellation();

    ASSERT_TRUE(surviving.ok());
    ASSERT_TRUE(surviving.value.has_value());
    EXPECT_EQ(surviving.value->id, 33U); // NOLINT(bugprone-unchecked-optional-access)
}

struct TwoResults {
    DecodeResult<SimulationStarted> first;
    DecodeResult<SimulationStarted> second;
};

// Answers the first of two concurrent requests with a protocol error frame carrying `status`, and
// the second with an ordinary response.
TwoResults results_for_an_error_frame(StatusCode status)
{
    boost::asio::io_context context;
    const auto registry = make_registry();
    auto transports = DuplexTransport::make_pair(context.get_executor());
    auto *client_transport = transports.first.get();
    ClientConnection connection(context.get_executor(), std::move(transports.first), registry);
    FramePeer peer(std::move(transports.second));
    TwoResults results{
        DecodeResult<SimulationStarted>::failure(StatusCode::operation_error, "scenario not run"),
        DecodeResult<SimulationStarted>::failure(StatusCode::operation_error, "scenario not run")};

    auto client_future = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<void> {
            auto first = co_await connection.start_request<StartSimulation, SimulationStarted>({1});
            auto second =
                co_await connection.start_request<StartSimulation, SimulationStarted>({2});
            EXPECT_TRUE(first.ok());
            EXPECT_TRUE(second.ok());
            if (!first.ok() || !second.ok()) {
                client_transport->close();
                co_return;
            }

            auto rejected = std::move(*first.operation);
            auto accepted = std::move(*second.operation);
            results.first = co_await rejected.async_wait();
            results.second = co_await accepted.async_wait();
            client_transport->close();
        },
        boost::asio::use_future);
    auto peer_future = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<void> {
            const auto first_request = co_await peer.receive();
            const auto second_request = co_await peer.receive();
            EXPECT_TRUE(first_request.has_value());
            EXPECT_TRUE(second_request.has_value());
            if (!first_request.has_value() || !second_request.has_value()) {
                co_return;
            }

            EXPECT_TRUE(co_await peer.send(make_error_frame(first_request->header.stream, status)));
            (void)co_await peer.send(
                make_terminal_response(registry, second_request->header.stream, {33}));
        },
        boost::asio::use_future);
    auto run_future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);

    run_until_idle(context, *client_transport, peer);
    join(client_future);
    join(peer_future);
    join(run_future);
    return results;
}

TEST(ClientConnectionTest, FailsOnlyTheTargetedOperationForAStreamLevelErrorFrame)
{
    const auto results = results_for_an_error_frame(StatusCode::unknown_message_type);

    EXPECT_FALSE(results.first.ok());
    EXPECT_EQ(results.first.status, StatusCode::unknown_message_type);
    ASSERT_TRUE(results.second.ok());
    ASSERT_TRUE(results.second.value.has_value());
    EXPECT_EQ(results.second.value->id, 33U); // NOLINT(bugprone-unchecked-optional-access)
}

// BUG: ServerConnection answers requests received while draining with an error frame carrying
// StatusCode::connection_closed, but decode_error_status() rejects transport-category codes. The
// client reads its peer's orderly drain notice as a malformed payload and fails every operation
// on the connection with malformed_frame.
TEST(ClientConnectionTest, TearsDownTheConnectionForTheDrainErrorFrameTheServerSends)
{
    const auto results = results_for_an_error_frame(StatusCode::connection_closed);

    EXPECT_EQ(results.first.status, StatusCode::malformed_frame);
    EXPECT_EQ(results.second.status, StatusCode::malformed_frame);
}

TEST(ClientConnectionTest, ReportsTheDrainErrorFrameAsConnectionClosed)
{
    const auto results = results_for_an_error_frame(StatusCode::connection_closed);

    EXPECT_EQ(results.first.status, StatusCode::connection_closed);
    ASSERT_TRUE(results.second.ok());
    ASSERT_TRUE(results.second.value.has_value());
    EXPECT_EQ(results.second.value->id, 33U); // NOLINT(bugprone-unchecked-optional-access)
}

TEST(ClientConnectionTest, GracefulShutdownCompletesAsSoonAsAnInFlightResponseArrives)
{
    boost::asio::io_context context;
    const auto registry = make_registry();
    auto transports = DuplexTransport::make_pair(context.get_executor());
    auto *client_transport = transports.first.get();
    ClientConnection connection(context.get_executor(), std::move(transports.first), registry);
    FramePeer peer(std::move(transports.second));
    boost::asio::experimental::channel<void(boost::system::error_code)> started(
        context.get_executor(), 1);
    auto drain_duration = std::chrono::steady_clock::duration::max();
    auto result =
        DecodeResult<SimulationStarted>::failure(StatusCode::operation_error, "scenario not run");

    auto request_future = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<void> {
            auto started_request =
                co_await connection.start_request<StartSimulation, SimulationStarted>({7});
            EXPECT_TRUE(started_request.ok());
            if (!started_request.ok()) {
                client_transport->close();
                co_return;
            }
            auto operation = std::move(*started_request.operation);
            EXPECT_TRUE(started.try_send(boost::system::error_code{}));
            result = co_await operation.async_wait();
        },
        boost::asio::use_future);
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
            const auto request = co_await peer.receive();
            EXPECT_TRUE(request.has_value());
            if (!request.has_value()) {
                co_return;
            }
            EXPECT_TRUE(
                co_await peer.send(make_terminal_response(registry, request->header.stream, {7})));
        },
        boost::asio::use_future);
    auto run_future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);

    run_until_idle(context, *client_transport, peer);
    join(request_future);
    join(shutdown_future);
    join(peer_future);
    join(run_future);

    ASSERT_TRUE(result.ok());
    ASSERT_TRUE(result.value.has_value());
    EXPECT_EQ(result.value->id, 7U); // NOLINT(bugprone-unchecked-optional-access)
    EXPECT_LT(drain_duration, std::chrono::seconds{25});
}

TEST(ClientConnectionTest, ReportsDiagnosticsForTheWholeLifeOfARequest)
{
    boost::asio::io_context context;
    const auto registry = make_registry();
    std::vector<DiagnosticsEvent> events;
    const DiagnosticsHooks hooks{
        [&events](const DiagnosticsEvent &event) { events.push_back(event); }};
    ClientConnection connection(
        context.get_executor(),
        std::make_unique<InMemoryTransport>(encode_response_bytes(registry, {42})), registry, {},
        std::numeric_limits<std::uint64_t>::max(), hooks);

    auto request_future = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<DecodeResult<SimulationStarted>> {
            co_return co_await connection.request<StartSimulation, SimulationStarted>({7});
        },
        boost::asio::use_future);
    auto run_future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);

    context.run();

    EXPECT_TRUE(request_future.get().ok());
    run_future.get();

    const auto count = [&events](DiagnosticsEventType type) {
        return std::count_if(events.begin(), events.end(),
                             [type](const DiagnosticsEvent &event) { return event.type == type; });
    };
    const auto position = [&events](DiagnosticsEventType type) {
        return std::distance(events.begin(), std::find_if(events.begin(), events.end(),
                                                          [type](const DiagnosticsEvent &event) {
                                                              return event.type == type;
                                                          }));
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
    EXPECT_EQ(count(DiagnosticsEventType::timeout), 0);
    EXPECT_EQ(count(DiagnosticsEventType::protocol_error), 0);
    EXPECT_LT(position(DiagnosticsEventType::request_started),
              position(DiagnosticsEventType::request_completed));
    EXPECT_LT(position(DiagnosticsEventType::request_completed),
              position(DiagnosticsEventType::connection_closed));
    ASSERT_FALSE(events.empty());
    EXPECT_TRUE(transferred(DiagnosticsEventType::bytes_transferred, false));
    EXPECT_TRUE(transferred(DiagnosticsEventType::bytes_transferred, true));
    EXPECT_TRUE(transferred(DiagnosticsEventType::message_transferred, false));
    EXPECT_TRUE(transferred(DiagnosticsEventType::message_transferred, true));
    for (const auto &event : events) {
        if (event.type == DiagnosticsEventType::bytes_transferred) {
            EXPECT_GT(event.bytes, 0U);
        }
        if (event.type == DiagnosticsEventType::request_started ||
            event.type == DiagnosticsEventType::request_completed) {
            EXPECT_EQ(event.stream, StreamId{1});
        }
    }
}

// BUG: connection_opened is only reported from run(), and only while the connection is still in
// the created state. Shutting a connection down before run() is spawned therefore produces a
// connection_closed event with no matching connection_opened event.
TEST(ClientConnectionTest, OmitsConnectionOpenedWhenShutdownRunsBeforeTheConnection)
{
    boost::asio::io_context context;
    const auto registry = make_registry();
    std::vector<DiagnosticsEventType> types;
    const DiagnosticsHooks hooks{
        [&types](const DiagnosticsEvent &event) { types.push_back(event.type); }};
    ClientConnection connection(context.get_executor(),
                                std::make_unique<InMemoryTransport>(std::vector<std::byte>{}),
                                registry, {}, std::numeric_limits<std::uint64_t>::max(), hooks);

    auto shutdown_future = boost::asio::co_spawn(
        context,
        [&]() -> boost::asio::awaitable<void> {
            co_await connection.shutdown(std::chrono::milliseconds{1});
            EXPECT_EQ(connection.state(), rillnet::ConnectionState::closed);
            // Draining an already closed connection is a no-op rather than an error.
            co_await connection.shutdown(std::chrono::milliseconds{1});
            EXPECT_EQ(connection.state(), rillnet::ConnectionState::closed);
        },
        boost::asio::use_future);
    auto run_future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);

    context.run();

    shutdown_future.get();
    run_future.get();
    EXPECT_EQ(std::count(types.begin(), types.end(), DiagnosticsEventType::connection_opened), 0);
    EXPECT_EQ(std::count(types.begin(), types.end(), DiagnosticsEventType::connection_closed), 1);
}

TEST(ClientConnectionTest, ReportsConnectionOpenedWhenShutdownRunsBeforeTheConnection)
{
    boost::asio::io_context context;
    const auto registry = make_registry();
    std::vector<DiagnosticsEventType> types;
    const DiagnosticsHooks hooks{
        [&types](const DiagnosticsEvent &event) { types.push_back(event.type); }};
    ClientConnection connection(context.get_executor(),
                                std::make_unique<InMemoryTransport>(std::vector<std::byte>{}),
                                registry, {}, std::numeric_limits<std::uint64_t>::max(), hooks);

    auto shutdown_future = boost::asio::co_spawn(
        context, [&]() { return connection.shutdown(std::chrono::milliseconds{1}); },
        boost::asio::use_future);
    auto run_future =
        boost::asio::co_spawn(context, [&]() { return connection.run(); }, boost::asio::use_future);

    context.run();

    shutdown_future.get();
    run_future.get();
    EXPECT_EQ(std::count(types.begin(), types.end(), DiagnosticsEventType::connection_opened), 1);
}

} // namespace
