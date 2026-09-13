#include <rillnet/frame_codec.hpp>
#include <rillnet/message_codec.hpp>
#include <rillnet/message_registry.hpp>
#include <rillnet/server_connection.hpp>

#include "in_memory_transport.hpp"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <stdexcept>
#include <thread>
#include <vector>

namespace {

using rillnet::decode_message;
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
using rillnet::StreamId;
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

} // namespace