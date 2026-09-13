#include <rillnet/tcp_server.hpp>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/experimental/channel.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/system/error_code.hpp>

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <future>
#include <memory>
#include <stdexcept>
#include <vector>

namespace {

using boost::asio::ip::tcp;
using rillnet::TcpServer;
using rillnet::Transport;

TEST(TcpServerTest, AcceptsMultipleConnectionsAndStopsCleanly)
{
    boost::asio::io_context context;
    TcpServer server(context, tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), 0));
    std::vector<std::unique_ptr<Transport>> connections;

    server.on_connection([&server, &connections](std::unique_ptr<Transport> connection) {
        connections.push_back(std::move(connection));
        if (connections.size() == 3) {
            server.stop();
        }
    });

    auto server_future = boost::asio::co_spawn(context, server.run(), boost::asio::use_future);
    std::vector<decltype(boost::asio::co_spawn(
        context, []() -> boost::asio::awaitable<void> { co_return; }, boost::asio::use_future))>
        client_futures;

    for (std::size_t index = 0; index < 3; ++index) {
        client_futures.push_back(boost::asio::co_spawn(
            context,
            [&context, endpoint = server.local_endpoint()]() -> boost::asio::awaitable<void> {
                tcp::socket socket(context);
                co_await socket.async_connect(endpoint, boost::asio::use_awaitable);
            },
            boost::asio::use_future));
    }

    context.run();

    server_future.get();
    for (auto &client_future : client_futures) {
        client_future.get();
    }

    ASSERT_EQ(connections.size(), 3U);
    for (const auto &connection : connections) {
        EXPECT_TRUE(connection->is_open());
    }
}

TEST(TcpServerTest, StopBeforeRunCompletesImmediately)
{
    boost::asio::io_context context;
    TcpServer server(context, tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), 0));
    server.stop();

    auto server_future = boost::asio::co_spawn(context, server.run(), boost::asio::use_future);
    context.run();

    server_future.get();
}

TEST(TcpServerTest, ShutdownDrainsRegisteredConnectionsToOneDeadline)
{
    using Clock = std::chrono::steady_clock;

    boost::asio::io_context context;
    TcpServer server(context, tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), 0));
    std::size_t shutdowns = 0;
    Clock::time_point first_deadline;
    Clock::time_point second_deadline;

    server.register_connection_shutdown(
        [&shutdowns, &first_deadline](Clock::time_point deadline) -> boost::asio::awaitable<void> {
            ++shutdowns;
            first_deadline = deadline;
            co_return;
        });
    server.register_connection_shutdown(
        [&shutdowns, &second_deadline](Clock::time_point deadline) -> boost::asio::awaitable<void> {
            ++shutdowns;
            second_deadline = deadline;
            co_return;
        });

    auto shutdown_future = boost::asio::co_spawn(context, server.shutdown(std::chrono::seconds(1)),
                                                 boost::asio::use_future);
    context.run();

    shutdown_future.get();
    EXPECT_EQ(shutdowns, 2U);
    EXPECT_EQ(first_deadline, second_deadline);
    EXPECT_GT(first_deadline, Clock::now());

    context.restart();
    auto second_shutdown_future = boost::asio::co_spawn(
        context, server.shutdown(std::chrono::seconds(1)), boost::asio::use_future);
    context.run();
    second_shutdown_future.get();
    EXPECT_EQ(shutdowns, 2U);
}

TEST(TcpServerTest, ShutdownWithoutRegisteredConnectionsCompletesImmediately)
{
    boost::asio::io_context context;
    TcpServer server(context, tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), 0));

    auto run_future = boost::asio::co_spawn(context, server.run(), boost::asio::use_future);
    auto shutdown_future = boost::asio::co_spawn(context, server.shutdown(std::chrono::seconds(30)),
                                                 boost::asio::use_future);

    context.run_for(std::chrono::seconds(5));

    ASSERT_EQ(shutdown_future.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    shutdown_future.get();
    ASSERT_EQ(run_future.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    run_future.get();
}

TEST(TcpServerTest, ShutdownDrainsRegisteredConnectionsConcurrently)
{
    using Clock = std::chrono::steady_clock;
    using Signal = boost::asio::experimental::channel<void(boost::system::error_code)>;

    boost::asio::io_context context;
    TcpServer server(context, tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), 0));
    Signal rendezvous(context.get_executor(), 1);
    bool first_resumed = false;
    bool second_started = false;

    // The first drain only finishes once the second one has started, so it can only complete if
    // the handlers are run concurrently rather than one after another.
    server.register_connection_shutdown([&](Clock::time_point) -> boost::asio::awaitable<void> {
        boost::system::error_code error;
        co_await rendezvous.async_receive(
            boost::asio::redirect_error(boost::asio::use_awaitable, error));
        first_resumed = !error;
    });
    server.register_connection_shutdown([&](Clock::time_point) -> boost::asio::awaitable<void> {
        second_started = true;
        EXPECT_TRUE(rendezvous.try_send(boost::system::error_code{}));
        co_return;
    });

    auto shutdown_future = boost::asio::co_spawn(context, server.shutdown(std::chrono::seconds(30)),
                                                 boost::asio::use_future);

    context.run_for(std::chrono::seconds(5));
    rendezvous.close();
    context.restart();
    context.run_for(std::chrono::milliseconds(100));

    ASSERT_EQ(shutdown_future.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    shutdown_future.get();
    EXPECT_TRUE(second_started);
    EXPECT_TRUE(first_resumed);
}

TEST(TcpServerTest, ShutdownCompletesWhenAConnectionDrainThrows)
{
    using Clock = std::chrono::steady_clock;

    boost::asio::io_context context;
    TcpServer server(context, tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), 0));
    bool second_drained = false;

    server.register_connection_shutdown([](Clock::time_point) -> boost::asio::awaitable<void> {
        throw std::runtime_error("drain failed");
        co_return;
    });
    server.register_connection_shutdown(
        [&second_drained](Clock::time_point) -> boost::asio::awaitable<void> {
            second_drained = true;
            co_return;
        });

    auto shutdown_future = boost::asio::co_spawn(context, server.shutdown(std::chrono::seconds(30)),
                                                 boost::asio::use_future);

    context.run_for(std::chrono::seconds(5));

    ASSERT_EQ(shutdown_future.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    shutdown_future.get();
    EXPECT_TRUE(second_drained);
}

// A connection that has already closed has no way of withdrawing the drain handler it registered
// when it was accepted, so shutdown() still invokes it. A handler that captures its connection
// therefore has to outlive every connection the server ever accepted, and the registration list
// grows for the lifetime of the server.
TEST(TcpServerTest, ShutdownStillDrainsConnectionsThatHaveAlreadyFinished)
{
    using Clock = std::chrono::steady_clock;

    boost::asio::io_context context;
    TcpServer server(context, tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), 0));
    bool connection_finished = false;
    bool drained_after_finishing = false;

    server.register_connection_shutdown([&connection_finished, &drained_after_finishing](
                                            Clock::time_point) -> boost::asio::awaitable<void> {
        drained_after_finishing = connection_finished;
        co_return;
    });
    connection_finished = true;

    auto shutdown_future = boost::asio::co_spawn(context, server.shutdown(std::chrono::seconds(30)),
                                                 boost::asio::use_future);

    context.run_for(std::chrono::seconds(5));

    ASSERT_EQ(shutdown_future.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    shutdown_future.get();
    EXPECT_TRUE(drained_after_finishing);
}

TEST(TcpServerTest, ShutdownStopsAcceptingBeforeDrainingConnections)
{
    using Clock = std::chrono::steady_clock;

    boost::asio::io_context context;
    TcpServer server(context, tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), 0));
    const auto endpoint = server.local_endpoint();
    std::size_t accepted = 0;
    bool connect_refused = false;

    server.on_connection([&accepted](std::unique_ptr<Transport>) { ++accepted; });
    // Probing the listening endpoint from inside a drain handler shows that the acceptor is
    // already closed by the time connections are asked to drain.
    server.register_connection_shutdown(
        [endpoint, &connect_refused](Clock::time_point) -> boost::asio::awaitable<void> {
            tcp::socket socket(co_await boost::asio::this_coro::executor);
            boost::system::error_code error;
            co_await socket.async_connect(
                endpoint, boost::asio::redirect_error(boost::asio::use_awaitable, error));
            connect_refused = static_cast<bool>(error);
            socket.close(error);
        });

    auto run_future = boost::asio::co_spawn(context, server.run(), boost::asio::use_future);
    auto shutdown_future = boost::asio::co_spawn(context, server.shutdown(std::chrono::seconds(30)),
                                                 boost::asio::use_future);

    context.run_for(std::chrono::seconds(5));

    ASSERT_EQ(shutdown_future.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    shutdown_future.get();
    ASSERT_EQ(run_future.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    run_future.get();
    EXPECT_TRUE(connect_refused);
    EXPECT_EQ(accepted, 0U);
}

} // namespace