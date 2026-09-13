#pragma once

#include <rillnet/tcp_transport.hpp>
#include <rillnet/transport.hpp>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/experimental/channel.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/error_code.hpp>

#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

namespace rillnet {

// Accepts TCP connections on a supplied endpoint. Each accepted socket is transferred to the
// connection handler as a Transport, leaving the protocol layer independent of TCP details.
class TcpServer {
  public:
    using ConnectionHandler = std::function<void(std::unique_ptr<Transport>)>;
    using ConnectionShutdownHandler =
        std::function<boost::asio::awaitable<void>(std::chrono::steady_clock::time_point)>;

    TcpServer(boost::asio::io_context &context, const boost::asio::ip::tcp::endpoint &endpoint)
        : acceptor_(context, endpoint)
    {
    }

    ~TcpServer() { stop(); }

    TcpServer(const TcpServer &) = delete;
    TcpServer &operator=(const TcpServer &) = delete;

    void on_connection(ConnectionHandler handler) { connection_handler_ = std::move(handler); }

    // Registers the drain operation for an accepted connection. The handler receives the server's
    // absolute shutdown deadline and is responsible for closing the connection when it expires.
    void register_connection_shutdown(ConnectionShutdownHandler handler)
    {
        connection_shutdown_handlers_.push_back(std::move(handler));
    }

    // Repeatedly accepts connections until stop() is called. Transient accept failures are ignored
    // so that a server can continue accepting later connections.
    boost::asio::awaitable<void> run()
    {
        using boost::asio::ip::tcp;

        while (!stopping_) {
            boost::system::error_code error;
            tcp::socket socket = co_await acceptor_.async_accept(
                boost::asio::redirect_error(boost::asio::use_awaitable, error));

            if (error) {
                if (stopping_ || error == boost::asio::error::operation_aborted) {
                    break;
                }
                continue;
            }

            if (connection_handler_) {
                connection_handler_(std::make_unique<TcpTransport>(std::move(socket)));
            }
        }
    }

    // Stops accepting new connections and wakes a pending run() accept operation.
    void stop() noexcept
    {
        if (stopping_) {
            return;
        }

        stopping_ = true;
        boost::system::error_code ignored;
        acceptor_.cancel(ignored);
        acceptor_.close(ignored);
    }

    // Stops accepting connections and drains registered connections until the shared deadline.
    template <typename Rep, typename Period>
    boost::asio::awaitable<void> shutdown(std::chrono::duration<Rep, Period> drain_period)
    {
        stop();
        const auto deadline =
            std::chrono::steady_clock::now() +
            std::chrono::duration_cast<std::chrono::steady_clock::duration>(drain_period);
        using CompletionChannel =
            boost::asio::experimental::channel<void(boost::system::error_code)>;
        auto completion = std::make_shared<CompletionChannel>(acceptor_.get_executor(),
                                                              connection_shutdown_handlers_.size());

        for (auto &handler : connection_shutdown_handlers_) {
            boost::asio::co_spawn(
                acceptor_.get_executor(),
                [handler = std::move(handler), deadline,
                 completion]() mutable -> boost::asio::awaitable<void> {
                    try {
                        co_await handler(deadline);
                    } catch (...) {
                    }
                    boost::system::error_code error;
                    co_await completion->async_send(
                        {}, boost::asio::redirect_error(boost::asio::use_awaitable, error));
                },
                boost::asio::detached);
        }

        for (std::size_t index = 0; index < connection_shutdown_handlers_.size(); ++index) {
            boost::system::error_code error;
            co_await completion->async_receive(
                boost::asio::redirect_error(boost::asio::use_awaitable, error));
        }
        connection_shutdown_handlers_.clear();
    }

    [[nodiscard]] boost::asio::ip::tcp::endpoint local_endpoint() const
    {
        return acceptor_.local_endpoint();
    }

  private:
    boost::asio::ip::tcp::acceptor acceptor_;
    ConnectionHandler connection_handler_;
    std::vector<ConnectionShutdownHandler> connection_shutdown_handlers_;
    bool stopping_ = false;
};

} // namespace rillnet