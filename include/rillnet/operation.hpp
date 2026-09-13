#pragma once

#include <rillnet/identifiers.hpp>
#include <rillnet/lifecycle.hpp>
#include <rillnet/status_code.hpp>

#include <chrono>
#include <exception>
#include <functional>
#include <optional>
#include <string>
#include <utility>

namespace rillnet {

class CancellationError final : public std::exception {
  public:
    [[nodiscard]] const char *what() const noexcept override { return "operation cancelled"; }
};

class OperationResult {
  public:
    [[nodiscard]] static OperationResult success(std::string message = {})
    {
        return OperationResult(StatusCode::ok, std::move(message));
    }

    [[nodiscard]] static OperationResult failure(StatusCode status, std::string message = {})
    {
        return OperationResult(status, std::move(message));
    }

    [[nodiscard]] bool ok() const noexcept { return status_ == StatusCode::ok; }
    [[nodiscard]] StatusCode status() const noexcept { return status_; }
    [[nodiscard]] const std::string &message() const noexcept { return message_; }

  private:
    explicit OperationResult(StatusCode status, std::string message)
        : status_(status), message_(std::move(message))
    {
    }

    StatusCode status_;
    std::string message_;
};

class Operation {
  public:
    using Clock = std::chrono::steady_clock;
    using Deadline = Clock::time_point;
    using CompletionHandler = std::function<void(const OperationResult &)>;

    explicit Operation(StreamId stream, CompletionHandler completion_handler = {})
        : stream_(stream), completion_handler_(std::move(completion_handler))
    {
    }

    [[nodiscard]] StreamId stream() const noexcept { return stream_; }
    [[nodiscard]] OperationState state() const noexcept { return lifecycle_.state(); }
    [[nodiscard]] bool is_terminal() const noexcept
    {
        return state() == OperationState::completed || state() == OperationState::failed ||
               state() == OperationState::cancelled;
    }
    [[nodiscard]] const std::optional<OperationResult> &result() const noexcept { return result_; }
    [[nodiscard]] const std::optional<Deadline> &deadline() const noexcept { return deadline_; }
    [[nodiscard]] bool cancellation_requested() const noexcept { return cancellation_requested_; }
    [[nodiscard]] bool timed_out() const noexcept { return timed_out_; }

    [[nodiscard]] LifecycleTransitionError activate() noexcept
    {
        return lifecycle_.transition(OperationState::active);
    }

    [[nodiscard]] bool set_deadline(Deadline deadline) noexcept
    {
        if (is_terminal()) {
            return false;
        }
        deadline_ = deadline;
        return true;
    }

    [[nodiscard]] bool request_cancellation() noexcept
    {
        if (is_terminal()) {
            return false;
        }
        cancellation_requested_ = true;
        return true;
    }

    [[nodiscard]] bool complete(std::string message = {})
    {
        return finish(OperationState::completed, OperationResult::success(std::move(message)));
    }

    [[nodiscard]] bool fail(StatusCode status, std::string message = {})
    {
        if (status == StatusCode::ok) {
            return false;
        }
        return finish(OperationState::failed, OperationResult::failure(status, std::move(message)));
    }

    [[nodiscard]] bool cancel(std::string message = {})
    {
        if (!request_cancellation()) {
            return false;
        }
        return finish(OperationState::cancelled,
                      OperationResult::failure(StatusCode::cancelled, std::move(message)));
    }

    [[nodiscard]] bool timeout(std::string message = {})
    {
        const bool did_timeout =
            finish(OperationState::failed,
                   OperationResult::failure(StatusCode::timeout_error, std::move(message)));
        timed_out_ = timed_out_ || did_timeout;
        return did_timeout;
    }

  private:
    [[nodiscard]] bool finish(OperationState terminal_state, OperationResult result)
    {
        if (lifecycle_.transition(terminal_state) != LifecycleTransitionError::none) {
            return false;
        }

        result_.emplace(std::move(result));
        if (completion_handler_) {
            completion_handler_(*result_);
        }
        return true;
    }

    StreamId stream_;
    OperationLifecycle lifecycle_;
    std::optional<OperationResult> result_;
    std::optional<Deadline> deadline_;
    bool cancellation_requested_ = false;
    bool timed_out_ = false;
    CompletionHandler completion_handler_;
};

} // namespace rillnet