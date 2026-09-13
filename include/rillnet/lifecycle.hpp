#pragma once

#include <cstdint>

namespace rillnet {

enum class ConnectionState : std::uint8_t {
    created,
    connecting,
    accepting,
    active,
    draining,
    closing,
    closed,
};

enum class OperationState : std::uint8_t {
    created,
    active,
    completed,
    failed,
    cancelled,
};

enum class LifecycleTransitionError : std::uint8_t {
    none,
    invalid_transition,
};

[[nodiscard]] constexpr bool can_transition(ConnectionState from, ConnectionState to) noexcept
{
    switch (from) {
    case ConnectionState::created:
        return to == ConnectionState::connecting || to == ConnectionState::accepting;
    case ConnectionState::connecting:
    case ConnectionState::accepting:
        return to == ConnectionState::active;
    case ConnectionState::active:
        return to == ConnectionState::draining || to == ConnectionState::closing ||
               to == ConnectionState::closed;
    case ConnectionState::draining:
        return to == ConnectionState::closing || to == ConnectionState::closed;
    case ConnectionState::closing:
        return to == ConnectionState::closed;
    case ConnectionState::closed:
        return false;
    }
    return false;
}

[[nodiscard]] constexpr bool can_transition(OperationState from, OperationState to) noexcept
{
    switch (from) {
    case OperationState::created:
        return to == OperationState::active;
    case OperationState::active:
        return to == OperationState::completed || to == OperationState::failed ||
               to == OperationState::cancelled;
    case OperationState::completed:
    case OperationState::failed:
    case OperationState::cancelled:
        return false;
    }
    return false;
}

class ConnectionLifecycle {
  public:
    [[nodiscard]] constexpr ConnectionState state() const noexcept { return state_; }

    constexpr LifecycleTransitionError transition(ConnectionState next) noexcept
    {
        if (!can_transition(state_, next)) {
            return LifecycleTransitionError::invalid_transition;
        }
        state_ = next;
        return LifecycleTransitionError::none;
    }

  private:
    ConnectionState state_ = ConnectionState::created;
};

class OperationLifecycle {
  public:
    [[nodiscard]] constexpr OperationState state() const noexcept { return state_; }

    constexpr LifecycleTransitionError transition(OperationState next) noexcept
    {
        if (!can_transition(state_, next)) {
            return LifecycleTransitionError::invalid_transition;
        }
        state_ = next;
        return LifecycleTransitionError::none;
    }

  private:
    OperationState state_ = OperationState::created;
};

} // namespace rillnet