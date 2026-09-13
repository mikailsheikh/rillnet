#pragma once

#include <rillnet/identifiers.hpp>
#include <rillnet/status_code.hpp>

#include <cstddef>
#include <functional>
#include <string>
#include <utility>

namespace rillnet {

enum class DiagnosticsEventType {
    connection_opened,
    connection_closed,
    request_started,
    request_completed,
    cancellation,
    timeout,
    protocol_error,
    bytes_transferred,
    message_transferred,
};

struct DiagnosticsEvent {
    DiagnosticsEventType type;
    StreamId stream{};
    StatusCode status = StatusCode::ok;
    std::size_t bytes = 0;
    bool inbound = false;
    std::string message;

    DiagnosticsEvent(DiagnosticsEventType type, StreamId stream = {},
                     StatusCode status = StatusCode::ok, std::size_t bytes = 0,
                     bool inbound = false, std::string message = {})
        : type(type), stream(stream), status(status), bytes(bytes), inbound(inbound),
          message(std::move(message))
    {
    }
};

struct DiagnosticsHooks {
    std::function<void(const DiagnosticsEvent &)> on_event;
};

inline void notify_diagnostics(const DiagnosticsHooks &hooks, DiagnosticsEvent event) noexcept
{
    if (!hooks.on_event) {
        return;
    }
    try {
        hooks.on_event(event);
    } catch (...) {
        // Diagnostics must never change protocol behavior.
    }
}

} // namespace rillnet