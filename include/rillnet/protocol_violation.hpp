#pragma once

#include <rillnet/frame.hpp>
#include <rillnet/frame_codec.hpp>
#include <rillnet/status_code.hpp>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

namespace rillnet {

[[nodiscard]] constexpr StatusCode status_for_frame_validation(FrameValidationError error) noexcept
{
    return error == FrameValidationError::unsupported_version ? StatusCode::unsupported_version
                                                              : StatusCode::malformed_frame;
}

[[nodiscard]] inline Frame make_error_frame(StreamId stream, StatusCode status)
{
    Frame frame;
    frame.header.type = FrameType::response;
    frame.header.flags = FrameFlags::error | FrameFlags::end_of_stream;
    frame.header.stream = stream;
    frame.payload.resize(sizeof(std::uint16_t));
    frame.payload[0] = static_cast<std::byte>((static_cast<std::uint16_t>(status) >> 8U) & 0xFFU);
    frame.payload[1] = static_cast<std::byte>(static_cast<std::uint16_t>(status) & 0xFFU);
    frame.header.payload_size = static_cast<std::uint32_t>(frame.payload.size());
    return frame;
}

[[nodiscard]] inline std::optional<StatusCode> decode_error_status(const Frame &frame) noexcept
{
    if (frame.payload.size() != sizeof(std::uint16_t)) {
        return std::nullopt;
    }
    const auto value = static_cast<std::uint16_t>(frame.payload[0]) << 8U |
                       static_cast<std::uint16_t>(frame.payload[1]);
    switch (value) {
    case 2000:
    case 2001:
    case 2002:
    case 2003:
    case 3000:
    case 3001:
    case 3002:
    case 4000:
    case 5000:
    case 6000:
    case 7000:
        return static_cast<StatusCode>(value);
    default:
        return std::nullopt;
    }
}

[[nodiscard]] constexpr std::string_view
frame_validation_message(FrameValidationError error) noexcept
{
    switch (error) {
    case FrameValidationError::unsupported_version:
        return "unsupported protocol version";
    case FrameValidationError::unknown_frame_type:
        return "unknown frame type";
    case FrameValidationError::unknown_flags:
        return "unknown frame flags";
    case FrameValidationError::invalid_flag_combination:
        return "invalid frame flag combination";
    case FrameValidationError::invalid_stream:
        return "invalid stream identifier";
    case FrameValidationError::payload_length_mismatch:
        return "payload length mismatch";
    case FrameValidationError::payload_too_large:
        return "payload exceeds configured limit";
    case FrameValidationError::none:
        return "no frame validation error";
    }
    return "malformed frame";
}

} // namespace rillnet