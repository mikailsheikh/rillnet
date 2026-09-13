#include <rillnet/protocol_violation.hpp>

#include <rillnet/frame.hpp>
#include <rillnet/frame_codec.hpp>
#include <rillnet/frame_decoder.hpp>
#include <rillnet/frame_flags.hpp>
#include <rillnet/identifiers.hpp>
#include <rillnet/status_code.hpp>

#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>
#include <vector>

namespace {

using rillnet::decode_error_status;
using rillnet::encode_frame;
using rillnet::Frame;
using rillnet::frame_validation_message;
using rillnet::FrameDecoder;
using rillnet::FrameFlags;
using rillnet::FrameType;
using rillnet::FrameValidationError;
using rillnet::has_flag;
using rillnet::make_error_frame;
using rillnet::status_for_frame_validation;
using rillnet::StatusCode;
using rillnet::StreamId;
using rillnet::validate_frame;

TEST(ProtocolViolationTest, ErrorFrameIsAValidTerminalResponseFrame)
{
    const auto frame = make_error_frame(StreamId{3}, StatusCode::decode_error);

    EXPECT_EQ(validate_frame(frame), FrameValidationError::none);
    EXPECT_EQ(frame.header.type, FrameType::response);
    EXPECT_TRUE(has_flag(frame.header.flags, FrameFlags::error));
    EXPECT_TRUE(has_flag(frame.header.flags, FrameFlags::end_of_stream));
    EXPECT_FALSE(has_flag(frame.header.flags, FrameFlags::cancel));
    EXPECT_EQ(frame.header.stream, StreamId{3});
    EXPECT_EQ(frame.header.payload_size, 2U);
    EXPECT_EQ(frame.payload.size(), 2U);
}

TEST(ProtocolViolationTest, ErrorFrameSurvivesAWireRoundTrip)
{
    const auto frame = make_error_frame(StreamId{5}, StatusCode::unknown_message_type);

    FrameDecoder decoder;
    const auto decoded = decoder.push(encode_frame(frame));

    ASSERT_EQ(decoded.size(), 1U);
    EXPECT_EQ(decoded[0].header.stream, StreamId{5});
    EXPECT_EQ(decoded[0].header.flags, frame.header.flags);
    EXPECT_EQ(decode_error_status(decoded[0]), StatusCode::unknown_message_type);
}

TEST(ProtocolViolationTest, EncodesTheStatusAsABigEndianPayload)
{
    const auto frame = make_error_frame(StreamId{1}, StatusCode::resource_limit_exceeded);

    ASSERT_EQ(frame.payload.size(), 2U);
    EXPECT_EQ(frame.payload[0], std::byte{0x1B}); // 7000 == 0x1B58
    EXPECT_EQ(frame.payload[1], std::byte{0x58});
}

TEST(ProtocolViolationTest, RoundTripsEveryStreamLevelStatusItAccepts)
{
    constexpr std::array statuses{
        StatusCode::protocol_error,         StatusCode::unsupported_version,
        StatusCode::malformed_frame,        StatusCode::unknown_stream,
        StatusCode::serialization_error,    StatusCode::unknown_message_type,
        StatusCode::decode_error,           StatusCode::operation_error,
        StatusCode::timeout_error,          StatusCode::cancelled,
        StatusCode::resource_limit_exceeded};

    for (const auto status : statuses) {
        EXPECT_EQ(decode_error_status(make_error_frame(StreamId{1}, status)), status)
            << "status " << rillnet::to_string(status);
    }
}

TEST(ProtocolViolationTest, RejectsPayloadsThatAreNotExactlyTwoBytes)
{
    for (const std::size_t size : {std::size_t{0}, std::size_t{1}, std::size_t{3}}) {
        Frame frame;
        frame.payload.resize(size);
        frame.header.payload_size = static_cast<std::uint32_t>(size);
        EXPECT_FALSE(decode_error_status(frame).has_value()) << "payload size " << size;
    }
}

TEST(ProtocolViolationTest, RejectsStatusValuesThatAreNotWireVisibleCodes)
{
    Frame frame;
    frame.payload = {std::byte{0x00}, std::byte{0x63}}; // 99, not a defined StatusCode
    frame.header.payload_size = 2;

    EXPECT_FALSE(decode_error_status(frame).has_value());
    EXPECT_FALSE(decode_error_status(make_error_frame(StreamId{1}, StatusCode::ok)).has_value());
}

TEST(ProtocolViolationTest, DoesNotDecodeTheDrainStatusTheServerActuallySends)
{
    const auto frame = make_error_frame(StreamId{1}, StatusCode::connection_closed);

    EXPECT_EQ(validate_frame(frame), FrameValidationError::none);
    EXPECT_FALSE(decode_error_status(frame).has_value());
}

TEST(ProtocolViolationTest, DecodesEveryStatusAPeerIsAbleToEncode)
{
    constexpr std::array statuses{StatusCode::transport_error, StatusCode::connection_closed,
                                  StatusCode::connection_reset, StatusCode::connection_timed_out};

    for (const auto status : statuses) {
        EXPECT_EQ(decode_error_status(make_error_frame(StreamId{1}, status)), status)
            << "status " << rillnet::to_string(status);
    }
}

TEST(ProtocolViolationTest, MapsFrameValidationErrorsOntoWireStatusCodes)
{
    EXPECT_EQ(status_for_frame_validation(FrameValidationError::unsupported_version),
              StatusCode::unsupported_version);

    constexpr std::array malformed{
        FrameValidationError::unknown_frame_type,       FrameValidationError::unknown_flags,
        FrameValidationError::invalid_flag_combination, FrameValidationError::invalid_stream,
        FrameValidationError::payload_length_mismatch,  FrameValidationError::payload_too_large};
    for (const auto error : malformed) {
        EXPECT_EQ(status_for_frame_validation(error), StatusCode::malformed_frame);
    }
}

// FrameValidationError::none never reaches status_for_frame_validation through the connections,
// which only call it once the decoder has reported an error, but the mapping still has to return
// something: it reports a malformed frame rather than ok.
TEST(ProtocolViolationTest, MapsTheAbsenceOfAValidationErrorOntoMalformedFrame)
{
    EXPECT_EQ(status_for_frame_validation(FrameValidationError::none), StatusCode::malformed_frame);
    EXPECT_EQ(frame_validation_message(FrameValidationError::none), "no frame validation error");
}

TEST(ProtocolViolationTest, DescribesEveryFrameValidationErrorDistinctly)
{
    constexpr std::array errors{FrameValidationError::none,
                                FrameValidationError::unsupported_version,
                                FrameValidationError::unknown_frame_type,
                                FrameValidationError::unknown_flags,
                                FrameValidationError::invalid_flag_combination,
                                FrameValidationError::invalid_stream,
                                FrameValidationError::payload_length_mismatch,
                                FrameValidationError::payload_too_large};

    std::vector<std::string_view> messages;
    for (const auto error : errors) {
        const auto message = frame_validation_message(error);
        EXPECT_FALSE(message.empty());
        messages.push_back(message);
    }

    for (std::size_t outer = 0; outer < messages.size(); ++outer) {
        for (std::size_t inner = outer + 1; inner < messages.size(); ++inner) {
            EXPECT_NE(messages[outer], messages[inner]);
        }
    }
}

} // namespace
