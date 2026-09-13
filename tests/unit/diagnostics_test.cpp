#include <rillnet/diagnostics.hpp>
#include <rillnet/operation.hpp>

#include <gtest/gtest.h>

#include <vector>

namespace {

TEST(DiagnosticsTest, DeliversApplicationEventsWithoutChangingProtocolBehavior)
{
    std::vector<rillnet::DiagnosticsEvent> events;
    rillnet::DiagnosticsHooks hooks{
        [&events](const rillnet::DiagnosticsEvent &event) { events.push_back(event); }};

    rillnet::notify_diagnostics(
        hooks, {rillnet::DiagnosticsEventType::request_started, rillnet::StreamId{7}});
    rillnet::notify_diagnostics(hooks, {rillnet::DiagnosticsEventType::bytes_transferred,
                                        {},
                                        rillnet::StatusCode::ok,
                                        42,
                                        true,
                                        {}});

    ASSERT_EQ(events.size(), 2U);
    EXPECT_EQ(events[0].type, rillnet::DiagnosticsEventType::request_started);
    EXPECT_EQ(events[0].stream, rillnet::StreamId{7});
    EXPECT_EQ(events[1].type, rillnet::DiagnosticsEventType::bytes_transferred);
    EXPECT_EQ(events[1].bytes, 42U);
    EXPECT_TRUE(events[1].inbound);
}

TEST(DiagnosticsTest, ObserverExceptionsAreIgnored)
{
    bool completed = false;
    rillnet::DiagnosticsHooks hooks{
        [](const rillnet::DiagnosticsEvent &) { throw std::runtime_error("observer failure"); }};

    rillnet::notify_diagnostics(hooks, {rillnet::DiagnosticsEventType::connection_opened});
    rillnet::Operation operation(rillnet::StreamId{1},
                                 [&completed](const auto &) { completed = true; });
    ASSERT_EQ(operation.activate(), rillnet::LifecycleTransitionError::none);
    EXPECT_TRUE(operation.complete());
    EXPECT_TRUE(completed);
}

} // namespace
