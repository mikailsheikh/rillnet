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

TEST(DiagnosticsTest, EmptyHooksAreSafeToNotify)
{
    const rillnet::DiagnosticsHooks hooks;

    rillnet::notify_diagnostics(hooks, {rillnet::DiagnosticsEventType::connection_opened});
    rillnet::notify_diagnostics(hooks, {rillnet::DiagnosticsEventType::connection_closed,
                                        rillnet::StreamId{1},
                                        rillnet::StatusCode::connection_closed, 7, true, "closed"});
}

TEST(DiagnosticsTest, ReportsTheTerminalResultExactlyOncePerOperation)
{
    std::vector<rillnet::StatusCode> reported;
    rillnet::Operation operation(rillnet::StreamId{1},
                                 [&reported](const rillnet::OperationResult &result) {
                                     reported.push_back(result.status());
                                 });

    ASSERT_EQ(operation.activate(), rillnet::LifecycleTransitionError::none);
    EXPECT_TRUE(operation.cancel("peer cancelled"));
    EXPECT_FALSE(operation.complete());
    EXPECT_FALSE(operation.timeout());
    EXPECT_FALSE(operation.fail(rillnet::StatusCode::connection_closed));

    ASSERT_EQ(reported.size(), 1U);
    EXPECT_EQ(reported[0], rillnet::StatusCode::cancelled);
}

TEST(DiagnosticsTest, ReportsATimeoutWithTheTimeoutStatus)
{
    std::vector<rillnet::StatusCode> reported;
    rillnet::Operation operation(rillnet::StreamId{1},
                                 [&reported](const rillnet::OperationResult &result) {
                                     reported.push_back(result.status());
                                 });

    ASSERT_EQ(operation.activate(), rillnet::LifecycleTransitionError::none);
    EXPECT_TRUE(operation.timeout("deadline exceeded"));

    ASSERT_EQ(reported.size(), 1U);
    EXPECT_EQ(reported[0], rillnet::StatusCode::timeout_error);
    EXPECT_TRUE(operation.timed_out());
}

// set_completion_handler() only affects results committed afterwards: an operation that has
// already reached a terminal state never replays it to a newly installed handler.
TEST(DiagnosticsTest, DoesNotReplayATerminalResultToAHandlerInstalledAfterwards)
{
    bool first_handler_called = false;
    bool late_handler_called = false;
    rillnet::Operation operation(
        rillnet::StreamId{1},
        [&first_handler_called](const rillnet::OperationResult &) { first_handler_called = true; });

    ASSERT_EQ(operation.activate(), rillnet::LifecycleTransitionError::none);
    EXPECT_TRUE(operation.complete("done"));
    operation.set_completion_handler(
        [&late_handler_called](const rillnet::OperationResult &) { late_handler_called = true; });

    EXPECT_TRUE(first_handler_called);
    EXPECT_FALSE(late_handler_called);
}

TEST(DiagnosticsTest, ReplacingTheHandlerBeforeCompletionNotifiesOnlyTheNewHandler)
{
    bool original_called = false;
    bool replacement_called = false;
    rillnet::Operation operation(
        rillnet::StreamId{1},
        [&original_called](const rillnet::OperationResult &) { original_called = true; });

    ASSERT_EQ(operation.activate(), rillnet::LifecycleTransitionError::none);
    operation.set_completion_handler(
        [&replacement_called](const rillnet::OperationResult &) { replacement_called = true; });
    EXPECT_TRUE(operation.fail(rillnet::StatusCode::operation_error, "handler failed"));

    EXPECT_FALSE(original_called);
    EXPECT_TRUE(replacement_called);
}

} // namespace
