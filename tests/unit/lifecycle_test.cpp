#include <rillnet/lifecycle.hpp>

#include <gtest/gtest.h>

#include <array>

namespace {

using rillnet::ConnectionState;
using rillnet::LifecycleTransitionError;
using rillnet::OperationState;

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
void advance_to(rillnet::ConnectionLifecycle &lifecycle, ConnectionState state)
{
    switch (state) {
    case ConnectionState::created:
        return;
    case ConnectionState::connecting:
        ASSERT_EQ(lifecycle.transition(ConnectionState::connecting),
                  LifecycleTransitionError::none);
        return;
    case ConnectionState::accepting:
        ASSERT_EQ(lifecycle.transition(ConnectionState::accepting), LifecycleTransitionError::none);
        return;
    case ConnectionState::active:
        ASSERT_EQ(lifecycle.transition(ConnectionState::connecting),
                  LifecycleTransitionError::none);
        ASSERT_EQ(lifecycle.transition(ConnectionState::active), LifecycleTransitionError::none);
        return;
    case ConnectionState::draining:
        ASSERT_EQ(lifecycle.transition(ConnectionState::connecting),
                  LifecycleTransitionError::none);
        ASSERT_EQ(lifecycle.transition(ConnectionState::active), LifecycleTransitionError::none);
        ASSERT_EQ(lifecycle.transition(ConnectionState::draining), LifecycleTransitionError::none);
        return;
    case ConnectionState::closing:
        ASSERT_EQ(lifecycle.transition(ConnectionState::connecting),
                  LifecycleTransitionError::none);
        ASSERT_EQ(lifecycle.transition(ConnectionState::active), LifecycleTransitionError::none);
        ASSERT_EQ(lifecycle.transition(ConnectionState::closing), LifecycleTransitionError::none);
        return;
    case ConnectionState::closed:
        ASSERT_EQ(lifecycle.transition(ConnectionState::connecting),
                  LifecycleTransitionError::none);
        ASSERT_EQ(lifecycle.transition(ConnectionState::active), LifecycleTransitionError::none);
        ASSERT_EQ(lifecycle.transition(ConnectionState::closed), LifecycleTransitionError::none);
        return;
    }
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(ConnectionLifecycleTest, AcceptsOnlySpecifiedTransitions)
{
    constexpr std::array states{ConnectionState::created,   ConnectionState::connecting,
                                ConnectionState::accepting, ConnectionState::active,
                                ConnectionState::draining,  ConnectionState::closing,
                                ConnectionState::closed};

    for (const auto from : states) {
        for (const auto to : states) {
            rillnet::ConnectionLifecycle lifecycle;
            advance_to(lifecycle, from);

            const auto result = lifecycle.transition(to);
            if (rillnet::can_transition(from, to)) {
                EXPECT_EQ(result, LifecycleTransitionError::none);
                EXPECT_EQ(lifecycle.state(), to);
            } else {
                EXPECT_EQ(result, LifecycleTransitionError::invalid_transition);
                EXPECT_EQ(lifecycle.state(), from);
            }
        }
    }
}

void advance_to(rillnet::OperationLifecycle &lifecycle, OperationState state)
{
    switch (state) {
    case OperationState::created:
        return;
    case OperationState::active:
        ASSERT_EQ(lifecycle.transition(OperationState::active), LifecycleTransitionError::none);
        return;
    case OperationState::completed:
    case OperationState::failed:
    case OperationState::cancelled:
        ASSERT_EQ(lifecycle.transition(OperationState::active), LifecycleTransitionError::none);
        ASSERT_EQ(lifecycle.transition(state), LifecycleTransitionError::none);
        return;
    }
}

TEST(ConnectionLifecycleTest, SupportsClientAndServerPaths)
{
    rillnet::ConnectionLifecycle client;
    EXPECT_EQ(client.transition(ConnectionState::connecting), LifecycleTransitionError::none);
    EXPECT_EQ(client.transition(ConnectionState::active), LifecycleTransitionError::none);
    EXPECT_EQ(client.transition(ConnectionState::closing), LifecycleTransitionError::none);
    EXPECT_EQ(client.transition(ConnectionState::closed), LifecycleTransitionError::none);

    rillnet::ConnectionLifecycle server;
    EXPECT_EQ(server.transition(ConnectionState::accepting), LifecycleTransitionError::none);
    EXPECT_EQ(server.transition(ConnectionState::active), LifecycleTransitionError::none);
    EXPECT_EQ(server.transition(ConnectionState::closed), LifecycleTransitionError::none);
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(OperationLifecycleTest, AcceptsOnlySpecifiedTransitions)
{
    constexpr std::array states{OperationState::created, OperationState::active,
                                OperationState::completed, OperationState::failed,
                                OperationState::cancelled};

    for (const auto from : states) {
        for (const auto to : states) {
            rillnet::OperationLifecycle lifecycle;
            advance_to(lifecycle, from);

            const auto result = lifecycle.transition(to);
            if (rillnet::can_transition(from, to)) {
                EXPECT_EQ(result, LifecycleTransitionError::none);
                EXPECT_EQ(lifecycle.state(), to);
            } else {
                EXPECT_EQ(result, LifecycleTransitionError::invalid_transition);
                EXPECT_EQ(lifecycle.state(), from);
            }
        }
    }
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
TEST(OperationLifecycleTest, HasExactlyOneTerminalTransition)
{
    constexpr std::array terminal_states{OperationState::completed, OperationState::failed,
                                         OperationState::cancelled};

    for (const auto terminal : terminal_states) {
        rillnet::OperationLifecycle lifecycle;
        ASSERT_EQ(lifecycle.transition(OperationState::active), LifecycleTransitionError::none);
        EXPECT_EQ(lifecycle.transition(terminal), LifecycleTransitionError::none);
        EXPECT_EQ(lifecycle.state(), terminal);
        EXPECT_EQ(lifecycle.transition(OperationState::completed),
                  LifecycleTransitionError::invalid_transition);
        EXPECT_EQ(lifecycle.state(), terminal);
    }
}

} // namespace