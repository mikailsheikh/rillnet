#include <rillnet/stream_id_allocator.hpp>

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>

namespace {

using rillnet::StatusCode;
using rillnet::StreamId;
using rillnet::StreamIdAllocator;
using rillnet::StreamInitiator;

TEST(StreamIdAllocatorTest, ClientAllocatesMonotonicallyIncreasingOddIdentifiers)
{
    StreamIdAllocator allocator(StreamInitiator::client, 5);

    const auto first = allocator.allocate();

    ASSERT_TRUE(first.stream().has_value());
    EXPECT_EQ(*first.stream(), StreamId{1});
    EXPECT_EQ(*allocator.allocate().stream(), StreamId{3});
    EXPECT_EQ(*allocator.allocate().stream(), StreamId{5});

    const auto exhausted = allocator.allocate();
    EXPECT_FALSE(exhausted.ok());
    EXPECT_EQ(exhausted.status(), StatusCode::resource_limit_exceeded);
    EXPECT_TRUE(allocator.exhausted());
}

TEST(StreamIdAllocatorTest, ServerAllocatesMonotonicallyIncreasingEvenIdentifiers)
{
    StreamIdAllocator allocator(StreamInitiator::server, 5);

    const auto first = allocator.allocate();
    const auto second = allocator.allocate();

    ASSERT_TRUE(first.stream().has_value());
    ASSERT_TRUE(second.stream().has_value());
    EXPECT_EQ(*first.stream(), StreamId{2});
    EXPECT_EQ(*second.stream(), StreamId{4});
    EXPECT_FALSE(allocator.allocate().ok());
    EXPECT_TRUE(allocator.exhausted());
}

TEST(StreamIdAllocatorTest, ExhaustionDoesNotWrapOrReuseIdentifiers)
{
    StreamIdAllocator allocator(StreamInitiator::client, 1);

    const auto first = allocator.allocate();
    const auto exhausted = allocator.allocate();
    const auto still_exhausted = allocator.allocate();

    ASSERT_TRUE(first.stream().has_value());
    EXPECT_EQ(*first.stream(), StreamId{1});
    EXPECT_FALSE(exhausted.stream().has_value());
    EXPECT_FALSE(still_exhausted.stream().has_value());
    EXPECT_EQ(allocator.maximum(), StreamId{1});
}

TEST(StreamIdAllocatorTest, ReleasesIdentifiersForReuseExactlyOnce)
{
    StreamIdAllocator allocator(StreamInitiator::client, 1);

    const auto allocation = allocator.allocate();
    ASSERT_TRUE(allocation.ok());
    allocator.release(*allocation.stream());
    allocator.release(*allocation.stream());

    const auto reused = allocator.allocate();
    ASSERT_TRUE(reused.ok());
    EXPECT_EQ(*reused.stream(), StreamId{1});
    EXPECT_FALSE(allocator.allocate().ok());
}

TEST(StreamIdAllocatorTest, HonorsTheFullRangeWithoutOverflowing)
{
    StreamIdAllocator client(StreamInitiator::client);
    StreamIdAllocator server(StreamInitiator::server);

    EXPECT_EQ(client.maximum(), StreamId{std::numeric_limits<std::uint64_t>::max()});
    EXPECT_EQ(server.maximum(), StreamId{std::numeric_limits<std::uint64_t>::max() - 1});
}

TEST(StreamIdAllocatorTest, ReportsExhaustionWhenTheCeilingExcludesItsParity)
{
    StreamIdAllocator client(StreamInitiator::client, 0);
    StreamIdAllocator server(StreamInitiator::server, 1);

    EXPECT_FALSE(client.allocate().ok());
    EXPECT_FALSE(server.allocate().ok());
    EXPECT_TRUE(client.exhausted());
    EXPECT_TRUE(server.exhausted());
}

} // namespace