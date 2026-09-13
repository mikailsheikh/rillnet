#pragma once

#include <rillnet/identifiers.hpp>
#include <rillnet/status_code.hpp>

#include <cstdint>
#include <limits>
#include <optional>
#include <set>

namespace rillnet {

enum class StreamInitiator : std::uint8_t {
    client,
    server,
};

class StreamIdAllocation {
  public:
    [[nodiscard]] static StreamIdAllocation success(StreamId stream) noexcept
    {
        return StreamIdAllocation(stream, StatusCode::ok);
    }

    [[nodiscard]] static StreamIdAllocation exhausted() noexcept
    {
        return StreamIdAllocation(std::nullopt, StatusCode::resource_limit_exceeded);
    }

    [[nodiscard]] bool ok() const noexcept { return stream_.has_value(); }
    [[nodiscard]] const std::optional<StreamId> &stream() const noexcept { return stream_; }
    [[nodiscard]] StatusCode status() const noexcept { return status_; }

  private:
    explicit StreamIdAllocation(std::optional<StreamId> stream, StatusCode status) noexcept
        : stream_(stream), status_(status)
    {
    }

    std::optional<StreamId> stream_;
    StatusCode status_;
};

class StreamIdAllocator {
  public:
    explicit StreamIdAllocator(
        StreamInitiator initiator,
        std::uint64_t maximum = std::numeric_limits<std::uint64_t>::max()) noexcept
        : initiator_(initiator), next_(first_id(initiator)), maximum_(last_id(initiator, maximum))
    {
    }

    [[nodiscard]] StreamInitiator initiator() const noexcept { return initiator_; }
    [[nodiscard]] StreamId maximum() const noexcept { return StreamId{maximum_}; }
    [[nodiscard]] bool exhausted() const noexcept { return next_ == 0; }

    [[nodiscard]] StreamIdAllocation allocate() noexcept
    {
        if (!available_.empty()) {
            const auto value = *available_.begin();
            available_.erase(available_.begin());
            in_use_.insert(value);
            return StreamIdAllocation::success(StreamId{value});
        }

        if (next_ == 0 || next_ > maximum_) {
            next_ = 0;
            return StreamIdAllocation::exhausted();
        }

        const StreamId stream{next_};
        in_use_.insert(stream.value());
        if (maximum_ - next_ < 2) {
            next_ = 0;
        } else {
            next_ += 2;
        }
        return StreamIdAllocation::success(stream);
    }

    void release(StreamId stream) noexcept
    {
        const auto value = stream.value();
        if (value == 0 || value > maximum_ || value % 2 != first_id(initiator_) % 2) {
            return;
        }
        if (in_use_.erase(value) != 0) {
            available_.insert(value);
        }
    }

  private:
    [[nodiscard]] static constexpr std::uint64_t first_id(StreamInitiator initiator) noexcept
    {
        return initiator == StreamInitiator::client ? 1 : 2;
    }

    [[nodiscard]] static constexpr std::uint64_t last_id(StreamInitiator initiator,
                                                         std::uint64_t maximum) noexcept
    {
        if (initiator == StreamInitiator::client) {
            return maximum % 2 == 1 ? maximum : maximum == 0 ? 0 : maximum - 1;
        }
        return maximum % 2 == 0 ? maximum : maximum - 1;
    }

    StreamInitiator initiator_;
    std::uint64_t next_;
    std::uint64_t maximum_;
    std::set<std::uint64_t> in_use_;
    std::set<std::uint64_t> available_;
};

} // namespace rillnet