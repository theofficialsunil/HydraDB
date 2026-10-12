#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

enum class RaftRole {
    FOLLOWER,
    CANDIDATE,
    LEADER
};

struct RaftLogEntry {
    std::uint64_t index = 0;
    std::uint64_t term = 0;

    std::string command;
};

struct RaftProposalResult {
    bool committed = false;

    std::uint64_t index = 0;
    std::uint64_t term = 0;

    std::size_t acknowledgements = 0;
    std::size_t majority = 0;

    std::string message;
};