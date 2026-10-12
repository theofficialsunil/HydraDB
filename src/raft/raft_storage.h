#pragma once

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "raft_types.h"

class RaftStorage {
private:
    std::filesystem::path directoryPath;

    std::filesystem::path metadataPath;

    std::filesystem::path walPath;

public:
    explicit RaftStorage(
        const std::filesystem::path& directory
    );

    bool initialize(
        std::string& error
    );

    bool load(
        std::uint64_t& currentTerm,
        std::string& votedFor,
        std::uint64_t& commitIndex,
        std::vector<RaftLogEntry>& log,
        std::string& error
    );

    bool persistMetadata(
        std::uint64_t currentTerm,
        const std::string& votedFor,
        std::uint64_t commitIndex,
        std::string& error
    );

    bool appendEntry(
        const RaftLogEntry& entry,
        std::string& error
    );

    bool rewriteLog(
        const std::vector<RaftLogEntry>& log,
        std::string& error
    );

    std::string getDirectoryPath() const;
};