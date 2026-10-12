#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cluster/cluster_manager.h"
#include "raft_storage.h"
#include "raft_types.h"

class RaftNode {
public:
    using ApplyCallback =
        std::function<
            void(const std::string&)
        >;

private:
    struct PeerReplicationResult {
        bool success = false;

        std::uint64_t responseTerm = 0;

        std::uint64_t matchIndex = 0;
    };

    std::string localNodeId;

    std::vector<PeerConfig> peers;

    ApplyCallback applyCallback;

    RaftStorage persistentStorage;

    mutable std::mutex stateMutex;

    std::mutex replicationMutex;

    std::atomic<bool> running{false};

    std::thread workerThread;

    RaftRole role =
        RaftRole::FOLLOWER;

    // ========================================================
    // Persistent Raft state
    // ========================================================

    std::uint64_t currentTerm = 0;

    std::string votedFor;

    std::vector<RaftLogEntry> log;

    std::uint64_t commitIndex = 0;

    // ========================================================
    // Volatile Raft state
    // ========================================================

    std::string leaderId;

    std::uint64_t lastApplied = 0;

    std::unordered_map<
        std::string,
        std::uint64_t
    > nextIndex;

    std::unordered_map<
        std::string,
        std::uint64_t
    > matchIndex;

    std::chrono::steady_clock::time_point
        electionDeadline;

    std::chrono::steady_clock::time_point
        nextHeartbeat;

    std::mt19937 randomGenerator;

    int heartbeatIntervalMs = 250;

    int electionMinMs = 1500;

    int electionMaxMs = 3000;

    void run();

    void startElection();

    void sendHeartbeats();

    void resetElectionDeadlineLocked();

    void initializeLeaderStateLocked();

    void stepDown(
        std::uint64_t newTerm,
        const std::string& newLeader = ""
    );

    std::size_t majorityCount() const;

    std::uint64_t lastLogIndexLocked() const;

    std::uint64_t lastLogTermLocked() const;

    bool persistMetadataLocked(
        const std::string& context
    );

    std::vector<std::string>
    collectCommittedCommandsLocked();

    void applyCommands(
        const std::vector<std::string>& commands
    );

    PeerReplicationResult replicatePeerToIndex(
        const PeerConfig& peer,
        std::uint64_t targetIndex,
        std::uint64_t leaderTerm
    );

    static std::string roleToString(
        RaftRole role
    );

public:
    RaftNode(
        const std::string& localNodeId,
        const std::vector<PeerConfig>& peers,
        ApplyCallback applyCallback
    );

    ~RaftNode();

    bool start();

    void stop();

    std::string handleRequest(
        const std::string& request
    );

    RaftProposalResult propose(
        const std::string& command
    );

    void printStatus() const;

    RaftRole getRole() const;

    std::uint64_t getCurrentTerm() const;

    std::uint64_t getCommitIndex() const;

    std::uint64_t getLastApplied() const;

    std::uint64_t getLastLogIndex() const;

    std::string getLeaderId() const;

    bool isLeader() const;
};