#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "cluster/cluster_manager.h"

enum class RaftRole {
    FOLLOWER,
    CANDIDATE,
    LEADER
};

class RaftNode {
private:
    std::string localNodeId;

    std::vector<PeerConfig> peers;

    mutable std::mutex stateMutex;

    std::atomic<bool> running{false};

    std::thread workerThread;

    RaftRole role = RaftRole::FOLLOWER;

    std::uint64_t currentTerm = 0;

    std::string votedFor;

    std::string leaderId;

    std::chrono::steady_clock::time_point electionDeadline;

    std::chrono::steady_clock::time_point nextHeartbeat;

    std::mt19937 randomGenerator;

    // Leader sends heartbeat frequently.
    int heartbeatIntervalMs = 250;

    // Election timeout must be comfortably larger
    // than heartbeat interval + network delay.
    int electionMinMs = 1500;
    int electionMaxMs = 3000;

    void run();

    void startElection();

    void sendHeartbeats();

    void resetElectionDeadlineLocked();

    void stepDown(
        std::uint64_t newTerm,
        const std::string& newLeader = ""
    );

    std::size_t majorityCount() const;

    static std::string roleToString(
        RaftRole role
    );

public:
    RaftNode(
        const std::string& localNodeId,
        const std::vector<PeerConfig>& peers
    );

    ~RaftNode();

    void start();

    void stop();

    std::string handleRequest(
        const std::string& request
    );

    void printStatus() const;

    RaftRole getRole() const;

    std::uint64_t getCurrentTerm() const;

    std::string getLeaderId() const;

    bool isLeader() const;
};