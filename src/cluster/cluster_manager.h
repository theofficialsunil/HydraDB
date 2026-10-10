#pragma once
#include <atomic>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

struct PeerConfig {
    std::string nodeId;
    std::string host;
    int port;
};

enum class NodeHealth {
    UNKNOWN,
    HEALTHY,
    SUSPECTED,
    FAILED,
    RECOVERING
};

struct PeerStatus {
    PeerConfig config;

    NodeHealth health = NodeHealth::UNKNOWN;

    int consecutiveFailures = 0;
    int consecutiveSuccesses = 0;

    std::string lastResponse;
};

class ClusterManager {
private:
    std::string localNodeId;

    std::vector<PeerStatus> peers;

    mutable std::mutex peersMutex;

    std::atomic<bool> running{false};

    std::thread heartbeatThread;

    int heartbeatIntervalMs = 2000;
    int failureThreshold = 3;

    void heartbeatLoop();

    void checkPeer(PeerStatus& peer);

public:
    ClusterManager(
        const std::string& localNodeId,
        const std::vector<PeerConfig>& peerConfigs
    );

    ~ClusterManager();

    void start();

    void stop();

    void printClusterStatus() const;

    std::size_t peerCount() const;
};