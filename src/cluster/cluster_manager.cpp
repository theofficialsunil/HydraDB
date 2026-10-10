#include "cluster_manager.h"
#include "network/tcp_client.h"
#include <chrono>
#include <iostream>

using namespace std;
namespace {
string healthToString(NodeHealth health) {
    switch (health) {
        case NodeHealth::UNKNOWN:
            return "UNKNOWN";

        case NodeHealth::HEALTHY:
            return "HEALTHY";

        case NodeHealth::SUSPECTED:
            return "SUSPECTED";

        case NodeHealth::FAILED:
            return "FAILED";

        case NodeHealth::RECOVERING:
            return "RECOVERING";
    }

    return "UNKNOWN";
}

bool isSuccessfulPing(
    const string& response,
    const string& expectedNodeId
) {
    string expected = "PONG " + expectedNodeId;

    return response == expected;
}

} // namespace

ClusterManager::ClusterManager(
    const string& localNodeId,
    const vector<PeerConfig>& peerConfigs
)
    : localNodeId(localNodeId) {

    for (const auto& config : peerConfigs) {
        PeerStatus status;

        status.config = config;

        peers.push_back(status);
    }
}

ClusterManager::~ClusterManager() {
    stop();
}

void ClusterManager::start() {
    if (running) {
        return;
    }

    running = true;

    heartbeatThread =
        thread(&ClusterManager::heartbeatLoop, this);
}

void ClusterManager::stop() {
    if (!running) {
        return;
    }

    running = false;

    if (heartbeatThread.joinable()) {
        heartbeatThread.join();
    }
}

void ClusterManager::heartbeatLoop() {
    while (running) {

        {
            lock_guard<mutex> lock(peersMutex);

            for (auto& peer : peers) {
                checkPeer(peer);
            }
        }

        int slept = 0;

        while (running &&
               slept < heartbeatIntervalMs) {

            constexpr int sleepStep = 100;

            this_thread::sleep_for(
                chrono::milliseconds(sleepStep)
            );

            slept += sleepStep;
        }
    }
}

void ClusterManager::checkPeer(
    PeerStatus& peer
) {
    string response = TcpClient::ping(
        peer.config.host,
        peer.config.port
    );

    peer.lastResponse = response;

    bool success = isSuccessfulPing(
        response,
        peer.config.nodeId
    );

    if (success) {

        peer.consecutiveFailures = 0;
        ++peer.consecutiveSuccesses;

        if (peer.health == NodeHealth::FAILED) {

            peer.health = NodeHealth::RECOVERING;
            peer.consecutiveSuccesses = 1;

            cout
                << "\n[cluster] "
                << peer.config.nodeId
                << " is RECOVERING\n";
        }
        else if (
            peer.health == NodeHealth::RECOVERING
        ) {

            if (peer.consecutiveSuccesses >= 2) {

                peer.health = NodeHealth::HEALTHY;

                cout
                    << "\n[cluster] "
                    << peer.config.nodeId
                    << " is HEALTHY\n";
            }
        }
        else if (
            peer.health != NodeHealth::HEALTHY
        ) {

            peer.health = NodeHealth::HEALTHY;

            cout
                << "\n[cluster] "
                << peer.config.nodeId
                << " is HEALTHY\n";
        }

        return;
    }

    peer.consecutiveSuccesses = 0;
    ++peer.consecutiveFailures;

    if (peer.consecutiveFailures >= failureThreshold) {

        if (peer.health != NodeHealth::FAILED) {

            peer.health = NodeHealth::FAILED;

            cout
                << "\n[cluster] "
                << peer.config.nodeId
                << " is FAILED\n";
        }
    }
    else {

        if (peer.health != NodeHealth::SUSPECTED) {

            peer.health = NodeHealth::SUSPECTED;

            cout
                << "\n[cluster] "
                << peer.config.nodeId
                << " is SUSPECTED\n";
        }
    }
}

void ClusterManager::printClusterStatus() const {
    lock_guard<mutex> lock(peersMutex);

    cout << "\nCluster Membership\n";
    cout << "-------------------------------------------------------------\n";

    cout << localNodeId
         << "    LOCAL"
         << "    HEALTHY\n";

    for (const auto& peer : peers) {

        cout
            << peer.config.nodeId
            << "    "
            << peer.config.host
            << ':'
            << peer.config.port
            << "    "
            << healthToString(peer.health)
            << '\n';
    }

    cout << "-------------------------------------------------------------\n\n";
}

size_t ClusterManager::peerCount() const {
    lock_guard<mutex> lock(peersMutex);

    return peers.size();
}