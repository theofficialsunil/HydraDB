#include "raft_node.h"

#include "network/tcp_client.h"

#include <chrono>
#include <future>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using namespace std;

// ============================================================
// Constructor / Destructor
// ============================================================

RaftNode::RaftNode(
    const string& localNodeId,
    const vector<PeerConfig>& peers
)
    : localNodeId(localNodeId),
      peers(peers),
      randomGenerator(
          random_device{}()
      ) {
}

RaftNode::~RaftNode() {
    stop();
}

// ============================================================
// Helpers
// ============================================================

string RaftNode::roleToString(
    RaftRole role
) {
    switch (role) {
        case RaftRole::FOLLOWER:
            return "FOLLOWER";

        case RaftRole::CANDIDATE:
            return "CANDIDATE";

        case RaftRole::LEADER:
            return "LEADER";
    }

    return "UNKNOWN";
}

size_t RaftNode::majorityCount() const {
    size_t clusterSize =
        peers.size() + 1;

    return
        clusterSize / 2 + 1;
}

void RaftNode::resetElectionDeadlineLocked() {
    uniform_int_distribution<int>
        distribution(
            electionMinMs,
            electionMaxMs
        );

    int timeoutMs =
        distribution(
            randomGenerator
        );

    electionDeadline =
        chrono::steady_clock::now() +
        chrono::milliseconds(
            timeoutMs
        );
}

// ============================================================
// Lifecycle
// ============================================================

void RaftNode::start() {
    if (running) {
        return;
    }

    {
        lock_guard<mutex> lock(
            stateMutex
        );

        role =
            RaftRole::FOLLOWER;

        leaderId.clear();

        votedFor.clear();

        resetElectionDeadlineLocked();

        nextHeartbeat =
            chrono::steady_clock::now();
    }

    running = true;

    workerThread =
        thread(
            &RaftNode::run,
            this
        );
}

void RaftNode::stop() {
    if (!running) {
        return;
    }

    running = false;

    if (
        workerThread.joinable()
    ) {
        workerThread.join();
    }
}

// ============================================================
// Step Down
// ============================================================

void RaftNode::stepDown(
    uint64_t newTerm,
    const string& newLeader
) {
    lock_guard<mutex> lock(
        stateMutex
    );

    if (
        newTerm >
        currentTerm
    ) {
        currentTerm =
            newTerm;

        votedFor.clear();
    }

    role =
        RaftRole::FOLLOWER;

    leaderId =
        newLeader;

    resetElectionDeadlineLocked();
}

// ============================================================
// Main Raft Loop
// ============================================================

void RaftNode::run() {
    while (running) {
        bool shouldStartElection =
            false;

        bool shouldSendHeartbeat =
            false;

        {
            lock_guard<mutex> lock(
                stateMutex
            );

            auto now =
                chrono::steady_clock::now();

            if (
                role ==
                RaftRole::LEADER
            ) {
                if (
                    now >=
                    nextHeartbeat
                ) {
                    shouldSendHeartbeat =
                        true;

                    nextHeartbeat =
                        now +
                        chrono::milliseconds(
                            heartbeatIntervalMs
                        );
                }
            }
            else {
                if (
                    now >=
                    electionDeadline
                ) {
                    shouldStartElection =
                        true;
                }
            }
        }

        if (
            shouldStartElection
        ) {
            startElection();
        }

        if (
            shouldSendHeartbeat
        ) {
            sendHeartbeats();
        }

        this_thread::sleep_for(
            chrono::milliseconds(25)
        );
    }
}

// ============================================================
// Election
// ============================================================

void RaftNode::startElection() {
    uint64_t electionTerm;

    {
        lock_guard<mutex> lock(
            stateMutex
        );

        if (
            role ==
            RaftRole::LEADER
        ) {
            return;
        }

        role =
            RaftRole::CANDIDATE;

        ++currentTerm;

        electionTerm =
            currentTerm;

        votedFor =
            localNodeId;

        leaderId.clear();

        resetElectionDeadlineLocked();
    }

    cout
        << "\n[raft] "
        << localNodeId
        << " became CANDIDATE"
        << " term="
        << electionTerm
        << '\n';

    // We always vote for ourselves.
    size_t votes = 1;

    size_t majority =
        majorityCount();

    // Single-node configuration.
    if (
        votes >=
        majority
    ) {
        {
            lock_guard<mutex> lock(
                stateMutex
            );

            if (
                role ==
                    RaftRole::CANDIDATE &&
                currentTerm ==
                    electionTerm
            ) {
                role =
                    RaftRole::LEADER;

                leaderId =
                    localNodeId;

                nextHeartbeat =
                    chrono::steady_clock::now();
            }
        }

        return;
    }

    struct VoteResult {
        string peerId;
        string response;
    };

    vector<future<VoteResult>>
        futures;

    // --------------------------------------------------------
    // Send RequestVote RPCs concurrently.
    //
    // One dead node must not delay communication with
    // healthy nodes.
    // --------------------------------------------------------

    for (
        const auto& peer :
        peers
    ) {
        futures.push_back(
            async(
                launch::async,

                [
                    this,
                    peer,
                    electionTerm
                ]() {
                    string request =
                        "RAFT_REQUEST_VOTE " +
                        to_string(
                            electionTerm
                        ) +
                        " " +
                        localNodeId;

                    string response =
                        TcpClient::request(
                            peer.host,
                            peer.port,
                            request
                        );

                    return VoteResult{
                        peer.nodeId,
                        response
                    };
                }
            )
        );
    }

    // --------------------------------------------------------
    // Process responses
    // --------------------------------------------------------

    for (
        auto& future :
        futures
    ) {
        if (!running) {
            return;
        }

        VoteResult result =
            future.get();

        istringstream input(
            result.response
        );

        string command;

        uint64_t responseTerm =
            0;

        input
            >> command
            >> responseTerm;

        // Another node has a newer term.
        if (
            responseTerm >
            electionTerm
        ) {
            stepDown(
                responseTerm
            );

            return;
        }

        if (
            command ==
                "RAFT_VOTE_GRANTED" &&
            responseTerm ==
                electionTerm
        ) {
            ++votes;
        }
    }

    bool becameLeader =
        false;

    {
        lock_guard<mutex> lock(
            stateMutex
        );

        // We may have received a heartbeat while
        // RequestVote RPCs were running.
        //
        // In that case role would no longer be CANDIDATE.
        if (
            role ==
                RaftRole::CANDIDATE &&
            currentTerm ==
                electionTerm &&
            votes >=
                majority
        ) {
            role =
                RaftRole::LEADER;

            leaderId =
                localNodeId;

            nextHeartbeat =
                chrono::steady_clock::now();

            becameLeader =
                true;
        }
    }

    if (
        becameLeader
    ) {
        cout
            << "\n[raft] "
            << localNodeId
            << " became LEADER"
            << " term="
            << electionTerm
            << " votes="
            << votes
            << '\n';

        // Send first heartbeat immediately.
        sendHeartbeats();
    }
}

// ============================================================
// Leader Heartbeats
// ============================================================

void RaftNode::sendHeartbeats() {
    uint64_t leaderTerm;

    {
        lock_guard<mutex> lock(
            stateMutex
        );

        if (
            role !=
            RaftRole::LEADER
        ) {
            return;
        }

        leaderTerm =
            currentTerm;
    }

    struct HeartbeatResult {
        string peerId;
        string response;
    };

    vector<future<HeartbeatResult>>
        futures;

    // --------------------------------------------------------
    // Send heartbeats concurrently.
    //
    // Previously:
    //
    // leader -> dead node (wait)
    //        -> healthy node
    //
    // That delay could allow healthy followers to time out.
    //
    // Now all peers are contacted independently.
    // --------------------------------------------------------

    for (
        const auto& peer :
        peers
    ) {
        futures.push_back(
            async(
                launch::async,

                [
                    this,
                    peer,
                    leaderTerm
                ]() {
                    string request =
                        "RAFT_HEARTBEAT " +
                        to_string(
                            leaderTerm
                        ) +
                        " " +
                        localNodeId;

                    string response =
                        TcpClient::request(
                            peer.host,
                            peer.port,
                            request
                        );

                    return HeartbeatResult{
                        peer.nodeId,
                        response
                    };
                }
            )
        );
    }

    for (
        auto& future :
        futures
    ) {
        if (!running) {
            return;
        }

        HeartbeatResult result =
            future.get();

        istringstream input(
            result.response
        );

        string command;

        uint64_t responseTerm =
            0;

        input
            >> command
            >> responseTerm;

        // A follower/candidate reports a newer term.
        //
        // A correct Raft leader must immediately step down.
        if (
            responseTerm >
            leaderTerm
        ) {
            stepDown(
                responseTerm
            );

            cout
                << "\n[raft] "
                << localNodeId
                << " stepped down"
                << " term="
                << responseTerm
                << '\n';

            return;
        }
    }
}

// ============================================================
// Incoming Raft RPC
// ============================================================

string RaftNode::handleRequest(
    const string& request
) {
    istringstream input(
        request
    );

    string command;

    input >> command;

    // ========================================================
    // RequestVote
    // ========================================================

    if (
        command ==
        "RAFT_REQUEST_VOTE"
    ) {
        uint64_t candidateTerm;

        string candidateId;

        if (
            !(input
              >> candidateTerm
              >> candidateId)
        ) {
            return
                "ERROR INVALID_RAFT_REQUEST_VOTE";
        }

        lock_guard<mutex> lock(
            stateMutex
        );

        // ----------------------------------------------------
        // Candidate is stale.
        // ----------------------------------------------------

        if (
            candidateTerm <
            currentTerm
        ) {
            return
                "RAFT_VOTE_DENIED " +
                to_string(
                    currentTerm
                );
        }

        // ----------------------------------------------------
        // Newer term discovered.
        // ----------------------------------------------------

        if (
            candidateTerm >
            currentTerm
        ) {
            currentTerm =
                candidateTerm;

            role =
                RaftRole::FOLLOWER;

            votedFor.clear();

            leaderId.clear();
        }

        // ----------------------------------------------------
        // One vote per term.
        // ----------------------------------------------------

        bool canVote =
            votedFor.empty() ||
            votedFor ==
                candidateId;

        if (canVote) {
            votedFor =
                candidateId;

            role =
                RaftRole::FOLLOWER;

            leaderId.clear();

            resetElectionDeadlineLocked();

            cout
                << "\n[raft] "
                << localNodeId
                << " voted for "
                << candidateId
                << " term="
                << currentTerm
                << '\n';

            return
                "RAFT_VOTE_GRANTED " +
                to_string(
                    currentTerm
                );
        }

        return
            "RAFT_VOTE_DENIED " +
            to_string(
                currentTerm
            );
    }

    // ========================================================
    // Heartbeat
    // ========================================================

    if (
        command ==
        "RAFT_HEARTBEAT"
    ) {
        uint64_t heartbeatTerm;

        string heartbeatLeader;

        if (
            !(input
              >> heartbeatTerm
              >> heartbeatLeader)
        ) {
            return
                "ERROR INVALID_RAFT_HEARTBEAT";
        }

        lock_guard<mutex> lock(
            stateMutex
        );

        // ----------------------------------------------------
        // Stale leader.
        // ----------------------------------------------------

        if (
            heartbeatTerm <
            currentTerm
        ) {
            return
                "RAFT_HEARTBEAT_REJECT " +
                to_string(
                    currentTerm
                );
        }

        // ----------------------------------------------------
        // New term.
        // ----------------------------------------------------

        if (
            heartbeatTerm >
            currentTerm
        ) {
            currentTerm =
                heartbeatTerm;

            votedFor.clear();
        }

        // Valid AppendEntries/heartbeat from a leader
        // at current or newer term forces follower state.
        role =
            RaftRole::FOLLOWER;

        leaderId =
            heartbeatLeader;

        resetElectionDeadlineLocked();

        return
            "RAFT_HEARTBEAT_ACK " +
            to_string(
                currentTerm
            );
    }

    return
        "ERROR UNKNOWN_RAFT_COMMAND";
}

// ============================================================
// Status
// ============================================================

void RaftNode::printStatus() const {
    lock_guard<mutex> lock(
        stateMutex
    );

    cout
        << "\nRaft Status\n";

    cout
        << "---------------------------------\n";

    cout
        << "Node:        "
        << localNodeId
        << '\n';

    cout
        << "Role:        "
        << roleToString(
            role
        )
        << '\n';

    cout
        << "Term:        "
        << currentTerm
        << '\n';

    cout
        << "Leader:      "
        << (
            leaderId.empty()
                ? "UNKNOWN"
                : leaderId
        )
        << '\n';

    cout
        << "Voted For:   "
        << (
            votedFor.empty()
                ? "-"
                : votedFor
        )
        << '\n';

    cout
        << "Majority:    "
        << majorityCount()
        << '\n';

    cout << '\n';
}

// ============================================================
// State Accessors
// ============================================================

RaftRole RaftNode::getRole() const {
    lock_guard<mutex> lock(
        stateMutex
    );

    return role;
}

uint64_t
RaftNode::getCurrentTerm() const {
    lock_guard<mutex> lock(
        stateMutex
    );

    return currentTerm;
}

string RaftNode::getLeaderId() const {
    lock_guard<mutex> lock(
        stateMutex
    );

    return leaderId;
}

bool RaftNode::isLeader() const {
    lock_guard<mutex> lock(
        stateMutex
    );

    return
        role ==
        RaftRole::LEADER;
}