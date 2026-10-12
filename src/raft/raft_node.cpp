#include "raft_node.h"

#include "network/tcp_client.h"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <future>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace std;

// ============================================================
// Constructor / Destructor
// ============================================================

RaftNode::RaftNode(
    const string& localNodeId,
    const vector<PeerConfig>& peers,
    ApplyCallback applyCallback
)
    : localNodeId(localNodeId),
      peers(peers),
      applyCallback(
          std::move(applyCallback)
      ),
      persistentStorage(
          filesystem::path("data") /
          localNodeId /
          "raft"
      ),
      randomGenerator(
          random_device{}()
      ) {

    // Sentinel entry.
    // Real Raft entries begin from index 1.
    log.push_back(
        {
            0,
            0,
            ""
        }
    );
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

uint64_t
RaftNode::lastLogIndexLocked() const {
    return
        static_cast<uint64_t>(
            log.size() - 1
        );
}

uint64_t
RaftNode::lastLogTermLocked() const {
    return
        log.back().term;
}

// ============================================================
// Persistent Metadata
// ============================================================

bool RaftNode::persistMetadataLocked(
    const string& context
) {
    string error;

    if (
        persistentStorage.persistMetadata(
            currentTerm,
            votedFor,
            commitIndex,
            error
        )
    ) {
        return true;
    }

    cerr
        << "\n[raft-persistence] "
        << context
        << ": "
        << error
        << '\n';

    return false;
}

// ============================================================
// Election Timeout
// ============================================================

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
// Leader Replication State
// ============================================================

void RaftNode::initializeLeaderStateLocked() {
    nextIndex.clear();

    matchIndex.clear();

    uint64_t next =
        lastLogIndexLocked() + 1;

    for (
        const auto& peer :
        peers
    ) {
        nextIndex[
            peer.nodeId
        ] = next;

        matchIndex[
            peer.nodeId
        ] = 0;
    }
}

// ============================================================
// Apply Committed Entries
// ============================================================

vector<string>
RaftNode::collectCommittedCommandsLocked() {
    vector<string> commands;

    while (
        lastApplied <
        commitIndex
    ) {
        ++lastApplied;

        commands.push_back(
            log[
                static_cast<size_t>(
                    lastApplied
                )
            ].command
        );
    }

    return commands;
}

void RaftNode::applyCommands(
    const vector<string>& commands
) {
    if (!applyCallback) {
        return;
    }

    for (
        const auto& command :
        commands
    ) {
        applyCallback(
            command
        );
    }
}

// ============================================================
// Start Raft Node
//
// Recovery order:
//
// disk
//   ↓
// term / vote / log / commit
//   ↓
// replay committed entries
//   ↓
// start election worker
// ============================================================

bool RaftNode::start() {
    if (running) {
        return true;
    }

    string error;

    // --------------------------------------------------------
    // Create persistent directory/files if necessary.
    // --------------------------------------------------------

    if (
        !persistentStorage.initialize(
            error
        )
    ) {
        cerr
            << "[raft] persistent storage initialization failed: "
            << error
            << '\n';

        return false;
    }

    // --------------------------------------------------------
    // Load previous persistent state.
    // --------------------------------------------------------

    uint64_t loadedTerm = 0;

    uint64_t loadedCommit = 0;

    string loadedVote;

    vector<RaftLogEntry>
        loadedLog;

    if (
        !persistentStorage.load(
            loadedTerm,
            loadedVote,
            loadedCommit,
            loadedLog,
            error
        )
    ) {
        cerr
            << "[raft] recovery failed: "
            << error
            << '\n';

        return false;
    }

    vector<string>
        commandsToReplay;

    {
        lock_guard<mutex> lock(
            stateMutex
        );

        currentTerm =
            loadedTerm;

        votedFor =
            loadedVote;

        log =
            std::move(
                loadedLog
            );

        commitIndex =
            loadedCommit;

        // State machine is rebuilt from scratch.
        lastApplied = 0;

        role =
            RaftRole::FOLLOWER;

        leaderId.clear();

        nextIndex.clear();

        matchIndex.clear();

        resetElectionDeadlineLocked();

        nextHeartbeat =
            chrono::steady_clock::now();

        // ----------------------------------------------------
        // Rebuild in-memory KV state from all committed
        // Raft entries.
        // ----------------------------------------------------

        commandsToReplay =
            collectCommittedCommandsLocked();
    }

    applyCommands(
        commandsToReplay
    );

    cout
        << "[raft] recovered persistent state"
        << " term="
        << loadedTerm
        << " log="
        << (
            loadedLog.empty()
                ? 0
                : loadedLog.size() - 1
        )
        << " commit="
        << loadedCommit
        << '\n';

    running = true;

    workerThread =
        thread(
            &RaftNode::run,
            this
        );

    return true;
}

// ============================================================
// Stop
// ============================================================

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

    bool persistentStateChanged =
        false;

    if (
        newTerm >
        currentTerm
    ) {
        currentTerm =
            newTerm;

        votedFor.clear();

        persistentStateChanged =
            true;
    }

    role =
        RaftRole::FOLLOWER;

    leaderId =
        newLeader;

    nextIndex.clear();

    matchIndex.clear();

    resetElectionDeadlineLocked();

    // --------------------------------------------------------
    // currentTerm and votedFor are persistent Raft state.
    // --------------------------------------------------------

    if (
        persistentStateChanged
    ) {
        persistMetadataLocked(
            "failed to persist step-down term"
        );
    }
}

// ============================================================
// Worker Loop
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
// Start Election
// ============================================================

void RaftNode::startElection() {
    uint64_t electionTerm;

    uint64_t candidateLastIndex;

    uint64_t candidateLastTerm;

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

        candidateLastIndex =
            lastLogIndexLocked();

        candidateLastTerm =
            lastLogTermLocked();

        // ----------------------------------------------------
        // Term and self-vote MUST be durable before
        // sending RequestVote.
        // ----------------------------------------------------

        if (
            !persistMetadataLocked(
                "failed to persist election term/vote"
            )
        ) {
            role =
                RaftRole::FOLLOWER;

            resetElectionDeadlineLocked();

            return;
        }

        resetElectionDeadlineLocked();
    }

    cout
        << "\n[raft] "
        << localNodeId
        << " became CANDIDATE"
        << " term="
        << electionTerm
        << '\n';

    // Candidate votes for itself.
    size_t votes = 1;

    size_t majority =
        majorityCount();

    // ========================================================
    // Single-node cluster
    // ========================================================

    if (
        votes >=
        majority
    ) {
        bool becameLeader =
            false;

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

                initializeLeaderStateLocked();

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
        }

        return;
    }

    // ========================================================
    // RequestVote concurrently
    // ========================================================

    struct VoteResult {
        string response;
    };

    vector<
        future<VoteResult>
    > futures;

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
                    electionTerm,
                    candidateLastIndex,
                    candidateLastTerm
                ]() {
                    string request =
                        "RAFT_REQUEST_VOTE " +
                        to_string(
                            electionTerm
                        ) +
                        " " +
                        localNodeId +
                        " " +
                        to_string(
                            candidateLastIndex
                        ) +
                        " " +
                        to_string(
                            candidateLastTerm
                        );

                    return VoteResult{
                        TcpClient::request(
                            peer.host,
                            peer.port,
                            request
                        )
                    };
                }
            )
        );
    }

    // ========================================================
    // Process votes
    // ========================================================

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

        string responseType;

        uint64_t responseTerm = 0;

        input
            >> responseType
            >> responseTerm;

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
            responseType ==
                "RAFT_VOTE_GRANTED" &&
            responseTerm ==
                electionTerm
        ) {
            ++votes;
        }
    }

    // ========================================================
    // Become Leader
    // ========================================================

    bool becameLeader =
        false;

    {
        lock_guard<mutex> lock(
            stateMutex
        );

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

            initializeLeaderStateLocked();

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

        // Start follower synchronization immediately.
        sendHeartbeats();
    }
}

// ============================================================
// Replicate / Catch Up One Peer
//
// Uses nextIndex / matchIndex.
//
// Example:
//
// Leader:
// 1 2 3 4 5
//
// Follower:
// 1 2 3
//
// try prev=5 -> reject
// nextIndex decreases
//
// then:
//
// send 4
// send 5
//
// follower becomes synchronized.
// ============================================================

RaftNode::PeerReplicationResult
RaftNode::replicatePeerToIndex(
    const PeerConfig& peer,
    uint64_t targetIndex,
    uint64_t leaderTerm
) {
    PeerReplicationResult result;

    result.responseTerm =
        leaderTerm;

    while (running) {
        uint64_t currentNext;

        uint64_t previousIndex;

        uint64_t previousTerm;

        uint64_t leaderCommit;

        uint64_t entryIndex = 0;

        uint64_t entryTerm = 0;

        string entryCommand;

        {
            lock_guard<mutex> lock(
                stateMutex
            );

            // ------------------------------------------------
            // Leadership changed during replication.
            // ------------------------------------------------

            if (
                role !=
                    RaftRole::LEADER ||
                currentTerm !=
                    leaderTerm
            ) {
                return result;
            }

            auto nextIt =
                nextIndex.find(
                    peer.nodeId
                );

            if (
                nextIt ==
                nextIndex.end()
            ) {
                nextIndex[
                    peer.nodeId
                ] =
                    lastLogIndexLocked() + 1;

                matchIndex[
                    peer.nodeId
                ] = 0;
            }

            currentNext =
                nextIndex[
                    peer.nodeId
                ];

            if (
                currentNext == 0
            ) {
                currentNext = 1;

                nextIndex[
                    peer.nodeId
                ] = 1;
            }

            uint64_t maximumNext =
                lastLogIndexLocked() + 1;

            if (
                currentNext >
                maximumNext
            ) {
                currentNext =
                    maximumNext;

                nextIndex[
                    peer.nodeId
                ] =
                    maximumNext;
            }

            previousIndex =
                currentNext - 1;

            previousTerm =
                log[
                    static_cast<size_t>(
                        previousIndex
                    )
                ].term;

            leaderCommit =
                commitIndex;

            // ------------------------------------------------
            // Peer is behind.
            // Send exactly one missing entry.
            // ------------------------------------------------

            if (
                currentNext <=
                lastLogIndexLocked()
            ) {
                const auto& entry =
                    log[
                        static_cast<size_t>(
                            currentNext
                        )
                    ];

                entryIndex =
                    entry.index;

                entryTerm =
                    entry.term;

                entryCommand =
                    entry.command;
            }
        }

        // ====================================================
        // AppendEntries RPC
        // ====================================================

        string request =
            "RAFT_APPEND_ENTRIES " +
            to_string(
                leaderTerm
            ) +
            " " +
            localNodeId +
            " " +
            to_string(
                previousIndex
            ) +
            " " +
            to_string(
                previousTerm
            ) +
            " " +
            to_string(
                leaderCommit
            ) +
            " " +
            to_string(
                entryIndex
            ) +
            " " +
            to_string(
                entryTerm
            );

        if (
            entryIndex != 0
        ) {
            request +=
                " " +
                entryCommand;
        }

        string response =
            TcpClient::request(
                peer.host,
                peer.port,
                request
            );

        istringstream input(
            response
        );

        string responseType;

        uint64_t responseTerm = 0;

        uint64_t followerIndex = 0;

        input
            >> responseType
            >> responseTerm
            >> followerIndex;

        // ====================================================
        // Higher term discovered
        // ====================================================

        if (
            responseTerm >
            leaderTerm
        ) {
            result.responseTerm =
                responseTerm;

            stepDown(
                responseTerm
            );

            return result;
        }

        // ====================================================
        // Successful replication
        // ====================================================

        if (
            responseType ==
                "RAFT_APPEND_ACK" &&
            responseTerm ==
                leaderTerm
        ) {
            lock_guard<mutex> lock(
                stateMutex
            );

            if (
                role !=
                    RaftRole::LEADER ||
                currentTerm !=
                    leaderTerm
            ) {
                return result;
            }

            matchIndex[
                peer.nodeId
            ] =
                max(
                    matchIndex[
                        peer.nodeId
                    ],
                    followerIndex
                );

            nextIndex[
                peer.nodeId
            ] =
                matchIndex[
                    peer.nodeId
                ] +
                1;

            result.matchIndex =
                matchIndex[
                    peer.nodeId
                ];

            // ------------------------------------------------
            // Follower has reached required index.
            // ------------------------------------------------

            if (
                result.matchIndex >=
                targetIndex
            ) {
                result.success =
                    true;

                return result;
            }

            // Follower still behind.
            continue;
        }

        // ====================================================
        // Previous-log mismatch
        // ====================================================

        if (
            responseType ==
                "RAFT_APPEND_REJECT" &&
            responseTerm ==
                leaderTerm
        ) {
            lock_guard<mutex> lock(
                stateMutex
            );

            if (
                role !=
                    RaftRole::LEADER ||
                currentTerm !=
                    leaderTerm
            ) {
                return result;
            }

            uint64_t oldNext =
                nextIndex[
                    peer.nodeId
                ];

            uint64_t suggestedNext =
                followerIndex + 1;

            // ------------------------------------------------
            // Follower log is shorter.
            // ------------------------------------------------

            if (
                suggestedNext <
                oldNext
            ) {
                nextIndex[
                    peer.nodeId
                ] =
                    max<uint64_t>(
                        1,
                        suggestedNext
                    );
            }

            // ------------------------------------------------
            // Same length but conflicting term.
            // Walk backwards.
            // ------------------------------------------------

            else if (
                oldNext >
                1
            ) {
                nextIndex[
                    peer.nodeId
                ] =
                    oldNext - 1;
            }
            else {
                nextIndex[
                    peer.nodeId
                ] = 1;
            }

            continue;
        }

        // Network failure or malformed response.
        return result;
    }

    return result;
}

// ============================================================
// Heartbeat + Automatic Catch-Up
// ============================================================

void RaftNode::sendHeartbeats() {
    // --------------------------------------------------------
    // If a client proposal is currently performing
    // replication, it already acts as AppendEntries traffic.
    // --------------------------------------------------------

    unique_lock<mutex> replicationLock(
        replicationMutex,
        try_to_lock
    );

    if (
        !replicationLock.owns_lock()
    ) {
        return;
    }

    uint64_t leaderTerm;

    uint64_t targetIndex;

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

        targetIndex =
            lastLogIndexLocked();
    }

    vector<
        future<PeerReplicationResult>
    > futures;

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
                    targetIndex,
                    leaderTerm
                ]() {
                    return
                        replicatePeerToIndex(
                            peer,
                            targetIndex,
                            leaderTerm
                        );
                }
            )
        );
    }

    for (
        auto& future :
        futures
    ) {
        PeerReplicationResult result =
            future.get();

        if (
            result.responseTerm >
            leaderTerm
        ) {
            return;
        }
    }
}

// ============================================================
// Client Proposal
//
// Durability:
//
// leader WAL fsync
//     ↓
// replicate followers
//     ↓
// majority durable ACK
//     ↓
// persist commit index
//     ↓
// apply state machine
// ============================================================

RaftProposalResult
RaftNode::propose(
    const string& command
) {
    RaftProposalResult result;

    uint64_t proposalTerm;

    uint64_t entryIndex;

    RaftLogEntry newEntry;

    // ========================================================
    // Append and persist leader entry
    // ========================================================

    {
        lock_guard<mutex> lock(
            stateMutex
        );

        if (
            role !=
            RaftRole::LEADER
        ) {
            result.message =
                "NOT_LEADER";

            return result;
        }

        proposalTerm =
            currentTerm;

        entryIndex =
            lastLogIndexLocked() + 1;

        newEntry =
            {
                entryIndex,
                proposalTerm,
                command
            };

        // ----------------------------------------------------
        // Persist BEFORE exposing entry as locally durable.
        // ----------------------------------------------------

        string error;

        if (
            !persistentStorage.appendEntry(
                newEntry,
                error
            )
        ) {
            cerr
                << "\n[raft-persistence] "
                << error
                << '\n';

            result.message =
                "PERSISTENCE_ERROR";

            return result;
        }

        log.push_back(
            newEntry
        );
    }

    result.index =
        entryIndex;

    result.term =
        proposalTerm;

    result.majority =
        majorityCount();

    // Leader itself.
    size_t acknowledgements = 1;

    // ========================================================
    // Replicate
    // ========================================================

    {
        unique_lock<mutex> replicationLock(
            replicationMutex
        );

        vector<
            future<PeerReplicationResult>
        > futures;

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
                        entryIndex,
                        proposalTerm
                    ]() {
                        return
                            replicatePeerToIndex(
                                peer,
                                entryIndex,
                                proposalTerm
                            );
                    }
                )
            );
        }

        for (
            auto& future :
            futures
        ) {
            PeerReplicationResult peerResult =
                future.get();

            if (
                peerResult.responseTerm >
                proposalTerm
            ) {
                result.acknowledgements =
                    acknowledgements;

                result.message =
                    "HIGHER_TERM";

                return result;
            }

            if (
                peerResult.success &&
                peerResult.matchIndex >=
                    entryIndex
            ) {
                ++acknowledgements;
            }
        }
    }

    result.acknowledgements =
        acknowledgements;

    vector<string>
        commandsToApply;

    // ========================================================
    // Majority Commit
    // ========================================================

    {
        lock_guard<mutex> lock(
            stateMutex
        );

        if (
            role !=
                RaftRole::LEADER ||
            currentTerm !=
                proposalTerm
        ) {
            result.message =
                "NOT_LEADER";

            return result;
        }

        if (
            acknowledgements <
            result.majority
        ) {
            result.message =
                "QUORUM_NOT_REACHED";

            return result;
        }

        uint64_t oldCommitIndex =
            commitIndex;

        commitIndex =
            max(
                commitIndex,
                entryIndex
            );

        // ----------------------------------------------------
        // Persist commit decision before applying command.
        // ----------------------------------------------------

        if (
            !persistMetadataLocked(
                "failed to persist leader commit index"
            )
        ) {
            commitIndex =
                oldCommitIndex;

            result.message =
                "PERSISTENCE_ERROR";

            return result;
        }

        commandsToApply =
            collectCommittedCommandsLocked();

        result.committed =
            true;

        result.message =
            "COMMITTED";
    }

    // Apply outside Raft mutex.
    applyCommands(
        commandsToApply
    );

    // --------------------------------------------------------
    // Propagate new commit index.
    // --------------------------------------------------------

    sendHeartbeats();

    return result;
}

// ============================================================
// Incoming RPC
// ============================================================

string RaftNode::handleRequest(
    const string& request
) {
    istringstream input(
        request
    );

    string commandType;

    input >> commandType;

    // ========================================================
    // RequestVote
    // ========================================================

    if (
        commandType ==
        "RAFT_REQUEST_VOTE"
    ) {
        uint64_t candidateTerm;

        string candidateId;

        uint64_t candidateLastIndex;

        uint64_t candidateLastTerm;

        if (
            !(input
              >> candidateTerm
              >> candidateId
              >> candidateLastIndex
              >> candidateLastTerm)
        ) {
            return
                "ERROR INVALID_RAFT_REQUEST_VOTE";
        }

        lock_guard<mutex> lock(
            stateMutex
        );

        // ----------------------------------------------------
        // Stale candidate.
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
        // New term discovered.
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

            nextIndex.clear();

            matchIndex.clear();

            if (
                !persistMetadataLocked(
                    "failed to persist newer candidate term"
                )
            ) {
                return
                    "ERROR RAFT_PERSISTENCE_FAILED";
            }
        }

        uint64_t localLastIndex =
            lastLogIndexLocked();

        uint64_t localLastTerm =
            lastLogTermLocked();

        // ----------------------------------------------------
        // Raft voting rule:
        //
        // candidate log must be at least as up to date.
        // ----------------------------------------------------

        bool logUpToDate =
            candidateLastTerm >
                localLastTerm ||
            (
                candidateLastTerm ==
                    localLastTerm &&
                candidateLastIndex >=
                    localLastIndex
            );

        bool canVote =
            (
                votedFor.empty() ||
                votedFor ==
                    candidateId
            ) &&
            logUpToDate;

        if (canVote) {
            votedFor =
                candidateId;

            role =
                RaftRole::FOLLOWER;

            leaderId.clear();

            resetElectionDeadlineLocked();

            // ------------------------------------------------
            // Vote must reach disk before granting.
            // ------------------------------------------------

            if (
                !persistMetadataLocked(
                    "failed to persist granted vote"
                )
            ) {
                return
                    "ERROR RAFT_PERSISTENCE_FAILED";
            }

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
    // AppendEntries
    // ========================================================

    if (
        commandType ==
        "RAFT_APPEND_ENTRIES"
    ) {
        uint64_t leaderTerm;

        string incomingLeaderId;

        uint64_t previousIndex;

        uint64_t previousTerm;

        uint64_t leaderCommit;

        uint64_t entryIndex;

        uint64_t entryTerm;

        if (
            !(input
              >> leaderTerm
              >> incomingLeaderId
              >> previousIndex
              >> previousTerm
              >> leaderCommit
              >> entryIndex
              >> entryTerm)
        ) {
            return
                "ERROR INVALID_APPEND_ENTRIES";
        }

        string entryCommand;

        if (
            entryIndex != 0
        ) {
            getline(
                input >> ws,
                entryCommand
            );

            if (
                entryCommand.empty()
            ) {
                return
                    "ERROR INVALID_APPEND_ENTRY";
            }
        }

        vector<string>
            commandsToApply;

        string response;

        {
            lock_guard<mutex> lock(
                stateMutex
            );

            // =================================================
            // Reject stale leader
            // =================================================

            if (
                leaderTerm <
                currentTerm
            ) {
                return
                    "RAFT_APPEND_REJECT " +
                    to_string(
                        currentTerm
                    ) +
                    " " +
                    to_string(
                        lastLogIndexLocked()
                    );
            }

            // =================================================
            // Higher term discovered
            // =================================================

            if (
                leaderTerm >
                currentTerm
            ) {
                currentTerm =
                    leaderTerm;

                votedFor.clear();

                if (
                    !persistMetadataLocked(
                        "failed to persist leader term"
                    )
                ) {
                    return
                        "ERROR RAFT_PERSISTENCE_FAILED";
                }
            }

            role =
                RaftRole::FOLLOWER;

            leaderId =
                incomingLeaderId;

            nextIndex.clear();

            matchIndex.clear();

            resetElectionDeadlineLocked();

            // =================================================
            // Previous entry must exist
            // =================================================

            if (
                previousIndex >=
                log.size()
            ) {
                return
                    "RAFT_APPEND_REJECT " +
                    to_string(
                        currentTerm
                    ) +
                    " " +
                    to_string(
                        lastLogIndexLocked()
                    );
            }

            // =================================================
            // Previous entry term must match
            // =================================================

            if (
                log[
                    static_cast<size_t>(
                        previousIndex
                    )
                ].term !=
                previousTerm
            ) {
                return
                    "RAFT_APPEND_REJECT " +
                    to_string(
                        currentTerm
                    ) +
                    " " +
                    to_string(
                        lastLogIndexLocked()
                    );
            }

            uint64_t acknowledgedIndex =
                previousIndex;

            // =================================================
            // Append New Entry
            // =================================================

            if (
                entryIndex != 0
            ) {
                if (
                    entryIndex !=
                    previousIndex + 1
                ) {
                    return
                        "RAFT_APPEND_REJECT " +
                        to_string(
                            currentTerm
                        ) +
                        " " +
                        to_string(
                            lastLogIndexLocked()
                        );
                }

                // ------------------------------------------------
                // Existing entry at this index.
                // ------------------------------------------------

                if (
                    entryIndex <
                    log.size()
                ) {
                    const auto&
                        existingEntry =
                            log[
                                static_cast<size_t>(
                                    entryIndex
                                )
                            ];

                    // --------------------------------------------
                    // Conflict:
                    // delete this entry and entire suffix.
                    // --------------------------------------------

                    if (
                        existingEntry.term !=
                        entryTerm
                    ) {
                        vector<RaftLogEntry>
                            rewrittenLog =
                                log;

                        rewrittenLog.resize(
                            static_cast<size_t>(
                                entryIndex
                            )
                        );

                        // New entry becomes the replacement.
                        rewrittenLog.push_back(
                            {
                                entryIndex,
                                entryTerm,
                                entryCommand
                            }
                        );

                        string error;

                        // Persist complete corrected log first.
                        if (
                            !persistentStorage.rewriteLog(
                                rewrittenLog,
                                error
                            )
                        ) {
                            cerr
                                << "\n[raft-persistence] "
                                << error
                                << '\n';

                            return
                                "ERROR RAFT_PERSISTENCE_FAILED";
                        }

                        log =
                            std::move(
                                rewrittenLog
                            );
                    }

                    // Same index + term already exists.
                    // No new disk write required.
                }

                // ------------------------------------------------
                // Entry belongs directly after current log.
                // ------------------------------------------------

                else if (
                    entryIndex ==
                    log.size()
                ) {
                    RaftLogEntry newEntry{
                        entryIndex,
                        entryTerm,
                        entryCommand
                    };

                    string error;

                    // --------------------------------------------
                    // Follower must persist before ACK.
                    // --------------------------------------------

                    if (
                        !persistentStorage.appendEntry(
                            newEntry,
                            error
                        )
                    ) {
                        cerr
                            << "\n[raft-persistence] "
                            << error
                            << '\n';

                        return
                            "ERROR RAFT_PERSISTENCE_FAILED";
                    }

                    log.push_back(
                        newEntry
                    );
                }

                else {
                    return
                        "RAFT_APPEND_REJECT " +
                        to_string(
                            currentTerm
                        ) +
                        " " +
                        to_string(
                            lastLogIndexLocked()
                        );
                }

                acknowledgedIndex =
                    entryIndex;
            }

            // =================================================
            // Update Commit Index
            // =================================================

            if (
                leaderCommit >
                commitIndex
            ) {
                uint64_t newCommitIndex =
                    min(
                        leaderCommit,
                        lastLogIndexLocked()
                    );

                if (
                    newCommitIndex >
                    commitIndex
                ) {
                    uint64_t oldCommitIndex =
                        commitIndex;

                    commitIndex =
                        newCommitIndex;

                    // ------------------------------------------
                    // Persist commit before state-machine apply.
                    // ------------------------------------------

                    if (
                        !persistMetadataLocked(
                            "failed to persist follower commit index"
                        )
                    ) {
                        commitIndex =
                            oldCommitIndex;

                        return
                            "ERROR RAFT_PERSISTENCE_FAILED";
                    }

                    commandsToApply =
                        collectCommittedCommandsLocked();
                }
            }

            response =
                "RAFT_APPEND_ACK " +
                to_string(
                    currentTerm
                ) +
                " " +
                to_string(
                    acknowledgedIndex
                );
        }

        // ----------------------------------------------------
        // Never hold Raft mutex while applying state-machine
        // operations.
        // ----------------------------------------------------

        applyCommands(
            commandsToApply
        );

        return response;
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
        << "Node:          "
        << localNodeId
        << '\n';

    cout
        << "Role:          "
        << roleToString(
            role
        )
        << '\n';

    cout
        << "Term:          "
        << currentTerm
        << '\n';

    cout
        << "Leader:        "
        << (
            leaderId.empty()
                ? "UNKNOWN"
                : leaderId
        )
        << '\n';

    cout
        << "Voted For:     "
        << (
            votedFor.empty()
                ? "-"
                : votedFor
        )
        << '\n';

    cout
        << "Last Log:      "
        << lastLogIndexLocked()
        << '\n';

    cout
        << "Commit Index:  "
        << commitIndex
        << '\n';

    cout
        << "Last Applied:  "
        << lastApplied
        << '\n';

    cout
        << "Majority:      "
        << majorityCount()
        << '\n';

    cout
        << "Persistent:    YES\n";

    cout
        << "Raft Data:     "
        << persistentStorage
               .getDirectoryPath()
        << '\n';

    // ========================================================
    // Leader replication information
    // ========================================================

    if (
        role ==
        RaftRole::LEADER
    ) {
        cout
            << "\nFollower Progress\n";

        for (
            const auto& peer :
            peers
        ) {
            uint64_t next = 0;

            uint64_t matched = 0;

            auto nextIt =
                nextIndex.find(
                    peer.nodeId
                );

            if (
                nextIt !=
                nextIndex.end()
            ) {
                next =
                    nextIt->second;
            }

            auto matchIt =
                matchIndex.find(
                    peer.nodeId
                );

            if (
                matchIt !=
                matchIndex.end()
            ) {
                matched =
                    matchIt->second;
            }

            cout
                << "  "
                << peer.nodeId
                << "  match="
                << matched
                << "  next="
                << next
                << '\n';
        }
    }

    cout << '\n';
}

// ============================================================
// Accessors
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

uint64_t
RaftNode::getCommitIndex() const {
    lock_guard<mutex> lock(
        stateMutex
    );

    return commitIndex;
}

uint64_t
RaftNode::getLastApplied() const {
    lock_guard<mutex> lock(
        stateMutex
    );

    return lastApplied;
}

uint64_t
RaftNode::getLastLogIndex() const {
    lock_guard<mutex> lock(
        stateMutex
    );

    return
        lastLogIndexLocked();
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