#include "raft_node.h"

#include "network/tcp_client.h"

#include <algorithm>
#include <chrono>
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
      randomGenerator(
          random_device{}()
      ) {

    // Sentinel log entry.
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

void RaftNode::resetElectionDeadlineLocked() {
    uniform_int_distribution<int>
        distribution(
            electionMinMs,
            electionMaxMs
        );

    electionDeadline =
        chrono::steady_clock::now() +
        chrono::milliseconds(
            distribution(
                randomGenerator
            )
        );
}

// ============================================================
// Initialize nextIndex / matchIndex
//
// Called whenever this node becomes leader.
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
// Apply committed entries
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

    nextIndex.clear();

    matchIndex.clear();

    resetElectionDeadlineLocked();
}

// ============================================================
// Main Worker
// ============================================================

void RaftNode::run() {
    while (running) {
        bool startElectionNow =
            false;

        bool heartbeatNow =
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
                    heartbeatNow =
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
                    startElectionNow =
                        true;
                }
            }
        }

        if (
            startElectionNow
        ) {
            startElection();
        }

        if (
            heartbeatNow
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

        resetElectionDeadlineLocked();
    }

    cout
        << "\n[raft] "
        << localNodeId
        << " became CANDIDATE"
        << " term="
        << electionTerm
        << '\n';

    size_t votes = 1;

    size_t majority =
        majorityCount();

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

        // Immediately start synchronizing followers.
        sendHeartbeats();
    }
}

// ============================================================
// Replicate one follower until it reaches targetIndex
//
// This is the core missing piece from the previous version.
//
// Example:
//
// leader:
// 1 2 3 4 5
//
// follower:
// 1 2 3
//
// nextIndex initially 6
//
// prev=5 -> reject
// nextIndex -> 4
//
// send entry 4
// send entry 5
//
// follower caught up.
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

            // Leadership changed while replication
            // was happening.
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

            previousIndex =
                currentNext - 1;

            if (
                previousIndex >=
                log.size()
            ) {
                previousIndex =
                    lastLogIndexLocked();

                currentNext =
                    previousIndex + 1;

                nextIndex[
                    peer.nodeId
                ] =
                    currentNext;
            }

            previousTerm =
                log[
                    static_cast<size_t>(
                        previousIndex
                    )
                ].term;

            leaderCommit =
                commitIndex;

            // ------------------------------------------------
            // If peer is behind, send exactly one missing
            // log entry.
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

        // ----------------------------------------------------
        // Higher term discovered.
        // ----------------------------------------------------

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

        // ----------------------------------------------------
        // Successful AppendEntries
        // ----------------------------------------------------

        if (
            responseType ==
                "RAFT_APPEND_ACK" &&
            responseTerm ==
                leaderTerm
        ) {
            {
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

                uint64_t acknowledgedIndex =
                    followerIndex;

                matchIndex[
                    peer.nodeId
                ] =
                    max(
                        matchIndex[
                            peer.nodeId
                        ],
                        acknowledgedIndex
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

                if (
                    result.matchIndex >=
                    targetIndex
                ) {
                    result.success =
                        true;

                    return result;
                }
            }

            // Peer acknowledged one missing entry,
            // but is still behind targetIndex.
            continue;
        }

        // ----------------------------------------------------
        // Follower rejected because previous log entry
        // did not match.
        //
        // Move nextIndex backwards and retry.
        // ----------------------------------------------------

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

            // Follower is clearly shorter.
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
            else {
                // Same length but conflicting term.
                // Walk backwards one entry.
                if (
                    oldNext > 1
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
            }

            continue;
        }

        // Network failure / malformed response.
        return result;
    }

    return result;
}

// ============================================================
// Heartbeat + automatic follower catch-up
// ============================================================

void RaftNode::sendHeartbeats() {
    // If a client proposal is already replicating,
    // that traffic itself acts like AppendEntries/heartbeat.
    //
    // Skip this heartbeat round instead of competing with it.

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

    // --------------------------------------------------------
    // A heartbeat now does more than "I'm alive".
    //
    // If a follower is behind, this also repairs its log.
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
// Propose command
// ============================================================

RaftProposalResult
RaftNode::propose(
    const string& command
) {
    RaftProposalResult result;

    uint64_t proposalTerm;

    uint64_t entryIndex;

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

        log.push_back(
            {
                entryIndex,
                proposalTerm,
                command
            }
        );
    }

    result.index =
        entryIndex;

    result.term =
        proposalTerm;

    result.majority =
        majorityCount();

    // Leader itself contains the entry.
    size_t acknowledgements = 1;

    // --------------------------------------------------------
    // Only one outbound replication round at a time.
    // --------------------------------------------------------

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
    // Majority reached -> commit
    // ========================================================

    {
        lock_guard<mutex> lock(
            stateMutex
        );

        if (
            role ==
                RaftRole::LEADER &&
            currentTerm ==
                proposalTerm &&
            acknowledgements >=
                result.majority
        ) {
            commitIndex =
                max(
                    commitIndex,
                    entryIndex
                );

            commandsToApply =
                collectCommittedCommandsLocked();

            result.committed =
                true;

            result.message =
                "COMMITTED";
        }
        else {
            result.message =
                "QUORUM_NOT_REACHED";
        }
    }

    applyCommands(
        commandsToApply
    );

    // --------------------------------------------------------
    // Send updated commitIndex to followers.
    // --------------------------------------------------------

    if (
        result.committed
    ) {
        sendHeartbeats();
    }

    return result;
}

// ============================================================
// Incoming Raft RPCs
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
        }

        uint64_t localLastIndex =
            lastLogIndexLocked();

        uint64_t localLastTerm =
            lastLogTermLocked();

        // Candidate must have a log at least as
        // up-to-date as this node.
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

            // ------------------------------------------------
            // Stale leader.
            // ------------------------------------------------

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

            // ------------------------------------------------
            // Discover newer term.
            // ------------------------------------------------

            if (
                leaderTerm >
                currentTerm
            ) {
                currentTerm =
                    leaderTerm;

                votedFor.clear();
            }

            role =
                RaftRole::FOLLOWER;

            leaderId =
                incomingLeaderId;

            nextIndex.clear();

            matchIndex.clear();

            resetElectionDeadlineLocked();

            // =================================================
            // Previous entry must exist.
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
            // Previous entry must have same term.
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
            // Append one entry
            // =================================================

            if (
                entryIndex != 0
            ) {
                // Entry must immediately follow prevLogIndex.
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

                // ---------------------------------------------
                // Existing entry at same index.
                // ---------------------------------------------

                if (
                    entryIndex <
                    log.size()
                ) {
                    // Conflict:
                    //
                    // delete this entry and everything after it.
                    if (
                        log[
                            static_cast<size_t>(
                                entryIndex
                            )
                        ].term !=
                        entryTerm
                    ) {
                        log.resize(
                            static_cast<size_t>(
                                entryIndex
                            )
                        );
                    }
                }

                // ---------------------------------------------
                // Append if entry no longer exists.
                // ---------------------------------------------

                if (
                    entryIndex ==
                    log.size()
                ) {
                    log.push_back(
                        {
                            entryIndex,
                            entryTerm,
                            entryCommand
                        }
                    );
                }

                // If entry already existed with same term,
                // it is considered matched.

                acknowledgedIndex =
                    entryIndex;
            }

            // =================================================
            // Advance follower commit index.
            // =================================================

            if (
                leaderCommit >
                commitIndex
            ) {
                commitIndex =
                    min(
                        leaderCommit,
                        lastLogIndexLocked()
                    );

                commandsToApply =
                    collectCommittedCommandsLocked();
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

        // Apply outside Raft mutex.
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

    // --------------------------------------------------------
    // Leader-only replication information
    // --------------------------------------------------------

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