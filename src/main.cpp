#include <cstdlib>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "cluster/cluster_manager.h"
#include "network/tcp_client.h"
#include "network/tcp_server.h"
#include "raft/raft_node.h"
#include "sharding/shard_manager.h"

using namespace std;

// ============================================================
// Node Configuration
// ============================================================

struct NodeConfig {
    string nodeId = "node-1";
    string host = "127.0.0.1";
    int port = 5001;

    vector<PeerConfig> peers;
};

// ============================================================
// Temporary Local Storage Engine
//
// Later:
//
// WAL
//   ↓
// MemTable
//   ↓
// SSTables
// ============================================================

class StorageEngine {
private:
    unordered_map<string, string> data;

    mutable mutex dataMutex;

public:
    void put(
        const string& key,
        const string& value
    ) {
        lock_guard<mutex> lock(dataMutex);

        data[key] = value;
    }

    bool get(
        const string& key,
        string& value
    ) const {
        lock_guard<mutex> lock(dataMutex);

        auto it = data.find(key);

        if (it == data.end()) {
            return false;
        }

        value = it->second;

        return true;
    }

    bool erase(
        const string& key
    ) {
        lock_guard<mutex> lock(dataMutex);

        return data.erase(key) > 0;
    }

    size_t size() const {
        lock_guard<mutex> lock(dataMutex);

        return data.size();
    }
};

// ============================================================
// Build Shard Node List
// ============================================================

vector<ShardNode> buildShardNodes(
    const NodeConfig& config
) {
    vector<ShardNode> nodes;

    nodes.push_back(
        {
            config.nodeId,
            config.host,
            config.port
        }
    );

    for (const auto& peer : config.peers) {
        nodes.push_back(
            {
                peer.nodeId,
                peer.host,
                peer.port
            }
        );
    }

    return nodes;
}

// ============================================================
// HydraDB Node
// ============================================================

class HydraNode {
private:
    NodeConfig config;

    StorageEngine storage;

    string state = "STARTING";

    // --------------------------------------------------------
    // Distributed components
    // --------------------------------------------------------

    ShardManager shardManager;

    TcpServer networkServer;

    ClusterManager clusterManager;

    RaftNode raftNode;

    // ========================================================
    // Replicate PUT to follower replicas
    //
    // IMPORTANT:
    // This is still our Step-5 replication mechanism.
    //
    // Raft is currently used only for leader election.
    // In the next step, writes will go through the Raft log.
    // ========================================================

    size_t replicatePut(
        size_t shardId,
        const string& key,
        const string& value
    ) {
        vector<ShardNode> replicas =
            shardManager.replicasForShard(
                shardId
            );

        // Primary has already stored the value locally.
        size_t acknowledgements = 1;

        for (
            size_t i = 1;
            i < replicas.size();
            ++i
        ) {
            const auto& replica =
                replicas[i];

            string request =
                "REPLICA_PUT " +
                to_string(shardId) +
                " " +
                key +
                " " +
                value;

            string response =
                TcpClient::request(
                    replica.host,
                    replica.port,
                    request
                );

            if (
                response ==
                "OK REPLICA_STORED"
            ) {
                ++acknowledgements;
            }
            else {
                cout
                    << "\n[replication] "
                    << replica.nodeId
                    << " failed: "
                    << response
                    << '\n';
            }
        }

        return acknowledgements;
    }

    // ========================================================
    // Replicate DELETE
    // ========================================================

    size_t replicateDelete(
        size_t shardId,
        const string& key
    ) {
        vector<ShardNode> replicas =
            shardManager.replicasForShard(
                shardId
            );

        // Primary operation counts as one acknowledgement.
        size_t acknowledgements = 1;

        for (
            size_t i = 1;
            i < replicas.size();
            ++i
        ) {
            const auto& replica =
                replicas[i];

            string request =
                "REPLICA_DELETE " +
                to_string(shardId) +
                " " +
                key;

            string response =
                TcpClient::request(
                    replica.host,
                    replica.port,
                    request
                );

            if (
                response ==
                    "OK REPLICA_DELETED" ||
                response ==
                    "NOT_FOUND"
            ) {
                ++acknowledgements;
            }
            else {
                cout
                    << "\n[replication] "
                    << replica.nodeId
                    << " failed: "
                    << response
                    << '\n';
            }
        }

        return acknowledgements;
    }

    // ========================================================
    // Handle Internal Network Request
    // ========================================================

    string handleNetworkRequest(
        const string& request
    ) {
        // ----------------------------------------------------
        // Raft RPCs
        //
        // Examples:
        //
        // RAFT_REQUEST_VOTE
        // RAFT_HEARTBEAT
        //
        // Let RaftNode handle all RAFT_* traffic.
        // ----------------------------------------------------

        if (
            request.rfind(
                "RAFT_",
                0
            ) == 0
        ) {
            return raftNode.handleRequest(
                request
            );
        }

        // ----------------------------------------------------
        // Existing data-plane messages
        // ----------------------------------------------------

        istringstream input(request);

        string command;

        input >> command;

        // ====================================================
        // KVPUT
        //
        // A request routed to this shard's fixed primary.
        //
        // NOTE:
        // Fixed primary is still used for DATA in this step.
        // Raft leader is not yet controlling writes.
        // ====================================================

        if (
            command ==
            "KVPUT"
        ) {
            string key;

            if (!(input >> key)) {
                return
                    "ERROR INVALID_KVPUT";
            }

            string value;

            getline(
                input >> ws,
                value
            );

            if (value.empty()) {
                return
                    "ERROR INVALID_KVPUT";
            }

            size_t shardId =
                shardManager.shardForKey(
                    key
                );

            ShardNode primary =
                shardManager.primaryForShard(
                    shardId
                );

            if (
                primary.nodeId !=
                config.nodeId
            ) {
                return
                    "ERROR WRONG_PRIMARY " +
                    primary.nodeId;
            }

            storage.put(
                key,
                value
            );

            size_t acknowledgements =
                replicatePut(
                    shardId,
                    key,
                    value
                );

            size_t replicationFactor =
                shardManager
                    .getReplicationFactor();

            size_t quorum =
                replicationFactor /
                    2 +
                1;

            if (
                acknowledgements <
                quorum
            ) {
                return
                    "ERROR QUORUM_NOT_REACHED " +
                    to_string(
                        acknowledgements
                    ) +
                    "/" +
                    to_string(
                        replicationFactor
                    );
            }

            return
                "OK REPLICATED " +
                to_string(
                    acknowledgements
                ) +
                "/" +
                to_string(
                    replicationFactor
                ) +
                " SHARD " +
                to_string(
                    shardId
                );
        }

        // ====================================================
        // REPLICA_PUT
        // ====================================================

        if (
            command ==
            "REPLICA_PUT"
        ) {
            size_t shardId;

            string key;

            if (
                !(input >>
                  shardId >>
                  key)
            ) {
                return
                    "ERROR INVALID_REPLICA_PUT";
            }

            string value;

            getline(
                input >> ws,
                value
            );

            if (value.empty()) {
                return
                    "ERROR INVALID_REPLICA_PUT";
            }

            if (
                !shardManager
                    .nodeOwnsReplica(
                        config.nodeId,
                        shardId
                    )
            ) {
                return
                    "ERROR NOT_REPLICA";
            }

            storage.put(
                key,
                value
            );

            return
                "OK REPLICA_STORED";
        }

        // ====================================================
        // KVGET
        // ====================================================

        if (
            command ==
            "KVGET"
        ) {
            string key;

            if (!(input >> key)) {
                return
                    "ERROR INVALID_KVGET";
            }

            size_t shardId =
                shardManager.shardForKey(
                    key
                );

            ShardNode primary =
                shardManager.primaryForShard(
                    shardId
                );

            if (
                primary.nodeId !=
                config.nodeId
            ) {
                return
                    "ERROR WRONG_PRIMARY " +
                    primary.nodeId;
            }

            string value;

            if (
                !storage.get(
                    key,
                    value
                )
            ) {
                return
                    "NOT_FOUND";
            }

            return
                "VALUE " +
                value;
        }

        // ====================================================
        // REPLICA_GET
        // ====================================================

        if (
            command ==
            "REPLICA_GET"
        ) {
            size_t shardId;

            string key;

            if (
                !(input >>
                  shardId >>
                  key)
            ) {
                return
                    "ERROR INVALID_REPLICA_GET";
            }

            if (
                !shardManager
                    .nodeOwnsReplica(
                        config.nodeId,
                        shardId
                    )
            ) {
                return
                    "ERROR NOT_REPLICA";
            }

            string value;

            if (
                !storage.get(
                    key,
                    value
                )
            ) {
                return
                    "NOT_FOUND";
            }

            return
                "VALUE " +
                value;
        }

        // ====================================================
        // KVDELETE
        // ====================================================

        if (
            command ==
            "KVDELETE"
        ) {
            string key;

            if (!(input >> key)) {
                return
                    "ERROR INVALID_KVDELETE";
            }

            size_t shardId =
                shardManager.shardForKey(
                    key
                );

            ShardNode primary =
                shardManager.primaryForShard(
                    shardId
                );

            if (
                primary.nodeId !=
                config.nodeId
            ) {
                return
                    "ERROR WRONG_PRIMARY " +
                    primary.nodeId;
            }

            bool existed =
                storage.erase(
                    key
                );

            size_t acknowledgements =
                replicateDelete(
                    shardId,
                    key
                );

            size_t replicationFactor =
                shardManager
                    .getReplicationFactor();

            size_t quorum =
                replicationFactor /
                    2 +
                1;

            if (
                acknowledgements <
                quorum
            ) {
                return
                    "ERROR QUORUM_NOT_REACHED " +
                    to_string(
                        acknowledgements
                    ) +
                    "/" +
                    to_string(
                        replicationFactor
                    );
            }

            if (!existed) {
                return
                    "NOT_FOUND";
            }

            return
                "OK DELETED";
        }

        // ====================================================
        // REPLICA_DELETE
        // ====================================================

        if (
            command ==
            "REPLICA_DELETE"
        ) {
            size_t shardId;

            string key;

            if (
                !(input >>
                  shardId >>
                  key)
            ) {
                return
                    "ERROR INVALID_REPLICA_DELETE";
            }

            if (
                !shardManager
                    .nodeOwnsReplica(
                        config.nodeId,
                        shardId
                    )
            ) {
                return
                    "ERROR NOT_REPLICA";
            }

            bool existed =
                storage.erase(
                    key
                );

            if (!existed) {
                return
                    "NOT_FOUND";
            }

            return
                "OK REPLICA_DELETED";
        }

        return
            "ERROR UNKNOWN_COMMAND";
    }

public:
    // ========================================================
    // Constructor
    // ========================================================

    explicit HydraNode(
        const NodeConfig& nodeConfig
    )
        : config(nodeConfig),

          shardManager(
              buildShardNodes(
                  nodeConfig
              ),
              1024,
              3
          ),

          networkServer(
              nodeConfig.nodeId,
              nodeConfig.port,

              [this](
                  const string& request
              ) {
                  return
                      handleNetworkRequest(
                          request
                      );
              }
          ),

          clusterManager(
              nodeConfig.nodeId,
              nodeConfig.peers
          ),

          raftNode(
              nodeConfig.nodeId,
              nodeConfig.peers
          ) {
    }

    // ========================================================
    // Destructor
    //
    // Explicitly stop background services before member
    // destruction begins.
    // ========================================================

    ~HydraNode() {
        raftNode.stop();

        clusterManager.stop();

        networkServer.stop();
    }

    // ========================================================
    // Start Node
    // ========================================================

    bool start() {
        // Network must start first because both cluster
        // heartbeats and Raft depend on TCP communication.

        if (
            !networkServer.start()
        ) {
            state =
                "FAILED";

            return false;
        }

        // Cluster health monitoring.
        clusterManager.start();

        // Raft leader election.
        raftNode.start();

        state =
            "RUNNING";

        cout
            << "=================================\n";

        cout
            << "          HydraDB Node\n";

        cout
            << "=================================\n";

        cout
            << "Node ID:      "
            << config.nodeId
            << '\n';

        cout
            << "Address:      "
            << config.host
            << ':'
            << config.port
            << '\n';

        cout
            << "State:        "
            << state
            << '\n';

        cout
            << "Network:      TCP\n";

        cout
            << "Storage:      In-memory\n";

        cout
            << "Peers:        "
            << clusterManager
                   .peerCount()
            << '\n';

        cout
            << "Shards:       "
            << shardManager
                   .getShardCount()
            << '\n';

        cout
            << "Replication:  "
            << shardManager
                   .getReplicationFactor()
            << '\n';

        cout
            << "Raft:         Election enabled\n";

        cout
            << "=================================\n";

        cout
            << "Type HELP for commands\n\n";

        return true;
    }

    // ========================================================
    // Distributed PUT
    // ========================================================

    void put(
        const string& key,
        const string& value
    ) {
        size_t shardId =
            shardManager.shardForKey(
                key
            );

        ShardNode primary =
            shardManager.primaryForShard(
                shardId
            );

        // ----------------------------------------------------
        // Local fixed shard primary
        // ----------------------------------------------------

        if (
            primary.nodeId ==
            config.nodeId
        ) {
            storage.put(
                key,
                value
            );

            size_t acknowledgements =
                replicatePut(
                    shardId,
                    key,
                    value
                );

            size_t replicationFactor =
                shardManager
                    .getReplicationFactor();

            size_t quorum =
                replicationFactor /
                    2 +
                1;

            cout
                << "Shard:    "
                << shardId
                << '\n';

            cout
                << "Primary:  "
                << config.nodeId
                << '\n';

            cout
                << "Acks:     "
                << acknowledgements
                << '/'
                << replicationFactor
                << '\n';

            if (
                acknowledgements >=
                quorum
            ) {
                cout
                    << "OK: write reached quorum\n";
            }
            else {
                cout
                    << "ERROR: quorum not reached\n";
            }

            return;
        }

        // ----------------------------------------------------
        // Route to fixed primary
        // ----------------------------------------------------

        string response =
            TcpClient::request(
                primary.host,
                primary.port,

                "KVPUT " +
                    key +
                    " " +
                    value
            );

        cout
            << "ROUTED -> "
            << primary.nodeId
            << " | shard="
            << shardId
            << '\n';

        cout
            << response
            << '\n';
    }

    // ========================================================
    // Distributed GET
    // ========================================================

    void get(
        const string& key
    ) const {
        size_t shardId =
            shardManager.shardForKey(
                key
            );

        vector<ShardNode> replicas =
            shardManager.replicasForShard(
                shardId
            );

        const ShardNode& primary =
            replicas[0];

        // ----------------------------------------------------
        // Local primary
        // ----------------------------------------------------

        if (
            primary.nodeId ==
            config.nodeId
        ) {
            string value;

            if (
                storage.get(
                    key,
                    value
                )
            ) {
                cout
                    << value
                    << '\n';
            }
            else {
                cout
                    << "NOT_FOUND\n";
            }

            return;
        }

        // ----------------------------------------------------
        // First try primary
        // ----------------------------------------------------

        string response =
            TcpClient::request(
                primary.host,
                primary.port,

                "KVGET " +
                    key
            );

        if (
            response.rfind(
                "VALUE ",
                0
            ) == 0
        ) {
            cout
                << "READ -> primary "
                << primary.nodeId
                << " | shard="
                << shardId
                << '\n';

            cout
                << response.substr(6)
                << '\n';

            return;
        }

        if (
            response ==
            "NOT_FOUND"
        ) {
            cout
                << "NOT_FOUND\n";

            return;
        }

        // ----------------------------------------------------
        // Primary unavailable.
        // Try replicas.
        // ----------------------------------------------------

        cout
            << "Primary "
            << primary.nodeId
            << " unavailable, "
            << "trying replicas...\n";

        for (
            size_t i = 1;
            i < replicas.size();
            ++i
        ) {
            const auto& replica =
                replicas[i];

            // ----------------------------------------------
            // Local replica
            // ----------------------------------------------

            if (
                replica.nodeId ==
                config.nodeId
            ) {
                string value;

                if (
                    storage.get(
                        key,
                        value
                    )
                ) {
                    cout
                        << "READ FALLBACK -> "
                        << replica.nodeId
                        << '\n';

                    cout
                        << value
                        << '\n';

                    return;
                }

                continue;
            }

            // ----------------------------------------------
            // Remote replica
            // ----------------------------------------------

            string replicaResponse =
                TcpClient::request(
                    replica.host,
                    replica.port,

                    "REPLICA_GET " +
                        to_string(
                            shardId
                        ) +
                        " " +
                        key
                );

            if (
                replicaResponse.rfind(
                    "VALUE ",
                    0
                ) == 0
            ) {
                cout
                    << "READ FALLBACK -> "
                    << replica.nodeId
                    << '\n';

                cout
                    << replicaResponse
                           .substr(6)
                    << '\n';

                return;
            }
        }

        cout
            << "ERROR: no replica "
            << "could serve the read\n";
    }

    // ========================================================
    // Distributed DELETE
    // ========================================================

    void erase(
        const string& key
    ) {
        size_t shardId =
            shardManager.shardForKey(
                key
            );

        ShardNode primary =
            shardManager.primaryForShard(
                shardId
            );

        if (
            primary.nodeId ==
            config.nodeId
        ) {
            bool existed =
                storage.erase(
                    key
                );

            size_t acknowledgements =
                replicateDelete(
                    shardId,
                    key
                );

            size_t replicationFactor =
                shardManager
                    .getReplicationFactor();

            size_t quorum =
                replicationFactor /
                    2 +
                1;

            if (
                acknowledgements <
                quorum
            ) {
                cout
                    << "ERROR: delete quorum "
                    << "not reached\n";

                return;
            }

            if (existed) {
                cout
                    << "OK: deleted\n";
            }
            else {
                cout
                    << "NOT_FOUND\n";
            }

            return;
        }

        string response =
            TcpClient::request(
                primary.host,
                primary.port,

                "KVDELETE " +
                    key
            );

        cout
            << "ROUTED -> "
            << primary.nodeId
            << " | shard="
            << shardId
            << '\n';

        cout
            << response
            << '\n';
    }

    // ========================================================
    // Local Node Status
    // ========================================================

    void status() const {
        cout
            << "\nHydraDB Node Status\n";

        cout
            << "---------------------------------\n";

        cout
            << "Node ID:        "
            << config.nodeId
            << '\n';

        cout
            << "Address:        "
            << config.host
            << ':'
            << config.port
            << '\n';

        cout
            << "State:          "
            << state
            << '\n';

        cout
            << "Network:        "
            << (
                networkServer
                    .isRunning()
                    ? "RUNNING"
                    : "STOPPED"
            )
            << '\n';

        cout
            << "Peers:          "
            << clusterManager
                   .peerCount()
            << '\n';

        cout
            << "Virtual shards: "
            << shardManager
                   .getShardCount()
            << '\n';

        cout
            << "Replication:    "
            << shardManager
                   .getReplicationFactor()
            << '\n';

        cout
            << "Local records:  "
            << storage.size()
            << '\n';

        cout
            << "Raft role:      ";

        if (
            raftNode.getRole() ==
            RaftRole::LEADER
        ) {
            cout << "LEADER";
        }
        else if (
            raftNode.getRole() ==
            RaftRole::CANDIDATE
        ) {
            cout << "CANDIDATE";
        }
        else {
            cout << "FOLLOWER";
        }

        cout << '\n';

        cout
            << "Raft term:      "
            << raftNode
                   .getCurrentTerm()
            << '\n';

        string leaderId =
            raftNode.getLeaderId();

        cout
            << "Raft leader:    "
            << (
                leaderId.empty()
                    ? "UNKNOWN"
                    : leaderId
            )
            << '\n';

        cout
            << "Storage:        In-memory\n\n";
    }

    // ========================================================
    // Cluster Status
    // ========================================================

    void clusterStatus() const {
        clusterManager
            .printClusterStatus();
    }

    // ========================================================
    // Raft Status
    // ========================================================

    void raftStatus() const {
        raftNode.printStatus();
    }

    // ========================================================
    // Sharding Diagnostics
    // ========================================================

    void route(
        const string& key
    ) const {
        shardManager.printRoute(
            key
        );
    }

    void replicas(
        const string& key
    ) const {
        shardManager
            .printReplicaPlacement(
                key
            );
    }

    void shardStatus() const {
        shardManager
            .printDistribution();
    }

    // ========================================================
    // Networking Diagnostics
    // ========================================================

    void pingNode(
        const string& host,
        int port
    ) const {
        cout
            << "PING "
            << host
            << ':'
            << port
            << " ... ";

        cout
            << TcpClient::ping(
                host,
                port
            )
            << '\n';
    }

    void infoNode(
        const string& host,
        int port
    ) const {
        cout
            << TcpClient::info(
                host,
                port
            )
            << '\n';
    }
};

// ============================================================
// Peer Parser
//
// Format:
//
// node-2@127.0.0.1:5002
// ============================================================

bool parsePeer(
    const string& value,
    PeerConfig& peer
) {
    size_t atPosition =
        value.find('@');

    if (
        atPosition ==
        string::npos
    ) {
        return false;
    }

    size_t colonPosition =
        value.find(
            ':',
            atPosition + 1
        );

    if (
        colonPosition ==
        string::npos
    ) {
        return false;
    }

    peer.nodeId =
        value.substr(
            0,
            atPosition
        );

    peer.host =
        value.substr(
            atPosition + 1,
            colonPosition -
                atPosition -
                1
        );

    string portValue =
        value.substr(
            colonPosition + 1
        );

    if (
        peer.nodeId.empty() ||
        peer.host.empty() ||
        portValue.empty()
    ) {
        return false;
    }

    try {
        peer.port =
            stoi(portValue);
    }
    catch (...) {
        return false;
    }

    return
        peer.port >= 1 &&
        peer.port <= 65535;
}

// ============================================================
// Program Help
// ============================================================

void printProgramHelp() {
    cout
        << "HydraDB Node\n\n";

    cout
        << "Usage:\n";

    cout
        << "  hydra "
        << "--node-id <id> "
        << "--host <host> "
        << "--port <port> "
        << "[--peer <id@host:port> ...]\n\n";

    cout
        << "Options:\n";

    cout
        << "  --node-id <id>\n";

    cout
        << "  --host <host>\n";

    cout
        << "  --port <port>\n";

    cout
        << "  --peer <id@host:port>\n";

    cout
        << "  --help, -h\n";
}

// ============================================================
// Argument Parsing
// ============================================================

bool parseArguments(
    int argc,
    char* argv[],
    NodeConfig& config
) {
    for (
        int i = 1;
        i < argc;
        ++i
    ) {
        string argument =
            argv[i];

        // ----------------------------------------------------
        // Node ID
        // ----------------------------------------------------

        if (
            argument ==
            "--node-id"
        ) {
            if (
                i + 1 >= argc
            ) {
                cerr
                    << "Missing value for --node-id\n";

                return false;
            }

            config.nodeId =
                argv[++i];
        }

        // ----------------------------------------------------
        // Host
        // ----------------------------------------------------

        else if (
            argument ==
            "--host"
        ) {
            if (
                i + 1 >= argc
            ) {
                cerr
                    << "Missing value for --host\n";

                return false;
            }

            config.host =
                argv[++i];
        }

        // ----------------------------------------------------
        // Port
        // ----------------------------------------------------

        else if (
            argument ==
            "--port"
        ) {
            if (
                i + 1 >= argc
            ) {
                cerr
                    << "Missing value for --port\n";

                return false;
            }

            try {
                config.port =
                    stoi(
                        argv[++i]
                    );
            }
            catch (...) {
                cerr
                    << "Invalid port\n";

                return false;
            }

            if (
                config.port < 1 ||
                config.port > 65535
            ) {
                cerr
                    << "Port must be between "
                    << "1 and 65535\n";

                return false;
            }
        }

        // ----------------------------------------------------
        // Peer
        // ----------------------------------------------------

        else if (
            argument ==
            "--peer"
        ) {
            if (
                i + 1 >= argc
            ) {
                cerr
                    << "Missing value for --peer\n";

                return false;
            }

            PeerConfig peer;

            string peerValue =
                argv[++i];

            if (
                !parsePeer(
                    peerValue,
                    peer
                )
            ) {
                cerr
                    << "Invalid peer format: "
                    << peerValue
                    << '\n';

                cerr
                    << "Expected: "
                    << "node-id@host:port\n";

                return false;
            }

            config.peers.push_back(
                peer
            );
        }

        // ----------------------------------------------------
        // Help
        // ----------------------------------------------------

        else if (
            argument ==
                "--help" ||
            argument ==
                "-h"
        ) {
            printProgramHelp();

            exit(0);
        }

        // ----------------------------------------------------
        // Unknown
        // ----------------------------------------------------

        else {
            cerr
                << "Unknown argument: "
                << argument
                << '\n';

            return false;
        }
    }

    // ========================================================
    // Validate peer configuration after parsing everything.
    // ========================================================

    for (
        const auto& peer :
        config.peers
    ) {
        if (
            peer.nodeId ==
            config.nodeId
        ) {
            cerr
                << "Cannot add local node "
                << "as its own peer: "
                << peer.nodeId
                << '\n';

            return false;
        }
    }

    return true;
}

// ============================================================
// CLI Help
// ============================================================

void printCommandHelp() {
    cout
        << "\nHydraDB Commands\n";

    cout
        << "---------------------------------\n";

    cout
        << "PUT <key> <value>\n";

    cout
        << "GET <key>\n";

    cout
        << "DELETE <key>\n";

    cout
        << "ROUTE <key>\n";

    cout
        << "REPLICAS <key>\n";

    cout
        << "SHARDS\n";

    cout
        << "CLUSTER\n";

    cout
        << "RAFT\n";

    cout
        << "STATUS\n";

    cout
        << "PING <host> <port>\n";

    cout
        << "INFO <host> <port>\n";

    cout
        << "HELP\n";

    cout
        << "EXIT\n\n";
}

// ============================================================
// Main
// ============================================================

int main(
    int argc,
    char* argv[]
) {
    NodeConfig config;

    if (
        !parseArguments(
            argc,
            argv,
            config
        )
    ) {
        return 1;
    }

    HydraNode node(
        config
    );

    if (
        !node.start()
    ) {
        cerr
            << "Failed to start HydraDB node\n";

        return 1;
    }

    string line;

    while (true) {
        cout
            << "hydra> ";

        if (
            !getline(
                cin,
                line
            )
        ) {
            break;
        }

        istringstream input(
            line
        );

        string command;

        input >> command;

        if (
            command.empty()
        ) {
            continue;
        }

        if (
            command == "PUT" ||
            command == "put"
        ) {
            string key;

            if (!(input >> key)) {
                cout
                    << "Usage: "
                    << "PUT <key> <value>\n";

                continue;
            }

            string value;

            getline(
                input >> ws,
                value
            );

            if (
                value.empty()
            ) {
                cout
                    << "Usage: "
                    << "PUT <key> <value>\n";

                continue;
            }

            node.put(
                key,
                value
            );
        }

        else if (
            command == "GET" ||
            command == "get"
        ) {
            string key;

            if (!(input >> key)) {
                cout
                    << "Usage: GET <key>\n";

                continue;
            }

            node.get(
                key
            );
        }

        else if (
            command == "DELETE" ||
            command == "delete"
        ) {
            string key;

            if (!(input >> key)) {
                cout
                    << "Usage: DELETE <key>\n";

                continue;
            }

            node.erase(
                key
            );
        }
        else if (
            command == "ROUTE" ||
            command == "route"
        ) {
            string key;

            if (!(input >> key)) {
                cout
                    << "Usage: ROUTE <key>\n";

                continue;
            }

            node.route(
                key
            );
        }
        else if (
            command == "REPLICAS" ||
            command == "replicas"
        ) {
            string key;

            if (!(input >> key)) {
                cout
                    << "Usage: REPLICAS <key>\n";

                continue;
            }

            node.replicas(
                key
            );
        }

        else if (
            command == "SHARDS" ||
            command == "shards"
        ) {
            node.shardStatus();
        }

        else if (
            command == "CLUSTER" ||
            command == "cluster"
        ) {
            node.clusterStatus();
        }

        else if (
            command == "RAFT" ||
            command == "raft"
        ) {
            node.raftStatus();
        }

        else if (
            command == "STATUS" ||
            command == "status"
        ) {
            node.status();
        }

        else if (
            command == "PING" ||
            command == "ping"
        ) {
            string host;

            int port;

            if (
                !(input >>
                  host >>
                  port)
            ) {
                cout
                    << "Usage: "
                    << "PING <host> <port>\n";

                continue;
            }

            if (
                port < 1 ||
                port > 65535
            ) {
                cout
                    << "Invalid port\n";

                continue;
            }

            node.pingNode(
                host,
                port
            );
        }
        else if (
            command == "INFO" ||
            command == "info"
        ) {
            string host;

            int port;

            if (
                !(input >>
                  host >>
                  port)
            ) {
                cout
                    << "Usage: "
                    << "INFO <host> <port>\n";

                continue;
            }

            if (
                port < 1 ||
                port > 65535
            ) {
                cout
                    << "Invalid port\n";

                continue;
            }

            node.infoNode(
                host,
                port
            );
        }

        else if (
            command == "HELP" ||
            command == "help"
        ) {
            printCommandHelp();
        }

        else if (
            command == "EXIT" ||
            command == "exit"
        ) {
            cout
                << "Shutting down HydraDB node\n";

            break;
        }

        else {
            cout
                << "Unknown command. "
                << "Type HELP\n";
        }
    }

    return 0;
}