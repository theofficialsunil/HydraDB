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
// Configuration
// ============================================================

struct NodeConfig {
    string nodeId = "node-1";

    string host = "127.0.0.1";

    int port = 5001;

    vector<PeerConfig> peers;
};

// ============================================================
// Temporary Local Storage Engine
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
        lock_guard<mutex> lock(
            dataMutex
        );

        data[key] =
            value;
    }

    bool get(
        const string& key,
        string& value
    ) const {
        lock_guard<mutex> lock(
            dataMutex
        );

        auto it =
            data.find(key);

        if (
            it ==
            data.end()
        ) {
            return false;
        }

        value =
            it->second;

        return true;
    }

    bool erase(
        const string& key
    ) {
        lock_guard<mutex> lock(
            dataMutex
        );

        return
            data.erase(key) >
            0;
    }

    size_t size() const {
        lock_guard<mutex> lock(
            dataMutex
        );

        return
            data.size();
    }
};

// ============================================================
// Build Logical Shard Node List
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

    for (
        const auto& peer :
        config.peers
    ) {
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

    string state =
        "STARTING";

    ShardManager shardManager;

    TcpServer networkServer;

    ClusterManager clusterManager;

    RaftNode raftNode;

    // ========================================================
    // Resolve cluster node by ID
    // ========================================================

    bool findNode(
        const string& nodeId,
        ShardNode& result
    ) const {
        if (
            nodeId ==
            config.nodeId
        ) {
            result =
                {
                    config.nodeId,
                    config.host,
                    config.port
                };

            return true;
        }

        for (
            const auto& peer :
            config.peers
        ) {
            if (
                peer.nodeId ==
                nodeId
            ) {
                result =
                    {
                        peer.nodeId,
                        peer.host,
                        peer.port
                    };

                return true;
            }
        }

        return false;
    }

    // ========================================================
    // Apply committed Raft command
    //
    // Commands:
    //
    // PUT <shard> <key> <value>
    // DELETE <shard> <key>
    // ========================================================

    void applyRaftCommand(
        const string& command
    ) {
        istringstream input(
            command
        );

        string operation;

        size_t shardId;

        input
            >> operation
            >> shardId;

        // This is already useful for the later
        // per-shard architecture.
        //
        // Only replica owners apply shard data.

        if (
            !shardManager.nodeOwnsReplica(
                config.nodeId,
                shardId
            )
        ) {
            return;
        }

        if (
            operation ==
            "PUT"
        ) {
            string key;

            if (!(input >> key)) {
                return;
            }

            string value;

            getline(
                input >> ws,
                value
            );

            if (value.empty()) {
                return;
            }

            storage.put(
                key,
                value
            );

            return;
        }

        if (
            operation ==
            "DELETE"
        ) {
            string key;

            if (!(input >> key)) {
                return;
            }

            storage.erase(
                key
            );
        }
    }

    // ========================================================
    // Format proposal result
    // ========================================================

    string formatProposalResult(
        const RaftProposalResult& result
    ) const {
        if (
            result.committed
        ) {
            return
                "OK COMMITTED"
                " INDEX " +
                to_string(
                    result.index
                ) +
                " TERM " +
                to_string(
                    result.term
                ) +
                " ACKS " +
                to_string(
                    result.acknowledgements
                ) +
                "/" +
                to_string(
                    config.peers.size() + 1
                );
        }

        return
            "ERROR " +
            result.message +
            " ACKS " +
            to_string(
                result.acknowledgements
            ) +
            "/" +
            to_string(
                config.peers.size() + 1
            );
    }

    // ========================================================
    // Incoming Network Request
    // ========================================================

    string handleNetworkRequest(
        const string& request
    ) {
        // ----------------------------------------------------
        // Raft RPC
        // ----------------------------------------------------

        if (
            request.rfind(
                "RAFT_",
                0
            ) == 0
        ) {
            return
                raftNode.handleRequest(
                    request
                );
        }

        istringstream input(
            request
        );

        string command;

        input >> command;

        // ====================================================
        // Client PUT received by Raft leader
        // ====================================================

        if (
            command ==
            "CLIENT_PUT"
        ) {
            if (
                !raftNode.isLeader()
            ) {
                string leader =
                    raftNode.getLeaderId();

                return
                    "ERROR NOT_LEADER " +
                    (
                        leader.empty()
                            ? "UNKNOWN"
                            : leader
                    );
            }

            string key;

            if (!(input >> key)) {
                return
                    "ERROR INVALID_CLIENT_PUT";
            }

            string value;

            getline(
                input >> ws,
                value
            );

            if (value.empty()) {
                return
                    "ERROR INVALID_CLIENT_PUT";
            }

            size_t shardId =
                shardManager.shardForKey(
                    key
                );

            string raftCommand =
                "PUT " +
                to_string(
                    shardId
                ) +
                " " +
                key +
                " " +
                value;

            RaftProposalResult result =
                raftNode.propose(
                    raftCommand
                );

            return
                formatProposalResult(
                    result
                );
        }

        // ====================================================
        // Client DELETE received by Raft leader
        // ====================================================

        if (
            command ==
            "CLIENT_DELETE"
        ) {
            if (
                !raftNode.isLeader()
            ) {
                string leader =
                    raftNode.getLeaderId();

                return
                    "ERROR NOT_LEADER " +
                    (
                        leader.empty()
                            ? "UNKNOWN"
                            : leader
                    );
            }

            string key;

            if (!(input >> key)) {
                return
                    "ERROR INVALID_CLIENT_DELETE";
            }

            size_t shardId =
                shardManager.shardForKey(
                    key
                );

            string raftCommand =
                "DELETE " +
                to_string(
                    shardId
                ) +
                " " +
                key;

            RaftProposalResult result =
                raftNode.propose(
                    raftCommand
                );

            return
                formatProposalResult(
                    result
                );
        }

        // ====================================================
        // GET from logical shard primary
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

            if (
                !shardManager.nodeOwnsReplica(
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
        // Replica GET
        // ====================================================

        if (
            command ==
            "REPLICA_GET"
        ) {
            size_t shardId;

            string key;

            if (
                !(input
                  >> shardId
                  >> key)
            ) {
                return
                    "ERROR INVALID_REPLICA_GET";
            }

            if (
                !shardManager.nodeOwnsReplica(
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
        : config(
              nodeConfig
          ),

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
              nodeConfig.peers,

              [this](
                  const string& command
              ) {
                  applyRaftCommand(
                      command
                  );
              }
          ) {
    }

    ~HydraNode() {
        raftNode.stop();

        clusterManager.stop();

        networkServer.stop();
    }

    // ========================================================
    // Start
    // ========================================================

    bool start() {
        if (
            !networkServer.start()
        ) {
            state =
                "FAILED";

            return false;
        }

        clusterManager.start();

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
            << clusterManager.peerCount()
            << '\n';

        cout
            << "Shards:       "
            << shardManager.getShardCount()
            << '\n';

        cout
            << "Replication:  "
            << shardManager
                   .getReplicationFactor()
            << '\n';

        cout
            << "Raft:         Log replication enabled\n";

        cout
            << "=================================\n";

        cout
            << "Type HELP for commands\n\n";

        return true;
    }

    // ========================================================
    // PUT through Raft
    // ========================================================

    void put(
        const string& key,
        const string& value
    ) {
        size_t shardId =
            shardManager.shardForKey(
                key
            );

        // ----------------------------------------------------
        // Local node is leader.
        // ----------------------------------------------------

        if (
            raftNode.isLeader()
        ) {
            string command =
                "PUT " +
                to_string(
                    shardId
                ) +
                " " +
                key +
                " " +
                value;

            RaftProposalResult result =
                raftNode.propose(
                    command
                );

            cout
                << "Shard:  "
                << shardId
                << '\n';

            cout
                << formatProposalResult(
                    result
                )
                << '\n';

            return;
        }

        // ----------------------------------------------------
        // Route to elected leader.
        // ----------------------------------------------------

        string leaderId =
            raftNode.getLeaderId();

        if (
            leaderId.empty()
        ) {
            cout
                << "ERROR: no Raft leader available\n";

            return;
        }

        ShardNode leader;

        if (
            !findNode(
                leaderId,
                leader
            )
        ) {
            cout
                << "ERROR: leader address unknown\n";

            return;
        }

        string response =
            TcpClient::request(
                leader.host,
                leader.port,

                "CLIENT_PUT " +
                key +
                " " +
                value
            );

        cout
            << "RAFT ROUTE -> "
            << leaderId
            << " | shard="
            << shardId
            << '\n';

        cout
            << response
            << '\n';
    }

    // ========================================================
    // GET
    //
    // Reads still use replica routing in this prototype.
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

        // ----------------------------------------------------
        // Try local replica first.
        // ----------------------------------------------------

        if (
            shardManager.nodeOwnsReplica(
                config.nodeId,
                shardId
            )
        ) {
            string value;

            if (
                storage.get(
                    key,
                    value
                )
            ) {
                cout
                    << "READ -> "
                    << config.nodeId
                    << " | shard="
                    << shardId
                    << '\n';

                cout
                    << value
                    << '\n';

                return;
            }
        }

        // ----------------------------------------------------
        // Try remote replicas.
        // ----------------------------------------------------

        for (
            const auto& replica :
            replicas
        ) {
            if (
                replica.nodeId ==
                config.nodeId
            ) {
                continue;
            }

            string response =
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
                response.rfind(
                    "VALUE ",
                    0
                ) == 0
            ) {
                cout
                    << "READ -> "
                    << replica.nodeId
                    << " | shard="
                    << shardId
                    << '\n';

                cout
                    << response.substr(6)
                    << '\n';

                return;
            }
        }

        cout
            << "NOT_FOUND\n";
    }

    // ========================================================
    // DELETE through Raft
    // ========================================================

    void erase(
        const string& key
    ) {
        size_t shardId =
            shardManager.shardForKey(
                key
            );

        if (
            raftNode.isLeader()
        ) {
            string command =
                "DELETE " +
                to_string(
                    shardId
                ) +
                " " +
                key;

            RaftProposalResult result =
                raftNode.propose(
                    command
                );

            cout
                << formatProposalResult(
                    result
                )
                << '\n';

            return;
        }

        string leaderId =
            raftNode.getLeaderId();

        if (
            leaderId.empty()
        ) {
            cout
                << "ERROR: no Raft leader available\n";

            return;
        }

        ShardNode leader;

        if (
            !findNode(
                leaderId,
                leader
            )
        ) {
            cout
                << "ERROR: leader address unknown\n";

            return;
        }

        string response =
            TcpClient::request(
                leader.host,
                leader.port,

                "CLIENT_DELETE " +
                key
            );

        cout
            << "RAFT ROUTE -> "
            << leaderId
            << " | shard="
            << shardId
            << '\n';

        cout
            << response
            << '\n';
    }

    // ========================================================
    // Status
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
                networkServer.isRunning()
                    ? "RUNNING"
                    : "STOPPED"
            )
            << '\n';

        cout
            << "Peers:          "
            << clusterManager.peerCount()
            << '\n';

        cout
            << "Virtual shards: "
            << shardManager.getShardCount()
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
            << "Raft term:      "
            << raftNode.getCurrentTerm()
            << '\n';

        cout
            << "Raft log:       "
            << raftNode.getLastLogIndex()
            << '\n';

        cout
            << "Commit index:   "
            << raftNode.getCommitIndex()
            << '\n';

        cout
            << "Last applied:   "
            << raftNode.getLastApplied()
            << '\n';

        cout
            << "Raft leader:    "
            << (
                raftNode.getLeaderId().empty()
                    ? "UNKNOWN"
                    : raftNode.getLeaderId()
            )
            << '\n';

        cout
            << "Storage:        In-memory\n\n";
    }

    void clusterStatus() const {
        clusterManager
            .printClusterStatus();
    }

    void raftStatus() const {
        raftNode.printStatus();
    }

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

    void pingNode(
        const string& host,
        int port
    ) const {
        cout
            << "PING "
            << host
            << ':'
            << port
            << " ... "
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
            stoi(
                portValue
            );
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
}

// ============================================================
// Arguments
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
                    << "Invalid port\n";

                return false;
            }
        }

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

            string value =
                argv[++i];

            if (
                !parsePeer(
                    value,
                    peer
                )
            ) {
                cerr
                    << "Invalid peer format: "
                    << value
                    << '\n';

                return false;
            }

            config.peers.push_back(
                peer
            );
        }

        else if (
            argument ==
                "--help" ||
            argument ==
                "-h"
        ) {
            printProgramHelp();

            exit(0);
        }

        else {
            cerr
                << "Unknown argument: "
                << argument
                << '\n';

            return false;
        }
    }

    for (
        const auto& peer :
        config.peers
    ) {
        if (
            peer.nodeId ==
            config.nodeId
        ) {
            cerr
                << "Cannot add local node as peer\n";

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
                    << "Usage: PUT <key> <value>\n";

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
                    << "Usage: PUT <key> <value>\n";

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
                !(input
                  >> host
                  >> port)
            ) {
                cout
                    << "Usage: PING <host> <port>\n";

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
                !(input
                  >> host
                  >> port)
            ) {
                cout
                    << "Usage: INFO <host> <port>\n";

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
                << "Unknown command. Type HELP\n";
        }
    }

    return 0;
}