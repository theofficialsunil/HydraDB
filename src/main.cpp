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
// Local Storage Engine
//
// Still temporary.
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
        lock_guard<mutex> lock(
            dataMutex
        );

        data[key] = value;
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

        if (it == data.end()) {
            return false;
        }

        value = it->second;

        return true;
    }

    bool erase(
        const string& key
    ) {
        lock_guard<mutex> lock(
            dataMutex
        );

        return data.erase(key) > 0;
    }

    size_t size() const {
        lock_guard<mutex> lock(
            dataMutex
        );

        return data.size();
    }
};

// ============================================================
// Helpers
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

    string state = "STARTING";

    ShardManager shardManager;

    TcpServer networkServer;

    ClusterManager clusterManager;

    // --------------------------------------------------------
    // Request coming from another HydraDB node.
    // --------------------------------------------------------

    string handleNetworkRequest(
        const string& request
    ) {
        istringstream input(
            request
        );

        string command;

        input >> command;

        // ====================================================
        // Remote PUT
        // ====================================================

        if (command == "KVPUT") {

            string key;

            if (!(input >> key)) {
                return "ERROR INVALID_KVPUT";
            }

            string value;

            getline(
                input >> ws,
                value
            );

            if (value.empty()) {
                return "ERROR INVALID_KVPUT";
            }

            ShardNode owner =
                shardManager.ownerForKey(
                    key
                );

            if (
                owner.nodeId !=
                config.nodeId
            ) {
                return
                    "ERROR WRONG_OWNER " +
                    owner.nodeId;
            }

            storage.put(
                key,
                value
            );

            size_t shardId =
                shardManager.shardForKey(
                    key
                );

            return
                "OK STORED " +
                config.nodeId +
                " SHARD " +
                to_string(shardId);
        }

        // ====================================================
        // Remote GET
        // ====================================================

        if (command == "KVGET") {

            string key;

            if (!(input >> key)) {
                return "ERROR INVALID_KVGET";
            }

            ShardNode owner =
                shardManager.ownerForKey(
                    key
                );

            if (
                owner.nodeId !=
                config.nodeId
            ) {
                return
                    "ERROR WRONG_OWNER " +
                    owner.nodeId;
            }

            string value;

            if (
                !storage.get(
                    key,
                    value
                )
            ) {
                return "NOT_FOUND";
            }

            return
                "VALUE " +
                value;
        }

        // ====================================================
        // Remote DELETE
        // ====================================================

        if (command == "KVDELETE") {

            string key;

            if (!(input >> key)) {
                return
                    "ERROR INVALID_KVDELETE";
            }

            ShardNode owner =
                shardManager.ownerForKey(
                    key
                );

            if (
                owner.nodeId !=
                config.nodeId
            ) {
                return
                    "ERROR WRONG_OWNER " +
                    owner.nodeId;
            }

            if (
                storage.erase(key)
            ) {
                return "OK DELETED";
            }

            return "NOT_FOUND";
        }

        return "ERROR UNKNOWN_COMMAND";
    }

public:
    explicit HydraNode(
        const NodeConfig& nodeConfig
    )
        : config(nodeConfig),

          shardManager(
              buildShardNodes(
                  nodeConfig
              ),
              1024
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
          ) {
    }

    bool start() {
        if (!networkServer.start()) {
            state = "FAILED";

            return false;
        }

        clusterManager.start();

        state = "RUNNING";

        cout
            << "=================================\n";

        cout
            << "          HydraDB Node\n";

        cout
            << "=================================\n";

        cout
            << "Node ID:  "
            << config.nodeId
            << '\n';

        cout
            << "Address:  "
            << config.host
            << ':'
            << config.port
            << '\n';

        cout
            << "State:    "
            << state
            << '\n';

        cout
            << "Storage:  In-memory\n";

        cout
            << "Network:  TCP\n";

        cout
            << "Peers:    "
            << clusterManager.peerCount()
            << '\n';

        cout
            << "Shards:   "
            << shardManager.getShardCount()
            << '\n';

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

        ShardNode owner =
            shardManager.ownerForKey(
                key
            );

        if (
            owner.nodeId ==
            config.nodeId
        ) {
            storage.put(
                key,
                value
            );

            cout
                << "OK: stored locally"
                << " | shard="
                << shardId
                << " | owner="
                << owner.nodeId
                << '\n';

            return;
        }

        string response =
            TcpClient::request(
                owner.host,
                owner.port,
                "KVPUT " +
                    key +
                    " " +
                    value
            );

        cout
            << "ROUTED -> "
            << owner.nodeId
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

        ShardNode owner =
            shardManager.ownerForKey(
                key
            );

        if (
            owner.nodeId ==
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

        string response =
            TcpClient::request(
                owner.host,
                owner.port,
                "KVGET " +
                    key
            );

        cout
            << "ROUTED -> "
            << owner.nodeId
            << " | shard="
            << shardId
            << '\n';

        if (
            response.rfind(
                "VALUE ",
                0
            ) == 0
        ) {
            cout
                << response.substr(6)
                << '\n';
        }
        else {
            cout
                << response
                << '\n';
        }
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

        ShardNode owner =
            shardManager.ownerForKey(
                key
            );

        if (
            owner.nodeId ==
            config.nodeId
        ) {
            if (
                storage.erase(key)
            ) {
                cout
                    << "OK: deleted locally\n";
            }
            else {
                cout
                    << "NOT_FOUND\n";
            }

            return;
        }

        string response =
            TcpClient::request(
                owner.host,
                owner.port,
                "KVDELETE " +
                    key
            );

        cout
            << "ROUTED -> "
            << owner.nodeId
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
            << "Node ID:       "
            << config.nodeId
            << '\n';

        cout
            << "Address:       "
            << config.host
            << ':'
            << config.port
            << '\n';

        cout
            << "State:         "
            << state
            << '\n';

        cout
            << "Network:       "
            << (
                networkServer.isRunning()
                ? "RUNNING"
                : "STOPPED"
            )
            << '\n';

        cout
            << "Peers:         "
            << clusterManager.peerCount()
            << '\n';

        cout
            << "Virtual shards:"
            << ' '
            << shardManager.getShardCount()
            << '\n';

        cout
            << "Local records: "
            << storage.size()
            << '\n';

        cout
            << "Storage:       In-memory\n\n";
    }

    void clusterStatus() const {
        clusterManager.printClusterStatus();
    }

    void route(
        const string& key
    ) const {
        shardManager.printRoute(
            key
        );
    }

    void shardStatus() const {
        shardManager.printDistribution();
    }

    // ========================================================
    // Networking diagnostics
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
    cout << "HydraDB Node\n\n";

    cout << "Usage:\n";

    cout
        << "  hydra "
        << "--node-id <id> "
        << "--host <host> "
        << "--port <port> "
        << "[--peer <id@host:port> ...]\n\n";

    cout << "Options:\n";

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
                    stoi(argv[++i]);
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
                    << "Port must be between 1 and 65535\n";

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

                return false;
            }

            if (
                peer.nodeId ==
                config.nodeId
            ) {
                cerr
                    << "Cannot add local node as peer\n";

                return false;
            }

            config.peers.push_back(
                peer
            );
        }

        else if (
            argument == "--help" ||
            argument == "-h"
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

    return true;
}

// ============================================================
// CLI Help
// ============================================================

void printCommandHelp() {
    cout << "\nHydraDB Commands\n";

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
        << "SHARDS\n";

    cout
        << "CLUSTER\n";

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

    if (!node.start()) {
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

        if (command.empty()) {
            continue;
        }

        // ====================================================
        // PUT
        // ====================================================

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

            if (value.empty()) {
                cout
                    << "Usage: PUT <key> <value>\n";

                continue;
            }

            node.put(
                key,
                value
            );
        }

        // ====================================================
        // GET
        // ====================================================

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

        // ====================================================
        // DELETE
        // ====================================================

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

        // ====================================================
        // ROUTE
        // ====================================================

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

        // ====================================================
        // SHARDS
        // ====================================================

        else if (
            command == "SHARDS" ||
            command == "shards"
        ) {
            node.shardStatus();
        }

        // ====================================================
        // CLUSTER
        // ====================================================

        else if (
            command == "CLUSTER" ||
            command == "cluster"
        ) {
            node.clusterStatus();
        }

        // ====================================================
        // STATUS
        // ====================================================

        else if (
            command == "STATUS" ||
            command == "status"
        ) {
            node.status();
        }

        // ====================================================
        // PING
        // ====================================================

        else if (
            command == "PING" ||
            command == "ping"
        ) {
            string host;
            int port;

            if (
                !(input >> host >> port)
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

        // ====================================================
        // INFO
        // ====================================================

        else if (
            command == "INFO" ||
            command == "info"
        ) {
            string host;
            int port;

            if (
                !(input >> host >> port)
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

        // ====================================================
        // HELP
        // ====================================================

        else if (
            command == "HELP" ||
            command == "help"
        ) {
            printCommandHelp();
        }

        // ====================================================
        // EXIT
        // ====================================================

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