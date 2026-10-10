#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>
#include "cluster/cluster_manager.h"
#include "network/tcp_client.h"
#include "network/tcp_server.h"

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
// Temporary in-memory engine.
// Later this will become WAL + MemTable + SSTables.
// ============================================================

class StorageEngine {
private:
    unordered_map<string, string> data;

public:
    void put(
        const string& key,
        const string& value
    ) {
        data[key] = value;
        cout << "OK: Value stored\n";
    }

    void get(
        const string& key
    ) const {
        auto it = data.find(key);

        if (it == data.end()) {
            cout << "NOT FOUND\n";
            return;
        }

        cout << it->second << '\n';
    }

    void erase(
        const string& key
    ) {
        if (data.erase(key)) {
            cout << "OK: Key deleted\n";
        }
        else {
            cout << "NOT FOUND\n";
        }
    }

    size_t size() const {
        return data.size();
    }
};

// ============================================================
// HydraDB Node
// ============================================================

class HydraNode {
private:
    NodeConfig config;
    StorageEngine storage;
    string state = "STARTING";
    TcpServer networkServer;
    ClusterManager clusterManager;

public:
    explicit HydraNode(
        const NodeConfig& nodeConfig
    )
        : config(nodeConfig),
          networkServer(
              nodeConfig.nodeId,
              nodeConfig.port
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

        cout << "=================================\n";
        cout << "          HydraDB Node\n";
        cout << "=================================\n";

        cout << "Node ID:  "
             << config.nodeId
             << '\n';

        cout << "Address:  "
             << config.host
             << ':'
             << config.port
             << '\n';

        cout << "State:    "
             << state
             << '\n';

        cout << "Storage:  In-memory\n";

        cout << "Network:  TCP\n";

        cout << "Peers:    "
             << clusterManager.peerCount()
             << '\n';

        cout << "=================================\n";

        cout << "Type HELP for commands\n\n";

        return true;
    }

    void status() const {
        cout << "\nHydraDB Node Status\n";
        cout << "---------------------------------\n";

        cout << "Node ID:  "
             << config.nodeId
             << '\n';

        cout << "Address:  "
             << config.host
             << ':'
             << config.port
             << '\n';

        cout << "State:    "
             << state
             << '\n';

        cout << "Network:  "
             << (
                    networkServer.isRunning()
                    ? "RUNNING"
                    : "STOPPED"
                )
             << '\n';

        cout << "Peers:    "
             << clusterManager.peerCount()
             << '\n';

        cout << "Records:  "
             << storage.size()
             << '\n';

        cout << "Storage:  In-memory\n\n";
    }

    void clusterStatus() const {
        clusterManager.printClusterStatus();
    }

    void put(
        const string& key,
        const string& value
    ) {
        storage.put(key, value);
    }

    void get(
        const string& key
    ) const {
        storage.get(key);
    }

    void erase(
        const string& key
    ) {
        storage.erase(key);
    }

    void pingNode(
        const string& host,
        int port
    ) const {
        cout << "PING "
             << host
             << ':'
             << port
             << " ... ";

        string response =
            TcpClient::ping(host, port);

        cout << response << '\n';
    }

    void infoNode(
        const string& host,
        int port
    ) const {
        string response =
            TcpClient::info(host, port);

        cout << response << '\n';
    }
};

// ============================================================
// Peer Parsing
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

    if (atPosition == string::npos) {
        return false;
    }

    size_t colonPosition =
        value.find(
            ':',
            atPosition + 1
        );

    if (colonPosition == string::npos) {
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
            colonPosition - atPosition - 1
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
        peer.port = stoi(portValue);
    }
    catch (...) {
        return false;
    }

    if (
        peer.port < 1 ||
        peer.port > 65535
    ) {
        return false;
    }

    return true;
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
        << "  --node-id <id>          "
        << "Node identifier\n";

    cout
        << "  --host <host>           "
        << "Node host address\n";

    cout
        << "  --port <port>           "
        << "Node TCP port\n";

    cout
        << "  --peer <id@host:port>   "
        << "Cluster peer; may be repeated\n";

    cout
        << "  --help, -h              "
        << "Show this help\n";
}

// ============================================================
// Command-line Argument Parsing
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

        if (argument == "--node-id") {

            if (i + 1 >= argc) {
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

        else if (argument == "--host") {

            if (i + 1 >= argc) {
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

        else if (argument == "--port") {

            if (i + 1 >= argc) {
                cerr
                    << "Missing value for --port\n";

                return false;
            }

            try {
                config.port =
                    stoi(argv[++i]);
            }
            catch (...) {
                cerr << "Invalid port\n";
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

        else if (argument == "--peer") {

            if (i + 1 >= argc) {
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
                    << "Expected format: "
                    << "node-id@host:port\n";

                return false;
            }

            if (
                peer.nodeId ==
                config.nodeId
            ) {
                cerr
                    << "Cannot add local node "
                    << "as its own peer\n";

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
            argument == "--help" ||
            argument == "-h"
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

    return true;
}

// ============================================================
// Interactive CLI Help
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
        << "PING <host> <port>\n";

    cout
        << "INFO <host> <port>\n";

    cout
        << "STATUS\n";

    cout
        << "CLUSTER\n";

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

    HydraNode node(config);

    if (!node.start()) {
        cerr
            << "Failed to start HydraDB node\n";

        return 1;
    }

    string line;

    // ========================================================
    // CLI Loop
    // ========================================================

    while (true) {

        cout << "hydra> ";

        if (!getline(cin, line)) {
            break;
        }

        istringstream input(line);

        string command;

        input >> command;

        if (command.empty()) {
            continue;
        }

        // ----------------------------------------------------
        // PUT
        // ----------------------------------------------------

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

            if (value.empty()) {
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

        // ----------------------------------------------------
        // GET
        // ----------------------------------------------------

        else if (
            command == "GET" ||
            command == "get"
        ) {
            string key;

            if (!(input >> key)) {
                cout
                    << "Usage: "
                    << "GET <key>\n";

                continue;
            }

            node.get(key);
        }

        // ----------------------------------------------------
        // DELETE
        // ----------------------------------------------------

        else if (
            command == "DELETE" ||
            command == "delete"
        ) {
            string key;

            if (!(input >> key)) {
                cout
                    << "Usage: "
                    << "DELETE <key>\n";

                continue;
            }

            node.erase(key);
        }

        // ----------------------------------------------------
        // Manual PING
        // ----------------------------------------------------

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

        // ----------------------------------------------------
        // Node INFO
        // ----------------------------------------------------

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

        // ----------------------------------------------------
        // Local Node Status
        // ----------------------------------------------------

        else if (
            command == "STATUS" ||
            command == "status"
        ) {
            node.status();
        }

        // ----------------------------------------------------
        // Cluster Membership
        // ----------------------------------------------------

        else if (
            command == "CLUSTER" ||
            command == "cluster"
        ) {
            node.clusterStatus();
        }

        // ----------------------------------------------------
        // Help
        // ----------------------------------------------------

        else if (
            command == "HELP" ||
            command == "help"
        ) {
            printCommandHelp();
        }

        // ----------------------------------------------------
        // Exit
        // ----------------------------------------------------

        else if (
            command == "EXIT" ||
            command == "exit"
        ) {
            cout
                << "Shutting down HydraDB node\n";

            break;
        }

        // ----------------------------------------------------
        // Unknown
        // ----------------------------------------------------

        else {
            cout
                << "Unknown command. "
                << "Type HELP\n";
        }
    }

    return 0;
}