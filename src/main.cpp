#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include "network/tcp_client.h"
#include "network/tcp_server.h"

using namespace std;

struct NodeConfig {
    string nodeId = "node-1";
    string host = "127.0.0.1";
    int port = 5001;
};

class StorageEngine {
private:
    unordered_map<string, string> data;

public:
    void put(const string& key, const string& value) {
        data[key] = value;
        cout << "OK: Value stored\n";
    }

    void get(const string& key) const {
        auto it = data.find(key);

        if (it == data.end()) {
            cout << "NOT FOUND\n";
            return;
        }

        cout << it->second << '\n';
    }

    void erase(const string& key) {
        if (data.erase(key)) {
            cout << "OK: Key deleted\n";
        } else {
            cout << "NOT FOUND\n";
        }
    }

    size_t size() const {
        return data.size();
    }
};

class HydraNode {
private:
    NodeConfig config;
    StorageEngine storage;
    string state = "STARTING";

    TcpServer networkServer;

public:
    explicit HydraNode(const NodeConfig& nodeConfig)
        : config(nodeConfig),
          networkServer(nodeConfig.nodeId, nodeConfig.port) {}

    bool start() {
        if (!networkServer.start()) {
            state = "FAILED";
            return false;
        }

        state = "RUNNING";

        cout << "=================================\n";
        cout << "          HydraDB Node\n";
        cout << "=================================\n";
        cout << "Node ID:  " << config.nodeId << '\n';
        cout << "Address:  " << config.host << ':' << config.port << '\n';
        cout << "State:    " << state << '\n';
        cout << "Storage:  In-memory\n";
        cout << "Network:  TCP\n";
        cout << "=================================\n";
        cout << "Type HELP for commands\n\n";

        return true;
    }

    void status() const {
        cout << "\nHydraDB Node Status\n";
        cout << "-------------------\n";
        cout << "Node ID:  " << config.nodeId << '\n';
        cout << "Address:  " << config.host << ':' << config.port << '\n';
        cout << "State:    " << state << '\n';

        cout << "Network:  "
             << (networkServer.isRunning() ? "RUNNING" : "STOPPED")
             << '\n';

        cout << "Records:  " << storage.size() << '\n';
        cout << "Storage:  In-memory\n\n";
    }

    void put(const string& key, const string& value) {
        storage.put(key, value);
    }

    void get(const string& key) const {
        storage.get(key);
    }

    void erase(const string& key) {
        storage.erase(key);
    }

    void pingNode(const string& host, int port) const {
        cout << "PING "
             << host
             << ':'
             << port
             << " ... ";

        string response = TcpClient::ping(host, port);

        cout << response << '\n';
    }

    void infoNode(const string& host, int port) const {
        string response = TcpClient::info(host, port);

        cout << response << '\n';
    }
};

bool parseArguments(
    int argc,
    char* argv[],
    NodeConfig& config
) {
    for (int i = 1; i < argc; ++i) {
        string argument = argv[i];

        if (argument == "--node-id") {
            if (i + 1 >= argc) {
                cerr << "Missing value for --node-id\n";
                return false;
            }

            config.nodeId = argv[++i];
        }
        else if (argument == "--host") {
            if (i + 1 >= argc) {
                cerr << "Missing value for --host\n";
                return false;
            }

            config.host = argv[++i];
        }
        else if (argument == "--port") {
            if (i + 1 >= argc) {
                cerr << "Missing value for --port\n";
                return false;
            }

            try {
                config.port = stoi(argv[++i]);
            }
            catch (...) {
                cerr << "Invalid port\n";
                return false;
            }

            if (config.port < 1 || config.port > 65535) {
                cerr << "Port must be between 1 and 65535\n";
                return false;
            }
        }
        else if (argument == "--help" ||
                 argument == "-h") {

            cout << "HydraDB Node\n\n";

            cout << "Usage:\n";
            cout << "  hydra "
                 << "--node-id <id> "
                 << "--host <host> "
                 << "--port <port>\n\n";

            cout << "Options:\n";
            cout << "  --node-id <id>    Node identifier\n";
            cout << "  --host <host>     Node host address\n";
            cout << "  --port <port>     Node TCP port\n";

            return false;
        }
        else {
            cerr << "Unknown argument: "
                 << argument
                 << '\n';

            return false;
        }
    }

    return true;
}

void printHelp() {
    cout << "\nCommands\n";
    cout << "-----------------------------\n";

    cout << "PUT <key> <value>\n";
    cout << "GET <key>\n";
    cout << "DELETE <key>\n";

    cout << "PING <host> <port>\n";
    cout << "INFO <host> <port>\n";

    cout << "STATUS\n";
    cout << "HELP\n";
    cout << "EXIT\n\n";
}

int main(int argc, char* argv[]) {
    NodeConfig config;

    if (!parseArguments(argc, argv, config)) {
        return 1;
    }

    HydraNode node(config);

    if (!node.start()) {
        cerr << "Failed to start HydraDB node\n";
        return 1;
    }

    string line;

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

        if (command == "PUT" ||
            command == "put") {

            string key;

            if (!(input >> key)) {
                cout << "Usage: PUT <key> <value>\n";
                continue;
            }

            string value;

            getline(input >> ws, value);

            if (value.empty()) {
                cout << "Usage: PUT <key> <value>\n";
                continue;
            }

            node.put(key, value);
        }

        else if (command == "GET" ||
                 command == "get") {

            string key;

            if (!(input >> key)) {
                cout << "Usage: GET <key>\n";
                continue;
            }

            node.get(key);
        }

        else if (command == "DELETE" ||
                 command == "delete") {

            string key;

            if (!(input >> key)) {
                cout << "Usage: DELETE <key>\n";
                continue;
            }

            node.erase(key);
        }

        else if (command == "PING" ||
                 command == "ping") {

            string host;
            int port;

            if (!(input >> host >> port)) {
                cout << "Usage: PING <host> <port>\n";
                continue;
            }

            if (port < 1 || port > 65535) {
                cout << "Invalid port\n";
                continue;
            }

            node.pingNode(host, port);
        }

        else if (command == "INFO" ||
                 command == "info") {

            string host;
            int port;

            if (!(input >> host >> port)) {
                cout << "Usage: INFO <host> <port>\n";
                continue;
            }

            if (port < 1 || port > 65535) {
                cout << "Invalid port\n";
                continue;
            }

            node.infoNode(host, port);
        }

        else if (command == "STATUS" ||
                 command == "status") {

            node.status();
        }

        else if (command == "HELP" ||
                 command == "help") {

            printHelp();
        }

        else if (command == "EXIT" ||
                 command == "exit") {

            cout << "Shutting down HydraDB node\n";
            break;
        }

        else {
            cout << "Unknown command. Type HELP\n";
        }
    }

    return 0;
}