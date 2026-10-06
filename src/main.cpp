#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>

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

public:
    explicit HydraNode(const NodeConfig& nodeConfig)
        : config(nodeConfig) {}

    void start() {
        state = "RUNNING";

        cout << "=================================\n";
        cout << "          HydraDB Node\n";
        cout << "=================================\n";
        cout << "Node ID:  " << config.nodeId << '\n';
        cout << "Address:  " << config.host << ':' << config.port << '\n';
        cout << "State:    " << state << '\n';
        cout << "Storage:  In-memory\n";
        cout << "=================================\n";
        cout << "Type HELP for commands\n\n";
    }

    void status() const {
        cout << "\nHydraDB Node Status\n";
        cout << "-------------------\n";
        cout << "Node ID:  " << config.nodeId << '\n';
        cout << "Address:  " << config.host << ':' << config.port << '\n';
        cout << "State:    " << state << '\n';
        cout << "Records:  " << storage.size() << '\n';
        cout << "Storage:  In-memory\n";
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
};

bool parseArguments(int argc, char* argv[], NodeConfig& config) {
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
        else if (argument == "--help" || argument == "-h") {
            cout << "HydraDB Node\n\n";
            cout << "Usage:\n";
            cout << "  hydra --node-id <id> --host <host> --port <port>\n\n";
            cout << "Options:\n";
            cout << "  --node-id <id>    Node identifier\n";
            cout << "  --host <host>     Node host address\n";
            cout << "  --port <port>     Node port\n";
            return false;
        }
        else {
            cerr << "Unknown argument: " << argument << '\n';
            return false;
        }
    }

    return true;
}

int main(int argc, char* argv[]) {
    NodeConfig config;

    if (!parseArguments(argc, argv, config)) {
        return 1;
    }

    HydraNode node(config);
    node.start();

    string line;

    while (true) {
        cout << "hydra> ";

        if (!getline(cin, line)) {
            break;
        }

        istringstream input(line);

        string command;
        string key;

        input >> command;

        if (command == "PUT" || command == "put") {
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
        else if (command == "GET" || command == "get") {
            if (!(input >> key)) {
                cout << "Usage: GET <key>\n";
                continue;
            }

            node.get(key);
        }
        else if (command == "DELETE" || command == "delete") {
            if (!(input >> key)) {
                cout << "Usage: DELETE <key>\n";
                continue;
            }

            node.erase(key);
        }
        else if (command == "STATUS" || command == "status") {
            node.status();
        }
        else if (command == "HELP" || command == "help") {
            cout << "\nCommands:\n";
            cout << "  PUT <key> <value>\n";
            cout << "  GET <key>\n";
            cout << "  DELETE <key>\n";
            cout << "  STATUS\n";
            cout << "  HELP\n";
            cout << "  EXIT\n\n";
        }
        else if (command == "EXIT" || command == "exit") {
            cout << "Shutting down HydraDB node\n";
            break;
        }
        else if (!command.empty()) {
            cout << "Unknown command. Type HELP\n";
        }
    }

    return 0;
}