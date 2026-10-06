
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>

using namespace std;

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

    void status() const {
        cout << "Storage Engine Status\n";
        cout << "Records: " << data.size() << '\n';
        cout << "Mode: In-memory\n";
    }
};

int main() {
    StorageEngine db;

    cout << "HydraDB v0.1.0\n";
    cout << "Type HELP for commands\n";

    string line;

    while (true) {
        cout << "hydra> ";

        if (!getline(cin, line)) {
            break;
        }

        istringstream input(line);
        string command, key;

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

            db.put(key, value);
        }
        else if (command == "GET" || command == "get") {
            if (!(input >> key)) {
                cout << "Usage: GET <key>\n";
                continue;
            }

            db.get(key);
        }
        else if (command == "DELETE" || command == "delete") {
            if (!(input >> key)) {
                cout << "Usage: DELETE <key>\n";
                continue;
            }

            db.erase(key);
        }
        else if (command == "STATUS" || command == "status") {
            db.status();
        }
        else if (command == "HELP" || command == "help") {
            cout << "PUT <key> <value>\n";
            cout << "GET <key>\n";
            cout << "DELETE <key>\n";
            cout << "STATUS\n";
            cout << "EXIT\n";
        }
        else if (command == "EXIT" || command == "exit") {
            cout << "Shutting down HydraDB\n";
            break;
        }
        else if (!command.empty()) {
            cout << "Unknown command. Type HELP\n";
        }
    }

    return 0;
}