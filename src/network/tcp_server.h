#pragma once

#include <atomic>
#include <string>
#include <thread>

#ifdef _WIN32
#include <winsock2.h>
using SocketHandle = SOCKET;
#else
using SocketHandle = int;
#endif

class TcpServer {
private:
    std::string nodeId;
    int port;

    std::atomic<bool> running{false};
    std::thread serverThread;

    SocketHandle serverSocket;

    void run();
    void handleClient(SocketHandle clientSocket);

public:
    TcpServer(const std::string& nodeId, int port);

    ~TcpServer();

    bool start();
    void stop();

    bool isRunning() const;
};