#pragma once

#include <atomic>
#include <functional>
#include <string>
#include <thread>

#ifdef _WIN32
#include <winsock2.h>
using SocketHandle = SOCKET;
#else
using SocketHandle = int;
#endif

class TcpServer {
public:
    using RequestHandler =
        std::function<
            std::string(
                const std::string&
            )
        >;

private:
    std::string nodeId;

    int port;

    RequestHandler requestHandler;

    std::atomic<bool> running{false};

    std::thread serverThread;

    SocketHandle serverSocket;

    void run();

    void handleClient(
        SocketHandle clientSocket
    );

public:
    TcpServer(
        const std::string& nodeId,
        int port,
        RequestHandler requestHandler
    );

    ~TcpServer();

    bool start();

    void stop();

    bool isRunning() const;
};