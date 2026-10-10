#include "tcp_server.h"

#include <cstring>
#include <iostream>
#include <string>

#ifdef _WIN32

#include <winsock2.h>
#include <ws2tcpip.h>

#pragma comment(lib, "ws2_32.lib")

#else

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#endif
namespace {
void closeSocket(SocketHandle socket) {
#ifdef _WIN32
    closesocket(socket);
#else
    close(socket);
#endif
}

bool initializeSockets() {
#ifdef _WIN32
    WSADATA wsaData;

    return WSAStartup(
        MAKEWORD(2, 2),
        &wsaData
    ) == 0;
#else
    return true;
#endif
}

void cleanupSockets() {
#ifdef _WIN32
    WSACleanup();
#endif
}

} // namespace

TcpServer::TcpServer(const std::string& nodeId, int port)
    : nodeId(nodeId),
      port(port),
#ifdef _WIN32
      serverSocket(INVALID_SOCKET) {
#else
      serverSocket(-1) {
#endif
}

TcpServer::~TcpServer() {
    stop();
}

bool TcpServer::start() {
    if (running) {
        return true;
    }

    if (!initializeSockets()) {
        std::cerr << "Failed to initialize socket subsystem\n";
        return false;
    }

    serverSocket = socket(
        AF_INET,
        SOCK_STREAM,
        0
    );

#ifdef _WIN32
    if (serverSocket == INVALID_SOCKET) {
#else
    if (serverSocket < 0) {
#endif
        std::cerr << "Failed to create server socket\n";
        cleanupSockets();
        return false;
    }

    int reuse = 1;

#ifdef _WIN32
    setsockopt(
        serverSocket,
        SOL_SOCKET,
        SO_REUSEADDR,
        reinterpret_cast<const char*>(&reuse),
        sizeof(reuse)
    );
#else
    setsockopt(
        serverSocket,
        SOL_SOCKET,
        SO_REUSEADDR,
        &reuse,
        sizeof(reuse)
    );
#endif

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(static_cast<unsigned short>(port));

    if (bind(
            serverSocket,
            reinterpret_cast<sockaddr*>(&address),
            sizeof(address)) < 0) {

        std::cerr << "Failed to bind port " << port << '\n';

        closeSocket(serverSocket);
        cleanupSockets();

        return false;
    }

    if (listen(serverSocket, 16) < 0) {
        std::cerr << "Failed to listen on port " << port << '\n';

        closeSocket(serverSocket);
        cleanupSockets();

        return false;
    }

    running = true;

    serverThread = std::thread(&TcpServer::run, this);

    return true;
}

void TcpServer::run() {
    std::cout << "Network server listening on port "
              << port << '\n';

    while (running) {
        sockaddr_in clientAddress{};

#ifdef _WIN32
        int clientAddressLength = sizeof(clientAddress);
#else
        socklen_t clientAddressLength = sizeof(clientAddress);
#endif

        SocketHandle clientSocket = accept(
            serverSocket,
            reinterpret_cast<sockaddr*>(&clientAddress),
            &clientAddressLength
        );

#ifdef _WIN32
        if (clientSocket == INVALID_SOCKET) {
#else
        if (clientSocket < 0) {
#endif
            if (running) {
                std::cerr << "Failed to accept connection\n";
            }

            continue;
        }

        std::thread(
            &TcpServer::handleClient,
            this,
            clientSocket
        ).detach();
    }
}

void TcpServer::handleClient(SocketHandle clientSocket) {
    char buffer[1024]{};

    int received = recv(
        clientSocket,
        buffer,
        sizeof(buffer) - 1,
        0
    );

    if (received <= 0) {
        closeSocket(clientSocket);
        return;
    }

    buffer[received] = '\0';

    std::string request(buffer);

    while (!request.empty() &&
           (request.back() == '\n' ||
            request.back() == '\r')) {
        request.pop_back();
    }

    std::string response;

    if (request == "PING") {
        response = "PONG " + nodeId + "\n";
    }
    else if (request == "INFO") {
        response =
            "NODE_ID=" + nodeId +
            " PORT=" + std::to_string(port) +
            "\n";
    }
    else {
        response = "ERROR UNKNOWN_COMMAND\n";
    }

    send(
        clientSocket,
        response.c_str(),
        static_cast<int>(response.size()),
        0
    );

    closeSocket(clientSocket);
}

void TcpServer::stop() {
    if (!running) {
        return;
    }

    running = false;

    closeSocket(serverSocket);

#ifdef _WIN32
    serverSocket = INVALID_SOCKET;
#else
    serverSocket = -1;
#endif

    if (serverThread.joinable()) {
        serverThread.join();
    }

    cleanupSockets();
}

bool TcpServer::isRunning() const {
    return running;
}