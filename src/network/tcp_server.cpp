#include "tcp_server.h"

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

using namespace std;

namespace {

void closeSocket(
    SocketHandle socketHandle
) {
#ifdef _WIN32
    closesocket(socketHandle);
#else
    close(socketHandle);
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

bool sendAll(
    SocketHandle socketHandle,
    const string& data
) {
    size_t totalSent = 0;

    while (totalSent < data.size()) {

        int sent = send(
            socketHandle,
            data.data() + totalSent,
            static_cast<int>(
                data.size() - totalSent
            ),
            0
        );

        if (sent <= 0) {
            return false;
        }

        totalSent +=
            static_cast<size_t>(sent);
    }

    return true;
}

string receiveLine(
    SocketHandle socketHandle
) {
    string request;

    char buffer[1024];

    constexpr size_t maxRequestSize =
        64 * 1024;

    while (
        request.size() <
        maxRequestSize
    ) {
        int received = recv(
            socketHandle,
            buffer,
            sizeof(buffer),
            0
        );

        if (received <= 0) {
            break;
        }

        request.append(
            buffer,
            static_cast<size_t>(received)
        );

        size_t newline =
            request.find('\n');

        if (newline != string::npos) {
            request.resize(newline);
            break;
        }
    }

    if (
        !request.empty() &&
        request.back() == '\r'
    ) {
        request.pop_back();
    }

    return request;
}

} // namespace

TcpServer::TcpServer(
    const string& nodeId,
    int port,
    RequestHandler requestHandler
)
    : nodeId(nodeId),
      port(port),
      requestHandler(
          std::move(requestHandler)
      )
#ifdef _WIN32
      ,
      serverSocket(INVALID_SOCKET)
#else
      ,
      serverSocket(-1)
#endif
{
}

TcpServer::~TcpServer() {
    stop();
}

bool TcpServer::start() {
    if (running) {
        return true;
    }

    if (!initializeSockets()) {
        cerr
            << "Failed to initialize sockets\n";

        return false;
    }

    serverSocket =
        socket(
            AF_INET,
            SOCK_STREAM,
            0
        );

#ifdef _WIN32
    if (
        serverSocket ==
        INVALID_SOCKET
    ) {
#else
    if (serverSocket < 0) {
#endif
        cerr
            << "Failed to create server socket\n";

        cleanupSockets();

        return false;
    }

    int reuse = 1;

#ifdef _WIN32
    setsockopt(
        serverSocket,
        SOL_SOCKET,
        SO_REUSEADDR,
        reinterpret_cast<const char*>(
            &reuse
        ),
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

    address.sin_family =
        AF_INET;

    address.sin_addr.s_addr =
        htonl(INADDR_ANY);

    address.sin_port =
        htons(
            static_cast<unsigned short>(
                port
            )
        );

    if (
        bind(
            serverSocket,
            reinterpret_cast<sockaddr*>(
                &address
            ),
            sizeof(address)
        ) < 0
    ) {
        cerr
            << "Failed to bind port "
            << port
            << '\n';

        closeSocket(serverSocket);

        cleanupSockets();

        return false;
    }

    if (
        listen(
            serverSocket,
            32
        ) < 0
    ) {
        cerr
            << "Failed to listen on port "
            << port
            << '\n';

        closeSocket(serverSocket);

        cleanupSockets();

        return false;
    }

    running = true;

    serverThread =
        thread(
            &TcpServer::run,
            this
        );

    return true;
}

void TcpServer::run() {
    cout
        << "Network server listening on port "
        << port
        << '\n';

    while (running) {

        sockaddr_in clientAddress{};

#ifdef _WIN32
        int clientAddressLength =
            sizeof(clientAddress);
#else
        socklen_t clientAddressLength =
            sizeof(clientAddress);
#endif

        SocketHandle clientSocket =
            accept(
                serverSocket,
                reinterpret_cast<sockaddr*>(
                    &clientAddress
                ),
                &clientAddressLength
            );

#ifdef _WIN32
        if (
            clientSocket ==
            INVALID_SOCKET
        ) {
#else
        if (clientSocket < 0) {
#endif
            if (running) {
                cerr
                    << "Failed to accept connection\n";
            }

            continue;
        }

        thread(
            &TcpServer::handleClient,
            this,
            clientSocket
        ).detach();
    }
}

void TcpServer::handleClient(
    SocketHandle clientSocket
) {
    string request =
        receiveLine(clientSocket);

    if (request.empty()) {
        closeSocket(clientSocket);
        return;
    }

    string response;

    if (request == "PING") {
        response =
            "PONG " + nodeId;
    }

    else if (request == "INFO") {
        response =
            "NODE_ID=" +
            nodeId +
            " PORT=" +
            to_string(port);
    }

    else if (requestHandler) {
        response =
            requestHandler(request);
    }

    else {
        response =
            "ERROR UNKNOWN_COMMAND";
    }

    response += '\n';

    sendAll(
        clientSocket,
        response
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
    serverSocket =
        INVALID_SOCKET;
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