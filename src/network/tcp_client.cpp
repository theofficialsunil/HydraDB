#include "tcp_client.h"

#include <iostream>
#include <string>

#ifdef _WIN32

#include <winsock2.h>
#include <ws2tcpip.h>

#pragma comment(lib, "ws2_32.lib")

#else

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#endif

namespace {

#ifdef _WIN32
using SocketHandle = SOCKET;
#else
using SocketHandle = int;
#endif

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

std::string sendRequest(
    const std::string& host,
    int port,
    const std::string& request
) {
    if (!initializeSockets()) {
        return "ERROR SOCKET_INIT";
    }

    SocketHandle socketHandle = socket(
        AF_INET,
        SOCK_STREAM,
        0
    );

#ifdef _WIN32
    if (socketHandle == INVALID_SOCKET) {
#else
    if (socketHandle < 0) {
#endif
        cleanupSockets();
        return "ERROR SOCKET_CREATE";
    }

    sockaddr_in serverAddress{};

    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port =
        htons(static_cast<unsigned short>(port));

    if (inet_pton(
            AF_INET,
            host.c_str(),
            &serverAddress.sin_addr) <= 0) {

        closeSocket(socketHandle);
        cleanupSockets();

        return "ERROR INVALID_ADDRESS";
    }

    if (connect(
            socketHandle,
            reinterpret_cast<sockaddr*>(&serverAddress),
            sizeof(serverAddress)) < 0) {

        closeSocket(socketHandle);
        cleanupSockets();

        return "ERROR CONNECTION_FAILED";
    }

    std::string requestWithNewline = request + "\n";

    int sent = send(
        socketHandle,
        requestWithNewline.c_str(),
        static_cast<int>(requestWithNewline.size()),
        0
    );

    if (sent <= 0) {
        closeSocket(socketHandle);
        cleanupSockets();

        return "ERROR SEND_FAILED";
    }

    char buffer[1024]{};

    int received = recv(
        socketHandle,
        buffer,
        sizeof(buffer) - 1,
        0
    );

    closeSocket(socketHandle);
    cleanupSockets();

    if (received <= 0) {
        return "ERROR RECEIVE_FAILED";
    }

    buffer[received] = '\0';

    std::string response(buffer);

    while (!response.empty() &&
           (response.back() == '\n' ||
            response.back() == '\r')) {
        response.pop_back();
    }

    return response;
}

} // namespace

std::string TcpClient::ping(
    const std::string& host,
    int port
) {
    return sendRequest(host, port, "PING");
}

std::string TcpClient::info(
    const std::string& host,
    int port
) {
    return sendRequest(host, port, "INFO");
}