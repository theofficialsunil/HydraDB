#include "tcp_client.h"

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

#ifdef _WIN32
using SocketHandle = SOCKET;
#else
using SocketHandle = int;
#endif

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
    string response;

    char buffer[1024];

    constexpr size_t maxResponseSize =
        64 * 1024;

    while (
        response.size() <
        maxResponseSize
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

        response.append(
            buffer,
            static_cast<size_t>(received)
        );

        size_t newline =
            response.find('\n');

        if (newline != string::npos) {
            response.resize(newline);
            break;
        }
    }

    if (
        !response.empty() &&
        response.back() == '\r'
    ) {
        response.pop_back();
    }

    return response;
}

} // namespace

string TcpClient::request(
    const string& host,
    int port,
    const string& requestText
) {
    if (!initializeSockets()) {
        return "ERROR SOCKET_INIT";
    }

    SocketHandle socketHandle =
        socket(
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

    serverAddress.sin_family =
        AF_INET;

    serverAddress.sin_port =
        htons(
            static_cast<unsigned short>(
                port
            )
        );

    if (
        inet_pton(
            AF_INET,
            host.c_str(),
            &serverAddress.sin_addr
        ) <= 0
    ) {
        closeSocket(socketHandle);

        cleanupSockets();

        return "ERROR INVALID_ADDRESS";
    }

    if (
        connect(
            socketHandle,
            reinterpret_cast<sockaddr*>(
                &serverAddress
            ),
            sizeof(serverAddress)
        ) < 0
    ) {
        closeSocket(socketHandle);

        cleanupSockets();

        return "ERROR CONNECTION_FAILED";
    }

    string payload =
        requestText + "\n";

    if (
        !sendAll(
            socketHandle,
            payload
        )
    ) {
        closeSocket(socketHandle);

        cleanupSockets();

        return "ERROR SEND_FAILED";
    }

    string response =
        receiveLine(socketHandle);

    closeSocket(socketHandle);

    cleanupSockets();

    if (response.empty()) {
        return "ERROR RECEIVE_FAILED";
    }

    return response;
}

string TcpClient::ping(
    const string& host,
    int port
) {
    return request(
        host,
        port,
        "PING"
    );
}

string TcpClient::info(
    const string& host,
    int port
) {
    return request(
        host,
        port,
        "INFO"
    );
}