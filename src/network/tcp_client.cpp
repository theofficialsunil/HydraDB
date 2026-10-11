#include "tcp_client.h"

#include <cerrno>
#include <cstring>
#include <string>

#ifdef _WIN32

#include <winsock2.h>
#include <ws2tcpip.h>

#pragma comment(lib, "ws2_32.lib")

#else

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

#endif

using namespace std;

namespace {

// ============================================================
// Platform Socket Type
// ============================================================

#ifdef _WIN32

using SocketHandle =
    SOCKET;

constexpr SocketHandle
    InvalidSocket =
        INVALID_SOCKET;

#else

using SocketHandle =
    int;

constexpr SocketHandle
    InvalidSocket =
        -1;

#endif

// ============================================================
// Constants
// ============================================================

constexpr int
    NetworkTimeoutMs =
        500;

// ============================================================
// Socket Startup / Cleanup
// ============================================================

bool initializeSockets() {
#ifdef _WIN32

    WSADATA wsaData;

    return
        WSAStartup(
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

// ============================================================
// Close Socket
// ============================================================

void closeSocket(
    SocketHandle socketHandle
) {
#ifdef _WIN32

    closesocket(
        socketHandle
    );

#else

    close(
        socketHandle
    );

#endif
}

// ============================================================
// Set Blocking / Non-Blocking
// ============================================================

bool setNonBlocking(
    SocketHandle socketHandle,
    bool nonBlocking
) {
#ifdef _WIN32

    u_long mode =
        nonBlocking
            ? 1
            : 0;

    return
        ioctlsocket(
            socketHandle,
            FIONBIO,
            &mode
        ) == 0;

#else

    int flags =
        fcntl(
            socketHandle,
            F_GETFL,
            0
        );

    if (
        flags == -1
    ) {
        return false;
    }

    if (nonBlocking) {
        flags |=
            O_NONBLOCK;
    }
    else {
        flags &=
            ~O_NONBLOCK;
    }

    return
        fcntl(
            socketHandle,
            F_SETFL,
            flags
        ) == 0;

#endif
}

// ============================================================
// Send / Receive Timeouts
// ============================================================

bool setSocketTimeouts(
    SocketHandle socketHandle
) {
#ifdef _WIN32

    DWORD timeout =
        NetworkTimeoutMs;

    if (
        setsockopt(
            socketHandle,
            SOL_SOCKET,
            SO_RCVTIMEO,
            reinterpret_cast<const char*>(
                &timeout
            ),
            sizeof(timeout)
        ) != 0
    ) {
        return false;
    }

    if (
        setsockopt(
            socketHandle,
            SOL_SOCKET,
            SO_SNDTIMEO,
            reinterpret_cast<const char*>(
                &timeout
            ),
            sizeof(timeout)
        ) != 0
    ) {
        return false;
    }

#else

    timeval timeout{};

    timeout.tv_sec =
        NetworkTimeoutMs /
        1000;

    timeout.tv_usec =
        (
            NetworkTimeoutMs %
            1000
        ) *
        1000;

    if (
        setsockopt(
            socketHandle,
            SOL_SOCKET,
            SO_RCVTIMEO,
            &timeout,
            sizeof(timeout)
        ) != 0
    ) {
        return false;
    }

    if (
        setsockopt(
            socketHandle,
            SOL_SOCKET,
            SO_SNDTIMEO,
            &timeout,
            sizeof(timeout)
        ) != 0
    ) {
        return false;
    }

#endif

    return true;
}

// ============================================================
// Connect With Timeout
// ============================================================

bool connectWithTimeout(
    SocketHandle socketHandle,
    const sockaddr_in& serverAddress
) {
    if (
        !setNonBlocking(
            socketHandle,
            true
        )
    ) {
        return false;
    }

    int result =
        connect(
            socketHandle,
            reinterpret_cast<
                const sockaddr*
            >(
                &serverAddress
            ),
            sizeof(serverAddress)
        );

    // --------------------------------------------------------
    // Immediate connection success
    // --------------------------------------------------------

    if (
        result == 0
    ) {
        setNonBlocking(
            socketHandle,
            false
        );

        return true;
    }

    // --------------------------------------------------------
    // Verify this is an in-progress non-blocking connection.
    // --------------------------------------------------------

#ifdef _WIN32

    int error =
        WSAGetLastError();

    if (
        error !=
            WSAEWOULDBLOCK &&
        error !=
            WSAEINPROGRESS &&
        error !=
            WSAEINVAL
    ) {
        setNonBlocking(
            socketHandle,
            false
        );

        return false;
    }

#else

    if (
        errno !=
        EINPROGRESS
    ) {
        setNonBlocking(
            socketHandle,
            false
        );

        return false;
    }

#endif

    // --------------------------------------------------------
    // Wait until socket becomes writable or timeout occurs.
    // --------------------------------------------------------

    fd_set writeSet;

    FD_ZERO(
        &writeSet
    );

    FD_SET(
        socketHandle,
        &writeSet
    );

    fd_set errorSet;

    FD_ZERO(
        &errorSet
    );

    FD_SET(
        socketHandle,
        &errorSet
    );

    timeval timeout{};

    timeout.tv_sec =
        NetworkTimeoutMs /
        1000;

    timeout.tv_usec =
        (
            NetworkTimeoutMs %
            1000
        ) *
        1000;

#ifdef _WIN32

    int selectResult =
        select(
            0,
            nullptr,
            &writeSet,
            &errorSet,
            &timeout
        );

#else

    int selectResult =
        select(
            socketHandle + 1,
            nullptr,
            &writeSet,
            &errorSet,
            &timeout
        );

#endif

    if (
        selectResult <= 0
    ) {
        setNonBlocking(
            socketHandle,
            false
        );

        return false;
    }

    // --------------------------------------------------------
    // Check actual connection result.
    // --------------------------------------------------------

    int socketError = 0;

#ifdef _WIN32

    int socketErrorLength =
        sizeof(socketError);

#else

    socklen_t socketErrorLength =
        sizeof(socketError);

#endif

    if (
        getsockopt(
            socketHandle,
            SOL_SOCKET,
            SO_ERROR,
#ifdef _WIN32
            reinterpret_cast<char*>(
                &socketError
            ),
#else
            &socketError,
#endif
            &socketErrorLength
        ) != 0
    ) {
        setNonBlocking(
            socketHandle,
            false
        );

        return false;
    }

    setNonBlocking(
        socketHandle,
        false
    );

    return
        socketError == 0;
}

// ============================================================
// Send Entire Message
// ============================================================

bool sendAll(
    SocketHandle socketHandle,
    const string& data
) {
    size_t totalSent = 0;

    while (
        totalSent <
        data.size()
    ) {
        int sent =
            send(
                socketHandle,
                data.data() +
                    totalSent,
                static_cast<int>(
                    data.size() -
                    totalSent
                ),
                0
            );

        if (
            sent <= 0
        ) {
            return false;
        }

        totalSent +=
            static_cast<size_t>(
                sent
            );
    }

    return true;
}

// ============================================================
// Receive One Protocol Line
// ============================================================

string receiveLine(
    SocketHandle socketHandle
) {
    string response;

    char buffer[1024];

    constexpr size_t
        maxResponseSize =
            64 * 1024;

    while (
        response.size() <
        maxResponseSize
    ) {
        int received =
            recv(
                socketHandle,
                buffer,
                sizeof(buffer),
                0
            );

        if (
            received <= 0
        ) {
            break;
        }

        response.append(
            buffer,
            static_cast<size_t>(
                received
            )
        );

        size_t newline =
            response.find(
                '\n'
            );

        if (
            newline !=
            string::npos
        ) {
            response.resize(
                newline
            );

            break;
        }
    }

    if (
        !response.empty() &&
        response.back() ==
            '\r'
    ) {
        response.pop_back();
    }

    return response;
}

} // namespace

// ============================================================
// Generic Request
// ============================================================

string TcpClient::request(
    const string& host,
    int port,
    const string& requestText
) {
    if (
        !initializeSockets()
    ) {
        return
            "ERROR SOCKET_INIT";
    }

    SocketHandle socketHandle =
        socket(
            AF_INET,
            SOCK_STREAM,
            IPPROTO_TCP
        );

    if (
        socketHandle ==
        InvalidSocket
    ) {
        cleanupSockets();

        return
            "ERROR SOCKET_CREATE";
    }

    if (
        !setSocketTimeouts(
            socketHandle
        )
    ) {
        closeSocket(
            socketHandle
        );

        cleanupSockets();

        return
            "ERROR SOCKET_TIMEOUT_CONFIG";
    }

    sockaddr_in serverAddress{};

    serverAddress.sin_family =
        AF_INET;

    serverAddress.sin_port =
        htons(
            static_cast<
                unsigned short
            >(
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
        closeSocket(
            socketHandle
        );

        cleanupSockets();

        return
            "ERROR INVALID_ADDRESS";
    }

    // --------------------------------------------------------
    // Bounded connection attempt.
    // --------------------------------------------------------

    if (
        !connectWithTimeout(
            socketHandle,
            serverAddress
        )
    ) {
        closeSocket(
            socketHandle
        );

        cleanupSockets();

        return
            "ERROR CONNECTION_FAILED";
    }

    string payload =
        requestText +
        "\n";

    if (
        !sendAll(
            socketHandle,
            payload
        )
    ) {
        closeSocket(
            socketHandle
        );

        cleanupSockets();

        return
            "ERROR SEND_FAILED";
    }

    string response =
        receiveLine(
            socketHandle
        );

    closeSocket(
        socketHandle
    );

    cleanupSockets();

    if (
        response.empty()
    ) {
        return
            "ERROR RECEIVE_FAILED";
    }

    return response;
}

// ============================================================
// PING
// ============================================================

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

// ============================================================
// INFO
// ============================================================

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