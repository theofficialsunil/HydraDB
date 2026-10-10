#pragma once

#include <string>

class TcpClient {
public:
    static std::string request(
        const std::string& host,
        int port,
        const std::string& request
    );

    static std::string ping(
        const std::string& host,
        int port
    );

    static std::string info(
        const std::string& host,
        int port
    );
};