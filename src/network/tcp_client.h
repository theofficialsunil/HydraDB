#pragma once

#include <string>
class TcpClient {
public:
    static std::string ping(
        const std::string& host,
        int port
    );

    static std::string info(
        const std::string& host,
        int port
    );
};