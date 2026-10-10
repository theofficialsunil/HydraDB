#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

struct ShardNode {
    std::string nodeId;
    std::string host;
    int port;
};

class ShardManager {
private:
    std::vector<ShardNode> nodes;

    std::size_t shardCount;

    static std::uint64_t stableHash(
        const std::string& key
    );

public:
    explicit ShardManager(
        const std::vector<ShardNode>& clusterNodes,
        std::size_t shardCount = 1024
    );

    std::size_t shardForKey(
        const std::string& key
    ) const;

    ShardNode ownerForShard(
        std::size_t shardId
    ) const;

    ShardNode ownerForKey(
        const std::string& key
    ) const;

    void printRoute(
        const std::string& key
    ) const;

    void printDistribution() const;

    std::size_t getShardCount() const;
};