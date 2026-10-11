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

    std::size_t replicationFactor;

    static std::uint64_t stableHash(
        const std::string& key
    );

public:
    ShardManager(
        const std::vector<ShardNode>& clusterNodes,
        std::size_t shardCount = 1024,
        std::size_t replicationFactor = 3
    );

    std::size_t shardForKey(
        const std::string& key
    ) const;

    ShardNode primaryForShard(
        std::size_t shardId
    ) const;

    ShardNode primaryForKey(
        const std::string& key
    ) const;

    std::vector<ShardNode> replicasForShard(
        std::size_t shardId
    ) const;

    std::vector<ShardNode> replicasForKey(
        const std::string& key
    ) const;

    bool nodeOwnsReplica(
        const std::string& nodeId,
        std::size_t shardId
    ) const;

    void printRoute(
        const std::string& key
    ) const;

    void printReplicaPlacement(
        const std::string& key
    ) const;

    void printDistribution() const;

    std::size_t getShardCount() const;

    std::size_t getReplicationFactor() const;
};