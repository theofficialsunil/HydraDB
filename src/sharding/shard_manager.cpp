#include "shard_manager.h"

#include <algorithm>
#include <iostream>
#include <map>
#include <stdexcept>

using namespace std;

ShardManager::ShardManager(
    const vector<ShardNode>& clusterNodes,
    size_t shardCount
)
    : nodes(clusterNodes),
      shardCount(shardCount) {

    if (nodes.empty()) {
        throw runtime_error(
            "ShardManager requires at least one node"
        );
    }

    if (shardCount == 0) {
        throw runtime_error(
            "Shard count must be greater than zero"
        );
    }

    sort(
        nodes.begin(),
        nodes.end(),
        [](const ShardNode& a, const ShardNode& b) {
            return a.nodeId < b.nodeId;
        }
    );

    nodes.erase(
        unique(
            nodes.begin(),
            nodes.end(),
            [](const ShardNode& a, const ShardNode& b) {
                return a.nodeId == b.nodeId;
            }
        ),
        nodes.end()
    );
}

uint64_t ShardManager::stableHash(
    const string& key
) {
    // 64-bit FNV-1a hash.
    //
    // We deliberately avoid std::hash here because the exact
    // result is not guaranteed to be stable across platforms
    // or standard-library implementations.

    constexpr uint64_t offsetBasis =
        14695981039346656037ULL;

    constexpr uint64_t prime =
        1099511628211ULL;

    uint64_t hash = offsetBasis;

    for (unsigned char c : key) {
        hash ^= static_cast<uint64_t>(c);
        hash *= prime;
    }

    return hash;
}

size_t ShardManager::shardForKey(
    const string& key
) const {
    return static_cast<size_t>(
        stableHash(key) % shardCount
    );
}

ShardNode ShardManager::ownerForShard(
    size_t shardId
) const {
    if (shardId >= shardCount) {
        throw out_of_range(
            "Invalid shard ID"
        );
    }

    size_t nodeIndex =
        shardId % nodes.size();

    return nodes[nodeIndex];
}

ShardNode ShardManager::ownerForKey(
    const string& key
) const {
    size_t shardId =
        shardForKey(key);

    return ownerForShard(shardId);
}

void ShardManager::printRoute(
    const string& key
) const {
    size_t shardId =
        shardForKey(key);

    ShardNode owner =
        ownerForShard(shardId);

    cout << "\nKey Routing\n";
    cout << "---------------------------------\n";

    cout
        << "Key:      "
        << key
        << '\n';

    cout
        << "Shard:    "
        << shardId
        << " / "
        << shardCount
        << '\n';

    cout
        << "Owner:    "
        << owner.nodeId
        << '\n';

    cout
        << "Address:  "
        << owner.host
        << ':'
        << owner.port
        << "\n\n";
}

void ShardManager::printDistribution() const {
    map<string, size_t> shardCounts;

    for (const auto& node : nodes) {
        shardCounts[node.nodeId] = 0;
    }

    for (
        size_t shardId = 0;
        shardId < shardCount;
        ++shardId
    ) {
        ShardNode owner =
            ownerForShard(shardId);

        ++shardCounts[owner.nodeId];
    }

    cout << "\nShard Distribution\n";

    cout
        << "---------------------------------\n";

    cout
        << "Virtual shards: "
        << shardCount
        << '\n';

    for (const auto& node : nodes) {
        cout
            << node.nodeId
            << "    "
            << shardCounts[node.nodeId]
            << " shards\n";
    }

    cout << '\n';
}

size_t ShardManager::getShardCount() const {
    return shardCount;
}