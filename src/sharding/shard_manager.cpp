#include "shard_manager.h"

#include <algorithm>
#include <iostream>
#include <map>
#include <stdexcept>

using namespace std;

ShardManager::ShardManager(
    const vector<ShardNode>& clusterNodes,
    size_t shardCount,
    size_t replicationFactor
)
    : nodes(clusterNodes),
      shardCount(shardCount),
      replicationFactor(replicationFactor) {

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
        [](
            const ShardNode& a,
            const ShardNode& b
        ) {
            return a.nodeId < b.nodeId;
        }
    );

    nodes.erase(
        unique(
            nodes.begin(),
            nodes.end(),
            [](
                const ShardNode& a,
                const ShardNode& b
            ) {
                return a.nodeId == b.nodeId;
            }
        ),
        nodes.end()
    );

    if (replicationFactor == 0) {
        throw runtime_error(
            "Replication factor must be greater than zero"
        );
    }

    if (
        replicationFactor >
        nodes.size()
    ) {
        this->replicationFactor =
            nodes.size();
    }
}

uint64_t ShardManager::stableHash(
    const string& key
) {
    constexpr uint64_t offsetBasis =
        14695981039346656037ULL;

    constexpr uint64_t prime =
        1099511628211ULL;

    uint64_t hash =
        offsetBasis;

    for (
        unsigned char c :
        key
    ) {
        hash ^=
            static_cast<uint64_t>(c);

        hash *= prime;
    }

    return hash;
}

size_t ShardManager::shardForKey(
    const string& key
) const {
    return static_cast<size_t>(
        stableHash(key) %
        shardCount
    );
}

ShardNode ShardManager::primaryForShard(
    size_t shardId
) const {
    if (
        shardId >=
        shardCount
    ) {
        throw out_of_range(
            "Invalid shard ID"
        );
    }

    size_t primaryIndex =
        shardId %
        nodes.size();

    return nodes[
        primaryIndex
    ];
}

ShardNode ShardManager::primaryForKey(
    const string& key
) const {
    return primaryForShard(
        shardForKey(key)
    );
}

vector<ShardNode>
ShardManager::replicasForShard(
    size_t shardId
) const {
    if (
        shardId >=
        shardCount
    ) {
        throw out_of_range(
            "Invalid shard ID"
        );
    }

    vector<ShardNode> replicas;

    size_t primaryIndex =
        shardId %
        nodes.size();

    for (
        size_t i = 0;
        i < replicationFactor;
        ++i
    ) {
        size_t index =
            (
                primaryIndex +
                i
            ) %
            nodes.size();

        replicas.push_back(
            nodes[index]
        );
    }

    return replicas;
}

vector<ShardNode>
ShardManager::replicasForKey(
    const string& key
) const {
    return replicasForShard(
        shardForKey(key)
    );
}

bool ShardManager::nodeOwnsReplica(
    const string& nodeId,
    size_t shardId
) const {
    vector<ShardNode> replicas =
        replicasForShard(
            shardId
        );

    for (
        const auto& node :
        replicas
    ) {
        if (
            node.nodeId ==
            nodeId
        ) {
            return true;
        }
    }

    return false;
}

void ShardManager::printRoute(
    const string& key
) const {
    size_t shardId =
        shardForKey(key);

    ShardNode primary =
        primaryForShard(
            shardId
        );

    cout
        << "\nKey Routing\n";

    cout
        << "---------------------------------\n";

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
        << "Primary:  "
        << primary.nodeId
        << '\n';

    cout
        << "Address:  "
        << primary.host
        << ':'
        << primary.port
        << "\n\n";
}

void ShardManager::printReplicaPlacement(
    const string& key
) const {
    size_t shardId =
        shardForKey(key);

    vector<ShardNode> replicas =
        replicasForShard(
            shardId
        );

    cout
        << "\nReplica Placement\n";

    cout
        << "---------------------------------\n";

    cout
        << "Key:                 "
        << key
        << '\n';

    cout
        << "Shard:               "
        << shardId
        << '\n';

    cout
        << "Replication factor:  "
        << replicationFactor
        << '\n';

    for (
        size_t i = 0;
        i < replicas.size();
        ++i
    ) {
        cout
            << (
                i == 0
                ? "PRIMARY   "
                : "REPLICA   "
            )
            << replicas[i].nodeId
            << "  "
            << replicas[i].host
            << ':'
            << replicas[i].port
            << '\n';
    }

    cout << '\n';
}

void ShardManager::printDistribution() const {
    map<string, size_t>
        primaryCounts;

    map<string, size_t>
        replicaCounts;

    for (
        const auto& node :
        nodes
    ) {
        primaryCounts[
            node.nodeId
        ] = 0;

        replicaCounts[
            node.nodeId
        ] = 0;
    }

    for (
        size_t shardId = 0;
        shardId < shardCount;
        ++shardId
    ) {
        ShardNode primary =
            primaryForShard(
                shardId
            );

        ++primaryCounts[
            primary.nodeId
        ];

        vector<ShardNode> replicas =
            replicasForShard(
                shardId
            );

        for (
            const auto& replica :
            replicas
        ) {
            ++replicaCounts[
                replica.nodeId
            ];
        }
    }

    cout
        << "\nShard Distribution\n";

    cout
        << "-------------------------------------------------\n";

    cout
        << "Virtual shards:      "
        << shardCount
        << '\n';

    cout
        << "Replication factor:  "
        << replicationFactor
        << "\n\n";

    for (
        const auto& node :
        nodes
    ) {
        cout
            << node.nodeId
            << "  primary="
            << primaryCounts[
                   node.nodeId
               ]
            << "  replicas="
            << replicaCounts[
                   node.nodeId
               ]
            << '\n';
    }

    cout << '\n';
}

size_t
ShardManager::getShardCount() const {
    return shardCount;
}

size_t
ShardManager::getReplicationFactor() const {
    return replicationFactor;
}