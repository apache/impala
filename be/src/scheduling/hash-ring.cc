// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <map>
#include <random>

#include "scheduling/hash-ring.h"
#include "thirdparty/pcg-cpp-0.98/include/pcg_random.hpp"
#include "util/container-util.h"
#include "util/hash-util.h"
#include "util/network-util.h"
#include "gen-cpp/Types_types.h"
#include "common/names.h"

namespace impala {

HashRing::HashRing(const HashRing& hash_ring)
  : nodes_(hash_ring.nodes_), num_replicas_(hash_ring.num_replicas_) {
  for (const auto& old_pair : hash_ring.hash_to_node_) {
    uint32_t hash_value = old_pair.first;
    NodeIterator old_node_it = old_pair.second;
    // Look up the equivalent node iterator in the new nodes_ set.
    NodeIterator new_node_it = nodes_.find(*old_node_it);
    DCHECK(new_node_it != nodes_.end());
    hash_to_node_.emplace(hash_value, new_node_it);
  }
}

void HashRing::AddNode(const IpAddr& node) {
  // This node should not already be in the set.
  std::pair<NodeIterator, bool> node_pair = nodes_.insert(node);
  // 'second' tells whether a new element was inserted. It must be true.
  DCHECK(node_pair.second) << "Failed to add: " << node;
  NodeIterator node_it = node_pair.first;
  // Generate multiple hashes of the IpAddr by using the hash as a seed to a PRNG.
  uint32_t hash = HashUtil::Hash(node.data(), node.length(), 0);
  pcg32 prng(hash);
  for (uint32_t i = 0; i < num_replicas_; i++) {
    uint32_t hash_val = prng();
    // Check for hash collision
    auto hashmap_it = hash_to_node_.find(hash_val);
    // Write this new value if:
    // 1. There is no hash collision.
    // -OR-
    // 2. There is a hash collision, but the underlying IpAddr is less than
    //    the current value.
    // This guarantees consistency regardless of the order of adds and removes.
    if (hashmap_it == hash_to_node_.end() || *node_it < *hashmap_it->second) {
      hash_to_node_[hash_val] = node_it;
    }
  }
}

void HashRing::RemoveNode(const IpAddr& node) {
  // This node must be in the set. Keep the iterator to erase it later.
  NodeIterator node_it = nodes_.find(node);
  DCHECK(node_it != nodes_.end());

  // Walk the map and remove any entries that have a NodeIterator pointing to this node.
  size_t num_removed = 0;
  auto hash_to_node_it = hash_to_node_.begin();
  while (hash_to_node_it != hash_to_node_.end()) {
    if (hash_to_node_it->second == node_it) {
      hash_to_node_it = hash_to_node_.erase(hash_to_node_it);
      num_removed++;
    } else {
      hash_to_node_it++;
    }
  }
  DCHECK_GT(num_removed, 0);
  nodes_.erase(node_it);
}

const IpAddr* HashRing::GetNode(uint32_t hash_value) const {
  if (hash_to_node_.empty()) return nullptr;
  // Find the element that immediately follows this hash value
  auto next_elem = hash_to_node_.lower_bound(hash_value);
  if (next_elem == hash_to_node_.end()) {
    // This is larger than the largest elem. Return the smallest elem
    next_elem = hash_to_node_.begin();
  }
  NodeIterator node_it = next_elem->second;
  return &(*node_it);
}

void HashRing::GetDistributionMap(
    map<IpAddr, uint64_t>* distribution_map) const {
  // Start at zero and add up the ranges for each distinct node by walking the map.
  // There are UINT_MAX + 1 total values. GetNode() uses map::lower_bound, which is
  // inclusive of the value (i.e. GetNode(5) would match a hash value of 5).
  // This means all of ranges are off by one compared to typical indexes. This is
  // irrelevent for the internal ranges, but the (end - start) calculation is wrong for
  // the first range. It needs to be one longer. This is incorporated into the final
  // section.
  uint32_t last_index = 0;
  for (const auto& hash_to_node_pair : hash_to_node_) {
    const IpAddr& addr = *(hash_to_node_pair.second);
    uint32_t end_index = hash_to_node_pair.first;
    uint32_t range = end_index - last_index;
    (*distribution_map)[addr] += range;
    last_index = end_index;
  }
  // Any room from last_index to UINT_MAX goes to the min element
  uint64_t range = UINT_MAX - last_index;
  // Incorporate the missing +1 from the first range
  range++;
  const IpAddr& addr = *(hash_to_node_.begin()->second);
  (*distribution_map)[addr] += range;
}

}
