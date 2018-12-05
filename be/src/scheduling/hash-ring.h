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

#ifndef SCHEDULING_HASH_RING_H
#define SCHEDULING_HASH_RING_H

#include <map>
#include <set>
#include <vector>

#include "common/logging.h"

namespace impala {

typedef std::string IpAddr;

/// Simple hash ring implementation specialized for IpAddr.
///
/// A hash ring is a consistent hash. It allows distributing items consistently across
/// a set of nodes. An important property is that if nodes come or go, the number of
/// items that are remapped to different nodes should be small.
///
/// A hash ring works by hashing nodes to place them throughout the hash space (in this
/// case unsigned 4 byte integers). To balance the allocation of the hash space, a node
/// can be hashed multiple times to give it multiple locations throughout the hash space.
/// To look up a node for a given item, we hash the item and find the closest node after
/// that hash value in the hash space. This implementation is agnostic about the type of
/// item and expects the caller to do its own hashing for the item.
///
/// When nodes are added or removed, only the hash space in the immediate vicinity of the
/// node is remapped. This allows for bounded disruption that should be proportional to
/// 1 / # of nodes.
///
/// TODO: This can be modified to use any type for the node (rather than IpAddr)
/// by taking in a function pointer that generates a hash value for the templated type.
class HashRing {
 public:
  /// Create a new empty hash ring using the specified replication. Each node is placed
  /// into the hash space 'num_replicas' times. A higher value for 'num_replicas'
  /// results in a more even distribution of the hash space between nodes, but it requires
  /// more memory/time.
  HashRing(uint32_t num_replicas)
    : num_replicas_(num_replicas) {
    DCHECK_GT(num_replicas_, 0);
  }

  /// Copy constructor
  /// This is needed because HashRing is stored in a structure that is copy on write.
  /// The standard copy constructor does not know about the NodeIterators in the
  /// hash_to_node_. This copies the nodes_ structure and iterates over the
  /// hash_to_node_, inserting an equivalent pair in the new hash_to_node_.
  HashRing(const HashRing& hash_ring);

  /// Move constructor is not tested or implemented, so delete for now
  HashRing(HashRing&& hash_ring) = delete;

  /// Add a node to the hash ring. This hashes the node 'num_replicas_' times and tries to
  /// insert the node into the map at the hash location. In the event of a hash collision,
  /// the map will be set to the minimum of the current value and the new value.
  /// Nodes must be unique.
  void AddNode(const IpAddr& node);

  /// This removes the specified node from the hashring. This removes all elements that
  /// reference this node. Nodes must be unique.
  void RemoveNode(const IpAddr& node);

  /// Get the first node equal or larger than the specified 'hash_value'. If 'hash_value'
  /// is larger than the largest hash value, it gets the element with the smallest hash
  /// value. If the hash ring is empty, this returns nullptr.
  const IpAddr* GetNode(uint32_t hash_value) const;
 private:
  friend class HashRingTest;
  friend class HashRingDistributionCheck;

  /// Populate a map from IpAddr to the sum of ranges in the hash space that map to this
  /// IpAddr. The total hash space has 2^32 numbers, so the elements in this map sum to
  /// 2^32. The range is uint64_t as 2^32 is just outside the maximum value for uint32_t.
  /// This is useful for examining how the hash space is balanced between nodes.
  void GetDistributionMap(std::map<IpAddr, uint64_t>* distribution_map) const;
  uint32_t GetNumReplicas() const { return num_replicas_; }
  size_t GetNumNodes() const { return nodes_.size(); }
  size_t GetTotalReplicas() const { return hash_to_node_.size(); }

  /// To avoid keeping num_replicas_ copies of the IpAddr's, store them in
  /// a set and store iterators pointing to these elements in the hash_to_node_.
  std::set<IpAddr> nodes_;
  typedef std::set<IpAddr>::iterator NodeIterator;

  /// Map from a hash value to the associated node iterator, which can be used to lookup
  /// the base value. In the event of a collision, the value is set by taking the minimum
  /// of the two underlying IpAddr's. Collisions should be rare, so this will only rarely
  /// impact the actual allocations.
  std::map<uint32_t, NodeIterator> hash_to_node_;

  /// The number of times each node is added to the hash space.
  uint32_t num_replicas_;
};

}

#endif
