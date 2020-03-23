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

// Defines hash functions for TUniqueId.

#pragma once

#include <boost/functional/hash.hpp>

#include "gen-cpp/Types_types.h" // for TUniqueId
#include "gen-cpp/common.pb.h" // for UniqueIdPB

namespace impala {

inline std::size_t hash_value(const TUniqueId& id) {
  std::size_t seed = 0;
  boost::hash_combine(seed, id.lo);
  boost::hash_combine(seed, id.hi);
  return seed;
}

inline std::size_t hash_value(const UniqueIdPB& id) {
  std::size_t seed = 0;
  boost::hash_combine(seed, id.lo());
  boost::hash_combine(seed, id.hi());
  return seed;
}

} // namespace impala

/// Hash function for std:: containers
namespace std {

template <>
struct hash<impala::TUniqueId> {
  std::size_t operator()(const impala::TUniqueId& id) const {
    return impala::hash_value(id);
  }
};

template <>
struct hash<impala::UniqueIdPB> {
  std::size_t operator()(const impala::UniqueIdPB& id) const {
    return impala::hash_value(id);
  }
};

} // namespace std
