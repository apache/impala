// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef IMPALA_UTIL_UID_UTIL_H
#define IMPALA_UTIL_UID_UTIL_H

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>

#include "gen-cpp/Types_types.h"  // for TUniqueId
#include "util/debug-util.h"

using namespace std;

namespace impala {
// This function must be called 'hash_value' to be picked up by boost.
inline std::size_t hash_value(const impala::TUniqueId& id) {
  std::size_t seed = 0;
  boost::hash_combine(seed, id.lo);
  boost::hash_combine(seed, id.hi);
  return seed;
}

// Templated so that this method is not namespace-specific (since we also call this on
// llama::TUniqueId)
template <typename T>
inline void UUIDToTUniqueId(const boost::uuids::uuid& uuid, T* unique_id) {
  memcpy(&(unique_id->hi), &uuid.data[0], 8);
  memcpy(&(unique_id->lo), &uuid.data[8], 8);
}

template <typename F, typename T>
inline T CastTUniqueId(const F& from) {
  T to;
  to.hi = from.hi;
  to.lo = from.lo;
  return to;
}

// generates a 16 byte UUID
inline string GenerateUUIDString() {
  boost::uuids::basic_random_generator<boost::mt19937> gen;
  boost::uuids::uuid u = gen();
  string uuid(u.begin(), u.end());
  return uuid;
}

// generates a 16 byte UUID
inline TUniqueId GenerateUUID() {
  const string& u = GenerateUUIDString();
  TUniqueId uid;
  memcpy(&uid.hi, &u[0], sizeof(int64_t));
  memcpy(&uid.lo, &u[0] + sizeof(int64_t), sizeof(int64_t));
  return uid;
}

} // namespace impala
#endif
