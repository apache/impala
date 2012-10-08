// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_UTIL_CONTAINER_UTIL_H
#define IMPALA_UTIL_CONTAINER_UTIL_H

#include <map>
#include <boost/unordered_map.hpp>

#include "util/hash-util.h"
#include "gen-cpp/Types_types.h"

using namespace std;

namespace impala {

// Hash function for THostPort. This function must be called hash_value to be picked
// up properly by boost.
inline std::size_t hash_value(const THostPort& host_port) {
  uint32_t hash =
      HashUtil::Hash(host_port.ipaddress.c_str(), host_port.ipaddress.length(), 0);
  return HashUtil::Hash(&host_port.port, sizeof(host_port.port), hash);
}

struct HashTHostPortPtr : public std::unary_function<THostPort*, size_t> {
  size_t operator()(const THostPort* const& p) const { return hash_value(*p); }
};

struct THostPortPtrEquals : public std::unary_function<THostPort*, bool> {
  bool operator()(const THostPort* const& p1, const THostPort* const& p2) const {
    return p1->hostname == p2->hostname && p1->ipaddress == p2->ipaddress
        && p1->port == p2->port;
  }
};


// FindOrInsert(): if the key is present, return the value; if the key is not present,
// create a new entry (key, default_val) and return default_val.

template <typename K, typename V>
V* FindOrInsert(std::map<K,V>* m, const K& key, const V& default_val) {
  typename std::map<K,V>::iterator it = m->find(key);
  if (it == m->end()) {
    it = m->insert(make_pair(key, default_val)).first;
  }
  return &it->second;
}

template <typename K, typename V>
V* FindOrInsert(boost::unordered_map<K,V>* m, const K& key, const V& default_val) {
  typename boost::unordered_map<K,V>::iterator it = m->find(key);
  if (it == m->end()) {
    it = m->insert(make_pair(key, default_val)).first;
  }
  return &it->second;
}


// FindWithDefault: if the key is present, return the corresponding value; if the key
// is not present, return the supplied default value

template <typename K, typename V>
const V& FindWithDefault(const std::map<K, V>& m, const K& key, const V& default_val) {
  typename std::map<K,V>::const_iterator it = m.find(key);
  if (it == m.end()) return default_val;
  return it->second;
}

template <typename K, typename V>
const V& FindWithDefault(const boost::unordered_map<K, V>& m, const K& key,
                         const V& default_val) {
  typename boost::unordered_map<K,V>::const_iterator it = m.find(key);
  if (it == m.end()) return default_val;
  return it->second;
}

}

#endif
