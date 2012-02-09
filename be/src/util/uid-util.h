// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_UTIL_UID_UTIL_H
#define IMPALA_UTIL_UID_UTIL_H

#include <boost/functional/hash.hpp>
#include "gen-cpp/Types_types.h"  // for TUniqueId

namespace boost {
template <>
struct hash<impala::TUniqueId> : public std::unary_function<impala::TUniqueId, size_t> {
  std::size_t operator()(const impala::TUniqueId& id) const {
    std::size_t seed = 0;
    boost::hash_combine(seed, id.lo);
    boost::hash_combine(seed, id.hi);
    return seed;
  }
};
}

#endif
