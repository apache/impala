// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_UTIL_HASH_UTIL_H
#define IMPALA_UTIL_HASH_UTIL_H

#include <nmmintrin.h>
#include "util/cpu-info.h"

namespace impala {

// Utility class to compute hash values.   
class HashUtil {
 public:
  // Compute the Crc32 hash for data using SSE4 instructions.  The input hash parameter is 
  // the current hash/seed value.
  // This should only be called if SSE is supported.
  // This is ~4x faster than Fvn/Boost Hash.
  static uint32_t CrcHash(const void* data, size_t bytes, uint32_t hash) {
    DCHECK(CpuInfo::Instance()->IsSupported(CpuInfo::SSE4_2));
    uint32_t words = bytes / sizeof(uint32_t);
    bytes = bytes % sizeof(uint32_t);

    const uint32_t* p = reinterpret_cast<const uint32_t*>(data);
    while (words--) {
      hash = _mm_crc32_u32(hash, *p);
      ++p;
    }

    const uint8_t* s = reinterpret_cast<const uint8_t*>(p);
    while (bytes--) {
      hash = _mm_crc32_u8(hash, *s);
      ++s;
    }

    return hash;
  } 

  // default values recommended by http://isthe.com/chongo/tech/comp/fnv/
  static const uint32_t FVN_PRIME = 0x01000193; //   16777619
  static const uint32_t FVN_SEED = 0x811C9DC5; // 2166136261

  // Implementation of the Fowler–Noll–Vo hash function.  This is not as performant
  // as boost's hash on int types (2x slower) but has bit entropy.  
  // For ints, boost just returns the value of the int which can be pathological. 
  // For example, if the data is <1000, 2000, 3000, 4000, ..> and then the mod of 1000 
  // is taken on the hash, all values will collide to the same bucket.
  // For string values, Fvn is slightly faster than boost.
  static uint32_t FvnHash(const void* data, size_t bytes, uint32_t hash) {
    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(data);
    while (bytes--) {
      hash = (*ptr ^ hash) * FVN_PRIME;
      ++ptr;
    }
    return hash;
  }

  // Computes the hash value for data.  Will call either CrcHash or FvnHash
  // depending on hardware capabilities.
  static uint32_t Hash(const void* data, size_t bytes, uint32_t hash) {
    if (CpuInfo::Instance()->IsSupported(CpuInfo::SSE4_2)) {
      return CrcHash(data, bytes, hash);
    } else {
      return FvnHash(data, bytes, hash);
    }
  }

};

}

#endif
