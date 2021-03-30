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


#ifndef IMPALA_UTIL_HASH_UTIL_H
#define IMPALA_UTIL_HASH_UTIL_H

#include "common/logging.h"
#include "common/compiler-util.h"
#include "gutil/sysinfo.h"
#include "util/cpu-info.h"
#include "util/sse-util.h"

namespace impala {

/// Utility class to compute hash values.
class HashUtil {
 public:
  /// Compute the Crc32 hash for data using SSE4 instructions.  The input hash parameter is
  /// the current hash/seed value.
  /// This should only be called if SSE is supported.
  /// This is ~4x faster than Fnv/Boost Hash.
  /// NOTE: Any changes made to this function need to be reflected in Codegen::GetHashFn.
  /// TODO: crc32 hashes with different seeds do not result in different hash functions.
  /// The resulting hashes are correlated.
  /// TODO: update this to also use SSE4_crc32_u64 and SSE4_crc32_u16 where appropriate.
  static uint32_t CrcHash(const void* data, int32_t bytes, uint32_t hash) {
    DCHECK(CpuInfo::IsSupported(CpuInfo::SSE4_2) || base::IsAarch64());
    uint32_t words = bytes / sizeof(uint32_t);
    bytes = bytes % sizeof(uint32_t);

    const uint32_t* p = reinterpret_cast<const uint32_t*>(data);
    while (words--) {
      hash = SSE4_crc32_u32(hash, *p);
      ++p;
    }

    const uint8_t* s = reinterpret_cast<const uint8_t*>(p);
    while (bytes--) {
      hash = SSE4_crc32_u8(hash, *s);
      ++s;
    }

    // The lower half of the CRC hash has has poor uniformity, so swap the halves
    // for anyone who only uses the first several bits of the hash.
    hash = (hash << 16) | (hash >> 16);
    return hash;
  }

  /// CrcHash() specialized for 1-byte data
  static inline uint32_t CrcHash1(const void* v, uint32_t hash) {
    DCHECK(CpuInfo::IsSupported(CpuInfo::SSE4_2) || base::IsAarch64());
    const uint8_t* s = reinterpret_cast<const uint8_t*>(v);
    hash = SSE4_crc32_u8(hash, *s);
    hash = (hash << 16) | (hash >> 16);
    return hash;
  }

  /// CrcHash() specialized for 2-byte data
  static inline uint32_t CrcHash2(const void* v, uint32_t hash) {
    DCHECK(CpuInfo::IsSupported(CpuInfo::SSE4_2) || base::IsAarch64());
    const uint16_t* s = reinterpret_cast<const uint16_t*>(v);
    hash = SSE4_crc32_u16(hash, *s);
    hash = (hash << 16) | (hash >> 16);
    return hash;
  }

  /// CrcHash() specialized for 4-byte data
  static inline uint32_t CrcHash4(const void* v, uint32_t hash) {
    DCHECK(CpuInfo::IsSupported(CpuInfo::SSE4_2) || base::IsAarch64());
    const uint32_t* p = reinterpret_cast<const uint32_t*>(v);
    hash = SSE4_crc32_u32(hash, *p);
    hash = (hash << 16) | (hash >> 16);
    return hash;
  }

  /// CrcHash() specialized for 8-byte data
  static inline uint32_t CrcHash8(const void* v, uint32_t hash) {
    DCHECK(CpuInfo::IsSupported(CpuInfo::SSE4_2) || base::IsAarch64());
    const uint64_t* p = reinterpret_cast<const uint64_t*>(v);
    hash = SSE4_crc32_u64(hash, *p);
    hash = (hash << 16) | (hash >> 16);
    return hash;
  }

  /// CrcHash() specialized for 12-byte data
  static inline uint32_t CrcHash12(const void* v, uint32_t hash) {
    DCHECK(CpuInfo::IsSupported(CpuInfo::SSE4_2) || base::IsAarch64());
    const uint64_t* p = reinterpret_cast<const uint64_t*>(v);
    hash = SSE4_crc32_u64(hash, *p);
    ++p;
    hash = SSE4_crc32_u32(hash, *reinterpret_cast<const uint32_t *>(p));
    hash = (hash << 16) | (hash >> 16);
    return hash;
  }

  /// CrcHash() specialized for 16-byte data
  static inline uint32_t CrcHash16(const void* v, uint32_t hash) {
    DCHECK(CpuInfo::IsSupported(CpuInfo::SSE4_2) || base::IsAarch64());
    const uint64_t* p = reinterpret_cast<const uint64_t*>(v);
    hash = SSE4_crc32_u64(hash, *p);
    ++p;
    hash = SSE4_crc32_u64(hash, *p);
    hash = (hash << 16) | (hash >> 16);
    return hash;
  }

  static const uint64_t MURMUR_DEFAULT_SEED = 0x0;
  static const uint64_t MURMUR_PRIME = 0xc6a4a7935bd1e995;
  static const int MURMUR_R = 47;

  /// Murmur2 hash implementation returning 64-bit hashes.
  static uint64_t MurmurHash2_64(const void* input, int len, uint64_t seed) {
    uint64_t h = seed ^ (len * MURMUR_PRIME);

    const uint64_t* data = reinterpret_cast<const uint64_t*>(input);
    const uint64_t* end = data + (len / sizeof(uint64_t));

    while (data != end) {
      uint64_t k = *data++;
      k *= MURMUR_PRIME;
      k ^= k >> MURMUR_R;
      k *= MURMUR_PRIME;
      h ^= k;
      h *= MURMUR_PRIME;
    }

    const uint8_t* data2 = reinterpret_cast<const uint8_t*>(data);
    switch (len & 7) {
      case 7: h ^= uint64_t(data2[6]) << 48;
      case 6: h ^= uint64_t(data2[5]) << 40;
      case 5: h ^= uint64_t(data2[4]) << 32;
      case 4: h ^= uint64_t(data2[3]) << 24;
      case 3: h ^= uint64_t(data2[2]) << 16;
      case 2: h ^= uint64_t(data2[1]) << 8;
      case 1: h ^= uint64_t(data2[0]);
              h *= MURMUR_PRIME;
    }

    h ^= h >> MURMUR_R;
    h *= MURMUR_PRIME;
    h ^= h >> MURMUR_R;
    return h;
  }

  /// default values recommended by http://isthe.com/chongo/tech/comp/fnv/
  static const uint32_t FNV_PRIME = 0x01000193; //   16777619
  static const uint32_t FNV_SEED = 0x811C9DC5; // 2166136261
  static const uint64_t FNV64_PRIME = 1099511628211UL;
  static const uint64_t FNV64_SEED = 14695981039346656037UL;

  /// Implementation of the Fowler-Noll-Vo hash function. This is not as performant
  /// as boost's hash on int types (2x slower) but has bit entropy.
  /// For ints, boost just returns the value of the int which can be pathological.
  /// For example, if the data is <1000, 2000, 3000, 4000, ..> and then the mod of 1000
  /// is taken on the hash, all values will collide to the same bucket.
  /// For string values, Fnv is slightly faster than boost.
  /// IMPORTANT: FNV hash suffers from poor diffusion of the least significant bit,
  /// which can lead to poor results when input bytes are duplicated.
  /// See FnvHash64to32() for how this can be mitigated.
  static uint64_t FnvHash64(const void* data, int32_t bytes, uint64_t hash) {
    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(data);
    while (bytes--) {
      hash = (*ptr ^ hash) * FNV64_PRIME;
      ++ptr;
    }
    return hash;
  }

  /// Return a 32-bit hash computed by invoking FNV-64 and folding the result to 32-bits.
  /// This technique is recommended instead of FNV-32 since the LSB of an FNV hash is the
  /// XOR of the LSBs of its input bytes, leading to poor results for duplicate inputs.
  /// The input seed 'hash' is duplicated so the top half of the seed is not all zero.
  /// Data length must be at least 1 byte: zero-length data should be handled separately,
  /// for example using CombineHash with a unique constant value to avoid returning the
  /// hash argument. Zero-length data gives terrible results: the initial hash value is
  /// xored with itself cancelling all bits.
  static uint32_t FnvHash64to32(const void* data, int32_t bytes, uint32_t hash) {
    // IMPALA-2270: this function should never be used for zero-byte inputs.
    DCHECK_GT(bytes, 0);
    uint64_t hash_u64 = hash | ((uint64_t)hash << 32);
    hash_u64 = FnvHash64(data, bytes, hash_u64);
    return (hash_u64 >> 32) ^ (hash_u64 & 0xFFFFFFFF);
  }

  /// Computes the hash value for data.  Will call either CrcHash or MurmurHash depending on
  /// hardware capabilities.
  /// Seed values for different steps of the query execution should use different seeds
  /// to prevent accidental key collisions. (See IMPALA-219 for more details).
  static uint32_t Hash(const void* data, int32_t bytes, uint32_t seed) {
    if (base::IsAarch64() || LIKELY(CpuInfo::IsSupported(CpuInfo::SSE4_2))) {
      return CrcHash(data, bytes, seed);
    } else {
      return MurmurHash2_64(data, bytes, seed);
    }
  }

  /// The magic number (used in hash_combine()) 0x9e3779b9 = 2^32 / (golden ratio).
  static const uint32_t HASH_COMBINE_SEED = 0x9e3779b9;

  /// Combine hashes 'value' and 'seed' to get a new hash value.  Similar to
  /// boost::hash_combine(), but for uint32_t. This function should be used with a
  /// constant first argument to update the hash value for zero-length values such as
  /// NULL, boolean, and empty strings.
  static inline uint32_t HashCombine32(uint32_t value, uint32_t seed) {
    return seed ^ (HASH_COMBINE_SEED + value + (seed << 6) + (seed >> 2));
  }

  // Get 32 more bits of randomness from a 32-bit hash:
  static inline uint32_t Rehash32to32(const uint32_t hash) {
    // Constants generated by uuidgen(1) with the -r flag
    static const uint64_t m = 0x7850f11ec6d14889ull, a = 0x6773610597ca4c63ull;
    // This is strongly universal hashing following Dietzfelbinger's "Universal hashing
    // and k-wise independent random variables via integer arithmetic without primes". As
    // such, for any two distinct uint32_t's hash1 and hash2, the probability (over the
    // randomness of the constants) that any subset of bit positions of
    // Rehash32to32(hash1) is equal to the same subset of bit positions
    // Rehash32to32(hash2) is minimal.
    return (static_cast<uint64_t>(hash) * m + a) >> 32;
  }

  // The FastHash64 implementation is adapted from
  // https://code.google.com/archive/p/fast-hash/
  // available under MIT license.
  static inline uint64_t FastHashMix(uint64_t h) {
    h ^= h >> 23;
    h *= 0x2127599bf4325c37ULL;
    h ^= h >> 47;
    return h;
  }

  static inline uint64_t FastHash64(const void* buf, int64_t len, uint64_t seed)
  {
    const uint64_t m = 0x880355f21e6d1965ULL;
    const uint64_t* pos = reinterpret_cast<const uint64_t*>(buf);
    const uint64_t* end = pos + (len / 8);
    uint64_t h = seed ^ (len * m);
    uint64_t v;

    while (pos != end) {
      v  = *pos++;
      h ^= FastHashMix(v);
      h *= m;
    }

    const uint8_t* pos2 = reinterpret_cast<const uint8_t*>(pos);
    v = 0;

    switch (len & 7) {
      case 7: v ^= static_cast<uint64_t>(pos2[6]) << 48;
      case 6: v ^= static_cast<uint64_t>(pos2[5]) << 40;
      case 5: v ^= static_cast<uint64_t>(pos2[4]) << 32;
      case 4: v ^= static_cast<uint64_t>(pos2[3]) << 24;
      case 3: v ^= static_cast<uint64_t>(pos2[2]) << 16;
      case 2: v ^= static_cast<uint64_t>(pos2[1]) << 8;
      case 1: v ^= static_cast<uint64_t>(pos2[0]);
        h ^= FastHashMix(v);
        h *= m;
    }

    return FastHashMix(h);
  }
};

}

#endif
