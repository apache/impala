#ifndef IMPALA_EXPERIMENTS_HASHING_HASHING_UTIL_H
#define IMPALA_EXPERIMENTS_HASHING_HASHING_UTIL_H

#include <nmmintrin.h>

namespace impala {

// for now, always just hash on id
inline uint32_t hash_id(int32_t id) {
  //return HashUtil::Hash(&id, sizeof(id), 0);
  // copied and pasted in because the CPUInfo calls weren't inling.
  uint32_t hash = 0;
  hash = _mm_crc32_u32(hash, id);
  return hash;
}

}

#endif
