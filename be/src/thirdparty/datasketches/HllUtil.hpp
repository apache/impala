/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef _HLLUTIL_HPP_
#define _HLLUTIL_HPP_

#include "MurmurHash3.h"
#include "RelativeErrorTables.hpp"
#include "count_zeros.hpp"
#include "common_defs.hpp"
#include "ceiling_power_of_2.hpp"

#include <cmath>
#include <stdexcept>
#include <string>

namespace datasketches {

enum hll_mode { LIST = 0, SET, HLL };

// template provides internal consistency and allows static float values
// but we don't use the template parameter anywhere
template<typename A = std::allocator<uint8_t> >
class HllUtil final {
public:
  // preamble stuff
  static const int SER_VER = 1;
  static const int FAMILY_ID = 7;

  static const int EMPTY_FLAG_MASK          = 4;
  static const int COMPACT_FLAG_MASK        = 8;
  static const int OUT_OF_ORDER_FLAG_MASK   = 16;
  static const int FULL_SIZE_FLAG_MASK      = 32;

  static const int PREAMBLE_INTS_BYTE = 0;
  static const int SER_VER_BYTE       = 1;
  static const int FAMILY_BYTE        = 2;
  static const int LG_K_BYTE          = 3;
  static const int LG_ARR_BYTE        = 4;
  static const int FLAGS_BYTE         = 5;
  static const int LIST_COUNT_BYTE    = 6;
  static const int HLL_CUR_MIN_BYTE   = 6;
  static const int MODE_BYTE          = 7; // lo2bits = curMode, next 2 bits = tgtHllMode

  // Coupon List
  static const int LIST_INT_ARR_START = 8;
  static const int LIST_PREINTS = 2;
  // Coupon Hash Set
  static const int HASH_SET_COUNT_INT             = 8;
  static const int HASH_SET_INT_ARR_START         = 12;
  static const int HASH_SET_PREINTS         = 3;
  // HLL
  static const int HLL_PREINTS = 10;
  static const int HLL_BYTE_ARR_START = 40;
  static const int HIP_ACCUM_DOUBLE = 8;
  static const int KXQ0_DOUBLE = 16;
  static const int KXQ1_DOUBLE = 24;
  static const int CUR_MIN_COUNT_INT = 32;
  static const int AUX_COUNT_INT = 36;
  
  static const int EMPTY_SKETCH_SIZE_BYTES = 8;

  // other HllUtil stuff
  static const int KEY_BITS_26 = 26;
  static const int VAL_BITS_6 = 6;
  static const int KEY_MASK_26 = (1 << KEY_BITS_26) - 1;
  static const int VAL_MASK_6 = (1 << VAL_BITS_6) - 1;
  static const int EMPTY = 0;
  static const int MIN_LOG_K = 4;
  static const int MAX_LOG_K = 21;

  static const double HLL_HIP_RSE_FACTOR; // sqrt(log(2.0)) = 0.8325546
  static const double HLL_NON_HIP_RSE_FACTOR; // sqrt((3.0 * log(2.0)) - 1.0) = 1.03896
  static const double COUPON_RSE_FACTOR; // 0.409 at transition point not the asymptote
  static const double COUPON_RSE; // COUPON_RSE_FACTOR / (1 << 13);

  static const int LG_INIT_LIST_SIZE = 3;
  static const int LG_INIT_SET_SIZE = 5;
  static const int RESIZE_NUMER = 3;
  static const int RESIZE_DENOM = 4;

  static const int loNibbleMask = 0x0f;
  static const int hiNibbleMask = 0xf0;
  static const int AUX_TOKEN = 0xf;

  /**
  * Log2 table sizes for exceptions based on lgK from 0 to 26.
  * However, only lgK from 4 to 21 are used.
  */
  static const int LG_AUX_ARR_INTS[];

  static int coupon(const uint64_t hash[]);
  static int coupon(const HashState& hashState);
  static void hash(const void* key, int keyLen, uint64_t seed, HashState& result);
  static int checkLgK(int lgK);
  static void checkMemSize(uint64_t minBytes, uint64_t capBytes);
  static inline void checkNumStdDev(int numStdDev);
  static int pair(int slotNo, int value);
  static int getLow26(unsigned int coupon);
  static int getValue(unsigned int coupon);
  static double invPow2(int e);
  static unsigned int ceilingPowerOf2(unsigned int n);
  static unsigned int simpleIntLog2(unsigned int n); // n must be power of 2
  static int computeLgArrInts(hll_mode mode, int count, int lgConfigK);
  static double getRelErr(bool upperBound, bool unioned,
                          int lgConfigK, int numStdDev);
};

template<typename A>
const double HllUtil<A>::HLL_HIP_RSE_FACTOR = sqrt(log(2.0)); // 0.8325546
template<typename A>
const double HllUtil<A>::HLL_NON_HIP_RSE_FACTOR = sqrt((3.0 * log(2.0)) - 1.0); // 1.03896
template<typename A>
const double HllUtil<A>::COUPON_RSE_FACTOR = 0.409;
template<typename A>
const double HllUtil<A>::COUPON_RSE = COUPON_RSE_FACTOR / (1 << 13);

template<typename A>
const int HllUtil<A>::LG_AUX_ARR_INTS[] = {
      0, 2, 2, 2, 2, 2, 2, 3, 3, 3,   // 0 - 9
      4, 4, 5, 5, 6, 7, 8, 9, 10, 11, // 10-19
      12, 13, 14, 15, 16, 17, 18      // 20-26
      };

template<typename A>
inline int HllUtil<A>::coupon(const uint64_t hash[]) {
  int addr26 = (int) (hash[0] & KEY_MASK_26);
  int lz = count_leading_zeros_in_u64(hash[1]);
  int value = ((lz > 62 ? 62 : lz) + 1); 
  return (value << KEY_BITS_26) | addr26;
}

template<typename A>
inline int HllUtil<A>::coupon(const HashState& hashState) {
  int addr26 = (int) (hashState.h1 & KEY_MASK_26);
  int lz = count_leading_zeros_in_u64(hashState.h2);  
  int value = ((lz > 62 ? 62 : lz) + 1); 
  return (value << KEY_BITS_26) | addr26;
}

template<typename A>
inline void HllUtil<A>::hash(const void* key, const int keyLen, const uint64_t seed, HashState& result) {
  MurmurHash3_x64_128(key, keyLen, seed, result);
}

template<typename A>
inline double HllUtil<A>::getRelErr(const bool upperBound, const bool unioned,
                                    const int lgConfigK, const int numStdDev) {
  return RelativeErrorTables<A>::getRelErr(upperBound, unioned, lgConfigK, numStdDev);
}

template<typename A>
inline int HllUtil<A>::checkLgK(const int lgK) {
  if ((lgK >= HllUtil<A>::MIN_LOG_K) && (lgK <= HllUtil<A>::MAX_LOG_K)) {
    return lgK;
  } else {
    throw std::invalid_argument("Invalid value of k: " + std::to_string(lgK));
  }
}

template<typename A>
inline void HllUtil<A>::checkMemSize(const uint64_t minBytes, const uint64_t capBytes) {
  if (capBytes < minBytes) {
    throw std::invalid_argument("Given destination array is not large enough: " + std::to_string(capBytes));
  }
}

template<typename A>
inline void HllUtil<A>::checkNumStdDev(const int numStdDev) {
  if ((numStdDev < 1) || (numStdDev > 3)) {
    throw std::invalid_argument("NumStdDev may not be less than 1 or greater than 3.");
  }
}

template<typename A>
inline int HllUtil<A>::pair(const int slotNo, const int value) {
  return (value << HllUtil<A>::KEY_BITS_26) | (slotNo & HllUtil<A>::KEY_MASK_26);
}

template<typename A>
inline int HllUtil<A>::getLow26(const unsigned int coupon) {
  return coupon & HllUtil<A>::KEY_MASK_26;
}

template<typename A>
inline int HllUtil<A>::getValue(const unsigned int coupon) {
  return coupon >> HllUtil<A>::KEY_BITS_26;
}

template<typename A>
inline double HllUtil<A>::invPow2(const int e) {
  union {
    long long longVal;
    double doubleVal;
  } conv;
  conv.longVal = (1023L - e) << 52;
  return conv.doubleVal;
}

template<typename A>
inline uint32_t HllUtil<A>::simpleIntLog2(uint32_t n) {
  if (n == 0) {
    throw std::logic_error("cannot take log of 0");
  }
  return count_trailing_zeros_in_u32(n);
}

template<typename A>
inline int HllUtil<A>::computeLgArrInts(hll_mode mode, int count, int lgConfigK) {
  // assume value missing and recompute
  if (mode == LIST) { return HllUtil<A>::LG_INIT_LIST_SIZE; }
  int ceilPwr2 = ceiling_power_of_2(count);
  if ((HllUtil<A>::RESIZE_DENOM * count) > (HllUtil<A>::RESIZE_NUMER * ceilPwr2)) { ceilPwr2 <<= 1;}
  if (mode == SET) {
    return fmax(HllUtil<A>::LG_INIT_SET_SIZE, HllUtil<A>::simpleIntLog2(ceilPwr2));
  }
  //only used for HLL4
  return fmax(HllUtil<A>::LG_AUX_ARR_INTS[lgConfigK], HllUtil<A>::simpleIntLog2(ceilPwr2));
}

}

#endif /* _HLLUTIL_HPP_ */
