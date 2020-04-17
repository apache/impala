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

// author Kevin Lang, Yahoo Research
// author Jon Malkin, Yahoo Research

#ifndef _COMMONUTIL_HPP_
#define _COMMONUTIL_HPP_

#include <cstdint>

namespace datasketches {

class CommonUtil final {
  public:
    static unsigned int getNumberOfLeadingZeros(uint64_t x);
};

#define FCLZ_MASK_56 ((uint64_t) 0x00ffffffffffffff)
#define FCLZ_MASK_48 ((uint64_t) 0x0000ffffffffffff)
#define FCLZ_MASK_40 ((uint64_t) 0x000000ffffffffff)
#define FCLZ_MASK_32 ((uint64_t) 0x00000000ffffffff)
#define FCLZ_MASK_24 ((uint64_t) 0x0000000000ffffff)
#define FCLZ_MASK_16 ((uint64_t) 0x000000000000ffff)
#define FCLZ_MASK_08 ((uint64_t) 0x00000000000000ff)

inline unsigned int CommonUtil::getNumberOfLeadingZeros(const uint64_t x) {
  if (x == 0)
    return 64;

  static const uint8_t clzByteCount[256] = {8,7,6,6,5,5,5,5,4,4,4,4,4,4,4,4,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,3,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0};

  if (x > FCLZ_MASK_56)
    return ((unsigned int) ( 0 + clzByteCount[(x >> 56) & FCLZ_MASK_08]));
  if (x > FCLZ_MASK_48)
    return ((unsigned int) ( 8 + clzByteCount[(x >> 48) & FCLZ_MASK_08]));
  if (x > FCLZ_MASK_40)
    return ((unsigned int) (16 + clzByteCount[(x >> 40) & FCLZ_MASK_08]));
  if (x > FCLZ_MASK_32)
    return ((unsigned int) (24 + clzByteCount[(x >> 32) & FCLZ_MASK_08]));
  if (x > FCLZ_MASK_24)
    return ((unsigned int) (32 + clzByteCount[(x >> 24) & FCLZ_MASK_08]));
  if (x > FCLZ_MASK_16)
    return ((unsigned int) (40 + clzByteCount[(x >> 16) & FCLZ_MASK_08]));
  if (x > FCLZ_MASK_08)
    return ((unsigned int) (48 + clzByteCount[(x >>  8) & FCLZ_MASK_08]));
  if (1)
    return ((unsigned int) (56 + clzByteCount[(x >>  0) & FCLZ_MASK_08]));

}


}

#endif // _COMMONUTIL_HPP_