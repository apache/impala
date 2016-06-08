// Copyright 2016 Cloudera Inc.
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

#include "util/bit-util.h"

namespace impala {

void BitUtil::ByteSwap(void* dest, const void* source, int len) {
  uint8_t* dst = reinterpret_cast<uint8_t*>(dest);
  const uint8_t* src = reinterpret_cast<const uint8_t*>(source);
  switch (len) {
    case 1:
      *reinterpret_cast<uint8_t*>(dst) = *reinterpret_cast<const uint8_t*>(src);
      return;
    case 2:
      *reinterpret_cast<uint16_t*>(dst) =
          ByteSwap(*reinterpret_cast<const uint16_t*>(src));
      return;
    case 3:
      *reinterpret_cast<uint16_t*>(dst + 1) =
          ByteSwap(*reinterpret_cast<const uint16_t*>(src));
      *reinterpret_cast<uint8_t*>(dst) = *reinterpret_cast<const uint8_t*>(src + 2);
      return;
    case 4:
      *reinterpret_cast<uint32_t*>(dst) =
          ByteSwap(*reinterpret_cast<const uint32_t*>(src));
      return;
    case 5:
      *reinterpret_cast<uint32_t*>(dst + 1) =
          ByteSwap(*reinterpret_cast<const uint32_t*>(src));
      *reinterpret_cast<uint8_t*>(dst) = *reinterpret_cast<const uint8_t*>(src + 4);
      return;
    case 6:
      *reinterpret_cast<uint32_t*>(dst + 2) =
          ByteSwap(*reinterpret_cast<const uint32_t*>(src));
      *reinterpret_cast<uint16_t*>(dst) =
          ByteSwap(*reinterpret_cast<const uint16_t*>(src + 4));
      return;
    case 7:
      *reinterpret_cast<uint32_t*>(dst + 3) =
          ByteSwap(*reinterpret_cast<const uint32_t*>(src));
      *reinterpret_cast<uint16_t*>(dst + 1) =
          ByteSwap(*reinterpret_cast<const uint16_t*>(src + 4));
      *reinterpret_cast<uint8_t*>(dst) = *reinterpret_cast<const uint8_t*>(src + 6);
      return;
    case 8:
      *reinterpret_cast<uint64_t*>(dst) =
          ByteSwap(*reinterpret_cast<const uint64_t*>(src));
      return;
    case 9:
      *reinterpret_cast<uint64_t*>(dst + 1) =
          ByteSwap(*reinterpret_cast<const uint64_t*>(src));
      *reinterpret_cast<uint8_t*>(dst) = *reinterpret_cast<const uint8_t*>(src + 8);
      return;
    case 10:
      *reinterpret_cast<uint64_t*>(dst + 2) =
          ByteSwap(*reinterpret_cast<const uint64_t*>(src));
      *reinterpret_cast<uint16_t*>(dst) =
          ByteSwap(*reinterpret_cast<const uint16_t*>(src + 8));
      return;
    case 11:
      *reinterpret_cast<uint64_t*>(dst + 3) =
          ByteSwap(*reinterpret_cast<const uint64_t*>(src));
      *reinterpret_cast<uint16_t*>(dst + 1) =
          ByteSwap(*reinterpret_cast<const uint16_t*>(src + 8));
      *reinterpret_cast<uint8_t*>(dst) = *reinterpret_cast<const uint8_t*>(src + 10);
      return;
    case 12:
      *reinterpret_cast<uint64_t*>(dst + 4) =
          ByteSwap(*reinterpret_cast<const uint64_t*>(src));
      *reinterpret_cast<uint32_t*>(dst) =
          ByteSwap(*reinterpret_cast<const uint32_t*>(src + 8));
      return;
    case 13:
      *reinterpret_cast<uint64_t*>(dst + 5) =
          ByteSwap(*reinterpret_cast<const uint64_t*>(src));
      *reinterpret_cast<uint32_t*>(dst + 1) =
          ByteSwap(*reinterpret_cast<const uint32_t*>(src + 8));
      *reinterpret_cast<uint8_t*>(dst) = *reinterpret_cast<const uint8_t*>(src + 12);
      return;
    case 14:
      *reinterpret_cast<uint64_t*>(dst + 6) =
          ByteSwap(*reinterpret_cast<const uint64_t*>(src));
      *reinterpret_cast<uint32_t*>(dst + 2) =
          ByteSwap(*reinterpret_cast<const uint32_t*>(src + 8));
      *reinterpret_cast<uint16_t*>(dst) =
          ByteSwap(*reinterpret_cast<const uint16_t*>(src + 12));
      return;
    case 15:
      *reinterpret_cast<uint64_t*>(dst + 7) =
          ByteSwap(*reinterpret_cast<const uint64_t*>(src));
      *reinterpret_cast<uint32_t*>(dst + 3) =
          ByteSwap(*reinterpret_cast<const uint32_t*>(src + 8));
      *reinterpret_cast<uint16_t*>(dst + 1) =
          ByteSwap(*reinterpret_cast<const uint16_t*>(src + 12));
      *reinterpret_cast<uint8_t*>(dst) = *reinterpret_cast<const uint8_t*>(src + 14);
      return;
    case 16:
      *reinterpret_cast<uint64_t*>(dst + 8) =
          ByteSwap(*reinterpret_cast<const uint64_t*>(src));
      *reinterpret_cast<uint64_t*>(dst) =
          ByteSwap(*reinterpret_cast<const uint64_t*>(src + 8));
      return;
    default:
      // Revert to slow loop-based swap.
      for (int i = 0; i < len; ++i) {
        dst[i] = src[len - i - 1];
      }
      return;
  }
}

}
