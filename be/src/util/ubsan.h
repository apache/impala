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

#ifndef UTIL_UBSAN_H_
#define UTIL_UBSAN_H_

// Utilities mimicking parts of the standard prone to accidentally using in a way causeing
// undefined behavior.

#include <cstring>
#include <type_traits>

#include "common/logging.h"

class Ubsan {
 public:
  static void* MemSet(void* s, int c, size_t n) {
    if (s == nullptr) {
      DCHECK_EQ(n, 0);
      return s;
    }
    return std::memset(s, c, n);
  }
  static void* MemCpy(void* dest, const void* src, size_t n) {
    if (dest == nullptr || src == nullptr) {
      DCHECK_EQ(n, 0);
      return dest;
    }
    return std::memcpy(dest, src, n);
  }
  static int MemCmp(const void *s1, const void *s2, size_t n) {
    if (s1 == nullptr || s2 == nullptr) {
      DCHECK_EQ(n, 0);
      return 0;
    }
    return std::memcmp(s1, s2, n);
  }
  // Convert a potential enum value (that may be out of range) into its underlying integer
  // implementation. This is required because, according to the standard, enum values that
  // are out of range induce undefined behavior. For instance,
  //
  //     enum A {B = 0, C = 5};
  //     int d = 6;
  //     A e;
  //     memcpy(&e, &d, sizeof(e));
  //     if (e == B)
  //     ; // undefined behavior: load of value 6, which is not a valid value for type 'A'
  //     if (EnumToInt(&e) == B)
  //     ; // OK: the type of EnumToInt(&e) is int, and B is converted to int for the
  //       // comparison
  //
  // EnumToInt() is a worse alternative to not treating a pointer to arbitrary memory as a
  // pointer to an enum. To put it another way, the first block below is a better way to
  // handle possibly-out-of-range enum values:
  //
  // extern char * v;
  // enum A { B = 0, C = 45 };
  // A x;
  // if (good_way) {
  //   std::underlying_type_t<T> i;
  //   std::memcpy(&i, v, sizeof(i));
  //   if (B <= i && i <= C) x = i;
  // } else {
  //   A * y = reinterpret_cast<A *>(v);
  //   int i = EnumToInt(y);
  //   if (B <= i && i <= C) x = *y;
  // }
  //
  // The second block is worse because y is masquerading as a legitimate pointer to an A
  // and could get dereferenced illegally as the code evolves. Unfortunately,
  // deserialization methods don't always make the better way an option - sometimes the
  // possibly invalid pointer to A (like y) is created externally.
  template<typename T>
  static auto EnumToInt(const T * e) {
    std::underlying_type_t<T> i;
    static_assert(sizeof(i) == sizeof(*e), "enum underlying type is the wrong size");
    // We have to memcpy, rather than directly assigning i = *e, because dereferencing e
    // creates undefined behavior.
    memcpy(&i, e, sizeof(i));
    return i;
  }
};

#endif // UTIL_UBSAN_H_
