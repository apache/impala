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

#ifndef IMPALA_RUNTIME_MULTI_PRECISION_H
#define IMPALA_RUNTIME_MULTI_PRECISION_H


/// We want to use boost's multi precision library which is only available starting
/// in boost 1.5. For older version of boost we will use the copy in thirdparty.
#include <boost/version.hpp>
#if BOOST_VERSION < 105000
/// The boost library is for C++11 on a newer version of boost than we use.
/// We need to make these #defines to compile for pre c++11
#define BOOST_NOEXCEPT
#define BOOST_NOEXCEPT_IF(Predicate)
#define BOOST_FORCEINLINE inline __attribute__ ((__always_inline__))

#define BOOST_NO_CXX11_CONSTEXPR
#define BOOST_NO_CXX11_DECLTYPE
#define BOOST_NO_CXX11_EXPLICIT_CONVERSION_OPERATORS
#define BOOST_NO_CXX11_HDR_ARRAY
#define BOOST_NO_CXX11_RVALUE_REFERENCES
#define BOOST_NO_CXX11_USER_DEFINED_LITERALS
#define BOOST_NO_CXX11_VARIADIC_TEMPLATES

/// Finally include the boost library.
#include "boost_multiprecision/cpp_int.hpp"
#include "boost_multiprecision/cpp_dec_float.hpp"

#else
#include <boost/multiprecision/cpp_int.hpp>
#include <boost/multiprecision/cpp_dec_float.hpp>
#endif

#include <functional>
#include <limits>

#include "util/arithmetic-util.h"

namespace impala {

/// We use the c++ int128_t type. This is stored using 16 bytes and very performant.
typedef __int128_t int128_t;

/// Define 256 bit int type.
typedef boost::multiprecision::number<
    boost::multiprecision::cpp_int_backend<256, 256,
    boost::multiprecision::signed_magnitude,
    boost::multiprecision::unchecked, void>> int256_t;

/// There is no implicit assignment from int128_t to int256_t (or in general, the boost
/// multi precision types and __int128_t).
/// TODO: look into the perf of this. I think the boost library is very slow with bitwise
/// ops but reasonably fast with arithmetic ops so different implementations of this
/// could have big perf differences.
inline int256_t ConvertToInt256(const int128_t& x) {
  if (x < 0) {
    uint64_t hi = static_cast<uint64_t>(-x >> 64);
    uint64_t lo = static_cast<uint64_t>(-x);
    int256_t v = hi;
    v <<= 64;
    v |= lo;
    return -v;
  } else {
    uint64_t hi = static_cast<uint64_t>(x >> 64);
    uint64_t lo = static_cast<uint64_t>(x);
    int256_t v = hi;
    v <<= 64;
    v |= lo;
    return v;
  }
}

/// Converts an int256_t to an int128_t.  int256_t does support convert_to<int128_t>() but
/// that produces an approximate int128_t which makes it unusable.
/// Instead, we'll construct it using convert_to<int64_t> which is exact.
/// *overflow is set to true if the value cannot be converted. The return value is
/// undefined in this case.
inline int128_t ConvertToInt128(int256_t x, int128_t max_value, bool* overflow) {
  bool negative = false;
  if (x < 0) {
    x = -x;
    negative = true;
  }

  /// Extract the values in base int64_t::max() and reconstruct the new value
  /// as an int128_t.
  uint64_t base = std::numeric_limits<int64_t>::max();
  int128_t result = 0;
  int128_t scale = 1;
  while (x != 0) {
    uint64_t v = (x % base).convert_to<uint64_t>();
    x /= base;
    *overflow |= (v > max_value / scale);
    int128_t n =
        ArithmeticUtil::AsUnsigned<std::multiplies>(static_cast<int128_t>(v), scale);
    *overflow |= (result > ArithmeticUtil::AsUnsigned<std::minus>(max_value, n));
    result = ArithmeticUtil::AsUnsigned<std::plus>(result, n);
    scale =
        ArithmeticUtil::AsUnsigned<std::multiplies>(scale, static_cast<int128_t>(base));
  }
  return negative ? ArithmeticUtil::Negate(result) : result;
}

/// abs() is not defined for int128_t. Name it abs() so it can be compatible with
/// native int types in templates.
inline int128_t abs(const int128_t& x) { return (x < 0) ? -x : x; }

/// Get the high and low bits of an int128_t
inline uint64_t HighBits(int128_t x) {
  return x >> 64;
}
inline uint64_t LowBits(int128_t x) {
  return x & 0xffffffffffffffff;
}

// Doubles the width of integer types (e.g. int32_t -> int64_t).
// Currently only works with a few signed types.
// Feel free to extend it to other types as well.
template <typename T>
struct DoubleWidth {};

template <>
struct DoubleWidth<int32_t> {
  using type = int64_t;
};

template <>
struct DoubleWidth<int64_t> {
  using type = int128_t;
};

template <>
struct DoubleWidth<int128_t> {
  using type = int256_t;
};

/// Return an integer signifying the sign of the value, returning +1 for
/// positive integers (and zero), -1 for negative integers.
/// The extra shift is to silence GCC warnings about full width shift on
/// unsigned types. It compiles out in optimized builds into the expected increment.
template<typename T>
constexpr static inline T Sign(T value) {
  return 1 | ((value >> (ArithmeticUtil::UnsignedWidth<T>() - 1)) >> 1);
}

template<>
inline int256_t Sign(int256_t value) {
  return value < 0 ? -1 : 1;
}

}

#endif
