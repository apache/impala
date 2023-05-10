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

#ifndef IMPALA_ARITHMETIC_UTIL_H
#define IMPALA_ARITHMETIC_UTIL_H

#include <cstdint>
#include <limits>
#include <type_traits>

namespace impala {

// Nested 'type' corresponds to the unsigned version of T.
template <typename T>
struct MakeUnsigned {
  using type = std::make_unsigned_t<T>;
};

template <>
struct MakeUnsigned<__int128_t> {
  using type = __uint128_t;
};

// std::make_unsigned<>() also works for unsigned types and returns the same type.
template <>
struct MakeUnsigned< __uint128_t> {
  using type = __uint128_t;
};

template <typename T>
using UnsignedType = typename MakeUnsigned<T>::type;

// Nested 'type' corresponds to the signed version of T.
template <typename T>
struct MakeSigned {
  using type = std::make_signed_t<T>;
};

template <>
struct MakeSigned<__uint128_t> {
  using type = __int128_t;
};

template <typename T>
using SignedType = typename MakeSigned<T>::type;

class ArithmeticUtil {
 private:
  // There are times when it is useful to treat the bits in a signed integer as if they
  // represent an unsigned integer, or vice versa. This is known as "type punning".
  //
  // For type punning signed values to unsigned values we can use the assurance in the
  // standard's [conv.integral] to convert by simply returning the value: "A prvalue of an
  // integer type can be converted to a prvalue of another integer type. ... If the
  // destination type is unsigned, the resulting value is the least unsigned integer
  // congruent to the source integer (modulo 2n where n is the number of bits used to
  // represent the unsigned type). [Note: In a two's complement representation, this
  // conversion is conceptual and there is no change in the bit pattern (if there is no
  // truncation).]"
  //
  // For the other direction, the conversion is implementation-defined: "If the
  // destination type is signed, the value is unchanged if it can be represented in the
  // destination type (and bit-field width); otherwise, the value is
  // implementation-defined."
  //
  // In GCC, the docs promise that when "[t]he result of, or the signal raised by,
  // converting an integer to a signed integer type when the value cannot be represented
  // in an object of that type ... For conversion to a type of width N, the value is
  // reduced modulo 2^N to be within range of the type". As such, the same method of
  // converting by simply returning works.
  //
  // Note that Clang does not document its implementation-defined behavior,
  // https://bugs.llvm.org/show_bug.cgi?id=11272, so the static_asserts below are
  // important
  template <typename T>
  constexpr static inline SignedType<T> ToSigned(T x) {
    return x;
  }

  template <typename T>
  constexpr static inline UnsignedType<T> ToUnsigned(T x) {
    return x;
  }

  friend class ArithmeticUtilTest;

 public:
  // AsUnsigned can be used to perform arithmetic on signed integer types as if they were
  // unsigned. Expected Use looks like "AsUnsigned<std::plus>(-1, 28)".
  template <template <typename> class Operator, typename T>
  static T AsUnsigned(T x, T y) {
    const auto a = ToUnsigned(x), b = ToUnsigned(y);
    return ToSigned(Operator<UnsignedType<T>>()(a, b));
  }

  // Compute() is meant to be used like AsUnsigned(), but when the template context does
  // not enforce that the type T is integral. For floating point types, it performs
  // Operator (i.e. std::plus) without modification, and for integral types it calls
  // AsUnsigned().
  //
  // It is needed because AsUnsigned<std::plus>(1.0, 2.0) does not compile, since
  // UnsignedType<float> is not a valid type. In contrast, Compute<std::plus>(1.0, 2.0)
  // does compile and performs the usual addition on 1.0 and 2.0 to produce 3.0.
  template <template <typename> class Operator, typename T>
  static T Compute(T x, T y) {
    return OperateOn<T>::template Compute<Operator>(x, y);
  }

  // Negation of the least value of signed two's-complement types is undefined behavior.
  // This operator makes that behavior defined by doing it in the unsigned domain. Note
  // that this induces Negate(INT_MIN) == INT_MIN, though otherwise produces identical
  // behavior to just using the usual unary negation operator like "-x".
  template<typename T>
  static T Negate(T x) {
    return ToSigned(-ToUnsigned(x));
  }

 private:
  // Ring and OperateOn are used for compile-time dispatching on how Compute() should
  // perform an arithmetic operation: as an unsigned integer operation, as a
  // floating-point operation, or not at all.
  //
  // For example, OperatorOn<int>::Compute<std::plus> is really just an alias for
  // AsUnsigned<std::plus, int>, while OperatorOn<float>::Compute<std::plus> is really
  // just an alias for the usual addition operator on floats.
  enum class Ring { INTEGER, FLOAT, NEITHER };

  template <typename T,
      Ring R = std::is_integral<T>::value ?
          Ring::INTEGER :
          (std::is_floating_point<T>::value ? Ring::FLOAT : Ring::NEITHER)>
  struct OperateOn;

 public:
  /// Returns the width of the integer portion of the type, not counting the sign bit.
  /// Not safe for use with unknown or non-native types, so make it undefined
  template<typename T, typename CVR_REMOVED = typename std::decay<T>::type,
      typename std::enable_if<std::is_integral<CVR_REMOVED>{} ||
                              std::is_same<CVR_REMOVED, unsigned __int128>{} ||
                              std::is_same<CVR_REMOVED, __int128>{}, int>::type = 0>
  constexpr static inline int UnsignedWidth() {
    return std::is_integral<CVR_REMOVED>::value ?
        std::numeric_limits<CVR_REMOVED>::digits :
        std::is_same<CVR_REMOVED, unsigned __int128>::value ? 128 :
        std::is_same<CVR_REMOVED, __int128>::value ? 127 : -1;
  }

  /// Returns the max value that can be represented in T.
  template<typename T, typename CVR_REMOVED = typename std::decay<T>::type,
      typename std::enable_if<std::is_integral<CVR_REMOVED> {}||
                              std::is_same<CVR_REMOVED, __int128> {}, int>::type = 0>
  constexpr static inline CVR_REMOVED Max() {
    return std::is_integral<CVR_REMOVED>::value ?
        std::numeric_limits<CVR_REMOVED>::max() :
        std::is_same<CVR_REMOVED, __int128>::value ?
            static_cast<UnsignedType<CVR_REMOVED>>(-1) / 2 : -1;
  }

};

template <typename T>
struct ArithmeticUtil::OperateOn<T, ArithmeticUtil::Ring::FLOAT> {
  template <template <typename> class Operator>
  static T Compute(T a, T b) {
    return Operator<T>()(a, b);
  }
};

template <typename T>
struct ArithmeticUtil::OperateOn<T, ArithmeticUtil::Ring::INTEGER> {
  template <template <typename> class Operator>
  static T Compute(T x, T y) {
    return AsUnsigned<Operator>(x, y);
  }
};

template <typename T>
struct ArithmeticUtil::OperateOn<T, ArithmeticUtil::Ring::NEITHER> {
  template <template <typename> class Operator>
  static T Compute(T x, T y) = delete;
};

class ArithmeticUtilTest {
  static_assert(ArithmeticUtil::ToSigned<uint16_t>(0xffff) == -1
          && ArithmeticUtil::ToSigned<uint16_t>(0x8000) == -0x8000,
      "ToSigned is not a two's complement no-op");
  static_assert(ArithmeticUtil::ToUnsigned<int16_t>(-1) == 0xffff
          && ArithmeticUtil::ToUnsigned<int16_t>(-0x8000) == 0x8000,
      "ToUnsigned is not a two's complement no-op");
};

} // namespace impala

#endif // IMPALA_ARITHMETIC_UTIL_H
