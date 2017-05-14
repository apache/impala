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

#ifndef IMPALA_EXPRS_BIT_BYTE_FUNCTIONS_H
#define IMPALA_EXPRS_BIT_BYTE_FUNCTIONS_H

#include "udf/udf.h"

namespace impala {

using impala_udf::FunctionContext;
using impala_udf::TinyIntVal;
using impala_udf::SmallIntVal;
using impala_udf::IntVal;
using impala_udf::BigIntVal;

class BitByteFunctions {
 public:
  // Count number of bits set in binary representation of integer types (aka popcount)
  template <typename T> static IntVal CountSet(FunctionContext* ctx, const T& v);
  template <typename T> static IntVal CountSet(FunctionContext* ctx, const T& v,
      const IntVal& bitval);

  // Get and set individual bits in binary representation of integer types
  template <typename T> static TinyIntVal GetBit(FunctionContext* ctx, const T& v,
      const IntVal& bitpos);
  template <typename T> static T SetBit(FunctionContext* ctx, const T& v,
      const IntVal& bitpos);
  template <typename T> static T SetBit(FunctionContext* ctx, const T& v,
      const IntVal& bitpos, const IntVal&);

  // Bitwise rotation of integer types
  static TinyIntVal RotateLeft(FunctionContext* ctx, const TinyIntVal& v,
      const IntVal& shift);
  static SmallIntVal RotateLeft(FunctionContext* ctx, const SmallIntVal& v,
      const IntVal& shift);
  static IntVal RotateLeft(FunctionContext* ctx, const IntVal& v,
      const IntVal& shift);
  static BigIntVal RotateLeft(FunctionContext* ctx, const BigIntVal& v,
      const IntVal& shift);
  static TinyIntVal RotateRight(FunctionContext* ctx, const TinyIntVal& v,
      const IntVal& shift);
  static SmallIntVal RotateRight(FunctionContext* ctx, const SmallIntVal& v,
      const IntVal& shift);
  static IntVal RotateRight(FunctionContext* ctx, const IntVal& v,
      const IntVal& shift);
  static BigIntVal RotateRight(FunctionContext* ctx, const BigIntVal& v,
      const IntVal& shift);

  // Bitwise logical shifts of integer types
  static TinyIntVal ShiftLeft(FunctionContext* ctx, const TinyIntVal& v,
      const IntVal& shift);
  static SmallIntVal ShiftLeft(FunctionContext* ctx, const SmallIntVal& v,
      const IntVal& shift);
  static IntVal ShiftLeft(FunctionContext* ctx, const IntVal& v,
      const IntVal& shift);
  static BigIntVal ShiftLeft(FunctionContext* ctx, const BigIntVal& v,
      const IntVal& shift);
  static TinyIntVal ShiftRight(FunctionContext* ctx, const TinyIntVal& v,
      const IntVal& shift);
  static SmallIntVal ShiftRight(FunctionContext* ctx, const SmallIntVal& v,
      const IntVal& shift);
  static IntVal ShiftRight(FunctionContext* ctx, const IntVal& v,
      const IntVal& shift);
  static BigIntVal ShiftRight(FunctionContext* ctx, const BigIntVal& v,
      const IntVal& shift);
};

}

#endif
