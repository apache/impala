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

#pragma once

#include "udf/udf.h"

#include <string>
#include <vector>

namespace impala {

using impala_udf::BigIntVal;
using impala_udf::DateVal;
using impala_udf::DecimalVal;
using impala_udf::FunctionContext;
using impala_udf::IntVal;
using impala_udf::StringVal;
using impala_udf::TimestampVal;

/// This class holds functions that are related to Iceberg functionality.
/// E.g. implementations of partition transforms.
class IcebergFunctions {
public:
  /// The following functions implement the truncate partition transform that
  /// partitioned Iceberg tables use.
  static IntVal TruncatePartitionTransform(FunctionContext* ctx,
      const IntVal& input, const IntVal& width);
  static BigIntVal TruncatePartitionTransform(FunctionContext* ctx,
      const BigIntVal& input, const BigIntVal& width);
  static DecimalVal TruncatePartitionTransform(FunctionContext* ctx,
      const DecimalVal& input, const IntVal& width);
  static DecimalVal TruncatePartitionTransform(FunctionContext* ctx,
      const DecimalVal& input, const BigIntVal& width);
  static StringVal TruncatePartitionTransform(FunctionContext* ctx,
      const StringVal& input, const IntVal& width);

  /// The following functions implement the bucket partition transform that
  /// partitioned Iceberg tables use.
  static IntVal BucketPartitionTransform(FunctionContext* ctx,
      const IntVal& input, const IntVal& width);
  static IntVal BucketPartitionTransform(FunctionContext* ctx,
      const BigIntVal& input, const IntVal& width);
  static IntVal BucketPartitionTransform(FunctionContext* ctx,
      const DecimalVal& input, const IntVal& width);
  static IntVal BucketPartitionTransform(FunctionContext* ctx,
      const StringVal& input, const IntVal& width);
  static IntVal BucketPartitionTransform(FunctionContext* ctx,
      const DateVal& input, const IntVal& width);
  static IntVal BucketPartitionTransform(FunctionContext* ctx,
      const TimestampVal& input, const IntVal& width);
private:
  friend class IcebergTruncatePartitionTransformTests;
  friend class IcebergBucketPartitionTransformTests;

  /// Error message to raise when width parameter is not positive.
  static const std::string INCORRECT_WIDTH_ERROR_MSG;

  /// Error message to raise when truncate overflows for an int input.
  static const std::string TRUNCATE_OVERFLOW_ERROR_MSG;

  /// The seed used for bucket transform.
  static const unsigned BUCKET_TRANSFORM_SEED;

  /// Checks 'input' for null, 'width' for null and negative values. Returns false if any
  /// of the checks fail, true otherwise. If any of the width checks fail ths also sets
  /// an error message in 'ctx'.
  template<typename T, typename W>
  static bool CheckInputsAndSetError(FunctionContext* ctx, const T& input,
      const W& width);

  template<typename T, typename W>
  static T TruncatePartitionTransformNumericImpl(FunctionContext* ctx, const T& input,
      const W& width);

  /// Helper function for TruncatePartitionTransform for Decimals where the size of the
  /// decimal representation is given as a parameter.
  static DecimalVal TruncateDecimal(FunctionContext* ctx, const DecimalVal& input,
      const BigIntVal& width, int decimal_size);

  template<typename T>
  static DecimalVal TruncatePartitionTransformDecimalImpl(const T& decimal_val,
      int64_t width);

  template<typename T, typename W>
  static IntVal BucketPartitionTransformNumericImpl(FunctionContext* ctx, const T& input,
      const W& width);

  /// Helper function for BucketPartitionTransform for Decimals where the size of the
  /// decimal representation is given as a parameter.
  static IntVal BucketDecimal(FunctionContext* ctx, const DecimalVal& input,
      const IntVal& width, int decimal_size);
};

}
