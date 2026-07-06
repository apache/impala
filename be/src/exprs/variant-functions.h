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

#include "udf/udf-internal.h"

namespace impala {

using impala_udf::FunctionContext;
using impala_udf::AnyVal;
using impala_udf::BooleanVal;
using impala_udf::TinyIntVal;
using impala_udf::SmallIntVal;
using impala_udf::IntVal;
using impala_udf::BigIntVal;
using impala_udf::FloatVal;
using impala_udf::DoubleVal;
using impala_udf::StringVal;
using impala_udf::DateVal;
using impala_udf::VariantVal;

/// Built-in functions that operate on VARIANT values.
///
/// variant_get(VARIANT v, STRING path) -> VARIANT
///   Extracts the sub-value at 'path' (JSONPath-style: '$', '$.f', '$.f.g', '$.arr[0]',
///   mixed) and returns it as a VARIANT. The result shares 'v's metadata dictionary
///   unchanged and its value blob is a zero-copy slice of 'v's value blob (the encoding
///   shares one top-level dictionary, so navigating never re-encodes). '$' returns 'v'.
///
/// variant_get(VARIANT v, STRING path, STRING type) -> <type>
///   Extracts the sub-value at 'path' and coerces it to the named scalar 'type'
///   (BOOLEAN/TINYINT/SMALLINT/INT/BIGINT/FLOAT/DOUBLE/STRING/DATE). A STRING result is
///   the raw text of a string value, else its JSON rendering; TIME/TIMESTAMPTZ/UUID fail
///   to coerce, since the serializer cannot render them yet.
///
/// try_variant_get() are the non-strict variants: instead of raising an error on a
/// corrupt metadata blob or a 3-arg type mismatch/overflow, they return SQL NULL.
///
/// Common NULL semantics for all forms: NULL input, path-not-found, and a variant null
/// at the path all yield SQL NULL without raising an error.
class VariantFunctions {
 public:
  // Converts a variant (metadata + value blobs) to a JSON string.
  // Args: metadata (STRING/BINARY), value (STRING/BINARY)
  static StringVal VariantToJson(FunctionContext* ctx, const StringVal& metadata,
      const StringVal& value);

  // variant_to_json(VARIANT) -> STRING: renders a first-class VARIANT value as JSON.
  static StringVal VariantToJson(FunctionContext* ctx, const VariantVal& v);

  /// 2-arg: returns the sub-value at 'path' as a VARIANT (zero-copy slice).
  static VariantVal VariantGet(
      FunctionContext* ctx, const VariantVal& v, const StringVal& path);
  static VariantVal TryVariantGet(
      FunctionContext* ctx, const VariantVal& v, const StringVal& path);

  /// 3-arg: extract at 'path' and coerce to the named scalar type. 'type' is a string
  /// type tag validated by the frontend; the backend selects the overload by return type.
  static BooleanVal VariantGetBoolean(FunctionContext* ctx, const VariantVal& v,
      const StringVal& path, const StringVal& type);
  static TinyIntVal VariantGetTinyInt(FunctionContext* ctx, const VariantVal& v,
      const StringVal& path, const StringVal& type);
  static SmallIntVal VariantGetSmallInt(FunctionContext* ctx, const VariantVal& v,
      const StringVal& path, const StringVal& type);
  static IntVal VariantGetInt(FunctionContext* ctx, const VariantVal& v,
      const StringVal& path, const StringVal& type);
  static BigIntVal VariantGetBigInt(FunctionContext* ctx, const VariantVal& v,
      const StringVal& path, const StringVal& type);
  static FloatVal VariantGetFloat(FunctionContext* ctx, const VariantVal& v,
      const StringVal& path, const StringVal& type);
  static DoubleVal VariantGetDouble(FunctionContext* ctx, const VariantVal& v,
      const StringVal& path, const StringVal& type);
  static StringVal VariantGetString(FunctionContext* ctx, const VariantVal& v,
      const StringVal& path, const StringVal& type);
  static DateVal VariantGetDate(FunctionContext* ctx, const VariantVal& v,
      const StringVal& path, const StringVal& type);

  static BooleanVal TryVariantGetBoolean(FunctionContext* ctx, const VariantVal& v,
      const StringVal& path, const StringVal& type);
  static TinyIntVal TryVariantGetTinyInt(FunctionContext* ctx, const VariantVal& v,
      const StringVal& path, const StringVal& type);
  static SmallIntVal TryVariantGetSmallInt(FunctionContext* ctx, const VariantVal& v,
      const StringVal& path, const StringVal& type);
  static IntVal TryVariantGetInt(FunctionContext* ctx, const VariantVal& v,
      const StringVal& path, const StringVal& type);
  static BigIntVal TryVariantGetBigInt(FunctionContext* ctx, const VariantVal& v,
      const StringVal& path, const StringVal& type);
  static FloatVal TryVariantGetFloat(FunctionContext* ctx, const VariantVal& v,
      const StringVal& path, const StringVal& type);
  static DoubleVal TryVariantGetDouble(FunctionContext* ctx, const VariantVal& v,
      const StringVal& path, const StringVal& type);
  static StringVal TryVariantGetString(FunctionContext* ctx, const VariantVal& v,
      const StringVal& path, const StringVal& type);
  static DateVal TryVariantGetDate(FunctionContext* ctx, const VariantVal& v,
      const StringVal& path, const StringVal& type);
};

}  // namespace impala
