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

#include "exprs/variant-functions.h"

#include <cmath>
#include <limits>
#include <string>
#include <string_view>

#include "runtime/date-value.h"
#include "runtime/string-value.h"
#include "runtime/variant-value.h"
#include "util/string-parser.h"
#include "util/variant-util.h"

#include "common/names.h"

namespace impala {

StringVal VariantFunctions::VariantToJson(FunctionContext* ctx,
    const StringVal& metadata, const StringVal& value) {
  if (metadata.is_null || value.is_null) return StringVal::null();
  StringVal result;
  Status status = impala::VariantToJson(
      ctx, metadata.ptr, metadata.len, value.ptr, value.len, &result);
  if (!status.ok()) {
    // A corrupt/unrepresentable variant value yields SQL NULL; surface why via a warning.
    ctx->AddWarning(status.GetDetail().c_str());
    return StringVal::null();
  }
  return result;
}

StringVal VariantFunctions::VariantToJson(FunctionContext* ctx, const VariantVal& v) {
  if (v.is_null) return StringVal::null();
  return VariantFunctions::VariantToJson(ctx, v.metadata, v.value);
}

namespace {

// Outcome of resolving a variant_get path against an input VariantVal.
enum class NavStatus {
  OK,        // 'sub' holds the resolved, non-null sub-value.
  SQL_NULL,  // NULL input, path not found, or a variant null at the path -> SQL NULL.
  CORRUPT,   // metadata blob failed to parse, or the value at the path is empty.
};

// Navigates 'v' to the sub-value at 'path'. 'meta'/'root' are caller-owned storage kept
// alive for the lifetime of '*sub' (which may reference 'root's blob). The path is a
// per-query constant, but NavigatePath is a cheap non-allocating byte scan, so we view
// the per-row StringVal bytes directly.
NavStatus Navigate(const VariantVal& v, const StringVal& path,
    VariantMetadata* meta, VariantValue* sub) {
  if (v.is_null || v.metadata.is_null || v.value.is_null || path.is_null) {
    return NavStatus::SQL_NULL;
  }
  Status s = meta->Init(v.metadata.ptr, v.metadata.len);
  if (!s.ok()) return NavStatus::CORRUPT;
  VariantValue root(v.value.ptr, v.value.len, meta);
  const std::string_view path_view(
      reinterpret_cast<const char*>(path.ptr), path.len);
  if (!root.NavigatePath(path_view, sub)) return NavStatus::SQL_NULL; // segment missing
  // Every encoded value has at least a header byte. An empty value blob or a zero-length
  // field/element is corrupt, and reading its header would go out of bounds.
  if (UNLIKELY(!sub->IsValid() || sub->Len() < 1)) return NavStatus::CORRUPT;
  if (sub->IsNull()) return NavStatus::SQL_NULL; // VNULL at the path
  return NavStatus::OK;
}

// Reports a 3-arg coercion failure: strict variant_get raises an error, try_variant_get
// silently yields SQL NULL.
void FailCoercion(FunctionContext* ctx, bool is_try, const char* msg) {
  if (!is_try) ctx->SetError(msg);
}

// Reports a corrupt variant: strict raises an error, try_ emits a warning.
void FailCorrupt(FunctionContext* ctx, bool is_try) {
  if (is_try) {
    ctx->AddWarning("variant_get: corrupt or unreadable variant");
  } else {
    ctx->SetError("variant_get: corrupt or unreadable variant");
  }
}

// Navigates 'path' against 'v' and, on a corrupt outcome, reports it (strict
// error / try_ warning). Returns the NavStatus for the caller to handle OK/SQL_NULL.
NavStatus NavAndReport(FunctionContext* ctx, const VariantVal& v, const StringVal& path,
    bool is_try, VariantMetadata* meta, VariantValue* sub) {
  NavStatus st = Navigate(v, path, meta, sub);
  if (st == NavStatus::CORRUPT) FailCorrupt(ctx, is_try);
  return st;
}

bool IsStringLike(const VariantValue& sub) {
  return sub.GetBasicType() == VariantBasicType::SHORT_STRING
      || (sub.GetBasicType() == VariantBasicType::PRIMITIVE
             && sub.GetPhysicalType() == VariantPhysicalType::STRING);
}

// Physical types the JSON serializer renders as a fixed placeholder, not as the value.
bool IsUnrenderablePrimitive(const VariantValue& sub) {
  if (sub.GetBasicType() != VariantBasicType::PRIMITIVE) return false;
  switch (sub.GetPhysicalType()) {
    case VariantPhysicalType::TIMESTAMPTZ:
    case VariantPhysicalType::TIMESTAMPTZ_NANOS:
    case VariantPhysicalType::TIME:
    case VariantPhysicalType::UUID:
      return true;
    default:
      return false;
  }
}

// Coercion result for scalar extraction.
enum class Coerce { OK, MISMATCH, OVERFLOW };

// Extracts 'sub' as an int64. Integers widen; DATE yields its days-since-epoch; strings
// are parsed. Everything else is a type mismatch. Range narrowing is checked by callers.
Coerce ToInt64(const VariantValue& sub, int64_t* out) {
  const VariantBasicType bt = sub.GetBasicType();
  if (bt == VariantBasicType::PRIMITIVE) {
    switch (sub.GetPhysicalType()) {
      case VariantPhysicalType::INT8: {
        int8_t t;
        if (!sub.GetInt8(&t)) return Coerce::MISMATCH;
        *out = t;
        return Coerce::OK;
      }
      case VariantPhysicalType::INT16: {
        int16_t t;
        if (!sub.GetInt16(&t)) return Coerce::MISMATCH;
        *out = t;
        return Coerce::OK;
      }
      case VariantPhysicalType::INT32: {
        int32_t t;
        if (!sub.GetInt32(&t)) return Coerce::MISMATCH;
        *out = t;
        return Coerce::OK;
      }
      case VariantPhysicalType::INT64:
        return sub.GetInt64(out) ? Coerce::OK : Coerce::MISMATCH;
      case VariantPhysicalType::DATE: {
        int32_t t;
        if (!sub.ReadValue<int32_t>(&t)) return Coerce::MISMATCH;
        *out = t;
        return Coerce::OK;
      }
      default: break;
    }
  }
  if (IsStringLike(sub)) {
    StringValue sv;
    if (!sub.GetString(&sv)) return Coerce::MISMATCH;
    StringParser::ParseResult pr;
    int64_t r = StringParser::StringToInt<int64_t>(sv.Ptr(), sv.Len(), &pr);
    if (pr == StringParser::PARSE_SUCCESS) {
      *out = r;
      return Coerce::OK;
    }
    if (pr == StringParser::PARSE_OVERFLOW || pr == StringParser::PARSE_UNDERFLOW) {
      return Coerce::OVERFLOW;
    }
    return Coerce::MISMATCH;
  }
  return Coerce::MISMATCH;
}

// Extracts 'sub' as a double. Floats/ints widen; strings are parsed.
Coerce ToDouble(const VariantValue& sub, double* out) {
  const VariantBasicType bt = sub.GetBasicType();
  if (bt == VariantBasicType::PRIMITIVE) {
    switch (sub.GetPhysicalType()) {
      case VariantPhysicalType::FLOAT: {
        float t;
        if (!sub.GetFloat(&t)) return Coerce::MISMATCH;
        *out = t;
        return Coerce::OK;
      }
      case VariantPhysicalType::DOUBLE:
        return sub.GetDouble(out) ? Coerce::OK : Coerce::MISMATCH;
      case VariantPhysicalType::INT8: {
        int8_t t;
        if (!sub.GetInt8(&t)) return Coerce::MISMATCH;
        *out = t;
        return Coerce::OK;
      }
      case VariantPhysicalType::INT16: {
        int16_t t;
        if (!sub.GetInt16(&t)) return Coerce::MISMATCH;
        *out = t;
        return Coerce::OK;
      }
      case VariantPhysicalType::INT32: {
        int32_t t;
        if (!sub.GetInt32(&t)) return Coerce::MISMATCH;
        *out = t;
        return Coerce::OK;
      }
      case VariantPhysicalType::INT64: {
        int64_t t;
        if (!sub.GetInt64(&t)) return Coerce::MISMATCH;
        *out = t;
        return Coerce::OK;
      }
      default: break;
    }
  }
  if (IsStringLike(sub)) {
    StringValue sv;
    if (!sub.GetString(&sv)) return Coerce::MISMATCH;
    StringParser::ParseResult pr;
    double r = StringParser::StringToFloat<double>(sv.Ptr(), sv.Len(), &pr);
    if (pr == StringParser::PARSE_SUCCESS) {
      *out = r;
      return Coerce::OK;
    }
    return Coerce::MISMATCH;
  }
  return Coerce::MISMATCH;
}

// ------------------------------- 2-arg core --------------------------------

VariantVal DoVariantGet(
    FunctionContext* ctx, const VariantVal& v, const StringVal& path, bool is_try) {
  VariantMetadata meta;
  VariantValue sub;
  if (NavAndReport(ctx, v, path, is_try, &meta, &sub) != NavStatus::OK) {
    return VariantVal::null();
  }
  // Share the parent metadata dictionary unchanged and point 'value' at the sub-value's
  // zero-copy slice of the parent value blob. The slice need only stay valid for this
  // row; materialization copies the bytes into the destination pool.
  VariantVal out;
  out.is_null = false;
  out.metadata = v.metadata;
  out.value = StringVal(const_cast<uint8_t*>(sub.Data()), static_cast<int>(sub.Len()));
  return out;
}

// ------------------------------- 3-arg cores -------------------------------

BooleanVal DoGetBoolean(
    FunctionContext* ctx, const VariantVal& v, const StringVal& path, bool is_try) {
  VariantMetadata meta;
  VariantValue sub;
  if (NavAndReport(ctx, v, path, is_try, &meta, &sub) != NavStatus::OK) {
    return BooleanVal::null();
  }
  if (sub.GetBasicType() == VariantBasicType::PRIMITIVE) {
    VariantPhysicalType pt = sub.GetPhysicalType();
    if (pt == VariantPhysicalType::BOOLEAN_TRUE) return BooleanVal(true);
    if (pt == VariantPhysicalType::BOOLEAN_FALSE) return BooleanVal(false);
  }
  StringValue sv;
  if (IsStringLike(sub) && sub.GetString(&sv)) {
    StringParser::ParseResult pr;
    bool r = StringParser::StringToBool(sv.Ptr(), sv.Len(), &pr);
    if (pr == StringParser::PARSE_SUCCESS) return BooleanVal(r);
  }
  FailCoercion(ctx, is_try, "variant_get: value at path is not convertible to BOOLEAN");
  return BooleanVal::null();
}

// Shared integer core for TINYINT/SMALLINT/INT/BIGINT. On success writes 'result' and
// returns true. Returns false for every non-success outcome (SQL NULL, mismatch, or
// overflow); mismatch/overflow additionally report an error under strict variant_get.
bool DoGetInt(FunctionContext* ctx, const VariantVal& v, const StringVal& path,
    bool is_try, int64_t min_val, int64_t max_val, int64_t* result) {
  VariantMetadata meta;
  VariantValue sub;
  if (NavAndReport(ctx, v, path, is_try, &meta, &sub) != NavStatus::OK) return false;
  int64_t x = 0;
  Coerce c = ToInt64(sub, &x);
  if (c == Coerce::MISMATCH) {
    FailCoercion(ctx, is_try, "variant_get: value at path is not convertible to an "
        "integer type");
    return false;
  }
  if (c == Coerce::OVERFLOW || x < min_val || x > max_val) {
    FailCoercion(ctx, is_try, "variant_get: integer value at path is out of range for "
        "the requested type");
    return false;
  }
  *result = x;
  return true;
}

DoubleVal DoGetDouble(
    FunctionContext* ctx, const VariantVal& v, const StringVal& path, bool is_try) {
  VariantMetadata meta;
  VariantValue sub;
  if (NavAndReport(ctx, v, path, is_try, &meta, &sub) != NavStatus::OK) {
    return DoubleVal::null();
  }
  double d = 0;
  if (ToDouble(sub, &d) == Coerce::OK) return DoubleVal(d);
  FailCoercion(ctx, is_try, "variant_get: value at path is not convertible to DOUBLE");
  return DoubleVal::null();
}

FloatVal DoGetFloat(
    FunctionContext* ctx, const VariantVal& v, const StringVal& path, bool is_try) {
  VariantMetadata meta;
  VariantValue sub;
  if (NavAndReport(ctx, v, path, is_try, &meta, &sub) != NavStatus::OK) {
    return FloatVal::null();
  }
  double d = 0;
  if (ToDouble(sub, &d) != Coerce::OK) {
    FailCoercion(ctx, is_try, "variant_get: value at path is not convertible to FLOAT");
    return FloatVal::null();
  }
  // A finite double outside the FLOAT range narrows to +/-inf. Treat that as an
  // out-of-range coercion failure rather than silently returning an infinity.
  float f = static_cast<float>(d);
  if (std::isinf(f) && std::isfinite(d)) {
    FailCoercion(ctx, is_try, "variant_get: value at path is out of range for FLOAT");
    return FloatVal::null();
  }
  return FloatVal(f);
}

DateVal DoGetDate(
    FunctionContext* ctx, const VariantVal& v, const StringVal& path, bool is_try) {
  VariantMetadata meta;
  VariantValue sub;
  if (NavAndReport(ctx, v, path, is_try, &meta, &sub) != NavStatus::OK) {
    return DateVal::null();
  }
  if (sub.GetBasicType() == VariantBasicType::PRIMITIVE) {
    // Only the DATE physical type is treated as a date. A bare INT32 is intentionally NOT
    // coerced: its interpretation as days-since-epoch is not something the value itself
    // asserts, so accepting it would silently turn arbitrary integers into dates.
    if (sub.GetPhysicalType() == VariantPhysicalType::DATE) {
      int32_t days = 0;
      if (!sub.ReadValue<int32_t>(&days)) {
        FailCoercion(ctx, is_try, "variant_get: invalid DATE value at path");
        return DateVal::null();
      }
      DateValue dv(static_cast<int64_t>(days));
      if (dv.IsValid()) return dv.ToDateVal();
      FailCoercion(ctx, is_try, "variant_get: invalid DATE value at path");
      return DateVal::null();
    }
  }
  StringValue sv;
  if (IsStringLike(sub) && sub.GetString(&sv)) {
    StringParser::ParseResult pr;
    DateValue dv = StringParser::StringToDate(sv.Ptr(), sv.Len(), &pr);
    if (pr == StringParser::PARSE_SUCCESS && dv.IsValid()) return dv.ToDateVal();
  }
  FailCoercion(ctx, is_try, "variant_get: value at path is not convertible to DATE");
  return DateVal::null();
}

StringVal DoGetString(
    FunctionContext* ctx, const VariantVal& v, const StringVal& path, bool is_try) {
  VariantMetadata meta;
  VariantValue sub;
  if (NavAndReport(ctx, v, path, is_try, &meta, &sub) != NavStatus::OK) {
    return StringVal::null();
  }
  const VariantBasicType bt = sub.GetBasicType();
  // Raw string value (no JSON quoting). 'sv' points into the input blob, which outlives
  // the row, so return a zero-copy view instead of copying per row.
  if (IsStringLike(sub)) {
    StringValue sv;
    if (!sub.GetString(&sv)) { FailCorrupt(ctx, is_try); return StringVal::null(); }
    return StringVal(reinterpret_cast<uint8_t*>(sv.Ptr()), sv.Len());
  }
  // Returning the serializer's placeholder would fabricate a value, so fail to coerce.
  if (IsUnrenderablePrimitive(sub)) {
    FailCoercion(ctx, is_try, "variant_get: value at path is not convertible to STRING");
    return StringVal::null();
  }
  // Everything else is rendered by the JSON serializer, which owns the canonical
  // formatting for each type (e.g. FLOAT via %g, DATE as an ISO string) and allocates the
  // result once in the function's results pool -- no per-row std::string/heap churn.
  StringVal result;
  Status s = sub.ToJson(ctx, &result);
  if (!s.ok()) { FailCorrupt(ctx, is_try); return StringVal::null(); }
  // Objects and arrays keep their JSON text verbatim. A scalar that the serializer wraps
  // in double quotes (DATE and other temporal/binary types) is unwrapped so the STRING
  // result is the bare value rather than a quoted JSON literal. These types never contain
  // JSON escape sequences, so a simple outer-quote strip is safe.
  if (bt != VariantBasicType::OBJECT && bt != VariantBasicType::ARRAY
      && result.len >= 2 && result.ptr[0] == '"' && result.ptr[result.len - 1] == '"') {
    return StringVal(result.ptr + 1, result.len - 2);
  }
  return result;
}

}  // namespace

// --------------------------------- 2-arg -----------------------------------

VariantVal VariantFunctions::VariantGet(
    FunctionContext* ctx, const VariantVal& v, const StringVal& path) {
  return DoVariantGet(ctx, v, path, /*is_try=*/false);
}

VariantVal VariantFunctions::TryVariantGet(
    FunctionContext* ctx, const VariantVal& v, const StringVal& path) {
  return DoVariantGet(ctx, v, path, /*is_try=*/true);
}

// --------------------------------- 3-arg -----------------------------------

#define VARIANT_GET_INT(FnName, ValType, CType)                                        \
  ValType VariantFunctions::FnName(FunctionContext* ctx, const VariantVal& v,          \
      const StringVal& path, const StringVal& type) {                                  \
    int64_t r = 0;                                                                     \
    if (!DoGetInt(ctx, v, path, /*is_try=*/false,                                      \
            std::numeric_limits<CType>::min(), std::numeric_limits<CType>::max(),      \
            &r)) {                                                                     \
      return ValType::null();                                                          \
    }                                                                                  \
    return ValType(static_cast<CType>(r));                                             \
  }                                                                                    \
  ValType VariantFunctions::Try##FnName(FunctionContext* ctx, const VariantVal& v,     \
      const StringVal& path, const StringVal& type) {                                  \
    int64_t r = 0;                                                                     \
    if (!DoGetInt(ctx, v, path, /*is_try=*/true,                                       \
            std::numeric_limits<CType>::min(), std::numeric_limits<CType>::max(),      \
            &r)) {                                                                     \
      return ValType::null();                                                          \
    }                                                                                  \
    return ValType(static_cast<CType>(r));                                             \
  }

VARIANT_GET_INT(VariantGetTinyInt, TinyIntVal, int8_t)
VARIANT_GET_INT(VariantGetSmallInt, SmallIntVal, int16_t)
VARIANT_GET_INT(VariantGetInt, IntVal, int32_t)
VARIANT_GET_INT(VariantGetBigInt, BigIntVal, int64_t)
#undef VARIANT_GET_INT

#define VARIANT_GET_SIMPLE(FnName, ValType, CoreFn)                                    \
  ValType VariantFunctions::FnName(FunctionContext* ctx, const VariantVal& v,          \
      const StringVal& path, const StringVal& type) {                                  \
    return CoreFn(ctx, v, path, /*is_try=*/false);                                     \
  }                                                                                    \
  ValType VariantFunctions::Try##FnName(FunctionContext* ctx, const VariantVal& v,     \
      const StringVal& path, const StringVal& type) {                                  \
    return CoreFn(ctx, v, path, /*is_try=*/true);                                      \
  }

VARIANT_GET_SIMPLE(VariantGetBoolean, BooleanVal, DoGetBoolean)
VARIANT_GET_SIMPLE(VariantGetFloat, FloatVal, DoGetFloat)
VARIANT_GET_SIMPLE(VariantGetDouble, DoubleVal, DoGetDouble)
VARIANT_GET_SIMPLE(VariantGetString, StringVal, DoGetString)
VARIANT_GET_SIMPLE(VariantGetDate, DateVal, DoGetDate)
#undef VARIANT_GET_SIMPLE

}  // namespace impala
