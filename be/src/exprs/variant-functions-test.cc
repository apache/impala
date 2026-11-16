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

#include <algorithm>
#include <cstring>
#include <limits>
#include <sstream>
#include <vector>

#include "gutil/strings/substitute.h"
#include "runtime/decimal-value.inline.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/types.h"
#include "runtime/variant-value.h"
#include "testutil/gtest-util.h"
#include "udf/udf-internal.h"
#include "util/decimal-constants.h"

#include "common/names.h"

using impala_udf::FunctionContext;

namespace impala {

// ------------------------- variant-blob builders ---------------------------
// (Mirrors the encoding used in be/src/util/variant-util-test.cc, with 1-byte field ids
// and offsets only.)

static vector<uint8_t> BuildMetadata(const vector<string>& field_names) {
  vector<uint8_t> buf;
  uint8_t header = 0x01 | (1 << 4) | (0 << 6);  // version=1, sorted, offset_size=1
  buf.push_back(header);
  buf.push_back(static_cast<uint8_t>(field_names.size()));
  uint32_t offset = 0;
  for (size_t i = 0; i <= field_names.size(); ++i) {
    buf.push_back(static_cast<uint8_t>(offset));
    if (i < field_names.size()) offset += field_names[i].size();
  }
  for (const string& name : field_names) {
    for (char c : name) buf.push_back(static_cast<uint8_t>(c));
  }
  return buf;
}

static vector<uint8_t> BuildNull() { return {0x00}; }

static vector<uint8_t> BuildBoolean(bool val) {
  uint8_t type_info = val ? 1 : 2;
  return {static_cast<uint8_t>(type_info << 2)};
}

// Primitive header byte = physical-type-id << 2 (basic_type PRIMITIVE=0 in bits 0-1).
template <typename T>
static vector<uint8_t> BuildPrimitive(uint8_t phys_type_id, T val) {
  vector<uint8_t> buf = {static_cast<uint8_t>(phys_type_id << 2)};
  uint8_t bytes[sizeof(T)];
  memcpy(bytes, &val, sizeof(T));
  buf.insert(buf.end(), bytes, bytes + sizeof(T));
  return buf;
}

static vector<uint8_t> BuildInt8(int8_t val) { return BuildPrimitive<int8_t>(3, val); }
static vector<uint8_t> BuildInt16(int16_t val) { return BuildPrimitive<int16_t>(4, val); }
static vector<uint8_t> BuildInt32(int32_t val) { return BuildPrimitive<int32_t>(5, val); }
static vector<uint8_t> BuildInt64(int64_t val) { return BuildPrimitive<int64_t>(6, val); }
static vector<uint8_t> BuildDouble(double val) { return BuildPrimitive<double>(7, val); }
static vector<uint8_t> BuildFloat(float val) { return BuildPrimitive<float>(14, val); }
// DATE (physical type 11) stores a signed int32 day count since the epoch.
static vector<uint8_t> BuildDate(int32_t days) {
  return BuildPrimitive<int32_t>(11, days);
}

// DECIMAL4/8/16 (physical types 8/9/10) store a 1-byte scale, then the unscaled value.
template <typename T>
static vector<uint8_t> BuildDecimal(uint8_t phys_type_id, uint8_t scale, T unscaled) {
  vector<uint8_t> buf = BuildPrimitive<T>(phys_type_id, unscaled);
  buf.insert(buf.begin() + 1, scale);
  return buf;
}
static vector<uint8_t> BuildDecimal4(uint8_t scale, int32_t unscaled) {
  return BuildDecimal<int32_t>(8, scale, unscaled);
}
static vector<uint8_t> BuildDecimal8(uint8_t scale, int64_t unscaled) {
  return BuildDecimal<int64_t>(9, scale, unscaled);
}
static vector<uint8_t> BuildDecimal16(uint8_t scale, __int128_t unscaled) {
  return BuildDecimal<__int128_t>(10, scale, unscaled);
}

// Types the serializer cannot render. TIME (17), TIMESTAMPTZ (12) and TIMESTAMPTZ_NANOS
// (18) store an int64; UUID (20) stores 16 raw bytes.
static vector<uint8_t> BuildTime(int64_t micros) {
  return BuildPrimitive<int64_t>(17, micros);
}
static vector<uint8_t> BuildTimestampTz(int64_t micros) {
  return BuildPrimitive<int64_t>(12, micros);
}
static vector<uint8_t> BuildTimestampTzNanos(int64_t nanos) {
  return BuildPrimitive<int64_t>(18, nanos);
}
static vector<uint8_t> BuildUuid() {
  vector<uint8_t> buf = {static_cast<uint8_t>(20 << 2)};
  for (uint8_t i = 0; i < 16; ++i) buf.push_back(i);
  return buf;
}

static vector<uint8_t> BuildShortString(const string& s) {
  uint8_t header = 0x01 | (static_cast<uint8_t>(s.size()) << 2);
  vector<uint8_t> buf = {header};
  for (char c : s) buf.push_back(static_cast<uint8_t>(c));
  return buf;
}

// Lays out 'values' in the order given by 'storage_order', a permutation of
// [0, values.size()) (empty means the identity). Sets '*offsets' to where each value
// starts, followed by the total size, and appends the value data to '*data'.
static void LayoutValues(const vector<vector<uint8_t>>& values,
    const vector<int>& storage_order, vector<uint32_t>* offsets, vector<uint8_t>* data) {
  vector<int> order = storage_order;
  if (order.empty()) {
    for (int i = 0; i < values.size(); ++i) order.push_back(i);
  }
  CHECK_EQ(order.size(), values.size());
  vector<int> sorted_order = order;
  std::sort(sorted_order.begin(), sorted_order.end());
  for (int i = 0; i < sorted_order.size(); ++i) CHECK_EQ(i, sorted_order[i]);
  offsets->assign(values.size() + 1, 0);
  for (int k : order) {
    (*offsets)[k] = data->size();
    data->insert(data->end(), values[k].begin(), values[k].end());
  }
  (*offsets)[values.size()] = data->size();
  CHECK_LE(data->size(), 255);
}

// 'values[i]' is the value of 'field_ids[i]'. The field ids must be listed in field name
// order, but the values may be stored in any order: 'storage_order' lists the indices of
// 'values' in the order they are stored in (empty means the same order as 'field_ids').
static vector<uint8_t> BuildObject(const vector<int>& field_ids,
    const vector<vector<uint8_t>>& values, const vector<int>& storage_order = {}) {
  vector<uint32_t> offsets;
  vector<uint8_t> data;
  LayoutValues(values, storage_order, &offsets, &data);
  vector<uint8_t> buf = {0x02};  // OBJECT, 1-byte fields/offsets
  buf.push_back(static_cast<uint8_t>(field_ids.size()));
  for (int fid : field_ids) buf.push_back(static_cast<uint8_t>(fid));
  for (uint32_t o : offsets) buf.push_back(static_cast<uint8_t>(o));
  buf.insert(buf.end(), data.begin(), data.end());
  return buf;
}

// The elements are stored in 'storage_order' (see BuildObject()).
static vector<uint8_t> BuildArray(const vector<vector<uint8_t>>& elements,
    const vector<int>& storage_order = {}) {
  vector<uint32_t> offsets;
  vector<uint8_t> data;
  LayoutValues(elements, storage_order, &offsets, &data);
  vector<uint8_t> buf = {0x03};  // ARRAY, 1-byte offsets
  buf.push_back(static_cast<uint8_t>(elements.size()));
  for (uint32_t o : offsets) buf.push_back(static_cast<uint8_t>(o));
  buf.insert(buf.end(), data.begin(), data.end());
  return buf;
}

// Parses space-separated hex bytes, e.g. "02 01 00".
static vector<uint8_t> FromHex(const string& hex) {
  vector<uint8_t> buf;
  std::istringstream in(hex);
  string byte;
  while (in >> byte) buf.push_back(static_cast<uint8_t>(std::stoul(byte, nullptr, 16)));
  return buf;
}

// What try_variant_get() writes to stderr for a corrupt variant (see CaptureStderr()).
static const char* CORRUPT_WARNING =
    "UDF WARNING: variant_get: corrupt or unreadable variant";

// --------------------------------- fixture ---------------------------------

class VariantFunctionsTest : public ::testing::Test {
 protected:
  virtual void SetUp() override {
    pool_.reset(new MemPool(&tracker_));
    FunctionContext::TypeDesc ret;  // return type is unused by the tested logic
    ret.type = FunctionContext::TYPE_STRING;
    std::vector<FunctionContext::TypeDesc> args;  // args not registered as constant
    ctx_ = FunctionContextImpl::CreateContext(
        nullptr, pool_.get(), pool_.get(), ret, args, 0, true);
    ASSERT_TRUE(ctx_ != nullptr);
  }

  virtual void TearDown() override {
    for (FunctionContext* c : owned_ctxs_) {
      c->impl()->Close();
      delete c;
    }
    owned_ctxs_.clear();
    if (ctx_ != nullptr) {
      ctx_->impl()->Close();
      delete ctx_;
      ctx_ = nullptr;
    }
    pool_->FreeAll();
  }

  // Builds a non-null VariantVal referencing the given blobs (kept alive by the caller).
  VariantVal MakeVariant(const vector<uint8_t>& meta, const vector<uint8_t>& value) {
    StringVal m(const_cast<uint8_t*>(meta.data()), meta.size());
    StringVal v(const_cast<uint8_t*>(value.data()), value.size());
    return VariantVal(m, v);
  }

  // Decodes a returned VARIANT's value as an int32 (asserts it is one).
  int32_t DecodeInt32(const VariantVal& out) {
    EXPECT_FALSE(out.is_null);
    VariantMetadata meta;
    EXPECT_OK(meta.Init(out.metadata.ptr, out.metadata.len));
    VariantValue v(out.value.ptr, out.value.len, &meta);
    EXPECT_EQ(VariantBasicType::PRIMITIVE, v.GetBasicType());
    EXPECT_EQ(VariantPhysicalType::INT32, v.GetPhysicalType());
    int32_t result = 0;
    EXPECT_TRUE(v.GetInt32(&result));
    return result;
  }

  string DecodeString(const VariantVal& out) {
    EXPECT_FALSE(out.is_null);
    VariantMetadata meta;
    EXPECT_OK(meta.Init(out.metadata.ptr, out.metadata.len));
    VariantValue v(out.value.ptr, out.value.len, &meta);
    StringValue sv;
    EXPECT_TRUE(v.GetString(&sv));
    return string(sv.Ptr(), sv.Len());
  }

  // Renders a returned StringVal (asserts non-null).
  string AsString(const StringVal& s) {
    EXPECT_FALSE(s.is_null);
    return string(reinterpret_cast<char*>(s.ptr), s.len);
  }

  // Renders a VARIANT as JSON with variant_to_json() (asserts non-null).
  string Json(const VariantVal& v) {
    return AsString(VariantFunctions::VariantToJson(ctx_, v));
  }

  // Runs 'fn' and returns what it wrote to stderr. A test FunctionContext has no
  // RuntimeState, so FunctionContext::AddWarning() writes "UDF WARNING: ..." to stderr.
  template <typename Fn>
  string CaptureStderr(const Fn& fn) {
    testing::internal::CaptureStderr();
    fn();
    return testing::internal::GetCapturedStderr();
  }

  // Creates an owned FunctionContext with 'num_args' (all non-constant by default).
  // Registered in owned_ctxs_ for teardown. Used by tests that raise a strict error
  // (which poisons the context) so the shared ctx_ stays clean for later assertions.
  FunctionContext* MakeCtx(int num_args) {
    FunctionContext::TypeDesc td;
    td.type = FunctionContext::TYPE_STRING;
    std::vector<FunctionContext::TypeDesc> args(num_args, td);
    FunctionContext* c = FunctionContextImpl::CreateContext(
        nullptr, pool_.get(), pool_.get(), td, args, 0, true);
    owned_ctxs_.push_back(c);
    return c;
  }

  // Creates a context with DECIMAL(precision, scale) as the return type, which is how
  // the frontend passes the 'decimal(p,s)' type argument of variant_get().
  FunctionContext* MakeDecimalCtx(int precision, int scale) {
    FunctionContext::TypeDesc arg;
    arg.type = FunctionContext::TYPE_STRING;
    FunctionContext::TypeDesc ret;
    ret.type = FunctionContext::TYPE_DECIMAL;
    ret.precision = precision;
    ret.scale = scale;
    FunctionContext* c = FunctionContextImpl::CreateContext(
        nullptr, pool_.get(), pool_.get(), ret, {arg, arg, arg}, 0, true);
    owned_ctxs_.push_back(c);
    return c;
  }

  // Returns variant_get(v, path, 'decimal(precision,scale)') as a string, "NULL" for a
  // SQL NULL. Expects that the function does not fail.
  string GetDecimal(const VariantVal& v, const char* path, int precision, int scale) {
    FunctionContext* c = MakeDecimalCtx(precision, scale);
    DecimalVal d = VariantFunctions::VariantGetDecimal(
        c, v, StringVal(path), StringVal("decimal"));
    EXPECT_FALSE(c->has_error()) << path << ": " << c->error_msg();
    if (d.is_null) return "NULL";
    // Read the bytes that a DECIMAL of this precision occupies.
    __int128_t unscaled;
    switch (ColumnType::GetDecimalByteSize(precision)) {
      case 4: unscaled = d.val4; break;
      case 8: unscaled = d.val8; break;
      default: unscaled = d.val16; break;
    }
    return Decimal16Value(unscaled).ToString(precision, scale);
  }

  // Expects that variant_get(v, path, 'decimal(precision,scale)') fails with an error
  // that contains 'error', and that try_variant_get() returns NULL without an error.
  void ExpectDecimalFails(const VariantVal& v, const char* path, int precision,
      int scale, const string& error) {
    FunctionContext* c = MakeDecimalCtx(precision, scale);
    EXPECT_TRUE(VariantFunctions::VariantGetDecimal(
        c, v, StringVal(path), StringVal("decimal")).is_null) << path;
    ASSERT_TRUE(c->has_error()) << path;
    EXPECT_NE(string::npos, string(c->error_msg()).find(error))
        << path << ": " << c->error_msg();
    FunctionContext* try_ctx = MakeDecimalCtx(precision, scale);
    EXPECT_TRUE(VariantFunctions::TryVariantGetDecimal(
        try_ctx, v, StringVal(path), StringVal("decimal")).is_null) << path;
    EXPECT_FALSE(try_ctx->has_error()) << path;
  }

  MemTracker tracker_;
  boost::scoped_ptr<MemPool> pool_;
  FunctionContext* ctx_ = nullptr;
  std::vector<FunctionContext*> owned_ctxs_;
};

// {"age": 30, "name": "Alice"} with a sorted dictionary ["age", "name"].
#define BUILD_PERSON()                                                              \
  vector<uint8_t> meta = BuildMetadata({"age", "name"});                            \
  vector<uint8_t> obj = BuildObject({0, 1}, {BuildInt32(30), BuildShortString("Alice")})

TEST_F(VariantFunctionsTest, TwoArgObjectField) {
  BUILD_PERSON();
  VariantVal v = MakeVariant(meta, obj);
  EXPECT_EQ(30, DecodeInt32(VariantFunctions::VariantGet(ctx_, v, StringVal("$.age"))));
  EXPECT_EQ("Alice",
      DecodeString(VariantFunctions::VariantGet(ctx_, v, StringVal("$.name"))));
}

TEST_F(VariantFunctionsTest, TwoArgIdentityAndMissing) {
  BUILD_PERSON();
  VariantVal v = MakeVariant(meta, obj);
  // '$' returns the whole object; its value blob is the same slice.
  VariantVal whole = VariantFunctions::VariantGet(ctx_, v, StringVal("$"));
  EXPECT_FALSE(whole.is_null);
  EXPECT_EQ(v.value.ptr, whole.value.ptr);
  // Missing field -> SQL NULL.
  EXPECT_TRUE(VariantFunctions::VariantGet(ctx_, v, StringVal("$.nope")).is_null);
}

TEST_F(VariantFunctionsTest, TwoArgNullInputsAndVNull) {
  BUILD_PERSON();
  EXPECT_TRUE(VariantFunctions::VariantGet(ctx_, VariantVal::null(),
      StringVal("$.age")).is_null);
  // A VNULL at the path is SQL NULL.
  vector<uint8_t> meta2 = BuildMetadata({"a"});
  vector<uint8_t> obj2 = BuildObject({0}, {BuildNull()});
  VariantVal v2 = MakeVariant(meta2, obj2);
  EXPECT_TRUE(VariantFunctions::VariantGet(ctx_, v2, StringVal("$.a")).is_null);
}

TEST_F(VariantFunctionsTest, TwoArgArrayIndexAndChaining) {
  vector<uint8_t> meta = BuildMetadata({"arr"});
  vector<uint8_t> arr = BuildArray({BuildInt32(1), BuildInt32(2), BuildInt32(3)});
  vector<uint8_t> obj = BuildObject({0}, {arr});
  VariantVal v = MakeVariant(meta, obj);
  EXPECT_EQ(2, DecodeInt32(VariantFunctions::VariantGet(ctx_, v, StringVal("$.arr[1]"))));
  // Chaining: outer get consumes the VARIANT produced by the inner get.
  VariantVal inner = VariantFunctions::VariantGet(ctx_, v, StringVal("$.arr"));
  ASSERT_FALSE(inner.is_null);
  EXPECT_EQ(3, DecodeInt32(VariantFunctions::VariantGet(ctx_, inner, StringVal("$[2]"))));
}

TEST_F(VariantFunctionsTest, ThreeArgTypedExtraction) {
  BUILD_PERSON();
  VariantVal v = MakeVariant(meta, obj);
  // Integer widening to the requested width.
  EXPECT_EQ(30,
      VariantFunctions::VariantGetInt(ctx_, v, StringVal("$.age"), StringVal("int")).val);
  EXPECT_EQ(30, VariantFunctions::VariantGetBigInt(
      ctx_, v, StringVal("$.age"), StringVal("bigint")).val);
  // Raw string value, no JSON quoting.
  StringVal s = VariantFunctions::VariantGetString(
      ctx_, v, StringVal("$.name"), StringVal("string"));
  ASSERT_FALSE(s.is_null);
  EXPECT_EQ("Alice", string(reinterpret_cast<char*>(s.ptr), s.len));
}

TEST_F(VariantFunctionsTest, ThreeArgBoolean) {
  vector<uint8_t> meta = BuildMetadata({"b"});
  vector<uint8_t> obj = BuildObject({0}, {BuildBoolean(true)});
  VariantVal v = MakeVariant(meta, obj);
  BooleanVal r = VariantFunctions::VariantGetBoolean(
      ctx_, v, StringVal("$.b"), StringVal("boolean"));
  ASSERT_FALSE(r.is_null);
  EXPECT_TRUE(r.val);
}

TEST_F(VariantFunctionsTest, ThreeArgTryMismatchIsNull) {
  BUILD_PERSON();
  VariantVal v = MakeVariant(meta, obj);
  // "Alice" is not convertible to INT; try_ returns NULL and does not set an error.
  EXPECT_TRUE(VariantFunctions::TryVariantGetInt(
      ctx_, v, StringVal("$.name"), StringVal("int")).is_null);
  EXPECT_FALSE(ctx_->has_error());
}

TEST_F(VariantFunctionsTest, ThreeArgNarrowingOverflowIsNullForTry) {
  // 30000 fits INT but overflows TINYINT.
  vector<uint8_t> meta = BuildMetadata({"n"});
  vector<uint8_t> obj = BuildObject({0}, {BuildInt32(30000)});
  VariantVal v = MakeVariant(meta, obj);
  EXPECT_EQ(30000,
      VariantFunctions::VariantGetInt(ctx_, v, StringVal("$.n"), StringVal("int")).val);
  EXPECT_TRUE(VariantFunctions::TryVariantGetTinyInt(
      ctx_, v, StringVal("$.n"), StringVal("tinyint")).is_null);
}

// Strict narrowing overflow raises an error (not just a NULL). Uses an owned context
// because SetError poisons it.
TEST_F(VariantFunctionsTest, ThreeArgStrictOverflowSetsError) {
  vector<uint8_t> meta = BuildMetadata({"n"});
  vector<uint8_t> obj = BuildObject({0}, {BuildInt32(30000)});
  VariantVal v = MakeVariant(meta, obj);
  FunctionContext* c = MakeCtx(3);
  EXPECT_TRUE(VariantFunctions::VariantGetTinyInt(
      c, v, StringVal("$.n"), StringVal("tinyint")).is_null);
  EXPECT_TRUE(c->has_error());
}

// Native integer widths extract into their matching typed getter. The dictionary is
// sorted (BuildMetadata sets the sorted flag), so field names are listed in sorted order.
TEST_F(VariantFunctionsTest, ThreeArgIntWidths) {
  vector<uint8_t> meta = BuildMetadata({"i16", "i64", "i8"});
  vector<uint8_t> obj = BuildObject({0, 1, 2},
      {BuildInt16(1000), BuildInt64(10000000000LL), BuildInt8(34)});
  VariantVal v = MakeVariant(meta, obj);
  EXPECT_EQ(34, VariantFunctions::VariantGetTinyInt(
      ctx_, v, StringVal("$.i8"), StringVal("tinyint")).val);
  EXPECT_EQ(1000, VariantFunctions::VariantGetSmallInt(
      ctx_, v, StringVal("$.i16"), StringVal("smallint")).val);
  // int64 widens into BIGINT.
  EXPECT_EQ(10000000000LL, VariantFunctions::VariantGetBigInt(
      ctx_, v, StringVal("$.i64"), StringVal("bigint")).val);
  // A smaller value also widens up when a wider type is requested.
  EXPECT_EQ(34, VariantFunctions::VariantGetBigInt(
      ctx_, v, StringVal("$.i8"), StringVal("bigint")).val);
}

// FLOAT extraction: native float and widening from an integer.
TEST_F(VariantFunctionsTest, ThreeArgFloat) {
  vector<uint8_t> meta = BuildMetadata({"f", "i"});
  vector<uint8_t> obj = BuildObject({0, 1}, {BuildFloat(1.5f), BuildInt32(5)});
  VariantVal v = MakeVariant(meta, obj);
  EXPECT_FLOAT_EQ(1.5f, VariantFunctions::VariantGetFloat(
      ctx_, v, StringVal("$.f"), StringVal("float")).val);
  EXPECT_FLOAT_EQ(5.0f, VariantFunctions::VariantGetFloat(
      ctx_, v, StringVal("$.i"), StringVal("float")).val);
}

// A double outside the FLOAT range is an out-of-range coercion: strict errors, try_
// NULLs.
TEST_F(VariantFunctionsTest, ThreeArgFloatOverflow) {
  vector<uint8_t> meta = BuildMetadata({"d"});
  vector<uint8_t> obj = BuildObject({0}, {BuildDouble(1e100)});
  VariantVal v = MakeVariant(meta, obj);
  EXPECT_TRUE(VariantFunctions::TryVariantGetFloat(
      ctx_, v, StringVal("$.d"), StringVal("float")).is_null);
  EXPECT_FALSE(ctx_->has_error());
  FunctionContext* c = MakeCtx(3);
  EXPECT_TRUE(VariantFunctions::VariantGetFloat(
      c, v, StringVal("$.d"), StringVal("float")).is_null);
  EXPECT_TRUE(c->has_error());
}

// A value whose type has no numeric coercion (here a BOOLEAN) is a mismatch for both
// FLOAT and DOUBLE: strict errors, try_ NULLs.
TEST_F(VariantFunctionsTest, ThreeArgFloatDoubleMismatch) {
  vector<uint8_t> meta = BuildMetadata({"b"});
  vector<uint8_t> obj = BuildObject({0}, {BuildBoolean(true)});
  VariantVal v = MakeVariant(meta, obj);
  // try_ -> NULL, no error, for both target types.
  EXPECT_TRUE(VariantFunctions::TryVariantGetFloat(
      ctx_, v, StringVal("$.b"), StringVal("float")).is_null);
  EXPECT_TRUE(VariantFunctions::TryVariantGetDouble(
      ctx_, v, StringVal("$.b"), StringVal("double")).is_null);
  EXPECT_FALSE(ctx_->has_error());
  // Strict -> error. Each strict call uses its own context, since SetError poisons it.
  FunctionContext* cf = MakeCtx(3);
  EXPECT_TRUE(VariantFunctions::VariantGetFloat(
      cf, v, StringVal("$.b"), StringVal("float")).is_null);
  EXPECT_TRUE(cf->has_error());
  FunctionContext* cd = MakeCtx(3);
  EXPECT_TRUE(VariantFunctions::VariantGetDouble(
      cd, v, StringVal("$.b"), StringVal("double")).is_null);
  EXPECT_TRUE(cd->has_error());
}

// DOUBLE extraction widens every native numeric physical type; a native DOUBLE reads
// through unchanged. Exercises ToDouble()'s FLOAT/DOUBLE/INT8/INT32/INT64 branches.
TEST_F(VariantFunctionsTest, ThreeArgDoubleWidening) {
  vector<uint8_t> meta = BuildMetadata({"d", "f", "i32", "i64", "i8"});  // sorted dict
  vector<uint8_t> obj = BuildObject({0, 1, 2, 3, 4},
      {BuildDouble(2.5), BuildFloat(1.5f), BuildInt32(5), BuildInt64(10000000000LL),
          BuildInt8(34)});
  VariantVal v = MakeVariant(meta, obj);
  EXPECT_DOUBLE_EQ(2.5, VariantFunctions::VariantGetDouble(
      ctx_, v, StringVal("$.d"), StringVal("double")).val);
  // 1.5f is exactly representable, so it widens to exactly 1.5.
  EXPECT_DOUBLE_EQ(1.5, VariantFunctions::VariantGetDouble(
      ctx_, v, StringVal("$.f"), StringVal("double")).val);
  EXPECT_DOUBLE_EQ(5.0, VariantFunctions::VariantGetDouble(
      ctx_, v, StringVal("$.i32"), StringVal("double")).val);
  EXPECT_DOUBLE_EQ(1e10, VariantFunctions::VariantGetDouble(
      ctx_, v, StringVal("$.i64"), StringVal("double")).val);
  EXPECT_DOUBLE_EQ(34.0, VariantFunctions::VariantGetDouble(
      ctx_, v, StringVal("$.i8"), StringVal("double")).val);
}

// 10^n as an int128, for DECIMAL16 values beyond the int64 range.
static __int128_t Pow10(int n) {
  __int128_t r = 1;
  for (int i = 0; i < n; ++i) r *= 10;
  return r;
}

// DECIMAL4/8/16 convert to DOUBLE and FLOAT like CAST(decimal AS DOUBLE/FLOAT).
TEST_F(VariantFunctionsTest, ThreeArgDecimalToDouble) {
  vector<uint8_t> meta = BuildMetadata({"d16", "d4", "d8", "huge"});  // sorted dict
  vector<uint8_t> obj = BuildObject({0, 1, 2, 3},
      {BuildDecimal16(9, Pow10(9) * 9876543210LL + 123456789), BuildDecimal4(2, 1234),
          BuildDecimal8(9, -123456789987654321LL), BuildDecimal16(0, Pow10(37))});
  VariantVal v = MakeVariant(meta, obj);
  EXPECT_EQ(12.34, VariantFunctions::VariantGetDouble(
      ctx_, v, StringVal("$.d4"), StringVal("double")).val);
  EXPECT_EQ(-123456789.987654321, VariantFunctions::VariantGetDouble(
      ctx_, v, StringVal("$.d8"), StringVal("double")).val);
  EXPECT_EQ(9876543210.123456789, VariantFunctions::TryVariantGetDouble(
      ctx_, v, StringVal("$.d16"), StringVal("double")).val);
  EXPECT_DOUBLE_EQ(1e37, VariantFunctions::VariantGetDouble(
      ctx_, v, StringVal("$.huge"), StringVal("double")).val);
  EXPECT_FLOAT_EQ(12.34f, VariantFunctions::VariantGetFloat(
      ctx_, v, StringVal("$.d4"), StringVal("float")).val);
  EXPECT_FLOAT_EQ(1e37f, VariantFunctions::TryVariantGetFloat(
      ctx_, v, StringVal("$.huge"), StringVal("float")).val);
  EXPECT_FALSE(ctx_->has_error());
}

// STRING is the lossless target type for decimals: like the JSON form, it has every digit
// of the value, including the ones that do not fit a DOUBLE.
TEST_F(VariantFunctionsTest, ThreeArgDecimalToString) {
  vector<uint8_t> meta = BuildMetadata({"d16", "d4", "d8"});  // sorted dictionary
  vector<uint8_t> obj = BuildObject({0, 1, 2},
      {BuildDecimal16(9, Pow10(9) * 9876543210LL + 123456789), BuildDecimal4(2, 1234),
          BuildDecimal8(9, -123456789987654321LL)});
  VariantVal v = MakeVariant(meta, obj);
  EXPECT_EQ("12.34", AsString(VariantFunctions::VariantGetString(
      ctx_, v, StringVal("$.d4"), StringVal("string"))));
  EXPECT_EQ("-123456789.987654321", AsString(VariantFunctions::VariantGetString(
      ctx_, v, StringVal("$.d8"), StringVal("string"))));
  EXPECT_EQ("9876543210.123456789", AsString(VariantFunctions::VariantGetString(
      ctx_, v, StringVal("$.d16"), StringVal("string"))));
  EXPECT_EQ(
      "{\"d16\":9876543210.123456789,\"d4\":12.34,\"d8\":-123456789.987654321}",
      AsString(VariantFunctions::VariantGetString(
          ctx_, v, StringVal("$"), StringVal("string"))));
  EXPECT_FALSE(ctx_->has_error());
}

// Decimals convert to the integer types like CAST(decimal AS <int type>): the fraction
// rounds half away from zero, and a rounded value outside the target range overflows.
TEST_F(VariantFunctionsTest, ThreeArgDecimalToInt) {
  vector<uint8_t> meta = BuildMetadata({"big", "d16", "frac", "half", "neg_half",
      "tiny_edge", "tiny_neg_edge", "tiny_neg_over", "tiny_over"});
  vector<uint8_t> obj = BuildObject({0, 1, 2, 3, 4, 5, 6, 7, 8},
      {BuildDecimal8(0, 3000000000LL), BuildDecimal16(9, Pow10(9) * 9876543210LL),
          BuildDecimal4(2, 1234), BuildDecimal4(1, 125), BuildDecimal4(1, -125),
          BuildDecimal4(1, 1274), BuildDecimal4(1, -1284), BuildDecimal4(1, -1285),
          BuildDecimal4(1, 1275)});
  VariantVal v = MakeVariant(meta, obj);
  EXPECT_EQ(12, VariantFunctions::VariantGetBigInt(
      ctx_, v, StringVal("$.frac"), StringVal("bigint")).val);
  EXPECT_EQ(13, VariantFunctions::VariantGetInt(
      ctx_, v, StringVal("$.half"), StringVal("int")).val);
  EXPECT_EQ(-13, VariantFunctions::VariantGetSmallInt(
      ctx_, v, StringVal("$.neg_half"), StringVal("smallint")).val);
  EXPECT_EQ(127, VariantFunctions::VariantGetTinyInt(
      ctx_, v, StringVal("$.tiny_edge"), StringVal("tinyint")).val);
  EXPECT_EQ(-128, VariantFunctions::VariantGetTinyInt(
      ctx_, v, StringVal("$.tiny_neg_edge"), StringVal("tinyint")).val);
  EXPECT_EQ(3000000000LL, VariantFunctions::VariantGetBigInt(
      ctx_, v, StringVal("$.big"), StringVal("bigint")).val);
  EXPECT_EQ(9876543210LL, VariantFunctions::VariantGetBigInt(
      ctx_, v, StringVal("$.d16"), StringVal("bigint")).val);
  // 127.5 and -128.5 round to 128 and -129, which overflow TINYINT; 3000000000
  // overflows INT.
  EXPECT_TRUE(VariantFunctions::TryVariantGetTinyInt(
      ctx_, v, StringVal("$.tiny_over"), StringVal("tinyint")).is_null);
  EXPECT_TRUE(VariantFunctions::TryVariantGetTinyInt(
      ctx_, v, StringVal("$.tiny_neg_over"), StringVal("tinyint")).is_null);
  EXPECT_TRUE(VariantFunctions::TryVariantGetInt(
      ctx_, v, StringVal("$.big"), StringVal("int")).is_null);
  EXPECT_FALSE(ctx_->has_error());
  FunctionContext* c = MakeCtx(3);
  EXPECT_TRUE(VariantFunctions::VariantGetTinyInt(
      c, v, StringVal("$.tiny_over"), StringVal("tinyint")).is_null);
  EXPECT_TRUE(c->has_error());
}

// A DECIMAL16 value outside the int64 range overflows even BIGINT.
TEST_F(VariantFunctionsTest, ThreeArgDecimalBigIntOverflow) {
  vector<uint8_t> meta = BuildMetadata({"d"});
  vector<uint8_t> obj = BuildObject({0}, {BuildDecimal16(2, Pow10(22))});
  VariantVal v = MakeVariant(meta, obj);
  EXPECT_TRUE(VariantFunctions::TryVariantGetBigInt(
      ctx_, v, StringVal("$.d"), StringVal("bigint")).is_null);
  EXPECT_FALSE(ctx_->has_error());
  FunctionContext* c = MakeCtx(3);
  EXPECT_TRUE(VariantFunctions::VariantGetBigInt(
      c, v, StringVal("$.d"), StringVal("bigint")).is_null);
  EXPECT_TRUE(c->has_error());
}

// A decimal whose scale or unscaled value exceeds the precision of its physical type, or
// whose payload is truncated, is not convertible.
TEST_F(VariantFunctionsTest, ThreeArgMalformedDecimal) {
  vector<uint8_t> truncated = BuildDecimal8(2, 1234);
  truncated.pop_back();
  // std::numeric_limits<__int128_t>::min() returns 0, so build INT128_MIN by hand.
  const __int128_t int128_min =
      static_cast<__int128_t>(static_cast<__uint128_t>(1) << 127);
  vector<uint8_t> meta = BuildMetadata({"s", "t", "u", "v", "w"});  // sorted dictionary
  vector<uint8_t> obj = BuildObject({0, 1, 2, 3, 4},
      {BuildDecimal4(10, 5), truncated, BuildDecimal4(0, MAX_UNSCALED_DECIMAL4 + 1),
          BuildDecimal16(0, -MAX_UNSCALED_DECIMAL16 - 1), BuildDecimal16(0, int128_min)});
  VariantVal v = MakeVariant(meta, obj);
  for (const char* path : {"$.s", "$.t", "$.u", "$.v", "$.w"}) {
    EXPECT_TRUE(VariantFunctions::TryVariantGetDouble(
        ctx_, v, StringVal(path), StringVal("double")).is_null) << path;
    EXPECT_TRUE(VariantFunctions::TryVariantGetBigInt(
        ctx_, v, StringVal(path), StringVal("bigint")).is_null) << path;
    FunctionContext* c = MakeCtx(3);
    EXPECT_TRUE(VariantFunctions::VariantGetDouble(
        c, v, StringVal(path), StringVal("double")).is_null) << path;
    EXPECT_TRUE(c->has_error()) << path;
  }
  EXPECT_FALSE(ctx_->has_error());
}

// A variant decimal converts to the DECIMAL(p,s) of the type argument like
// CAST(decimal AS DECIMAL(p,s)): digits beyond the scale round half away from zero, and
// a value that does not fit the precision is out of range.
TEST_F(VariantFunctionsTest, ThreeArgDecimalFromDecimal) {
  vector<uint8_t> meta = BuildMetadata({"d16", "d4", "d8", "half", "neg_half"});
  vector<uint8_t> obj = BuildObject({0, 1, 2, 3, 4},
      {BuildDecimal16(9, Pow10(9) * 9876543210LL + 123456789), BuildDecimal4(2, 1234),
          BuildDecimal8(9, -123456789987654321LL), BuildDecimal4(1, 995),
          BuildDecimal4(1, -995)});
  VariantVal v = MakeVariant(meta, obj);
  // The same scale, in each width of the result.
  EXPECT_EQ("12.34", GetDecimal(v, "$.d4", 4, 2));
  EXPECT_EQ("12.34", GetDecimal(v, "$.d4", 18, 2));
  EXPECT_EQ("9876543210.123456789", GetDecimal(v, "$.d16", 38, 9));
  // A larger scale.
  EXPECT_EQ("12.34000", GetDecimal(v, "$.d4", 9, 5));
  EXPECT_EQ("9876543210.1234567890000000000000000000", GetDecimal(v, "$.d16", 38, 28));
  // A smaller scale rounds.
  EXPECT_EQ("-123456789.987654", GetDecimal(v, "$.d8", 20, 6));
  EXPECT_EQ("-123456790", GetDecimal(v, "$.d8", 9, 0));
  EXPECT_EQ("100", GetDecimal(v, "$.half", 3, 0));
  EXPECT_EQ("-100", GetDecimal(v, "$.neg_half", 3, 0));
  // 12.34 has two integer digits, 99.5 rounds to 100, and 9876543210.123456789 with
  // scale 29 needs 10 + 29 = 39 digits.
  ExpectDecimalFails(v, "$.d4", 3, 2, "out of range");
  ExpectDecimalFails(v, "$.half", 2, 0, "out of range");
  ExpectDecimalFails(v, "$.d16", 38, 29, "out of range");
}

// Integers and floating-point values convert like CAST(... AS DECIMAL(p,s)).
TEST_F(VariantFunctionsTest, ThreeArgDecimalFromNumber) {
  vector<uint8_t> meta = BuildMetadata(
      {"d", "f", "i64", "i64_max", "i64_min", "i8", "inf", "nan"});
  vector<uint8_t> obj = BuildObject({0, 1, 2, 3, 4, 5, 6, 7},
      {BuildDouble(14.3), BuildFloat(-2.5f), BuildInt64(10000000000LL),
          BuildInt64(std::numeric_limits<int64_t>::max()),
          BuildInt64(std::numeric_limits<int64_t>::min()), BuildInt8(-34),
          BuildDouble(std::numeric_limits<double>::infinity()),
          BuildDouble(std::numeric_limits<double>::quiet_NaN())});
  VariantVal v = MakeVariant(meta, obj);
  // The int64 boundaries have 19 digits.
  EXPECT_EQ("9223372036854775807", GetDecimal(v, "$.i64_max", 19, 0));
  EXPECT_EQ("-9223372036854775808", GetDecimal(v, "$.i64_min", 19, 0));
  EXPECT_EQ("9223372036854775807.0000000000000000000",
      GetDecimal(v, "$.i64_max", 38, 19));
  ExpectDecimalFails(v, "$.i64_max", 18, 0, "out of range");
  ExpectDecimalFails(v, "$.i64_min", 38, 20, "out of range");
  EXPECT_EQ("-34.00", GetDecimal(v, "$.i8", 4, 2));
  EXPECT_EQ("10000000000.000", GetDecimal(v, "$.i64", 14, 3));
  EXPECT_EQ("14.30", GetDecimal(v, "$.d", 10, 2));
  EXPECT_EQ("14", GetDecimal(v, "$.d", 5, 0));
  EXPECT_EQ("-2.5", GetDecimal(v, "$.f", 4, 1));
  EXPECT_EQ("-3", GetDecimal(v, "$.f", 3, 0));
  ExpectDecimalFails(v, "$.i8", 3, 2, "out of range");
  ExpectDecimalFails(v, "$.i64", 13, 3, "out of range");
  ExpectDecimalFails(v, "$.d", 2, 1, "out of range");
  ExpectDecimalFails(v, "$.inf", 38, 0, "out of range");
  ExpectDecimalFails(v, "$.nan", 38, 0, "out of range");
}

// A string is parsed like in CAST(string AS DECIMAL(p,s)).
TEST_F(VariantFunctionsTest, ThreeArgDecimalFromString) {
  vector<uint8_t> meta = BuildMetadata({"bad", "big", "carry", "exp", "huge_exp", "max",
      "min", "num", "tiny_exp", "ws"});
  vector<uint8_t> obj = BuildObject({0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
      {BuildShortString("abc"), BuildShortString("123456"), BuildShortString("9.995"),
          BuildShortString("1.5e2"), BuildShortString("1e200"),
          BuildShortString("99999999.99"), BuildShortString("-99999999.99"),
          BuildShortString("12.345"), BuildShortString("1e-200"),
          BuildShortString(" -1.5 ")});
  VariantVal v = MakeVariant(meta, obj);
  EXPECT_EQ("12.34500", GetDecimal(v, "$.num", 10, 5));
  EXPECT_EQ("12.35", GetDecimal(v, "$.num", 10, 2));
  EXPECT_EQ("-1.50", GetDecimal(v, "$.ws", 10, 2));
  EXPECT_EQ("150.00", GetDecimal(v, "$.exp", 10, 2));
  // Rounding carries into a new integer digit.
  EXPECT_EQ("10.00", GetDecimal(v, "$.carry", 10, 2));
  // An exponent that is too negative to parse gives 0.
  EXPECT_EQ("0.00", GetDecimal(v, "$.tiny_exp", 10, 2));
  // The largest and smallest values of DECIMAL(10,2).
  EXPECT_EQ("99999999.99", GetDecimal(v, "$.max", 10, 2));
  EXPECT_EQ("-99999999.99", GetDecimal(v, "$.min", 10, 2));
  ExpectDecimalFails(v, "$.big", 4, 2, "out of range");
  // 9.995 rounds to 10.00, which does not fit DECIMAL(3,2).
  ExpectDecimalFails(v, "$.carry", 3, 2, "out of range");
  ExpectDecimalFails(v, "$.max", 10, 3, "out of range");
  ExpectDecimalFails(v, "$.huge_exp", 38, 0, "out of range");
  ExpectDecimalFails(v, "$.bad", 10, 2, "not convertible to DECIMAL");
}

// Other values and malformed decimals are not convertible to a decimal. A missing path
// and a variant null are a SQL NULL.
TEST_F(VariantFunctionsTest, ThreeArgDecimalNotConvertible) {
  vector<uint8_t> meta = BuildMetadata({"b", "dt", "malformed", "null"});
  vector<uint8_t> obj = BuildObject({0, 1, 2, 3},
      {BuildBoolean(true), BuildDate(0), BuildDecimal4(10, 5), BuildNull()});
  VariantVal v = MakeVariant(meta, obj);
  for (const char* path : {"$.b", "$.dt", "$.malformed", "$"}) {
    ExpectDecimalFails(v, path, 10, 2, "not convertible to DECIMAL");
  }
  EXPECT_EQ("NULL", GetDecimal(v, "$.null", 10, 2));
  EXPECT_EQ("NULL", GetDecimal(v, "$.missing", 10, 2));
}

// DATE extraction from the DATE physical type and from a date string; a bare INT32 is
// intentionally NOT coerced to a date.
TEST_F(VariantFunctionsTest, ThreeArgDate) {
  vector<uint8_t> meta = BuildMetadata({"d", "i", "s"});  // sorted dictionary
  vector<uint8_t> obj = BuildObject({0, 1, 2},
      {BuildDate(0), BuildInt32(19669), BuildShortString("2024-11-07")});
  VariantVal v = MakeVariant(meta, obj);
  // days=0 is 1970-01-01; DateVal carries the day count.
  DateVal d = VariantFunctions::VariantGetDate(
      ctx_, v, StringVal("$.d"), StringVal("date"));
  ASSERT_FALSE(d.is_null);
  EXPECT_EQ(0, d.val);
  // A date string parses.
  EXPECT_FALSE(VariantFunctions::VariantGetDate(
      ctx_, v, StringVal("$.s"), StringVal("date")).is_null);
  // A plain INT32 is rejected (try_ -> NULL, no coercion to a day count).
  EXPECT_TRUE(VariantFunctions::TryVariantGetDate(
      ctx_, v, StringVal("$.i"), StringVal("date")).is_null);
}

// Invalid DATE inputs fail: a day count outside the supported DATE range and an
// unparseable date string. Strict errors, try_ NULLs.
TEST_F(VariantFunctionsTest, ThreeArgInvalidDate) {
  // "big" is a DATE physical value whose day count is far past the max supported date;
  // "s" is a non-date string. (Dictionary is sorted: "big" < "s".)
  vector<uint8_t> meta = BuildMetadata({"big", "s"});
  vector<uint8_t> obj = BuildObject({0, 1},
      {BuildDate(2000000000), BuildShortString("not-a-date")});
  VariantVal v = MakeVariant(meta, obj);
  // try_ -> NULL, no error, for both the out-of-range value and the bad string.
  EXPECT_TRUE(VariantFunctions::TryVariantGetDate(
      ctx_, v, StringVal("$.big"), StringVal("date")).is_null);
  EXPECT_TRUE(VariantFunctions::TryVariantGetDate(
      ctx_, v, StringVal("$.s"), StringVal("date")).is_null);
  EXPECT_FALSE(ctx_->has_error());
  // Strict -> error. Each strict call uses its own context, since SetError poisons it.
  FunctionContext* cb = MakeCtx(3);
  EXPECT_TRUE(VariantFunctions::VariantGetDate(
      cb, v, StringVal("$.big"), StringVal("date")).is_null);
  EXPECT_TRUE(cb->has_error());
  FunctionContext* cs = MakeCtx(3);
  EXPECT_TRUE(VariantFunctions::VariantGetDate(
      cs, v, StringVal("$.s"), StringVal("date")).is_null);
  EXPECT_TRUE(cs->has_error());
}

// String extraction of objects/arrays renders JSON; scalars render their bare text.
TEST_F(VariantFunctionsTest, ThreeArgStringFormats) {
  BUILD_PERSON();
  VariantVal person = MakeVariant(meta, obj);
  // Whole object -> JSON text (field id order, i.e. name order).
  EXPECT_EQ("{\"age\":30,\"name\":\"Alice\"}",
      AsString(VariantFunctions::VariantGetString(
          ctx_, person, StringVal("$"), StringVal("string"))));

  vector<uint8_t> meta2 = BuildMetadata({"b", "d", "dt", "i"});  // sorted dictionary
  vector<uint8_t> obj2 = BuildObject({0, 1, 2, 3},
      {BuildBoolean(true), BuildDouble(14.3), BuildDate(0), BuildInt32(42)});
  VariantVal v = MakeVariant(meta2, obj2);
  EXPECT_EQ("42", AsString(VariantFunctions::VariantGetString(
      ctx_, v, StringVal("$.i"), StringVal("string"))));
  EXPECT_EQ("14.3", AsString(VariantFunctions::VariantGetString(
      ctx_, v, StringVal("$.d"), StringVal("string"))));
  EXPECT_EQ("true", AsString(VariantFunctions::VariantGetString(
      ctx_, v, StringVal("$.b"), StringVal("string"))));
  // DATE renders unquoted (the outer JSON quotes are stripped).
  EXPECT_EQ("1970-01-01", AsString(VariantFunctions::VariantGetString(
      ctx_, v, StringVal("$.dt"), StringVal("string"))));
}

// A type the serializer cannot render must fail to coerce to STRING rather than return
// its placeholder: strict errors, try_ NULLs.
TEST_F(VariantFunctionsTest, ThreeArgStringUnsupportedType) {
  vector<uint8_t> meta = BuildMetadata({"t", "tz", "tzn", "u"});  // sorted dictionary
  vector<uint8_t> obj = BuildObject({0, 1, 2, 3},
      {BuildTime(45234123456LL), BuildTimestampTz(1730000000000000LL),
          BuildTimestampTzNanos(1730000000000000000LL), BuildUuid()});
  VariantVal v = MakeVariant(meta, obj);
  for (const char* path : {"$.t", "$.tz", "$.tzn", "$.u"}) {
    EXPECT_TRUE(VariantFunctions::TryVariantGetString(
        ctx_, v, StringVal(path), StringVal("string")).is_null) << path;
    // Each strict call uses its own context, since SetError poisons it.
    FunctionContext* c = MakeCtx(3);
    EXPECT_TRUE(VariantFunctions::VariantGetString(
        c, v, StringVal(path), StringVal("string")).is_null) << path;
    EXPECT_TRUE(c->has_error()) << path;
  }
  EXPECT_FALSE(ctx_->has_error());
}

// A STRING result is a view into the input value blob, not a per-row copy.
TEST_F(VariantFunctionsTest, ThreeArgStringIsZeroCopy) {
  BUILD_PERSON();
  VariantVal v = MakeVariant(meta, obj);
  StringVal s = VariantFunctions::VariantGetString(
      ctx_, v, StringVal("$.name"), StringVal("string"));
  ASSERT_FALSE(s.is_null);
  EXPECT_EQ("Alice", AsString(s));
  EXPECT_GE(s.ptr, v.value.ptr);
  EXPECT_LE(s.ptr + s.len, v.value.ptr + v.value.len);
}

// A NULL path argument yields SQL NULL, not an error.
TEST_F(VariantFunctionsTest, NullPath) {
  BUILD_PERSON();
  VariantVal v = MakeVariant(meta, obj);
  EXPECT_TRUE(VariantFunctions::VariantGet(ctx_, v, StringVal::null()).is_null);
  EXPECT_TRUE(VariantFunctions::VariantGetInt(
      ctx_, v, StringVal::null(), StringVal("int")).is_null);
  EXPECT_FALSE(ctx_->has_error());
}

// A syntactically malformed path yields SQL NULL by the backend function. Normally
// the FE raises analysis errors for such paths.
TEST_F(VariantFunctionsTest, MalformedPath) {
  BUILD_PERSON();
  VariantVal v = MakeVariant(meta, obj);
  const char* bad_paths[] = {
      "",       // empty (no '$')
      "$age",   // missing '.' or '[' after '$'
      "$.",     // no segment after the prefix
      "$[abc]", // non-numeric array index
      "$[1",    // unterminated array index
  };
  for (const char* path : bad_paths) {
    EXPECT_TRUE(VariantFunctions::VariantGet(ctx_, v, StringVal(path)).is_null) << path;
    EXPECT_TRUE(VariantFunctions::VariantGetInt(
        ctx_, v, StringVal(path), StringVal("int")).is_null) << path;
  }
}

// Corrupt metadata: strict raises an error, try_ yields NULL without erroring.
TEST_F(VariantFunctionsTest, CorruptMetadata) {
  vector<uint8_t> bad_meta = {0x01};  // header only, no dictionary-size/offset bytes
  vector<uint8_t> val = BuildInt32(1);
  VariantVal v = MakeVariant(bad_meta, val);
  EXPECT_TRUE(VariantFunctions::TryVariantGet(ctx_, v, StringVal("$")).is_null);
  EXPECT_FALSE(ctx_->has_error());
  FunctionContext* c = MakeCtx(2);
  EXPECT_TRUE(VariantFunctions::VariantGet(c, v, StringVal("$")).is_null);
  EXPECT_TRUE(c->has_error());
}

// Every encoded value has at least a header byte, so an empty value blob and an object
// field that starts at the end of the value data are corrupt: strict raises an error,
// try_ yields NULL with a warning. Covers an empty value blob (at '$' and when looking up
// a field in it) and such a field.
TEST_F(VariantFunctionsTest, EmptyValueAtPath) {
  vector<uint8_t> meta = BuildMetadata({"a"});
  vector<uint8_t> empty_val;
  // {"a": <starts at the end of the (empty) value data>}
  vector<uint8_t> empty_field = {0x02, 0x01, 0x00, 0x00, 0x00};
  struct Case { const vector<uint8_t>* value; const char* path; };
  for (const Case& tc : {Case{&empty_val, "$"}, Case{&empty_val, "$.a"},
           Case{&empty_field, "$.a"}}) {
    VariantVal v = MakeVariant(meta, *tc.value);
    string warnings = CaptureStderr([&]() {
      EXPECT_TRUE(VariantFunctions::TryVariantGet(ctx_, v, StringVal(tc.path)).is_null);
    });
    EXPECT_NE(string::npos, warnings.find(CORRUPT_WARNING)) << tc.path << warnings;
    warnings = CaptureStderr([&]() {
      EXPECT_TRUE(VariantFunctions::TryVariantGetBoolean(
          ctx_, v, StringVal(tc.path), StringVal("boolean")).is_null);
    });
    EXPECT_NE(string::npos, warnings.find(CORRUPT_WARNING)) << tc.path << warnings;
    EXPECT_FALSE(ctx_->has_error()) << tc.path;
    FunctionContext* c = MakeCtx(2);
    EXPECT_TRUE(VariantFunctions::VariantGet(c, v, StringVal(tc.path)).is_null);
    EXPECT_TRUE(c->has_error()) << tc.path;
    FunctionContext* c3 = MakeCtx(3);
    EXPECT_TRUE(VariantFunctions::VariantGetInt(
        c3, v, StringVal(tc.path), StringVal("int")).is_null);
    EXPECT_TRUE(c3->has_error()) << tc.path;
  }
}

// ------------------ values stored out of name order (IMPALA-15460) ------------------

// Spark 4.1.3 bytes of {"b":2,"a":1} and {"a":1,"c":3,"b":2}: the field ids and offsets
// are listed in name order, but the values are stored in document order.
TEST_F(VariantFunctionsTest, OutOfOrderSparkBytes) {
  struct Case {
    const char* meta;
    const char* value;
    const char* json;
  };
  const vector<Case> cases = {
      {"01 02 00 01 02 62 61", "02 02 01 00 02 00 04 0c 02 0c 01", R"V({"a":1,"b":2})V"},
      {"01 03 00 01 02 03 61 63 62", "02 03 00 02 01 00 04 02 06 0c 01 0c 03 0c 02",
          R"V({"a":1,"b":2,"c":3})V"}};
  for (const Case& c : cases) {
    SCOPED_TRACE(c.value);
    vector<uint8_t> meta = FromHex(c.meta);
    vector<uint8_t> value = FromHex(c.value);
    VariantVal v = MakeVariant(meta, value);
    EXPECT_EQ(1, VariantFunctions::VariantGetInt(
        ctx_, v, StringVal("$.a"), StringVal("int")).val);
    EXPECT_EQ(2, VariantFunctions::VariantGetInt(
        ctx_, v, StringVal("$.b"), StringVal("int")).val);
    EXPECT_EQ(1, VariantFunctions::TryVariantGetInt(
        ctx_, v, StringVal("$.a"), StringVal("int")).val);
    EXPECT_EQ(2, VariantFunctions::TryVariantGetInt(
        ctx_, v, StringVal("$.b"), StringVal("int")).val);
    EXPECT_EQ(c.json, AsString(VariantFunctions::VariantGetString(
        ctx_, v, StringVal("$"), StringVal("string"))));
    EXPECT_EQ(c.json, Json(v));
    // The 2-arg variant_get returns the exact bytes of the field.
    VariantVal a = VariantFunctions::VariantGet(ctx_, v, StringVal("$.a"));
    ASSERT_FALSE(a.is_null);
    EXPECT_EQ(2, a.value.len);
    EXPECT_EQ("1", Json(a));
  }
  EXPECT_FALSE(ctx_->has_error());
}

// Spark 4.1.3 bytes of {"a":{"y":1,"x":2},"b":0}.
TEST_F(VariantFunctionsTest, OutOfOrderNested) {
  vector<uint8_t> meta = FromHex("01 04 00 01 02 03 04 61 79 78 62");
  vector<uint8_t> value =
      FromHex("02 02 00 03 00 0b 0d 02 02 02 01 02 00 04 0c 01 0c 02 0c 00");
  VariantVal v = MakeVariant(meta, value);
  EXPECT_EQ(2, VariantFunctions::VariantGetInt(
      ctx_, v, StringVal("$.a.x"), StringVal("int")).val);
  EXPECT_EQ(1, VariantFunctions::VariantGetInt(
      ctx_, v, StringVal("$.a.y"), StringVal("int")).val);
  EXPECT_EQ(0, VariantFunctions::VariantGetInt(
      ctx_, v, StringVal("$.b"), StringVal("int")).val);
  VariantVal a = VariantFunctions::VariantGet(ctx_, v, StringVal("$.a"));
  EXPECT_EQ(R"V({"x":2,"y":1})V", Json(a));
  EXPECT_EQ(R"V({"x":2,"y":1})V", AsString(VariantFunctions::VariantGetString(
      ctx_, v, StringVal("$.a"), StringVal("string"))));
  // Chaining: the outer get consumes the VARIANT returned by the inner one.
  EXPECT_EQ(2, VariantFunctions::VariantGetInt(
      ctx_, a, StringVal("$.x"), StringVal("int")).val);
  EXPECT_FALSE(ctx_->has_error());
}

// {"c":"s","b":true,"a":2.5}, stored in reverse name order, through the typed getters.
TEST_F(VariantFunctionsTest, OutOfOrderAllGetters) {
  vector<uint8_t> meta = BuildMetadata({"a", "b", "c"});
  vector<uint8_t> obj = BuildObject({0, 1, 2},
      {BuildDouble(2.5), BuildBoolean(true), BuildShortString("s")},
      /*storage_order=*/{2, 1, 0});
  VariantVal v = MakeVariant(meta, obj);
  EXPECT_EQ("s", AsString(VariantFunctions::VariantGetString(
      ctx_, v, StringVal("$.c"), StringVal("string"))));
  EXPECT_EQ("s", AsString(VariantFunctions::TryVariantGetString(
      ctx_, v, StringVal("$.c"), StringVal("string"))));
  BooleanVal b = VariantFunctions::VariantGetBoolean(
      ctx_, v, StringVal("$.b"), StringVal("boolean"));
  ASSERT_FALSE(b.is_null);
  EXPECT_TRUE(b.val);
  b = VariantFunctions::TryVariantGetBoolean(
      ctx_, v, StringVal("$.b"), StringVal("boolean"));
  ASSERT_FALSE(b.is_null);
  EXPECT_TRUE(b.val);
  EXPECT_DOUBLE_EQ(2.5, VariantFunctions::VariantGetDouble(
      ctx_, v, StringVal("$.a"), StringVal("double")).val);
  EXPECT_DOUBLE_EQ(2.5, VariantFunctions::TryVariantGetDouble(
      ctx_, v, StringVal("$.a"), StringVal("double")).val);
  EXPECT_EQ(R"V({"a":2.5,"b":true,"c":"s"})V", Json(v));
  EXPECT_FALSE(ctx_->has_error());
}

TEST_F(VariantFunctionsTest, OutOfOrderArrays) {
  // [{"b":2,"a":1}]
  vector<uint8_t> meta = BuildMetadata({"a", "b"});
  vector<uint8_t> arr = BuildArray(
      {BuildObject({0, 1}, {BuildInt8(1), BuildInt8(2)}, /*storage_order=*/{1, 0})});
  VariantVal v = MakeVariant(meta, arr);
  EXPECT_EQ(1, VariantFunctions::VariantGetInt(
      ctx_, v, StringVal("$[0].a"), StringVal("int")).val);
  EXPECT_EQ(2, VariantFunctions::VariantGetInt(
      ctx_, v, StringVal("$[0].b"), StringVal("int")).val);
  // [5,6] with the elements stored as 6, 5: the offsets are [2, 0, 4].
  vector<uint8_t> empty_meta = FromHex("01 00 00");
  vector<uint8_t> arr2 = FromHex("03 02 02 00 04 0c 06 0c 05");
  VariantVal v2 = MakeVariant(empty_meta, arr2);
  EXPECT_EQ(5, VariantFunctions::VariantGetInt(
      ctx_, v2, StringVal("$[0]"), StringVal("int")).val);
  EXPECT_EQ(6, VariantFunctions::VariantGetInt(
      ctx_, v2, StringVal("$[1]"), StringVal("int")).val);
  EXPECT_EQ("[5,6]", Json(v2));
  EXPECT_FALSE(ctx_->has_error());
}

// An unsorted dictionary ["x", "a", "a"] holds "a" twice, and {"a":7} uses the second id.
TEST_F(VariantFunctionsTest, DuplicateDictionaryStrings) {
  vector<uint8_t> meta = FromHex("01 03 00 01 02 03 78 61 61");
  vector<uint8_t> obj = FromHex("02 01 02 00 02 0c 07");
  VariantVal v = MakeVariant(meta, obj);
  IntVal a = VariantFunctions::VariantGetInt(ctx_, v, StringVal("$.a"), StringVal("int"));
  ASSERT_FALSE(a.is_null);
  EXPECT_EQ(7, a.val);
  EXPECT_EQ(R"V({"a":7})V", Json(v));
  EXPECT_FALSE(ctx_->has_error());
}

// A value on the path that cannot be read, the root included, is corrupt: strict
// variant_get raises an error, try_variant_get returns NULL with a warning.
TEST_F(VariantFunctionsTest, UnreadableValueIsCorrupt) {
  vector<uint8_t> meta = BuildMetadata({"a"});
  struct Case {
    const char* value;
    const char* path;
  };
  const vector<Case> cases = {
      // An INT32 field that runs past the last offset (2).
      {"02 01 00 00 02 14 01", "$.a"},
      // A field of an unknown primitive type (id 21).
      {"02 01 00 00 01 54", "$.a"},
      // Field id 5 is outside the 1-entry dictionary.
      {"02 01 05 00 02 0c 01", "$.a"},
      // A nested object with 100 fields in 3 bytes.
      {"02 01 00 00 03 02 64 00", "$.a.b"},
      // {"a":{"a":<INT32 without a payload>}}: '$.a' can be read, '$.a.a' cannot.
      {"02 01 00 00 07 02 01 00 00 02 14 01", "$.a.a"},
      // [<INT32 without a payload>]
      {"03 01 00 01 14", "$[0]"},
      // An object with 100 fields in 3 bytes.
      {"02 64 00", "$.a"},
      // A root INT32 without a full payload, and a root of an unknown type (id 21).
      {"14 01", "$"},
      {"54", "$"},
      {"54", "$.a"}};
  for (const Case& c : cases) {
    SCOPED_TRACE(Substitute("value: '$0' path: '$1'", c.value, c.path));
    vector<uint8_t> value = FromHex(c.value);
    VariantVal v = MakeVariant(meta, value);
    FunctionContext* c2 = MakeCtx(2);
    EXPECT_TRUE(VariantFunctions::VariantGet(c2, v, StringVal(c.path)).is_null);
    EXPECT_TRUE(c2->has_error());
    EXPECT_STREQ("variant_get: corrupt or unreadable variant", c2->error_msg());
    FunctionContext* c3 = MakeCtx(3);
    EXPECT_TRUE(VariantFunctions::VariantGetInt(
        c3, v, StringVal(c.path), StringVal("int")).is_null);
    EXPECT_TRUE(c3->has_error());
    EXPECT_STREQ("variant_get: corrupt or unreadable variant", c3->error_msg());
    string warnings = CaptureStderr([&]() {
      EXPECT_TRUE(VariantFunctions::TryVariantGet(ctx_, v, StringVal(c.path)).is_null);
    });
    EXPECT_NE(string::npos, warnings.find(CORRUPT_WARNING)) << warnings;
    warnings = CaptureStderr([&]() {
      EXPECT_TRUE(VariantFunctions::TryVariantGetInt(
          ctx_, v, StringVal(c.path), StringVal("int")).is_null);
    });
    EXPECT_NE(string::npos, warnings.find(CORRUPT_WARNING)) << warnings;
    EXPECT_FALSE(ctx_->has_error());
  }
}

// A path that does not match the value is SQL NULL, without an error or a warning.
TEST_F(VariantFunctionsTest, MissingPathsStayNull) {
  // {"a":1,"b":[1]}
  vector<uint8_t> meta = BuildMetadata({"a", "b"});
  vector<uint8_t> obj = BuildObject({0, 1}, {BuildInt8(1), BuildArray({BuildInt8(1)})});
  VariantVal v = MakeVariant(meta, obj);
  for (const char* path : {"$.zzz", "$.a.b", "$[0]", "$.b[5]", "$.b.c"}) {
    SCOPED_TRACE(path);
    FunctionContext* c2 = MakeCtx(2);
    EXPECT_TRUE(VariantFunctions::VariantGet(c2, v, StringVal(path)).is_null);
    EXPECT_FALSE(c2->has_error());
    FunctionContext* c3 = MakeCtx(3);
    EXPECT_TRUE(VariantFunctions::VariantGetInt(
        c3, v, StringVal(path), StringVal("int")).is_null);
    EXPECT_FALSE(c3->has_error());
    string warnings = CaptureStderr([&]() {
      EXPECT_TRUE(VariantFunctions::TryVariantGet(ctx_, v, StringVal(path)).is_null);
    });
    EXPECT_EQ(string::npos, warnings.find("UDF WARNING")) << warnings;
    warnings = CaptureStderr([&]() {
      EXPECT_TRUE(VariantFunctions::TryVariantGetInt(
          ctx_, v, StringVal(path), StringVal("int")).is_null);
    });
    EXPECT_EQ(string::npos, warnings.find("UDF WARNING")) << warnings;
  }
}

}  // namespace impala
