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

#include <cstring>
#include <vector>

#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/variant-value.h"
#include "testutil/gtest-util.h"
#include "udf/udf-internal.h"

#include "common/names.h"

using impala_udf::FunctionContext;

namespace impala {

// ------------------------- variant-blob builders ---------------------------
// (Mirrors the encoding used in be/src/util/variant-util-test.cc.)

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

static vector<uint8_t> BuildObject(const vector<int>& field_ids,
    const vector<vector<uint8_t>>& values) {
  int num_fields = field_ids.size();
  vector<uint32_t> offsets;
  uint32_t offset = 0;
  for (const auto& v : values) { offsets.push_back(offset); offset += v.size(); }
  offsets.push_back(offset);
  vector<uint8_t> buf = {0x02};  // OBJECT, 1-byte fields/offsets
  buf.push_back(static_cast<uint8_t>(num_fields));
  for (int fid : field_ids) buf.push_back(static_cast<uint8_t>(fid));
  for (uint32_t o : offsets) buf.push_back(static_cast<uint8_t>(o));
  for (const auto& v : values) buf.insert(buf.end(), v.begin(), v.end());
  return buf;
}

static vector<uint8_t> BuildArray(const vector<vector<uint8_t>>& elements) {
  int num_elements = elements.size();
  vector<uint32_t> offsets;
  uint32_t offset = 0;
  for (const auto& e : elements) { offsets.push_back(offset); offset += e.size(); }
  offsets.push_back(offset);
  vector<uint8_t> buf = {0x03};  // ARRAY, 1-byte offsets
  buf.push_back(static_cast<uint8_t>(num_elements));
  for (uint32_t o : offsets) buf.push_back(static_cast<uint8_t>(o));
  for (const auto& e : elements) buf.insert(buf.end(), e.begin(), e.end());
  return buf;
}

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
  // Whole object -> JSON text (stored field order: age then name).
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

// A zero-length value at the path is corrupt (every encoded value has a header byte):
// strict raises an error, try_ yields NULL without erroring. Covers an empty value blob
// and an object field whose start and end offsets are equal.
TEST_F(VariantFunctionsTest, EmptyValueAtPath) {
  vector<uint8_t> meta = BuildMetadata({"a"});
  vector<uint8_t> empty_val;
  vector<uint8_t> empty_field = {0x02, 0x01, 0x00, 0x00, 0x00};  // {"a": <0 bytes>}
  struct Case { const vector<uint8_t>* value; const char* path; };
  for (const Case& tc : {Case{&empty_val, "$"}, Case{&empty_field, "$.a"}}) {
    VariantVal v = MakeVariant(meta, *tc.value);
    EXPECT_TRUE(VariantFunctions::TryVariantGet(ctx_, v, StringVal(tc.path)).is_null);
    EXPECT_TRUE(VariantFunctions::TryVariantGetBoolean(
        ctx_, v, StringVal(tc.path), StringVal("boolean")).is_null);
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

}  // namespace impala
