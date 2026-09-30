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

#include "util/variant-util.h"

#include <algorithm>
#include <cstring>
#include <sstream>
#include <vector>

#include "gutil/strings/substitute.h"
#include "testutil/gtest-util.h"

#include "common/names.h"
#include "runtime/variant-value.h"

namespace impala {

// Appends 'val' to 'buf' as a 'size'-byte little-endian unsigned integer.
static void AppendUint(vector<uint8_t>* buf, uint32_t val, int size) {
  CHECK(size == 4 || val < (1u << (8 * size))) << val << " does not fit " << size;
  for (int i = 0; i < size; ++i) buf->push_back(static_cast<uint8_t>(val >> (8 * i)));
}

// Helper to build a metadata blob. 'sorted' sets the sorted_strings bit.
// Format: header(1) + dict_size + offsets(dict_size+1) + string_data, where dict_size and
// the offsets take 'offset_size' bytes each.
static vector<uint8_t> BuildMetadata(const vector<string>& field_names,
    bool sorted = true, int offset_size = 1) {
  vector<uint8_t> buf;
  // Header: version=1, sorted_strings (bit 4), offset_size_minus_one (bits 6-7).
  uint8_t header = 0x01 | ((sorted ? 1 : 0) << 4) | ((offset_size - 1) << 6);
  buf.push_back(header);
  AppendUint(&buf, field_names.size(), offset_size);
  // Offsets: dict_size+1 entries
  uint32_t offset = 0;
  for (size_t i = 0; i <= field_names.size(); ++i) {
    AppendUint(&buf, offset, offset_size);
    if (i < field_names.size()) offset += field_names[i].size();
  }
  // String data
  for (const string& name : field_names) {
    for (char c : name) buf.push_back(static_cast<uint8_t>(c));
  }
  return buf;
}

// Build a null primitive value.
static vector<uint8_t> BuildNull() {
  // basic_type=0 (PRIMITIVE), type_info=0 (NULL) → header = 0x00
  return {0x00};
}

// Build a boolean value.
static vector<uint8_t> BuildBoolean(bool val) {
  // basic_type=0, type_info=1(TRUE) or 2(FALSE)
  uint8_t type_info = val ? 1 : 2;
  return {static_cast<uint8_t>(type_info << 2)};
}

// Build an int32 value.
static vector<uint8_t> BuildInt32(int32_t val) {
  // basic_type=0, type_info=5 (INT32) → header = 5<<2 = 0x14
  vector<uint8_t> buf = {0x14};
  uint8_t bytes[4];
  memcpy(bytes, &val, 4);
  buf.insert(buf.end(), bytes, bytes + 4);
  return buf;
}

// Build a short string value (length < 64).
static vector<uint8_t> BuildShortString(const string& s) {
  DCHECK_LT(s.size(), 64);
  // basic_type=1 (SHORT_STRING), type_info=length
  uint8_t header = 0x01 | (static_cast<uint8_t>(s.size()) << 2);
  vector<uint8_t> buf = {header};
  for (char c : s) buf.push_back(static_cast<uint8_t>(c));
  return buf;
}

// Build a long string value: basic_type=0, type_info=16 (STRING), a 4-byte length and
// the bytes.
static vector<uint8_t> BuildLongString(const string& s) {
  vector<uint8_t> buf = {0x40};
  AppendUint(&buf, s.size(), 4);
  for (char c : s) buf.push_back(static_cast<uint8_t>(c));
  return buf;
}

// Byte widths of the field ids and offsets of an object or array, and its is_large flag
// (a 4-byte element count). Arrays have no field ids.
struct VariantWidths {
  int id_size = 1;
  int offset_size = 1;
  bool is_large = false;
};

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
}

// Build an object with the given field values. 'field_ids' are indices into the metadata
// dictionary and 'values[i]' is the value of 'field_ids[i]'. The spec requires the field
// ids (and so the offsets) to be listed in field name order, but allows the values to be
// stored in any order: 'storage_order' lists the indices of 'values' in the order they
// are stored in (empty means the same order as 'field_ids').
static vector<uint8_t> BuildObject(const vector<int>& field_ids,
    const vector<vector<uint8_t>>& values, const vector<int>& storage_order = {},
    const VariantWidths& w = VariantWidths()) {
  CHECK_EQ(field_ids.size(), values.size());
  vector<uint32_t> offsets;
  vector<uint8_t> data;
  LayoutValues(values, storage_order, &offsets, &data);
  // Header: basic_type=2 (OBJECT), field_offset_size_minus_one (bits 2-3),
  // field_id_size_minus_one (bits 4-5), is_large (bit 6).
  uint8_t header = 0x02 | ((w.offset_size - 1) << 2) | ((w.id_size - 1) << 4)
      | ((w.is_large ? 1 : 0) << 6);
  vector<uint8_t> buf = {header};
  AppendUint(&buf, field_ids.size(), w.is_large ? 4 : 1);
  for (int fid : field_ids) AppendUint(&buf, fid, w.id_size);
  for (uint32_t o : offsets) AppendUint(&buf, o, w.offset_size);
  buf.insert(buf.end(), data.begin(), data.end());
  return buf;
}

// Build an array with the given element values, stored in 'storage_order' (see
// BuildObject()). 'w.id_size' is ignored.
static vector<uint8_t> BuildArray(const vector<vector<uint8_t>>& elements,
    const vector<int>& storage_order = {}, const VariantWidths& w = VariantWidths()) {
  vector<uint32_t> offsets;
  vector<uint8_t> data;
  LayoutValues(elements, storage_order, &offsets, &data);
  // Header: basic_type=3 (ARRAY), offset_size_minus_one (bits 2-3), is_large (bit 4).
  uint8_t header = 0x03 | ((w.offset_size - 1) << 2) | ((w.is_large ? 1 : 0) << 4);
  vector<uint8_t> buf = {header};
  AppendUint(&buf, elements.size(), w.is_large ? 4 : 1);
  for (uint32_t o : offsets) AppendUint(&buf, o, w.offset_size);
  buf.insert(buf.end(), data.begin(), data.end());
  return buf;
}

const VariantLookupResult FOUND = VariantLookupResult::FOUND;
const VariantLookupResult NOT_FOUND = VariantLookupResult::NOT_FOUND;
const VariantLookupResult CORRUPT = VariantLookupResult::CORRUPT;

TEST(VariantUtilTest, MetadataParsing) {
  vector<string> names = {"age", "city", "name"};
  vector<uint8_t> meta_bytes = BuildMetadata(names);

  VariantMetadata metadata;
  ASSERT_OK(metadata.Init(meta_bytes.data(), meta_bytes.size()));
  EXPECT_EQ(3, metadata.DictionarySize());

  EXPECT_EQ(metadata.GetFieldName(0), "age");
  EXPECT_EQ(metadata.GetFieldName(1), "city");
  EXPECT_EQ(metadata.GetFieldName(2), "name");

  EXPECT_TRUE(metadata.IsSorted());

  vector<uint8_t> unsorted_bytes = BuildMetadata(names, /*sorted=*/false);
  VariantMetadata unsorted;
  ASSERT_OK(unsorted.Init(unsorted_bytes.data(), unsorted_bytes.size()));
  EXPECT_FALSE(unsorted.IsSorted());
  EXPECT_EQ(unsorted.GetFieldName(1), "city");
}

TEST(VariantUtilTest, NullValue) {
  vector<uint8_t> meta_bytes = BuildMetadata({});
  VariantMetadata metadata;
  ASSERT_OK(metadata.Init(meta_bytes.data(), meta_bytes.size()));

  vector<uint8_t> val_bytes = BuildNull();
  VariantValue val(val_bytes.data(), val_bytes.size(), &metadata);
  EXPECT_TRUE(val.IsNull());
  EXPECT_EQ(val.GetBasicType(), VariantBasicType::PRIMITIVE);
  EXPECT_EQ(val.GetPhysicalType(), VariantPhysicalType::VNULL);
}

TEST(VariantUtilTest, BooleanValue) {
  vector<uint8_t> meta_bytes = BuildMetadata({});
  VariantMetadata metadata;
  ASSERT_OK(metadata.Init(meta_bytes.data(), meta_bytes.size()));

  vector<uint8_t> true_bytes = BuildBoolean(true);
  VariantValue true_val(true_bytes.data(), true_bytes.size(), &metadata);
  EXPECT_FALSE(true_val.IsNull());
  bool b = false;
  EXPECT_TRUE(true_val.GetBoolean(&b));
  EXPECT_TRUE(b);

  vector<uint8_t> false_bytes = BuildBoolean(false);
  VariantValue false_val(false_bytes.data(), false_bytes.size(), &metadata);
  EXPECT_TRUE(false_val.GetBoolean(&b));
  EXPECT_FALSE(b);
}

TEST(VariantUtilTest, Int32Value) {
  vector<uint8_t> meta_bytes = BuildMetadata({});
  VariantMetadata metadata;
  ASSERT_OK(metadata.Init(meta_bytes.data(), meta_bytes.size()));

  vector<uint8_t> val_bytes = BuildInt32(42);
  VariantValue val(val_bytes.data(), val_bytes.size(), &metadata);
  EXPECT_EQ(val.GetBasicType(), VariantBasicType::PRIMITIVE);
  EXPECT_EQ(val.GetPhysicalType(), VariantPhysicalType::INT32);
  int32_t int_val;
  EXPECT_TRUE(val.GetInt32(&int_val));
  EXPECT_EQ(42, int_val);
}

TEST(VariantUtilTest, ShortString) {
  vector<uint8_t> meta_bytes = BuildMetadata({});
  VariantMetadata metadata;
  ASSERT_OK(metadata.Init(meta_bytes.data(), meta_bytes.size()));

  vector<uint8_t> val_bytes = BuildShortString("hello");
  VariantValue val(val_bytes.data(), val_bytes.size(), &metadata);
  EXPECT_EQ(val.GetBasicType(), VariantBasicType::SHORT_STRING);
  StringValue sv;
  ASSERT_TRUE(val.GetString(&sv));
  EXPECT_EQ(string(sv.Ptr(), sv.Len()), "hello");
}

TEST(VariantUtilTest, SimpleObject) {
  // Object with fields: {"age": 30, "name": "Alice"}
  vector<string> names = {"age", "name"};
  vector<uint8_t> meta_bytes = BuildMetadata(names);
  VariantMetadata metadata;
  ASSERT_OK(metadata.Init(meta_bytes.data(), meta_bytes.size()));

  vector<uint8_t> age_val = BuildInt32(30);
  vector<uint8_t> name_val = BuildShortString("Alice");
  vector<uint8_t> obj_bytes = BuildObject({0, 1}, {age_val, name_val});

  VariantValue obj(obj_bytes.data(), obj_bytes.size(), &metadata);
  EXPECT_EQ(obj.GetBasicType(), VariantBasicType::OBJECT);
  uint32_t obj_size;
  EXPECT_TRUE(obj.GetObjectSize(&obj_size));
  EXPECT_EQ(2, obj_size);

  VariantValue field_val;
  int32_t field_int;
  EXPECT_EQ(FOUND, obj.GetFieldByName("age", &field_val));
  EXPECT_TRUE(field_val.GetInt32(&field_int));
  EXPECT_EQ(30, field_int);

  EXPECT_EQ(FOUND, obj.GetFieldByName("name", &field_val));
  StringValue sv;
  ASSERT_TRUE(field_val.GetString(&sv));
  EXPECT_EQ(string(sv.Ptr(), sv.Len()), "Alice");

  EXPECT_EQ(NOT_FOUND, obj.GetFieldByName("unknown", &field_val));
}

TEST(VariantUtilTest, SimpleArray) {
  vector<uint8_t> meta_bytes = BuildMetadata({});
  VariantMetadata metadata;
  ASSERT_OK(metadata.Init(meta_bytes.data(), meta_bytes.size()));

  vector<uint8_t> elem0 = BuildInt32(1);
  vector<uint8_t> elem1 = BuildInt32(2);
  vector<uint8_t> elem2 = BuildInt32(3);
  vector<uint8_t> arr_bytes = BuildArray({elem0, elem1, elem2});

  VariantValue arr(arr_bytes.data(), arr_bytes.size(), &metadata);
  EXPECT_EQ(arr.GetBasicType(), VariantBasicType::ARRAY);
  uint32_t arr_size;
  EXPECT_TRUE(arr.GetArraySize(&arr_size));
  EXPECT_EQ(3, arr_size);

  VariantValue elem;
  int32_t elem_int;
  EXPECT_EQ(FOUND, arr.GetArrayElement(0, &elem));
  EXPECT_TRUE(elem.GetInt32(&elem_int));
  EXPECT_EQ(1, elem_int);
  EXPECT_EQ(FOUND, arr.GetArrayElement(2, &elem));
  EXPECT_TRUE(elem.GetInt32(&elem_int));
  EXPECT_EQ(3, elem_int);
  EXPECT_EQ(NOT_FOUND, arr.GetArrayElement(3, &elem));
}

TEST(VariantUtilTest, PathNavigation) {
  // {"data": {"items": [10, 20, 30]}}
  vector<string> names = {"data", "items"};
  vector<uint8_t> meta_bytes = BuildMetadata(names);
  VariantMetadata metadata;
  ASSERT_OK(metadata.Init(meta_bytes.data(), meta_bytes.size()));

  vector<uint8_t> items = BuildArray(
      {BuildInt32(10), BuildInt32(20), BuildInt32(30)});
  vector<uint8_t> inner_obj = BuildObject({1}, {items});  // field_id 1 = "items"
  vector<uint8_t> outer_obj = BuildObject({0}, {inner_obj});  // field_id 0 = "data"

  VariantValue root(outer_obj.data(), outer_obj.size(), &metadata);

  VariantValue result;
  int32_t nav_int;
  EXPECT_EQ(FOUND, root.NavigatePath("$.data.items[1]", &result));
  EXPECT_TRUE(result.GetInt32(&nav_int));
  EXPECT_EQ(20, nav_int);

  EXPECT_EQ(FOUND, root.NavigatePath("$.data.items[2]", &result));
  EXPECT_TRUE(result.GetInt32(&nav_int));
  EXPECT_EQ(30, nav_int);

  EXPECT_EQ(NOT_FOUND, root.NavigatePath("$.data.unknown", &result));
  EXPECT_EQ(NOT_FOUND, root.NavigatePath("$.data.items[100]", &result));

  // UINT32_MAX is always out of range, as an array has at most UINT32_MAX elements.
  EXPECT_EQ(NOT_FOUND, root.NavigatePath("$.data.items[4294967295]", &result));
  // Larger indices never resolve and must not overflow. 2^32 and 2^32 + 1 would wrap
  // around to the valid indices 0 and 1 in a 32-bit accumulator.
  EXPECT_EQ(NOT_FOUND, root.NavigatePath("$.data.items[4294967296]", &result));
  EXPECT_EQ(NOT_FOUND, root.NavigatePath("$.data.items[4294967297]", &result));
  EXPECT_EQ(NOT_FOUND, root.NavigatePath("$.data.items[100000000000000000000]", &result));
  EXPECT_EQ(NOT_FOUND, root.NavigatePath("$.data.items[18446744073709551617]", &result));
  // A long index with leading zeros is still a small index.
  EXPECT_EQ(FOUND, root.NavigatePath("$.data.items[000000000000000000000001]", &result));
  EXPECT_TRUE(result.GetInt32(&nav_int));
  EXPECT_EQ(20, nav_int);
}

TEST(VariantUtilTest, PathNavigationInvalidPaths) {
  // Test variant with invalid paths:
  // {
  //  "context":{"page":"home","retries":3},
  //  "device":"mobile","
  //  "int_array":[1,2,3,4,10],
  //  "user":"Alice"
  // }
  vector<uint8_t> meta_bytes{
      0x01, 0x06, 0x00, 0x07, 0x0b, 0x12, 0x18, 0x21, 0x25, 0x63, 0x6f, 0x6e,
      0x74, 0x65, 0x78, 0x74, 0x70, 0x61, 0x67, 0x65, 0x72, 0x65, 0x74, 0x72,
      0x69, 0x65, 0x73, 0x64, 0x65, 0x76, 0x69, 0x63, 0x65, 0x69, 0x6e, 0x74,
      0x5f, 0x61, 0x72, 0x72, 0x61, 0x79, 0x75, 0x73, 0x65, 0x72};
  vector<uint8_t> value_bytes{
      0x02, 0x04, 0x00, 0x03, 0x04, 0x05, 0x00, 0x0e, 0x15, 0x27, 0x2d, 0x02,
      0x02, 0x01, 0x02, 0x00, 0x05, 0x07, 0x11, 0x68, 0x6f, 0x6d, 0x65, 0x0c,
      0x03, 0x19, 0x6d, 0x6f, 0x62, 0x69, 0x6c, 0x65, 0x03, 0x05, 0x00, 0x02,
      0x04, 0x06, 0x08, 0x0a, 0x0c, 0x01, 0x0c, 0x02, 0x0c, 0x03, 0x0c, 0x04,
      0x0c, 0x0a, 0x15, 0x41, 0x6c, 0x69, 0x63, 0x65, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00};
  VariantMetadata metadata;
  ASSERT_OK(metadata.Init(meta_bytes.data(), meta_bytes.size()));
  VariantValue root(value_bytes.data(), value_bytes.size(), &metadata);
  VariantValue result;
  EXPECT_EQ(NOT_FOUND, root.NavigatePath("", &result));
  EXPECT_EQ(FOUND, root.NavigatePath("$", &result));
  EXPECT_EQ(NOT_FOUND, root.NavigatePath("@", &result));
  EXPECT_EQ(NOT_FOUND, root.NavigatePath(".", &result));
  EXPECT_EQ(NOT_FOUND, root.NavigatePath("..", &result));
  EXPECT_EQ(NOT_FOUND, root.NavigatePath("$.", &result));
  EXPECT_EQ(NOT_FOUND, root.NavigatePath(".$", &result));
  EXPECT_EQ(NOT_FOUND, root.NavigatePath("$..", &result));
  EXPECT_EQ(NOT_FOUND, root.NavigatePath("$.context..page", &result));
  EXPECT_EQ(FOUND, root.NavigatePath("$.int_array[0]", &result));
  EXPECT_EQ(NOT_FOUND, root.NavigatePath("$.int_array[]", &result));
  EXPECT_EQ(NOT_FOUND, root.NavigatePath("$.int_array]100[", &result));
  EXPECT_EQ(NOT_FOUND, root.NavigatePath("$.int_array[100]", &result));
}

TEST(VariantUtilTest, ToJson) {
  // {"age": 30, "name": "Alice", "active": true}
  vector<string> names = {"active", "age", "name"};
  vector<uint8_t> meta_bytes = BuildMetadata(names);
  VariantMetadata metadata;
  ASSERT_OK(metadata.Init(meta_bytes.data(), meta_bytes.size()));

  vector<uint8_t> active_val = BuildBoolean(true);
  vector<uint8_t> age_val = BuildInt32(30);
  vector<uint8_t> name_val = BuildShortString("Alice");
  // The values are stored in document order: age, name, active.
  vector<uint8_t> obj_bytes = BuildObject({0, 1, 2}, {active_val, age_val, name_val},
      /*storage_order=*/{1, 2, 0});

  string json;
  ASSERT_OK(VariantToJson(meta_bytes.data(), meta_bytes.size(),
      obj_bytes.data(), obj_bytes.size(), &json));
  // JSON lists the fields in field id order, which the spec requires to be name order,
  // whatever order the values are stored in.
  EXPECT_EQ(json, R"V({"active":true,"age":30,"name":"Alice"})V");
}

TEST(VariantUtilTest, JsonEscaping) {
  // Field names with special characters must be JSON-escaped.
  vector<string> names = {"a\"b", "c\\d", "e\nf"};
  vector<uint8_t> meta_bytes = BuildMetadata(names);
  VariantMetadata metadata;
  ASSERT_OK(metadata.Init(meta_bytes.data(), meta_bytes.size()));

  vector<uint8_t> val0 = BuildInt32(1);
  vector<uint8_t> val1 = BuildInt32(2);
  vector<uint8_t> val2 = BuildInt32(3);
  vector<uint8_t> obj_bytes = BuildObject({0, 1, 2}, {val0, val1, val2});

  string json;
  ASSERT_OK(VariantToJson(meta_bytes.data(), meta_bytes.size(),
      obj_bytes.data(), obj_bytes.size(), &json));
  EXPECT_EQ(json, R"V({"a\"b":1,"c\\d":2,"e\nf":3})V");
}

TEST(VariantUtilTest, JsonEscapingControlChars) {
  // Control characters U+0000 through U+001F must be escaped per RFC 8259.
  // Test \b (0x08), \f (0x0C), and a generic control char (0x01).
  string s_backspace = "a\bb";
  string s_formfeed = "c\fd";
  string s_ctrl = string("e") + '\x01' + "f";
  // Sorted dictionary.
  vector<string> names = {s_backspace, s_formfeed, s_ctrl};
  vector<uint8_t> meta_bytes = BuildMetadata(names);
  VariantMetadata metadata;
  ASSERT_OK(metadata.Init(meta_bytes.data(), meta_bytes.size()));

  vector<uint8_t> val0 = BuildInt32(10);
  vector<uint8_t> val1 = BuildInt32(30);
  vector<uint8_t> val2 = BuildInt32(20);
  vector<uint8_t> obj_bytes = BuildObject({0, 1, 2}, {val0, val1, val2});

  string json;
  ASSERT_OK(VariantToJson(meta_bytes.data(), meta_bytes.size(),
      obj_bytes.data(), obj_bytes.size(), &json));
  EXPECT_EQ(json, "{\"a\\bb\":10,\"c\\fd\":30,\"e\\u0001f\":20}");
}

TEST(VariantUtilTest, JsonEscapingStringValues) {
  // String values with control characters are also properly escaped.
  vector<uint8_t> meta_bytes = BuildMetadata({});
  VariantMetadata metadata;
  ASSERT_OK(metadata.Init(meta_bytes.data(), meta_bytes.size()));

  // Short string containing tab, quote, backslash, and a NULL byte.
  string raw = string("a\t\"\\") + '\x00' + "b";
  vector<uint8_t> val_bytes = BuildShortString(raw);

  string json;
  ASSERT_OK(VariantToJson(meta_bytes.data(), meta_bytes.size(),
      val_bytes.data(), val_bytes.size(), &json));
  EXPECT_EQ(json, "\"a\\t\\\"\\\\\\u0000b\"");
}

TEST(VariantUtilTest, HiveVariants) {
  // Tests parsing VARIANT values written by Hive.
  // Each entry: {metadata_bytes, value_bytes, expected_json}.
  struct TestCase {
    vector<uint8_t> metadata;
    vector<uint8_t> value;
    string expected_json;
  };

  vector<TestCase> test_cases = {
    // INT64: 10000000000
    {{0x01, 0x00, 0x00},
     {0x18, 0x00, 0xe4, 0x0b, 0x54, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
     R"V(10000000000)V"},
    // INT8: 1
    {{0x01, 0x00, 0x00},
     {0x0c, 0x01},
     R"V(1)V"},
    // INT8: 2
    {{0x01, 0x00, 0x00},
     {0x0c, 0x02},
     R"V(2)V"},
    // INT8: 3
    {{0x01, 0x00, 0x00},
     {0x0c, 0x03},
     R"V(3)V"},
    // Array of INT8: [1,2,3]
    {{0x01, 0x00, 0x00},
     {0x03, 0x03, 0x00, 0x02, 0x04, 0x06, 0x0c, 0x01, 0x0c, 0x02, 0x0c, 0x03,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
     R"V([1,2,3])V"},
    // INT8 with trailing byte: 1
    {{0x01, 0x00, 0x00},
     {0x0c, 0x01, 0x00},
     R"V(1)V"},
    // Short string: "impala"
    {{0x01, 0x00, 0x00},
     {0x19, 0x69, 0x6d, 0x70, 0x61, 0x6c, 0x61, 0x00, 0x00, 0x00},
     R"V("impala")V"},
    // INT32: 100000
    {{0x01, 0x00, 0x00},
     {0x14, 0xa0, 0x86, 0x01, 0x00, 0x00, 0x00},
     R"V(100000)V"},
    // Short string: "context"
    {{0x01, 0x00, 0x00},
     {0x1d, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x78, 0x74, 0x00, 0x00, 0x00, 0x00},
     R"V("context")V"},
    // DECIMAL4: scale=2, unscaled=314 -> "3.14"
    {{0x01, 0x00, 0x00},
     {0x20, 0x02, 0x3a, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00},
     R"V(3.14)V"},
    // Short string "home" with 6-field metadata dictionary
    {{0x01, 0x06, 0x00, 0x07, 0x0b, 0x12, 0x18, 0x21, 0x25, 0x63, 0x6f, 0x6e,
      0x74, 0x65, 0x78, 0x74, 0x70, 0x61, 0x67, 0x65, 0x72, 0x65, 0x74, 0x72,
      0x69, 0x65, 0x73, 0x64, 0x65, 0x76, 0x69, 0x63, 0x65, 0x69, 0x6e, 0x74,
      0x5f, 0x61, 0x72, 0x72, 0x61, 0x79, 0x75, 0x73, 0x65, 0x72},
     {0x11, 0x68, 0x6f, 0x6d, 0x65},
     R"V("home")V"},
    // Object: {"page":"home","retries":3}
    {{0x01, 0x06, 0x00, 0x07, 0x0b, 0x12, 0x18, 0x21, 0x25, 0x63, 0x6f, 0x6e,
      0x74, 0x65, 0x78, 0x74, 0x70, 0x61, 0x67, 0x65, 0x72, 0x65, 0x74, 0x72,
      0x69, 0x65, 0x73, 0x64, 0x65, 0x76, 0x69, 0x63, 0x65, 0x69, 0x6e, 0x74,
      0x5f, 0x61, 0x72, 0x72, 0x61, 0x79, 0x75, 0x73, 0x65, 0x72},
     {0x02, 0x02, 0x01, 0x02, 0x00, 0x05, 0x07, 0x11, 0x68, 0x6f, 0x6d, 0x65,
      0x0c, 0x03},
     R"V({"page":"home","retries":3})V"},
    // Short string: "mobile"
    {{0x01, 0x06, 0x00, 0x07, 0x0b, 0x12, 0x18, 0x21, 0x25, 0x63, 0x6f, 0x6e,
      0x74, 0x65, 0x78, 0x74, 0x70, 0x61, 0x67, 0x65, 0x72, 0x65, 0x74, 0x72,
      0x69, 0x65, 0x73, 0x64, 0x65, 0x76, 0x69, 0x63, 0x65, 0x69, 0x6e, 0x74,
      0x5f, 0x61, 0x72, 0x72, 0x61, 0x79, 0x75, 0x73, 0x65, 0x72},
     {0x19, 0x6d, 0x6f, 0x62, 0x69, 0x6c, 0x65},
     R"V("mobile")V"},
    // Array: [1,2,3,4,10]
    {{0x01, 0x06, 0x00, 0x07, 0x0b, 0x12, 0x18, 0x21, 0x25, 0x63, 0x6f, 0x6e,
      0x74, 0x65, 0x78, 0x74, 0x70, 0x61, 0x67, 0x65, 0x72, 0x65, 0x74, 0x72,
      0x69, 0x65, 0x73, 0x64, 0x65, 0x76, 0x69, 0x63, 0x65, 0x69, 0x6e, 0x74,
      0x5f, 0x61, 0x72, 0x72, 0x61, 0x79, 0x75, 0x73, 0x65, 0x72},
     {0x03, 0x05, 0x00, 0x02, 0x04, 0x06, 0x08, 0x0a, 0x0c, 0x01, 0x0c, 0x02,
      0x0c, 0x03, 0x0c, 0x04, 0x0c, 0x0a},
     R"V([1,2,3,4,10])V"},
    // Short string: "Alice"
    {{0x01, 0x06, 0x00, 0x07, 0x0b, 0x12, 0x18, 0x21, 0x25, 0x63, 0x6f, 0x6e,
      0x74, 0x65, 0x78, 0x74, 0x70, 0x61, 0x67, 0x65, 0x72, 0x65, 0x74, 0x72,
      0x69, 0x65, 0x73, 0x64, 0x65, 0x76, 0x69, 0x63, 0x65, 0x69, 0x6e, 0x74,
      0x5f, 0x61, 0x72, 0x72, 0x61, 0x79, 0x75, 0x73, 0x65, 0x72},
     {0x15, 0x41, 0x6c, 0x69, 0x63, 0x65},
     R"V("Alice")V"},
    // Nested object with all fields
    {{0x01, 0x06, 0x00, 0x07, 0x0b, 0x12, 0x18, 0x21, 0x25, 0x63, 0x6f, 0x6e,
      0x74, 0x65, 0x78, 0x74, 0x70, 0x61, 0x67, 0x65, 0x72, 0x65, 0x74, 0x72,
      0x69, 0x65, 0x73, 0x64, 0x65, 0x76, 0x69, 0x63, 0x65, 0x69, 0x6e, 0x74,
      0x5f, 0x61, 0x72, 0x72, 0x61, 0x79, 0x75, 0x73, 0x65, 0x72},
     {0x02, 0x04, 0x00, 0x03, 0x04, 0x05, 0x00, 0x0e, 0x15, 0x27, 0x2d, 0x02,
      0x02, 0x01, 0x02, 0x00, 0x05, 0x07, 0x11, 0x68, 0x6f, 0x6d, 0x65, 0x0c,
      0x03, 0x19, 0x6d, 0x6f, 0x62, 0x69, 0x6c, 0x65, 0x03, 0x05, 0x00, 0x02,
      0x04, 0x06, 0x08, 0x0a, 0x0c, 0x01, 0x0c, 0x02, 0x0c, 0x03, 0x0c, 0x04,
      0x0c, 0x0a, 0x15, 0x41, 0x6c, 0x69, 0x63, 0x65, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
     R"V({"context":{"page":"home","retries":3},"device":"mobile",)V"
     R"V("int_array":[1,2,3,4,10],"user":"Alice"})V"},
    // INT16: 1000
    {{0x01, 0x00, 0x00},
     {0x10, 0xe8, 0x03, 0x00},
     R"V(1000)V"},
    // Boolean: false
    {{0x01, 0x00, 0x00},
     {0x08},
     R"V(false)V"},
    // Boolean: true
    {{0x01, 0x00, 0x00},
     {0x04},
     R"V(true)V"},
  };

  for (int i = 0; i < test_cases.size(); ++i) {
    const TestCase& tc = test_cases[i];
    string json;
    ASSERT_OK(VariantToJson(tc.metadata.data(), tc.metadata.size(),
        tc.value.data(), tc.value.size(), &json));
    EXPECT_EQ(json, tc.expected_json) << "Mismatch on test case " << i;
  }
}

// Test metadata with offset_size > 1 (2-byte offsets).
TEST(VariantUtilTest, MetadataLargeOffsetSize) {
  // Build metadata manually with offset_size=2 (offset_size_minus_one=1).
  // Header: version=1, sorted=1, offset_size_minus_one=1
  // Bits: version(0-3)=1, sorted(4)=1, reserved(5)=0, offset_size_minus_one(6-7)=1
  uint8_t header = 0x01 | (1 << 4) | (1 << 6);  // = 0x51
  vector<uint8_t> buf = {header};
  // Dictionary size: 2 entries, encoded as 2 bytes (little-endian)
  buf.push_back(0x02);
  buf.push_back(0x00);
  // Offsets: 3 entries (dict_size + 1) of 2 bytes each
  // String "hello" at offset 0, "world" at offset 5, end at offset 10
  buf.push_back(0x00); buf.push_back(0x00);  // offset[0] = 0
  buf.push_back(0x05); buf.push_back(0x00);  // offset[1] = 5
  buf.push_back(0x0a); buf.push_back(0x00);  // offset[2] = 10
  // String data
  for (char c : string("helloworld")) buf.push_back(static_cast<uint8_t>(c));

  VariantMetadata metadata;
  ASSERT_OK(metadata.Init(buf.data(), buf.size()));
  EXPECT_EQ(2, metadata.DictionarySize());

  EXPECT_EQ(metadata.GetFieldName(0), "hello");
  EXPECT_EQ(metadata.GetFieldName(1), "world");
}

// Test object with field_id_size != offset_size.
TEST(VariantUtilTest, ObjectDifferentFieldIdAndOffsetSize) {
  constexpr uint32_t DICT_SIZE = 300;
  vector<string> names;
  names.reserve(DICT_SIZE);
  for (int i = 0; i < DICT_SIZE; ++i) names.push_back("f" + std::to_string(i));
  // Build metadata with offset_size=2 to accommodate large offsets.
  uint8_t meta_header = 0x01 | (1 << 4) | (1 << 6);  // version=1, sorted=1, os=2
  vector<uint8_t> meta_buf = {meta_header};
  // dict_size = 300, 2 bytes little-endian
  meta_buf.push_back(0x2c);
  meta_buf.push_back(0x01);
  // Offsets: 301 entries of 2 bytes each
  uint32_t offset = 0;
  for (size_t i = 0; i <= names.size(); ++i) {
    meta_buf.push_back(static_cast<uint8_t>(offset & 0xFF));
    meta_buf.push_back(static_cast<uint8_t>((offset >> 8) & 0xFF));
    if (i < names.size()) offset += names[i].size();
  }
  // String data
  for (const string& name : names) {
    for (char c : name) meta_buf.push_back(static_cast<uint8_t>(c));
  }

  VariantMetadata metadata;
  ASSERT_OK(metadata.Init(meta_buf.data(), meta_buf.size()));

  // Build an object with field_id_size=2 (since field_ids > 255) and offset_size=1.
  // Object header byte: basic_type=2 (bits0-1), field_offset_size_minus_one=0 (bits2-3),
  // field_id_size_minus_one=1 (bits4-5), is_large=0 (bit6)
  // = 0x02 | (0 << 2) | (1 << 4) = 0x12
  int field_id_size = 2;
  int offset_size = 1;
  uint8_t obj_header = 0x02 | (((offset_size - 1) & 0x03) << 2)
      | (((field_id_size - 1) & 0x03) << 4);

  // Two fields: field_id=0 ("f0") with int32 value 42, field_id=299 ("f299") with int32 7
  vector<uint8_t> val0 = BuildInt32(42);
  vector<uint8_t> val1 = BuildInt32(7);
  int num_fields = 2;

  vector<uint8_t> obj_buf = {obj_header};
  // num_fields: 1 byte (not large)
  obj_buf.push_back(static_cast<uint8_t>(num_fields));
  // Field IDs: 2 bytes each (little-endian)
  obj_buf.push_back(0x00); obj_buf.push_back(0x00);  // field_id = 0
  obj_buf.push_back(0x2b); obj_buf.push_back(0x01);  // field_id = 299
  // Offsets: 1 byte each, (num_fields + 1) entries
  obj_buf.push_back(0x00);  // offset[0] = 0
  obj_buf.push_back(static_cast<uint8_t>(val0.size()));  // offset[1] = 5
  obj_buf.push_back(static_cast<uint8_t>(val0.size() + val1.size()));  // offset[2] = 10
  // Value data
  obj_buf.insert(obj_buf.end(), val0.begin(), val0.end());
  obj_buf.insert(obj_buf.end(), val1.begin(), val1.end());

  VariantValue obj(obj_buf.data(), obj_buf.size(), &metadata);
  EXPECT_EQ(obj.GetBasicType(), VariantBasicType::OBJECT);
  uint32_t obj_size;
  EXPECT_TRUE(obj.GetObjectSize(&obj_size));
  EXPECT_EQ(2, obj_size);

  VariantValue field_val;
  int32_t field_int;
  EXPECT_EQ(FOUND, obj.GetFieldByName("f0", &field_val));
  EXPECT_TRUE(field_val.GetInt32(&field_int));
  EXPECT_EQ(42, field_int);

  EXPECT_EQ(FOUND, obj.GetFieldByName("f299", &field_val));
  EXPECT_TRUE(field_val.GetInt32(&field_int));
  EXPECT_EQ(7, field_int);

  EXPECT_EQ(NOT_FOUND, obj.GetFieldByName("f1", &field_val));
}

// Test long STRING primitive (basic_type=0, physical_type=STRING=16).
TEST(VariantUtilTest, LongStringPrimitive) {
  vector<uint8_t> meta_bytes = BuildMetadata({});
  VariantMetadata metadata;
  ASSERT_OK(metadata.Init(meta_bytes.data(), meta_bytes.size()));

  // Long string encoding: basic_type=0 (PRIMITIVE), type_info=16 (STRING)
  // Header byte: (16 << 2) | 0 = 0x40
  uint8_t header = 0x40;
  vector<uint8_t> val_buf = {header};
  string long_str(100, 'x');
  // Followed by 4-byte little-endian length
  uint32_t len = long_str.size();
  val_buf.push_back(static_cast<uint8_t>(len & 0xFF));
  val_buf.push_back(static_cast<uint8_t>((len >> 8) & 0xFF));
  val_buf.push_back(static_cast<uint8_t>((len >> 16) & 0xFF));
  val_buf.push_back(static_cast<uint8_t>((len >> 24) & 0xFF));
  // Then string data
  for (char c : long_str) val_buf.push_back(static_cast<uint8_t>(c));

  VariantValue val(val_buf.data(), val_buf.size(), &metadata);
  EXPECT_EQ(val.GetBasicType(), VariantBasicType::PRIMITIVE);
  EXPECT_EQ(val.GetPhysicalType(), VariantPhysicalType::STRING);
  StringValue sv;
  ASSERT_TRUE(val.GetString(&sv));
  EXPECT_EQ(string(sv.Ptr(), sv.Len()), long_str);
}

TEST(VariantUtilTest, DuckDbVariants) {
  // Tests parsing VARIANT values from DuckDB's unshredded Parquet test files.
  // Source: duckdb/data/parquet-testing/variant_*.parquet (non-shredded only).
  // Results can be checked in duckdb/test/parquet/variant/variant_basic.test
  struct TestCase {
    vector<uint8_t> metadata;
    vector<uint8_t> value;
    string expected_json;
  };

  vector<TestCase> test_cases = {
    // variant_null.parquet: NULL
    {{0x01, 0x00, 0x00},
     {0x00},
     R"V(null)V"},
    // variant_bool_true.parquet: true
    {{0x01, 0x00, 0x00},
     {0x04},
     R"V(true)V"},
    // variant_bool_false.parquet: false
    {{0x01, 0x00, 0x00},
     {0x08},
     R"V(false)V"},
    // variant_int8_positive.parquet: 34
    {{0x01, 0x00, 0x00},
     {0x0c, 0x22},
     R"V(34)V"},
    // variant_int8_negative.parquet: -34
    {{0x01, 0x00, 0x00},
     {0x0c, 0xde},
     R"V(-34)V"},
    // variant_int16.parquet: -1234
    {{0x01, 0x00, 0x00},
     {0x10, 0x2e, 0xfb},
     R"V(-1234)V"},
    // variant_int32.parquet: -12345
    {{0x01, 0x00, 0x00},
     {0x14, 0xc7, 0xcf, 0xff, 0xff},
     R"V(-12345)V"},
    // variant_int32_positive.parquet: 12345
    {{0x01, 0x00, 0x00},
     {0x14, 0x39, 0x30, 0x00, 0x00},
     R"V(12345)V"},
    // variant_string.parquet: short string "iceberg"
    {{0x01, 0x00, 0x00},
     {0x1d, 0x69, 0x63, 0x65, 0x62, 0x65, 0x72, 0x67},
     R"V("iceberg")V"},
    // variant_double_positive.parquet: 14.3
    {{0x01, 0x00, 0x00},
     {0x1c, 0x9a, 0x99, 0x99, 0x99, 0x99, 0x99, 0x2c, 0x40},
     R"V(14.3)V"},
    // variant_double_negative.parquet: -14.3
    {{0x01, 0x00, 0x00},
     {0x1c, 0x9a, 0x99, 0x99, 0x99, 0x99, 0x99, 0x2c, 0xc0},
     R"V(-14.3)V"},
    // variant_float_negative.parquet: -10.11
    {{0x01, 0x00, 0x00},
     {0x38, 0x8f, 0xc2, 0x21, 0xc1},
     R"V(-10.11)V"},
    // variant_decimal4_positive.parquet: 123456.789 (scale=3, unscaled=123456789)
    {{0x01, 0x00, 0x00},
     {0x20, 0x03, 0x15, 0xcd, 0x5b, 0x07},
     R"V(123456.789)V"},
    // variant_decimal4_negative.parquet: -123456.789
    {{0x01, 0x00, 0x00},
     {0x20, 0x03, 0xeb, 0x32, 0xa4, 0xf8},
     R"V(-123456.789)V"},
    // variant_decimal8_negative.parquet: -123456789.987654321 (scale=9)
    {{0x01, 0x00, 0x00},
     {0x24, 0x09, 0x4f, 0x05, 0xad, 0x1f, 0xb4, 0x64, 0x49, 0xfe},
     R"V(-123456789.987654321)V"},
    // variant_decimal16.parquet: 9876543210.123456789 (scale=9)
    {{0x01, 0x00, 0x00},
     {0x28, 0x09, 0x15, 0x71, 0x34, 0xb0, 0xb8, 0x87, 0x10, 0x89, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
     R"V(9876543210.123456789)V"},
    // variant_array_empty.parquet: []
    {{0x01, 0x00, 0x00},
     {0x03, 0x00, 0x00},
     R"V([])V"},
    // variant_array_string.parquet: ["iceberg","string"]
    {{0x01, 0x00, 0x00},
     {0x03, 0x02, 0x00, 0x08, 0x0f, 0x1d, 0x69, 0x63, 0x65, 0x62, 0x65, 0x72,
      0x67, 0x19, 0x73, 0x74, 0x72, 0x69, 0x6e, 0x67},
     R"V(["iceberg","string"])V"},
    // variant_array_array_string.parquet: [["string","iceberg"],["apple","banana"]]
    {{0x01, 0x00, 0x00},
     {0x03, 0x02, 0x00, 0x14, 0x26, 0x03, 0x02, 0x00, 0x07, 0x0f, 0x19, 0x73,
      0x74, 0x72, 0x69, 0x6e, 0x67, 0x1d, 0x69, 0x63, 0x65, 0x62, 0x65, 0x72,
      0x67, 0x03, 0x02, 0x00, 0x06, 0x0d, 0x15, 0x61, 0x70, 0x70, 0x6c, 0x65,
      0x19, 0x62, 0x61, 0x6e, 0x61, 0x6e, 0x61},
     R"V([["string","iceberg"],["apple","banana"]])V"},
    // variant_array_array_string_and_integer.parquet:
    // [["string","iceberg",34],[34,null],[],["string","iceberg"],34]
    {{0x01, 0x00, 0x00},
     {0x03, 0x05, 0x00, 0x1a, 0x25, 0x28, 0x3c, 0x41, 0x03, 0x03, 0x00, 0x07,
      0x0f, 0x14, 0x19, 0x73, 0x74, 0x72, 0x69, 0x6e, 0x67, 0x1d, 0x69, 0x63,
      0x65, 0x62, 0x65, 0x72, 0x67, 0x14, 0x22, 0x00, 0x00, 0x00, 0x03, 0x02,
      0x00, 0x05, 0x06, 0x14, 0x22, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00,
      0x03, 0x02, 0x00, 0x07, 0x0f, 0x19, 0x73, 0x74, 0x72, 0x69, 0x6e, 0x67,
      0x1d, 0x69, 0x63, 0x65, 0x62, 0x65, 0x72, 0x67, 0x14, 0x22, 0x00, 0x00,
      0x00},
     R"V([["string","iceberg",34],[34,null],[],["string","iceberg"],34])V"},
    // variant_array_object_string_and_integer.parquet:
    // [{"a":123456789,"c":"string"},{"a":123456789,"c":"string"},"iceberg",34]
    // Metadata dict: [a, b, c, d, e] (sorted)
    {{0x11, 0x05, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x61, 0x62, 0x63, 0x64,
      0x65},
     {0x03, 0x04, 0x00, 0x13, 0x26, 0x2e, 0x33, 0x02, 0x02, 0x00, 0x02, 0x00,
      0x05, 0x0c, 0x14, 0x15, 0xcd, 0x5b, 0x07, 0x19, 0x73, 0x74, 0x72, 0x69,
      0x6e, 0x67, 0x02, 0x02, 0x00, 0x02, 0x00, 0x05, 0x0c, 0x14, 0x15, 0xcd,
      0x5b, 0x07, 0x19, 0x73, 0x74, 0x72, 0x69, 0x6e, 0x67, 0x1d, 0x69, 0x63,
      0x65, 0x62, 0x65, 0x72, 0x67, 0x14, 0x22, 0x00, 0x00, 0x00},
     R"V([{"a":123456789,"c":"string"},{"a":123456789,"c":"string"},"iceberg",34])V"},
    // variant_object_empty.parquet: {}
    {{0x01, 0x00, 0x00},
     {0x02, 0x00, 0x00},
     R"V({})V"},
    // variant_object_null_and_string.parquet: {"a":null,"d":"iceberg"}
    // Metadata dict: [a, b, c, d, e] (sorted)
    {{0x11, 0x05, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x61, 0x62, 0x63, 0x64,
      0x65},
     {0x02, 0x02, 0x00, 0x03, 0x00, 0x01, 0x09, 0x00, 0x1d, 0x69, 0x63, 0x65,
      0x62, 0x65, 0x72, 0x67},
     R"V({"a":null,"d":"iceberg"})V"},
    // variant_object_string_and_array.parquet: {"a":123456789,"c":["string","iceberg"]}
    // Metadata dict: [a, b, c, d, e] (sorted)
    {{0x11, 0x05, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x61, 0x62, 0x63, 0x64,
      0x65},
     {0x02, 0x02, 0x00, 0x02, 0x00, 0x05, 0x19, 0x14, 0x15, 0xcd, 0x5b, 0x07,
      0x03, 0x02, 0x00, 0x07, 0x0f, 0x19, 0x73, 0x74, 0x72, 0x69, 0x6e, 0x67,
      0x1d, 0x69, 0x63, 0x65, 0x62, 0x65, 0x72, 0x67},
     R"V({"a":123456789,"c":["string","iceberg"]})V"},
    // variant_date_positive.parquet: 2024-11-07 (20034 days since epoch)
    {{0x01, 0x00, 0x00},
     {0x2c, 0x42, 0x4e, 0x00, 0x00},
     R"V("2024-11-07")V"},
    // variant_date_negative.parquet: 1957-11-07 (-4438 days since epoch)
    {{0x01, 0x00, 0x00},
     {0x2c, 0xaa, 0xee, 0xff, 0xff},
     R"V("1957-11-07")V"},
    // variant_binary.parquet: binary value (4 bytes: 0a 0b 0c 0d) -> base64
    {{0x01, 0x00, 0x00},
     {0x3c, 0x04, 0x00, 0x00, 0x00, 0x0a, 0x0b, 0x0c, 0x0d},
     R"V("CgsMDQ==")V"},
    // variant_time_ntz.parquet: TIME type
    {{0x01, 0x00, 0x00},
     {0x44, 0xc0, 0xf2, 0x29, 0x88, 0x0a, 0x00, 0x00, 0x00},
     R"V("<unsupported-type>")V"},
    // variant_timestamp_micros_positive.parquet: TIMESTAMPTZ
    {{0x01, 0x00, 0x00},
     {0x30, 0xc0, 0xb2, 0xf0, 0xd8, 0x51, 0x26, 0x06, 0x00},
     R"V("<unsupported-type>")V"},
    // variant_timestamp_micros_ntz_positive.parquet: TIMESTAMPNTZ
    {{0x01, 0x00, 0x00},
     {0x34, 0xc0, 0xb2, 0xf0, 0xd8, 0x51, 0x26, 0x06, 0x00},
     R"V("2024-11-07 12:33:54.123456000")V"},
    // variant_timestamp_nanos1.parquet: TIMESTAMPTZ_NANOS
    {{0x01, 0x00, 0x00},
     {0x48, 0x15, 0x41, 0x52, 0xd4, 0x94, 0xe5, 0xad, 0xfa},
     R"V("<unsupported-type>")V"},
    // variant_timestamp_nanos_ntz.parquet: TIMESTAMPNTZ_NANOS
    {{0x01, 0x00, 0x00},
     {0x4c, 0x15, 0x41, 0x52, 0xd4, 0x94, 0xe5, 0xad, 0xfa},
     R"V("1957-11-07 12:33:54.123456789")V"},
    // variant_uuid.parquet: UUID
    {{0x01, 0x00, 0x00},
     {0x50, 0xf2, 0x4f, 0x9b, 0x64, 0x81, 0xfa, 0x49, 0xd1, 0xb7, 0x4e, 0x8c,
      0x09, 0xa6, 0xe3, 0x1c, 0x56},
     R"V("<unsupported-type>")V"},
  };

  for (int i = 0; i < test_cases.size(); ++i) {
    const TestCase& tc = test_cases[i];
    string json;
    ASSERT_OK(VariantToJson(tc.metadata.data(), tc.metadata.size(),
        tc.value.data(), tc.value.size(), &json));
    EXPECT_EQ(json, tc.expected_json) << "Mismatch on test case " << i;
  }
}

// --- Corrupt / malformed input handling ---

// Expects VariantToJson to reject the (metadata, value) blobs with an error rather than
// reading out of bounds. If 'expected_msg' is not empty, the error must contain it.
static void ExpectVariantError(const vector<uint8_t>& metadata,
    const vector<uint8_t>& value, const string& expected_msg = "") {
  string json;
  Status s = VariantToJson(metadata.data(), metadata.size(),
      value.data(), value.size(), &json);
  EXPECT_FALSE(s.ok()) << "expected an error but succeeded with json: " << json;
  EXPECT_NE(string::npos, s.GetDetail().find(expected_msg)) << s.GetDetail();
}

// Expects VariantMetadata::Init to reject 'metadata' as malformed.
static void ExpectMetadataError(const vector<uint8_t>& metadata) {
  VariantMetadata m;
  EXPECT_FALSE(m.Init(metadata.data(), metadata.size()).ok());
}

TEST(VariantUtilTest, MetadataCorruptionRejected) {
  // Empty blob.
  ExpectMetadataError({});
  // Unsupported version (version=2).
  ExpectMetadataError({0x02, 0x00, 0x00});
  // offset_size=4 (header bits 6-7 = 3) but blob too short to hold the dictionary size.
  ExpectMetadataError({0xC1, 0x00});
  // dict_size=3 but no offset array / string data present.
  ExpectMetadataError({0x01, 0x03});
  // Integer-overflow attempt: offset_size=4, dict_size=0xFFFFFFFF, short blob. In 32-bit
  // math (dict_size+1)*offset_size wraps to 0, so the check would wrongly pass; the
  // 64-bit math must reject it.
  ExpectMetadataError({0xC1, 0xFF, 0xFF, 0xFF, 0xFF});
  // Non-monotonic dictionary offsets [0, 5, 3].
  ExpectMetadataError({0x01, 0x02, 0x00, 0x05, 0x03, 'a', 'b', 'c', 'd', 'e'});
  // Last offset (10) exceeds the 2-byte string-data region.
  ExpectMetadataError({0x01, 0x01, 0x00, 0x0A, 'a', 'b'});

  // Valid baseline still parses.
  VariantMetadata m;
  const vector<uint8_t> valid = {0x01, 0x01, 0x00, 0x03, 'a', 'b', 'c'};
  EXPECT_OK(m.Init(valid.data(), valid.size()));
  EXPECT_EQ(m.GetFieldName(0), "abc");
}

TEST(VariantUtilTest, ValueCorruptionRejected) {
  const vector<uint8_t> empty_meta = {0x01, 0x00, 0x00};
  // Metadata dictionary [a, b, c, d, e] (sorted).
  const vector<uint8_t> meta5 = {0x11, 0x05, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05,
      0x61, 0x62, 0x63, 0x64, 0x65};

  // Empty value buffer.
  ExpectVariantError(empty_meta, {});
  // Short string claims length 5 but has only 2 payload bytes.
  ExpectVariantError(empty_meta, {0x15, 'a', 'b'});
  // Long string (STRING=0x40) with a 4-byte length of 0xFFFFFFFF and no payload.
  ExpectVariantError(empty_meta, {0x40, 0xFF, 0xFF, 0xFF, 0xFF});
  // Long string with a truncated (2-byte) length prefix.
  ExpectVariantError(empty_meta, {0x40, 0x01, 0x00});
  // INT8 (0x0c) with no payload byte (needs 1).
  ExpectVariantError(empty_meta, {0x0c});
  // INT16 (0x10) with only 1 payload byte (needs 2).
  ExpectVariantError(empty_meta, {0x10, 0x01});
  // INT32 (0x14) with only 2 payload bytes (needs 4).
  ExpectVariantError(empty_meta, {0x14, 0x01, 0x02});
  // INT64 (0x18) with only 4 payload bytes (needs 8).
  ExpectVariantError(empty_meta, {0x18, 0x01, 0x02, 0x03, 0x04});
  // FLOAT (0x38) with only 2 payload bytes (needs 4).
  ExpectVariantError(empty_meta, {0x38, 0x01, 0x02});
  // DOUBLE (0x1c) with only 4 payload bytes (needs 8).
  ExpectVariantError(empty_meta, {0x1c, 0x01, 0x02, 0x03, 0x04});
  // Binary (0x3c) claiming a huge payload length.
  ExpectVariantError(empty_meta, {0x3c, 0xFF, 0xFF, 0xFF, 0x7F});
  // Object claiming num_fields=100 in a tiny buffer.
  ExpectVariantError(meta5, {0x02, 0x64, 0x00});
  // Object with a field offset (99) past the end of the data region.
  ExpectVariantError(meta5, {0x02, 0x01, 0x00, 0x00, 0x63});
  // Object whose only field starts at offset 5, while the last offset (0) says the value
  // data is empty. (Offsets that are not monotonic are valid on their own: the values
  // may be stored in any order.)
  ExpectVariantError(meta5,
      {0x02, 0x01, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00});
  // Empty object whose last offset (5) is past the end of the buffer.
  ExpectVariantError(empty_meta, {0x02, 0x00, 0x05});
  // Object whose last offset (6) is past the end of the value data.
  ExpectVariantError(meta5, {0x02, 0x01, 0x00, 0x00, 0x06, 0x0c, 0x01});
  // Object with an INT32 field that runs past the last offset (2), although the buffer
  // holds the whole INT32.
  ExpectVariantError(meta5, {0x02, 0x01, 0x00, 0x00, 0x02, 0x14, 0x01, 0x02, 0x03, 0x04},
      "Malformed variant object field value (out of bounds or unknown type)");
  // Object with a field of an unknown primitive type (id 21).
  ExpectVariantError(meta5, {0x02, 0x01, 0x00, 0x00, 0x01, 0x54},
      "Malformed variant object field value (out of bounds or unknown type)");
  // Object with a long string field whose length (0xFFFFFFFC) would wrap 5 + length
  // around in 32 bits.
  ExpectVariantError(meta5, {0x02, 0x01, 0x00, 0x00, 0x09, 0x40, 0xFC, 0xFF, 0xFF, 0xFF,
      0x61, 0x62, 0x63, 0x64});
  // Object with a field id (200) that is out of range of the 5-entry dictionary.
  ExpectVariantError(meta5, {0x02, 0x01, 0xC8, 0x00, 0x01, 0x00});
  // Array claiming num_elements=200 in a tiny buffer.
  ExpectVariantError(empty_meta, {0x03, 0xC8, 0x00});
  // Array with an element offset (50) past the end of the data region.
  ExpectVariantError(empty_meta, {0x03, 0x01, 0x00, 0x32});
  // Array whose only element starts past the (empty) value data.
  ExpectVariantError(empty_meta, {0x03, 0x01, 0x01, 0x00, 0x00});
  // Array with a truncated INT32 element.
  ExpectVariantError(empty_meta, {0x03, 0x01, 0x00, 0x02, 0x14, 0x01},
      "Malformed variant array element (out of bounds or unknown type)");
  // Primitives with an unknown type id (21 and 63).
  ExpectVariantError(empty_meta, {0x54},
      "Malformed variant value: unknown primitive type id 21");
  ExpectVariantError(empty_meta, {0xFC},
      "Malformed variant value: unknown primitive type id 63");
  // DECIMAL4 (0x20) with scale=255, past the max precision of 9. An unchecked scale would
  // underflow the output buffer in DecimalValue::ToString (OOB write).
  ExpectVariantError(empty_meta, {0x20, 0xFF, 0x00, 0x00, 0x00, 0x00});
  // DECIMAL4 with scale=10, one past the max precision of 9.
  ExpectVariantError(empty_meta, {0x20, 0x0A, 0x00, 0x00, 0x00, 0x00});
  // DECIMAL8 (0x24) with scale=200, past the max precision of 18.
  ExpectVariantError(empty_meta,
      {0x24, 0xC8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00});
  // DECIMAL16 (0x28) with scale=255, past the max precision of 38.
  ExpectVariantError(empty_meta,
      {0x28, 0xFF, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
       0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00});
  // Truncated fixed-width payloads: the scale byte is present but the unscaled value is
  // cut short (DECIMAL4/8/16 need 4/8/16 bytes past the scale byte).
  ExpectVariantError(empty_meta, {0x20, 0x02, 0x01});
  ExpectVariantError(empty_meta, {0x24, 0x02, 0x01, 0x02, 0x03, 0x04});
  ExpectVariantError(empty_meta,
      {0x28, 0x02, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08});
  // DATE (0x2c) whose day count (0x7FFFFFFF) is far outside the [0001-01-01, 9999-12-31]
  // range: structurally well-formed but unrepresentable, so it is rejected like any other
  // malformed value rather than silently emitting a placeholder string.
  ExpectVariantError(empty_meta, {0x2c, 0xFF, 0xFF, 0xFF, 0x7F});
  // DATE (0x2c) with only 1 payload byte (needs 4).
  ExpectVariantError(empty_meta, {0x2c, 0x01});
  // TIMESTAMPNTZ (0x34) with only 1 payload byte (needs 8).
  ExpectVariantError(empty_meta, {0x34, 0x01});
  // TIMESTAMPNTZ_NANOS (0x4c) with only 1 payload byte (needs 8).
  ExpectVariantError(empty_meta, {0x4c, 0x01});
}

// A decimal whose scale is exactly the max precision (the boundary of the valid range)
// must still serialize successfully.
TEST(VariantUtilTest, DecimalScaleBoundaryAccepted) {
  const vector<uint8_t> empty_meta = {0x01, 0x00, 0x00};
  string json;
  // DECIMAL4 with scale=9 (== MAX_DECIMAL4_PRECISION), unscaled=0.
  vector<uint8_t> dec4 = {0x20, 0x09, 0x00, 0x00, 0x00, 0x00};
  EXPECT_OK(VariantToJson(empty_meta.data(), empty_meta.size(),
      dec4.data(), dec4.size(), &json));
}

// Builds a value that wraps 'inner' in 'depth' nested single-element arrays. Uses 2-byte
// offsets so the growing child sizes stay representable as the nesting deepens.
static vector<uint8_t> WrapInArrays(vector<uint8_t> inner, int depth) {
  for (int i = 0; i < depth; ++i) {
    uint32_t child_size = inner.size();
    // header 0x07 = ARRAY with 2-byte offsets; num_elems=1; offsets [0, child_size].
    vector<uint8_t> a = {0x07, 0x01, 0x00, 0x00,
        static_cast<uint8_t>(child_size & 0xFF),
        static_cast<uint8_t>((child_size >> 8) & 0xFF)};
    a.insert(a.end(), inner.begin(), inner.end());
    inner.swap(a);
  }
  return inner;
}

TEST(VariantUtilTest, NestingDepthLimit) {
  const vector<uint8_t> empty_meta = {0x01, 0x00, 0x00};

  // A structurally valid but excessively deep value must be rejected (not overflow the
  // stack) when serialized.
  ExpectVariantError(empty_meta, WrapInArrays({0x00}, 105));

  // A moderately nested value within the cap serializes fine.
  vector<uint8_t> ok = WrapInArrays({0x00}, 50);
  string json;
  EXPECT_OK(VariantToJson(empty_meta.data(), empty_meta.size(),
      ok.data(), ok.size(), &json));
}

// --- Values stored out of field name order (IMPALA-15460) ---

// Parses space-separated hex bytes, e.g. "02 01 00".
static vector<uint8_t> FromHex(const string& hex) {
  vector<uint8_t> buf;
  std::istringstream in(hex);
  string byte;
  while (in >> byte) buf.push_back(static_cast<uint8_t>(std::stoul(byte, nullptr, 16)));
  return buf;
}

static vector<uint8_t> Concat(const vector<vector<uint8_t>>& parts) {
  vector<uint8_t> buf;
  for (const vector<uint8_t>& p : parts) buf.insert(buf.end(), p.begin(), p.end());
  return buf;
}

// Expects 'path' to resolve in 'v' to an INT8 of 'expected' that is exactly 2 bytes long.
static void ExpectInt8AtPath(const VariantValue& v, const string& path, int8_t expected) {
  VariantValue result;
  ASSERT_EQ(FOUND, v.NavigatePath(path, &result)) << path;
  EXPECT_EQ(2, result.Len()) << path;
  int8_t val = 0;
  ASSERT_TRUE(result.GetInt8(&val)) << path;
  EXPECT_EQ(expected, val) << path;
}

TEST(VariantUtilTest, SparkVariants) {
  // Values written by Spark 4.1.3. Spark lists the field ids and offsets in field name
  // order, but stores the values in document order (parse_json) or in struct field / map
  // entry order (to_variant_object), so the offsets are not monotonic.
  struct TestCase {
    string metadata;
    string value;
    string expected_json;
  };
  vector<TestCase> test_cases = {
    // {"b":2,"a":1}
    {"01 02 00 01 02 62 61",
     "02 02 01 00 02 00 04 0c 02 0c 01",
     R"V({"a":1,"b":2})V"},
    // {"a":1,"c":3,"b":2}
    {"01 03 00 01 02 03 61 63 62",
     "02 03 00 02 01 00 04 02 06 0c 01 0c 03 0c 02",
     R"V({"a":1,"b":2,"c":3})V"},
    // {"a":{"y":1,"x":2},"b":0}
    {"01 04 00 01 02 03 04 61 79 78 62",
     "02 02 00 03 00 0b 0d 02 02 02 01 02 00 04 0c 01 0c 02 0c 00",
     R"V({"a":{"x":2,"y":1},"b":0})V"},
    // {"c":3,"b":2,"a":1}, the example of the spec.
    {"01 03 00 01 02 03 63 62 61",
     "02 03 02 01 00 04 02 00 06 0c 03 0c 02 0c 01",
     R"V({"a":1,"b":2,"c":3})V"},
    // {"a":1,"B":2}: "B" sorts before "a" in byte order.
    {"01 02 00 01 02 61 42",
     "02 02 01 00 02 00 04 0c 01 0c 02",
     R"V({"B":2,"a":1})V"},
    // {"o":{"y":1,"x":2}}
    {"01 03 00 01 02 03 6f 79 78",
     "02 01 00 00 0b 02 02 02 01 02 00 04 0c 01 0c 02",
     R"V({"o":{"x":2,"y":1}})V"},
    // {"b":{"x":1,"y":2},"a":0}
    {"01 04 00 01 02 03 04 62 78 79 61",
     "02 02 03 00 0b 00 0d 02 02 01 02 00 02 04 0c 01 0c 02 0c 00",
     R"V({"a":0,"b":{"x":1,"y":2}})V"},
    // {"arr":[{"b":1,"a":2},{"d":3,"c":4}]}
    {"01 05 00 03 04 05 06 07 61 72 72 62 61 64 63",
     "02 01 00 00 1b 03 02 00 0b 16 02 02 02 01 02 00 04 0c 01 0c 02 02 02 04 03 02 00 "
     "04 0c 03 0c 04",
     R"V({"arr":[{"a":2,"b":1},{"c":4,"d":3}]})V"},
    // [{"b":1,"a":2}]
    {"01 02 00 01 02 62 61",
     "03 01 00 0b 02 02 01 00 02 00 04 0c 01 0c 02",
     R"V([{"a":2,"b":1}])V"},
    // {"a":1,"b":2,"c":3}, sorted keys (control)
    {"01 03 00 01 02 03 61 62 63",
     "02 03 00 01 02 00 02 04 06 0c 01 0c 02 0c 03",
     R"V({"a":1,"b":2,"c":3})V"},
    // [{"a":1,"b":2},{"c":3}], sorted keys (control)
    {"01 03 00 01 02 03 61 62 63",
     "03 02 00 0b 12 02 02 00 01 00 02 04 0c 01 0c 02 02 01 02 00 02 0c 03",
     R"V([{"a":1,"b":2},{"c":3}])V"},
    // {"b":2,"a":1} with 4-byte offsets, crafted with Spark's VariantVal.
    {"01 02 00 01 02 62 61",
     "0e 02 01 00 02 00 00 00 00 00 00 00 04 00 00 00 0c 02 0c 01",
     R"V({"a":1,"b":2})V"},
  };
  for (int i = 0; i < test_cases.size(); ++i) {
    const TestCase& tc = test_cases[i];
    vector<uint8_t> metadata = FromHex(tc.metadata);
    vector<uint8_t> value = FromHex(tc.value);
    string json;
    EXPECT_OK(VariantToJson(metadata.data(), metadata.size(), value.data(), value.size(),
        &json));
    EXPECT_EQ(tc.expected_json, json) << "Mismatch on test case " << i;
  }

  // {"b":"x...x","a":1} with a long string: 2-byte offsets for 300 characters and
  // 3-byte offsets for 70000 characters. The object headers are the ones Spark wrote.
  vector<uint8_t> metadata = FromHex("01 02 00 01 02 62 61");
  for (int len : {300, 70000}) {
    string header_hex = len == 300 ? "06 02 01 00 31 01 00 00 33 01"
                                   : "0a 02 01 00 75 11 01 00 00 00 77 11 01";
    vector<uint8_t> value = Concat({FromHex(header_hex),
        BuildLongString(string(len, 'x')), FromHex("0c 01")});
    string json;
    EXPECT_OK(VariantToJson(metadata.data(), metadata.size(), value.data(), value.size(),
        &json));
    EXPECT_EQ(R"V({"a":1,"b":")V" + string(len, 'x') + R"V("})V", json) << len;
  }
}

TEST(VariantUtilTest, OutOfOrderFieldLookup) {
  // {"b":2,"a":1}: the field ids are [a, b] and the offsets [2, 0, 4].
  vector<uint8_t> meta = FromHex("01 02 00 01 02 62 61");
  VariantMetadata metadata;
  ASSERT_OK(metadata.Init(meta.data(), meta.size()));
  vector<uint8_t> obj = FromHex("02 02 01 00 02 00 04 0c 02 0c 01");
  VariantValue v(obj.data(), obj.size(), &metadata);
  VariantValue result;
  ASSERT_EQ(FOUND, v.GetFieldByName("a", &result));
  EXPECT_EQ(2, result.Len());
  ExpectInt8AtPath(v, "$.a", 1);
  ExpectInt8AtPath(v, "$.b", 2);
  EXPECT_EQ(NOT_FOUND, v.GetFieldByName("c", &result));
  EXPECT_EQ(NOT_FOUND, v.NavigatePath("$[0]", &result));
  EXPECT_EQ(NOT_FOUND, v.NavigatePath("$.a.x", &result));

  // {"a":1,"c":3,"b":2}: 'a' must be an exact 2-byte slice, not the 4 bytes up to the
  // next offset in name order.
  vector<uint8_t> meta3 = FromHex("01 03 00 01 02 03 61 63 62");
  VariantMetadata metadata3;
  ASSERT_OK(metadata3.Init(meta3.data(), meta3.size()));
  vector<uint8_t> obj3 = FromHex("02 03 00 02 01 00 04 02 06 0c 01 0c 03 0c 02");
  VariantValue v3(obj3.data(), obj3.size(), &metadata3);
  ExpectInt8AtPath(v3, "$.a", 1);
  ExpectInt8AtPath(v3, "$.b", 2);
  ExpectInt8AtPath(v3, "$.c", 3);

  // {"a":{"y":1,"x":2},"b":0}
  vector<uint8_t> meta4 = FromHex("01 04 00 01 02 03 04 61 79 78 62");
  VariantMetadata metadata4;
  ASSERT_OK(metadata4.Init(meta4.data(), meta4.size()));
  vector<uint8_t> obj4 =
      FromHex("02 02 00 03 00 0b 0d 02 02 02 01 02 00 04 0c 01 0c 02 0c 00");
  VariantValue v4(obj4.data(), obj4.size(), &metadata4);
  ExpectInt8AtPath(v4, "$.a.x", 2);
  ExpectInt8AtPath(v4, "$.a.y", 1);
  ExpectInt8AtPath(v4, "$.b", 0);
  ASSERT_EQ(FOUND, v4.NavigatePath("$.a", &result));
  EXPECT_EQ(11, result.Len());
}

TEST(VariantUtilTest, OutOfOrderObjectPermutations) {
  // {"a":"hello","b":7,"c":null,"d":10000000000}: values of 6, 2, 1 and 9 bytes, stored
  // in every possible order.
  vector<uint8_t> meta = BuildMetadata({"a", "b", "c", "d"});
  VariantMetadata metadata;
  ASSERT_OK(metadata.Init(meta.data(), meta.size()));
  vector<vector<uint8_t>> values = {BuildShortString("hello"), FromHex("0c 07"),
      BuildNull(), FromHex("18 00 e4 0b 54 02 00 00 00")};
  vector<int> order = {0, 1, 2, 3};
  int num_orders = 0;
  do {
    SCOPED_TRACE(Substitute("storage order $0 $1 $2 $3", order[0], order[1], order[2],
        order[3]));
    vector<uint8_t> obj = BuildObject({0, 1, 2, 3}, values, order);
    VariantValue v(obj.data(), obj.size(), &metadata);
    VariantValue result;
    StringValue sv;
    ASSERT_EQ(FOUND, v.GetFieldByName("a", &result));
    EXPECT_EQ(6, result.Len());
    ASSERT_TRUE(result.GetString(&sv));
    EXPECT_EQ("hello", string(sv.Ptr(), sv.Len()));
    ExpectInt8AtPath(v, "$.b", 7);
    ASSERT_EQ(FOUND, v.GetFieldByName("c", &result));
    EXPECT_EQ(1, result.Len());
    EXPECT_TRUE(result.IsNull());
    ASSERT_EQ(FOUND, v.GetFieldByName("d", &result));
    EXPECT_EQ(9, result.Len());
    int64_t i64 = 0;
    ASSERT_TRUE(result.GetInt64(&i64));
    EXPECT_EQ(10000000000LL, i64);
    string json;
    EXPECT_OK(v.ToJson(&json));
    EXPECT_EQ(R"V({"a":"hello","b":7,"c":null,"d":10000000000})V", json);
    ++num_orders;
  } while (std::next_permutation(order.begin(), order.end()));
  EXPECT_EQ(24, num_orders);
}

TEST(VariantUtilTest, OutOfOrderWidths) {
  // {"c":3,"b":2,"a":1} with every field id and offset width, with and without is_large.
  vector<uint8_t> meta = BuildMetadata({"a", "b", "c"});
  VariantMetadata metadata;
  ASSERT_OK(metadata.Init(meta.data(), meta.size()));
  for (int offset_size = 1; offset_size <= 4; ++offset_size) {
    for (int id_size = 1; id_size <= 4; ++id_size) {
      for (bool is_large : {false, true}) {
        SCOPED_TRACE(Substitute("offset_size=$0 id_size=$1 is_large=$2", offset_size,
            id_size, is_large));
        vector<uint8_t> obj = BuildObject({0, 1, 2},
            {BuildInt32(1), BuildInt32(2), BuildInt32(3)}, /*storage_order=*/{2, 1, 0},
            VariantWidths{id_size, offset_size, is_large});
        VariantValue v(obj.data(), obj.size(), &metadata);
        for (int i = 0; i < 3; ++i) {
          VariantValue result;
          ASSERT_EQ(FOUND, v.GetFieldByName(string(1, 'a' + i), &result));
          EXPECT_EQ(5, result.Len());
          int32_t val = 0;
          ASSERT_TRUE(result.GetInt32(&val));
          EXPECT_EQ(i + 1, val);
        }
        string json;
        EXPECT_OK(v.ToJson(&json));
        EXPECT_EQ(R"V({"a":1,"b":2,"c":3})V", json);
      }
    }
  }
}

TEST(VariantUtilTest, LargeObjectOutOfOrder) {
  // 300 fields f000..f299, stored in reverse name order. The field ids and offsets need
  // 2 bytes, and the field count is 4 bytes (is_large).
  constexpr int NUM_FIELDS = 300;
  vector<string> names;
  vector<int> field_ids;
  vector<vector<uint8_t>> values;
  vector<int> storage_order;
  string expected_json = "{";
  for (int i = 0; i < NUM_FIELDS; ++i) {
    char name[16];
    snprintf(name, sizeof(name), "f%03d", i);
    names.push_back(name);
    field_ids.push_back(i);
    values.push_back(BuildInt32(i));
    storage_order.push_back(NUM_FIELDS - 1 - i);
    expected_json += Substitute("$0\"$1\":$2", i > 0 ? "," : "", name, i);
  }
  expected_json += "}";
  vector<uint8_t> meta = BuildMetadata(names, /*sorted=*/true, /*offset_size=*/2);
  VariantMetadata metadata;
  ASSERT_OK(metadata.Init(meta.data(), meta.size()));
  vector<uint8_t> obj = BuildObject(field_ids, values, storage_order,
      VariantWidths{/*id_size=*/2, /*offset_size=*/2, /*is_large=*/true});
  VariantValue v(obj.data(), obj.size(), &metadata);
  VariantValue result;
  for (int i : {0, 150, 299}) {
    ASSERT_EQ(FOUND, v.GetFieldByName(names[i], &result)) << i;
    int32_t val = -1;
    ASSERT_TRUE(result.GetInt32(&val));
    EXPECT_EQ(i, val);
  }
  EXPECT_EQ(NOT_FOUND, v.GetFieldByName("f300", &result));
  string json;
  EXPECT_OK(v.ToJson(&json));
  EXPECT_EQ(expected_json, json);
  EXPECT_EQ(3191, json.size());
}

TEST(VariantUtilTest, OutOfOrderArray) {
  vector<uint8_t> meta = BuildMetadata({});
  VariantMetadata metadata;
  ASSERT_OK(metadata.Init(meta.data(), meta.size()));
  // [5,6] with the elements stored as 6, 5: the offsets are [2, 0, 4]. The spec only
  // says where each element starts.
  vector<uint8_t> arr = FromHex("03 02 02 00 04 0c 06 0c 05");
  VariantValue v(arr.data(), arr.size(), &metadata);
  ExpectInt8AtPath(v, "$[0]", 5);
  ExpectInt8AtPath(v, "$[1]", 6);
  VariantValue result;
  ASSERT_EQ(FOUND, v.GetArrayElement(0, &result));
  EXPECT_EQ(2, result.Len());
  EXPECT_EQ(NOT_FOUND, v.GetArrayElement(2, &result));
  string json;
  EXPECT_OK(v.ToJson(&json));
  EXPECT_EQ("[5,6]", json);

  // ["hi",7,null] stored as null, "hi", 7, with 1-byte offsets and with 2-byte offsets
  // and a 4-byte element count.
  for (const VariantWidths& w : {VariantWidths(), VariantWidths{1, 2, true}}) {
    SCOPED_TRACE(Substitute("offset_size=$0 is_large=$1", w.offset_size, w.is_large));
    vector<uint8_t> arr2 = BuildArray(
        {BuildShortString("hi"), BuildInt32(7), BuildNull()}, /*storage_order=*/{2, 0, 1},
        w);
    VariantValue v2(arr2.data(), arr2.size(), &metadata);
    StringValue sv;
    ASSERT_EQ(FOUND, v2.GetArrayElement(0, &result));
    ASSERT_TRUE(result.GetString(&sv));
    EXPECT_EQ("hi", string(sv.Ptr(), sv.Len()));
    ASSERT_EQ(FOUND, v2.GetArrayElement(1, &result));
    int32_t val = 0;
    ASSERT_TRUE(result.GetInt32(&val));
    EXPECT_EQ(7, val);
    ASSERT_EQ(FOUND, v2.GetArrayElement(2, &result));
    EXPECT_TRUE(result.IsNull());
    EXPECT_EQ(NOT_FOUND, v2.GetArrayElement(3, &result));
    EXPECT_OK(v2.ToJson(&json));
    EXPECT_EQ(R"V(["hi",7,null])V", json);
  }
}

TEST(VariantUtilTest, DuplicateDictionaryStrings) {
  // An unsorted dictionary may hold a string more than once, and an object may use any
  // of its ids: here ["x", "a", "a"], and {"a":7} with field id 2 or 1.
  vector<uint8_t> meta = FromHex("01 03 00 01 02 03 78 61 61");
  VariantMetadata metadata;
  ASSERT_OK(metadata.Init(meta.data(), meta.size()));
  EXPECT_FALSE(metadata.IsSorted());
  for (const char* value_hex : {"02 01 02 00 02 0c 07", "02 01 01 00 02 0c 07"}) {
    SCOPED_TRACE(value_hex);
    vector<uint8_t> obj = FromHex(value_hex);
    VariantValue v(obj.data(), obj.size(), &metadata);
    VariantValue result;
    ASSERT_EQ(FOUND, v.GetFieldByName("a", &result));
    ExpectInt8AtPath(v, "$.a", 7);
    EXPECT_EQ(NOT_FOUND, v.GetFieldByName("x", &result));
    string json;
    EXPECT_OK(v.ToJson(&json));
    EXPECT_EQ(R"V({"a":7})V", json);
  }
}

TEST(VariantUtilTest, Utf16KeyOrder) {
  // Spark 4.1.3 wrote {"！":2,"\U0001F600":1} with the field ids in UTF-16 code unit
  // order (SPARK-58949), [U+1F600, U+FF01]: the surrogate pair of U+1F600 (D83D DE00)
  // sorts before U+FF01, although its UTF-8 bytes (F0 ...) sort after those of U+FF01
  // (EF ...).
  const string FULLWIDTH_EXCLAMATION_MARK = "\xEF\xBC\x81";
  const string GRINNING_FACE = "\xF0\x9F\x98\x80";
  vector<uint8_t> meta = FromHex("01 02 00 03 07 ef bc 81 f0 9f 98 80");
  VariantMetadata metadata;
  ASSERT_OK(metadata.Init(meta.data(), meta.size()));
  vector<uint8_t> obj = FromHex("02 02 01 00 02 00 04 0c 02 0c 01");
  VariantValue v(obj.data(), obj.size(), &metadata);
  ExpectInt8AtPath(v, "$." + FULLWIDTH_EXCLAMATION_MARK, 2);
  ExpectInt8AtPath(v, "$." + GRINNING_FACE, 1);
  string json;
  EXPECT_OK(v.ToJson(&json));
  EXPECT_EQ(
      "{\"" + GRINNING_FACE + "\":1,\"" + FULLWIDTH_EXCLAMATION_MARK + "\":2}", json);
}

TEST(VariantUtilTest, LargeObjectKeyOrder) {
  // 40 fields, so lookups use binary search. U+FF01 (EF BC 81) sorts before U+1F600
  // (F0 9F 98 80) in UTF-8 and after it in UTF-16 (SPARK-58949), so the field ids in
  // UTF-16 order are not in byte order at positions 20-21, where every search starts,
  // and at the last two. The names without a byte >= 0xEE compare the same in both
  // orders, so the search finds them, or finds that they are missing, without the
  // fallback scan. "é" (C3 A9) sorts after the ASCII names only in unsigned byte order.
  const string FULLWIDTH_EXCLAMATION_MARK = "\xEF\xBC\x81";
  const string GRINNING_FACE = "\xF0\x9F\x98\x80";
  vector<string> names;
  for (int i = 0; i < 20; ++i) names.push_back(Substitute("k$0$1", i / 10, i % 10));
  names.push_back("m" + FULLWIDTH_EXCLAMATION_MARK);
  names.push_back("m" + GRINNING_FACE);
  for (int i = 0; i < 15; ++i) names.push_back(Substitute("n$0$1", i / 10, i % 10));
  names.push_back("\xC3\xA9");
  names.push_back(FULLWIDTH_EXCLAMATION_MARK);
  names.push_back(GRINNING_FACE);
  const int num_fields = names.size();
  // The dictionary is in reverse order, so that the field ids decrease.
  vector<string> dict(names.rbegin(), names.rend());
  vector<uint8_t> meta = BuildMetadata(dict, /*sorted=*/false);
  VariantMetadata metadata;
  ASSERT_OK(metadata.Init(meta.data(), meta.size()));
  vector<int> utf8_ids;
  vector<vector<uint8_t>> utf8_values;
  for (int i = 0; i < num_fields; ++i) {
    utf8_ids.push_back(num_fields - 1 - i);
    utf8_values.push_back(BuildInt32(i));
  }
  vector<int> utf16_ids = utf8_ids;
  vector<vector<uint8_t>> utf16_values = utf8_values;
  for (int i : {20, num_fields - 2}) {
    std::swap(utf16_ids[i], utf16_ids[i + 1]);
    std::swap(utf16_values[i], utf16_values[i + 1]);
  }
  for (bool utf16 : {false, true}) {
    SCOPED_TRACE(utf16 ? "UTF-16 order" : "UTF-8 order");
    vector<uint8_t> obj = utf16 ? BuildObject(utf16_ids, utf16_values)
                                : BuildObject(utf8_ids, utf8_values);
    VariantValue v(obj.data(), obj.size(), &metadata);
    VariantValue result;
    for (int i = 0; i < num_fields; ++i) {
      ASSERT_EQ(FOUND, v.GetFieldByName(names[i], &result)) << i;
      int32_t val = -1;
      ASSERT_TRUE(result.GetInt32(&val));
      EXPECT_EQ(i, val);
    }
    for (const string& missing : {string(""), string("a"), string("k"), string("m"),
             string("mz"), string("n"), string("n150"), string("z"),
             string("m\xEF\xBC\x82"), string("\xEF\xBC\x82"),
             string("\xF4\x8F\xBF\xBF")}) {
      EXPECT_EQ(NOT_FOUND, v.GetFieldByName(missing, &result)) << missing;
    }
  }

  // A field id outside the dictionary is corrupt when the search compares its name.
  vector<int> bad_ids = utf8_ids;
  bad_ids[num_fields / 2] = num_fields;
  vector<uint8_t> obj = BuildObject(bad_ids, utf8_values);
  VariantValue v(obj.data(), obj.size(), &metadata);
  VariantValue result;
  EXPECT_EQ(CORRUPT, v.GetFieldByName("k00", &result));
}

TEST(VariantUtilTest, ValueSize) {
  uint32_t size = 0;
  // Every primitive type id except the strings, with its encoded size.
  const vector<pair<int, uint32_t>> primitive_sizes = {{0, 1}, {1, 1}, {2, 1}, {3, 2},
      {4, 3}, {5, 5}, {6, 9}, {7, 9}, {8, 6}, {9, 10}, {10, 18}, {11, 5}, {12, 9},
      {13, 9}, {14, 5}, {17, 9}, {18, 9}, {19, 9}, {20, 17}};
  for (const auto& p : primitive_sizes) {
    SCOPED_TRACE(p.first);
    vector<uint8_t> buf(p.second + 3, 0);
    buf[0] = p.first << 2;
    ASSERT_TRUE(VariantValue::ValueSize(buf.data(), buf.size(), &size));
    EXPECT_EQ(p.second, size);
    EXPECT_FALSE(VariantValue::ValueSize(buf.data(), p.second - 1, &size));
  }
  // BINARY and long STRING: 5 + length.
  for (const char* hex : {"3c 03 00 00 00 61 62 63", "40 03 00 00 00 61 62 63"}) {
    SCOPED_TRACE(hex);
    vector<uint8_t> buf = FromHex(hex);
    ASSERT_TRUE(VariantValue::ValueSize(buf.data(), buf.size(), &size));
    EXPECT_EQ(8, size);
    EXPECT_FALSE(VariantValue::ValueSize(buf.data(), 7, &size));
    // Truncated length.
    EXPECT_FALSE(VariantValue::ValueSize(buf.data(), 4, &size));
  }
  // Lengths for which 5 + length would wrap around in 32 bits.
  vector<uint8_t> wrap = FromHex("40 FC FF FF FF");
  wrap.resize(21, 0);
  EXPECT_FALSE(VariantValue::ValueSize(wrap.data(), wrap.size(), &size));
  wrap = FromHex("3c FF FF FF FF");
  EXPECT_FALSE(VariantValue::ValueSize(wrap.data(), wrap.size(), &size));
  // Short strings: 1 + length.
  vector<uint8_t> hello = FromHex("15 68 65 6c 6c 6f");
  ASSERT_TRUE(VariantValue::ValueSize(hello.data(), hello.size(), &size));
  EXPECT_EQ(6, size);
  EXPECT_FALSE(VariantValue::ValueSize(hello.data(), 5, &size));
  vector<uint8_t> empty_string = {0x01};
  ASSERT_TRUE(VariantValue::ValueSize(empty_string.data(), empty_string.size(), &size));
  EXPECT_EQ(1, size);
  // Unknown primitive type ids, and no bytes at all.
  for (int id = 21; id < 64; ++id) {
    vector<uint8_t> buf(32, 0);
    buf[0] = id << 2;
    EXPECT_FALSE(VariantValue::ValueSize(buf.data(), buf.size(), &size)) << id;
  }
  EXPECT_FALSE(VariantValue::ValueSize(hello.data(), 0, &size));
  // Objects and arrays: header, field ids, offsets and the value data, whose size is the
  // last offset. Trailing bytes are not part of the value.
  struct Container {
    const char* hex;
    uint32_t size;
  };
  const vector<Container> containers = {
      {"02 02 01 00 02 00 04 0c 02 0c 01 ff ff ff", 11},
      {"03 02 02 00 04 0c 06 0c 05 ff ff", 9},
      {"02 00 00", 3},
      {"03 00 00", 3},
      {"0e 02 01 00 02 00 00 00 00 00 00 00 04 00 00 00 0c 02 0c 01", 20}};
  for (const Container& c : containers) {
    SCOPED_TRACE(c.hex);
    vector<uint8_t> buf = FromHex(c.hex);
    ASSERT_TRUE(VariantValue::ValueSize(buf.data(), buf.size(), &size));
    EXPECT_EQ(c.size, size);
  }
  // An is_large object whose field count does not fit, and an object whose last offset
  // is past the end of the buffer.
  for (const char* hex : {"42 FF FF FF FF", "02 01 00 00 06 0c 01"}) {
    vector<uint8_t> buf = FromHex(hex);
    EXPECT_FALSE(VariantValue::ValueSize(buf.data(), buf.size(), &size)) << hex;
  }
}

TEST(VariantUtilTest, LookupResults) {
  // NOT_FOUND: the path does not match the value, callers return SQL NULL.
  // CORRUPT: the value on the path cannot be read, callers raise an error.
  vector<uint8_t> meta = BuildMetadata({"a"});
  VariantMetadata metadata;
  ASSERT_OK(metadata.Init(meta.data(), meta.size()));
  struct Case {
    const char* value_hex;
    const char* path;
    VariantLookupResult expected;
  };
  const char* OBJ = "02 01 00 00 02 0c 01";  // {"a":1}
  const char* ARR = "03 01 00 02 0c 01";  // [1]
  const char* ARR_TRUNCATED = "03 01 00 01 14";  // [<INT32 without a payload>]
  const char* NESTED_TRUNCATED = "02 01 00 00 03 02 64 00";  // {"a":<truncated object>}
  // {"a":{"a":<INT32 without a payload>}}: the inner object fits in the outer one, but
  // its field does not fit in the inner object.
  const char* NESTED_BAD_FIELD = "02 01 00 00 07 02 01 00 00 02 14 01";
  const vector<Case> cases = {
    {OBJ, "$", FOUND},
    {OBJ, "$.a", FOUND},
    {OBJ, "$.b", NOT_FOUND},
    // A field of an INT8, and an element of an object.
    {OBJ, "$.a.b", NOT_FOUND},
    {OBJ, "$[0]", NOT_FOUND},
    // Malformed paths.
    {OBJ, "", NOT_FOUND},
    {OBJ, "@", NOT_FOUND},
    {OBJ, ".", NOT_FOUND},
    {OBJ, "..", NOT_FOUND},
    {OBJ, "$.", NOT_FOUND},
    {OBJ, ".$", NOT_FOUND},
    {OBJ, "$..", NOT_FOUND},
    {OBJ, "$..a", NOT_FOUND},
    {OBJ, "$a", NOT_FOUND},
    {ARR, "$[]", NOT_FOUND},
    {ARR, "$]0[", NOT_FOUND},
    {ARR, "$[0", NOT_FOUND},
    {ARR, "$[x]", NOT_FOUND},
    {ARR, "$[0]", FOUND},
    // A field of an array, and an index out of range.
    {ARR, "$.a", NOT_FOUND},
    {ARR_TRUNCATED, "$[1]", NOT_FOUND},
    {ARR_TRUNCATED, "$[0]", CORRUPT},
    // Field 'a' starts at the end of the value data (the last offset is 0).
    {"02 01 00 00 00", "$.a", CORRUPT},
    // An object with 100 fields in 3 bytes.
    {"02 64 00", "$.a", CORRUPT},
    // An INT32 field that runs past the last offset (2).
    {"02 01 00 00 02 14 01", "$.a", CORRUPT},
    // A field of an unknown primitive type (id 21).
    {"02 01 00 00 01 54", "$.a", CORRUPT},
    // Field id 5 is outside the 1-entry dictionary.
    {"02 01 05 00 02 0c 01", "$.a", CORRUPT},
    // An empty value.
    {"", "$.a", CORRUPT},
    {"", "$[0]", CORRUPT},
    {NESTED_TRUNCATED, "$.a", CORRUPT},
    {NESTED_TRUNCATED, "$.a.b", CORRUPT},
    {NESTED_BAD_FIELD, "$.a", FOUND},
    {NESTED_BAD_FIELD, "$.a.b", NOT_FOUND},
    {NESTED_BAD_FIELD, "$.a.a", CORRUPT},
    // The root is sized from its header too. Bytes after it (e.g. padding) are ignored.
    {"", "$", CORRUPT},
    {"14 01", "$", CORRUPT},
    {"54", "$", CORRUPT},
    {"54", "$.a", CORRUPT},
    {"02 64 00", "$", CORRUPT},
    {"0c 01 00 00", "$", FOUND},
    {"02 01 00 00 02 0c 01 00 00", "$.a", FOUND},
  };
  for (const Case& c : cases) {
    vector<uint8_t> value = FromHex(c.value_hex);
    VariantValue v(value.data(), value.size(), &metadata);
    VariantValue result;
    EXPECT_EQ(c.expected, v.NavigatePath(c.path, &result))
        << "value: '" << c.value_hex << "' path: '" << c.path << "'";
  }
}

}  // namespace impala
