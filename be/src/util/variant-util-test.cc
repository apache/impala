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

#include <cstring>
#include <vector>

#include "testutil/gtest-util.h"

#include "common/names.h"
#include "runtime/variant-value.h"

namespace impala {

// Helper to build a metadata blob with a sorted dictionary.
// Format: header(1) + dict_size(1) + offsets(dict_size+1) + string_data
static vector<uint8_t> BuildMetadata(const vector<string>& field_names) {
  vector<uint8_t> buf;
  // Header: version=1, sorted=1, offset_size=1 (offset_size_minus_one=0)
  uint8_t header = 0x01 | (1 << 4) | (0 << 6);
  buf.push_back(header);
  // Dictionary size (1 byte since offset_size=1)
  buf.push_back(static_cast<uint8_t>(field_names.size()));
  // Offsets: dict_size+1 entries
  uint32_t offset = 0;
  for (size_t i = 0; i <= field_names.size(); ++i) {
    buf.push_back(static_cast<uint8_t>(offset));
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

// Build a simple object with given field values.
// field_ids: indices into the metadata dictionary.
static vector<uint8_t> BuildObject(const vector<int>& field_ids,
    const vector<vector<uint8_t>>& values) {
  DCHECK_EQ(field_ids.size(), values.size());
  int num_fields = field_ids.size();

  // Compute value offsets.
  vector<uint32_t> offsets;
  uint32_t offset = 0;
  for (const auto& v : values) {
    offsets.push_back(offset);
    offset += v.size();
  }
  offsets.push_back(offset);  // final offset = total data size

  // Header: basic_type=2 (OBJECT), field_offset_size=1 (bits2-3=0),
  // field_id_size=1 (bits4-5=0), is_large=0 (bit6=0) -> header = 0x02
  uint8_t header = 0x02;
  vector<uint8_t> buf = {header};
  // Num fields (1 byte since not large)
  buf.push_back(static_cast<uint8_t>(num_fields));
  // Field IDs (1 byte each)
  for (int fid : field_ids) buf.push_back(static_cast<uint8_t>(fid));
  // Offsets (1 byte each, num_fields+1 entries)
  for (uint32_t o : offsets) buf.push_back(static_cast<uint8_t>(o));
  // Value data
  for (const auto& v : values) buf.insert(buf.end(), v.begin(), v.end());
  return buf;
}

// Build a simple array with given element values.
static vector<uint8_t> BuildArray(const vector<vector<uint8_t>>& elements) {
  int num_elements = elements.size();

  vector<uint32_t> offsets;
  uint32_t offset = 0;
  for (const auto& e : elements) {
    offsets.push_back(offset);
    offset += e.size();
  }
  offsets.push_back(offset);

  // Header: basic_type=3 (ARRAY), offset_size=1 (bits2-3=0), is_large=0 (bit4=0)
  // → header = 0x03
  uint8_t header = 0x03;
  vector<uint8_t> buf = {header};
  // Num elements (1 byte since not large)
  buf.push_back(static_cast<uint8_t>(num_elements));
  // Offsets (1 byte each)
  for (uint32_t o : offsets) buf.push_back(static_cast<uint8_t>(o));
  // Element data
  for (const auto& e : elements) buf.insert(buf.end(), e.begin(), e.end());
  return buf;
}

TEST(VariantUtilTest, MetadataParsing) {
  vector<string> names = {"age", "city", "name"};
  vector<uint8_t> meta_bytes = BuildMetadata(names);

  VariantMetadata metadata;
  ASSERT_OK(metadata.Init(meta_bytes.data(), meta_bytes.size()));
  EXPECT_EQ(3, metadata.DictionarySize());

  EXPECT_EQ(metadata.GetFieldName(0), "age");
  EXPECT_EQ(metadata.GetFieldName(1), "city");
  EXPECT_EQ(metadata.GetFieldName(2), "name");

  EXPECT_EQ(0, metadata.FindFieldId("age"));
  EXPECT_EQ(1, metadata.FindFieldId("city"));
  EXPECT_EQ(2, metadata.FindFieldId("name"));
  EXPECT_EQ(-1, metadata.FindFieldId("unknown"));
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
  EXPECT_TRUE(true_val.GetBoolean());

  vector<uint8_t> false_bytes = BuildBoolean(false);
  VariantValue false_val(false_bytes.data(), false_bytes.size(), &metadata);
  EXPECT_FALSE(false_val.GetBoolean());
}

TEST(VariantUtilTest, Int32Value) {
  vector<uint8_t> meta_bytes = BuildMetadata({});
  VariantMetadata metadata;
  ASSERT_OK(metadata.Init(meta_bytes.data(), meta_bytes.size()));

  vector<uint8_t> val_bytes = BuildInt32(42);
  VariantValue val(val_bytes.data(), val_bytes.size(), &metadata);
  EXPECT_EQ(val.GetBasicType(), VariantBasicType::PRIMITIVE);
  EXPECT_EQ(val.GetPhysicalType(), VariantPhysicalType::INT32);
  EXPECT_EQ(42, val.GetInt32());
}

TEST(VariantUtilTest, ShortString) {
  vector<uint8_t> meta_bytes = BuildMetadata({});
  VariantMetadata metadata;
  ASSERT_OK(metadata.Init(meta_bytes.data(), meta_bytes.size()));

  vector<uint8_t> val_bytes = BuildShortString("hello");
  VariantValue val(val_bytes.data(), val_bytes.size(), &metadata);
  EXPECT_EQ(val.GetBasicType(), VariantBasicType::SHORT_STRING);
  StringValue sv = val.GetString();
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
  EXPECT_EQ(2, obj.GetObjectSize());

  VariantValue field_val;
  EXPECT_TRUE(obj.GetFieldByName("age", &field_val));
  EXPECT_EQ(30, field_val.GetInt32());

  EXPECT_TRUE(obj.GetFieldByName("name", &field_val));
  StringValue sv = field_val.GetString();
  EXPECT_EQ(string(sv.Ptr(), sv.Len()), "Alice");

  EXPECT_FALSE(obj.GetFieldByName("unknown", &field_val));
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
  EXPECT_EQ(3, arr.GetArraySize());

  VariantValue elem;
  EXPECT_TRUE(arr.GetArrayElement(0, &elem));
  EXPECT_EQ(1, elem.GetInt32());
  EXPECT_TRUE(arr.GetArrayElement(2, &elem));
  EXPECT_EQ(3, elem.GetInt32());
  EXPECT_FALSE(arr.GetArrayElement(3, &elem));
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
  EXPECT_TRUE(root.NavigatePath("$.data.items[1]", &result));
  EXPECT_EQ(20, result.GetInt32());

  EXPECT_TRUE(root.NavigatePath("$.data.items[2]", &result));
  EXPECT_EQ(30, result.GetInt32());

  EXPECT_FALSE(root.NavigatePath("$.data.unknown", &result));
  EXPECT_FALSE(root.NavigatePath("$.data.items[100]", &result));
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
  EXPECT_FALSE(root.NavigatePath("", &result));
  EXPECT_TRUE(root.NavigatePath("$", &result));
  EXPECT_FALSE(root.NavigatePath("@", &result));
  EXPECT_FALSE(root.NavigatePath(".", &result));
  EXPECT_FALSE(root.NavigatePath("..", &result));
  EXPECT_FALSE(root.NavigatePath("$.", &result));
  EXPECT_FALSE(root.NavigatePath(".$", &result));
  EXPECT_FALSE(root.NavigatePath("$..", &result));
  EXPECT_FALSE(root.NavigatePath("$.context..page", &result));
  EXPECT_TRUE(root.NavigatePath("$.int_array[0]", &result));
  EXPECT_FALSE(root.NavigatePath("$.int_array[]", &result));
  EXPECT_FALSE(root.NavigatePath("$.int_array]100[", &result));
  EXPECT_FALSE(root.NavigatePath("$.int_array[100]", &result));
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
  vector<uint8_t> obj_bytes = BuildObject({1, 2, 0}, {age_val, name_val, active_val});

  string json;
  ASSERT_OK(VariantToJson(meta_bytes.data(), meta_bytes.size(),
      obj_bytes.data(), obj_bytes.size(), &json));
  // Field order in JSON follows the object's field order, not alphabetical.
  EXPECT_EQ(json, R"V({"age":30,"name":"Alice","active":true})V");
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
  vector<string> names = {s_backspace, s_ctrl, s_formfeed};
  vector<uint8_t> meta_bytes = BuildMetadata(names);
  VariantMetadata metadata;
  ASSERT_OK(metadata.Init(meta_bytes.data(), meta_bytes.size()));

  vector<uint8_t> val0 = BuildInt32(10);
  vector<uint8_t> val1 = BuildInt32(20);
  vector<uint8_t> val2 = BuildInt32(30);
  vector<uint8_t> obj_bytes = BuildObject({0, 1, 2}, {val0, val1, val2});

  string json;
  ASSERT_OK(VariantToJson(meta_bytes.data(), meta_bytes.size(),
      obj_bytes.data(), obj_bytes.size(), &json));
  EXPECT_EQ(json, "{\"a\\bb\":10,\"e\\u0001f\":20,\"c\\fd\":30}");
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
  EXPECT_EQ(2, obj.GetObjectSize());

  VariantValue field_val;
  EXPECT_TRUE(obj.GetFieldByName("f0", &field_val));
  EXPECT_EQ(42, field_val.GetInt32());

  EXPECT_TRUE(obj.GetFieldByName("f299", &field_val));
  EXPECT_EQ(7, field_val.GetInt32());

  EXPECT_FALSE(obj.GetFieldByName("f1", &field_val));
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
  StringValue sv = val.GetString();
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

}  // namespace impala
