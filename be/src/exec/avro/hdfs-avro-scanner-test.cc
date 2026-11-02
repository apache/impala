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

#include "exec/avro/hdfs-avro-scanner.h"

#include <algorithm>
#include <avro/errors.h>
#include <avro/schema.h>
#include <limits.h>

#include "exec/read-write-util.h"
#include "runtime/date-value.h"
#include "runtime/decimal-value.inline.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.inline.h"
#include "testutil/gtest-util.h"
#include "util/avro-util.h"
#include "util/uuid-util.h"

#include "common/names.h"

// TODO: IMPALA-3658: complete CHAR unit tests.
// TODO: IMPALA-3658: complete VARCHAR unit tests.

namespace impala {

class HdfsAvroScannerTest : public testing::Test {
 public:
  void TestReadUnionType(int null_union_position, uint8_t* data, int64_t data_len,
      bool expected_is_null, TErrorCode::type expected_error = TErrorCode::OK) {
    // Reset parse_status_
    scanner_.parse_status_ = Status::OK();

    uint8_t* new_data = data;
    bool is_null = -1;
    bool expect_success = expected_error == TErrorCode::OK;

    bool success = scanner_.ReadUnionType(null_union_position, &new_data, data + data_len,
        &is_null);
    EXPECT_EQ(success, expect_success);

    if (success) {
      EXPECT_TRUE(scanner_.parse_status_.ok());
      EXPECT_EQ(is_null, expected_is_null);
      EXPECT_EQ(new_data, data + 1);
    } else {
      EXPECT_EQ(scanner_.parse_status_.code(), expected_error);
    }
  }

  template<typename T>
  void CheckReadResult(T expected_val, int expected_encoded_len,
      TErrorCode::type expected_error, T val, bool success, int actual_encoded_len) {
    bool expect_success = expected_error == TErrorCode::OK;
    EXPECT_EQ(success, expect_success);

    if (success) {
      EXPECT_TRUE(scanner_.parse_status_.ok());
      EXPECT_EQ(val, expected_val);
      EXPECT_EQ(expected_encoded_len, actual_encoded_len);
    } else {
      EXPECT_EQ(scanner_.parse_status_.code(), expected_error);
    }
  }

  // Templated function for calling different ReadAvro* functions.
  //
  // PrimitiveType is a template parameter so we can pass in int 'slot_byte_size' to
  // ReadAvroDecimal, but otherwise this argument is always the PrimitiveType 'type'
  // argument.
  template<typename T, typename ReadAvroTypeFn, typename PrimitiveType>
  void TestReadAvroType(ReadAvroTypeFn read_fn, PrimitiveType type, uint8_t* data,
      int64_t data_len, T expected_val, int expected_encoded_len,
      TErrorCode::type expected_error = TErrorCode::OK) {
    // Reset parse_status_
    scanner_.parse_status_ = Status::OK();
    uint8_t* new_data = data;
    T slot;
    bool success = (scanner_.*read_fn)(type, &new_data, data + data_len, true, &slot,
        NULL);
    CheckReadResult(expected_val, expected_encoded_len, expected_error, slot, success,
        new_data - data);
  }

  void TestReadAvroBoolean(uint8_t* data, int64_t data_len, bool expected_val,
      TErrorCode::type expected_error = TErrorCode::OK) {
    TestReadAvroType(&HdfsAvroScanner::ReadAvroBoolean, TYPE_BOOLEAN, data, data_len,
        expected_val, 1, expected_error);
  }

  void TestReadAvroInt32(uint8_t* data, int64_t data_len, int32_t expected_val,
      int expected_encoded_len, TErrorCode::type expected_error = TErrorCode::OK) {
    TestReadAvroType(&HdfsAvroScanner::ReadAvroInt32, TYPE_INT, data, data_len,
        expected_val, expected_encoded_len, expected_error);
    // Test type promotion to long, float, and double
    int64_t expected_bigint = expected_val;
    TestReadAvroType(&HdfsAvroScanner::ReadAvroInt32, TYPE_BIGINT, data, data_len,
        expected_bigint, expected_encoded_len, expected_error);
    float expected_float = expected_val;
    TestReadAvroType(&HdfsAvroScanner::ReadAvroInt32, TYPE_FLOAT, data, data_len,
        expected_float, expected_encoded_len, expected_error);
    double expected_double = expected_val;
    TestReadAvroType(&HdfsAvroScanner::ReadAvroInt32, TYPE_DOUBLE, data, data_len,
        expected_double, expected_encoded_len, expected_error);
  }

  void TestReadAvroDate(uint8_t* data, int64_t data_len, const DateValue& expected_val,
      int expected_encoded_len, TErrorCode::type expected_error = TErrorCode::OK) {
    TestReadAvroType(&HdfsAvroScanner::ReadAvroDate, TYPE_DATE, data, data_len,
        expected_val, expected_encoded_len, expected_error);
  }

  void TestReadAvroInt64(uint8_t* data, int64_t data_len, int64_t expected_val,
      int expected_encoded_len, TErrorCode::type expected_error = TErrorCode::OK) {
    TestReadAvroType(&HdfsAvroScanner::ReadAvroInt64, TYPE_BIGINT, data, data_len,
        expected_val, expected_encoded_len, expected_error);
    // Test type promotion to float and double
    float expected_float = expected_val;
    TestReadAvroType(&HdfsAvroScanner::ReadAvroInt64, TYPE_FLOAT, data, data_len,
        expected_float, expected_encoded_len, expected_error);
    double expected_double = expected_val;
    TestReadAvroType(&HdfsAvroScanner::ReadAvroInt64, TYPE_DOUBLE, data, data_len,
        expected_double, expected_encoded_len, expected_error);
  }

  void TestReadAvroFloat(uint8_t* data, int64_t data_len, float expected_val,
      TErrorCode::type expected_error = TErrorCode::OK) {
    TestReadAvroType(&HdfsAvroScanner::ReadAvroFloat, TYPE_FLOAT, data, data_len,
        expected_val, 4, expected_error);
    // Test type promotion to double
    double expected_double = expected_val;
    TestReadAvroType(&HdfsAvroScanner::ReadAvroFloat, TYPE_DOUBLE, data, data_len,
        expected_double, 4, expected_error);
  }

  void TestReadAvroDouble(uint8_t* data, int64_t data_len, double expected_val,
      TErrorCode::type expected_error = TErrorCode::OK) {
    TestReadAvroType(&HdfsAvroScanner::ReadAvroDouble, TYPE_DOUBLE, data, data_len,
        expected_val, 8, expected_error);
  }

  void TestReadAvroChar(int max_len, uint8_t* data, int64_t data_len,
      const StringValue& expected_val, int expected_encoded_len,
      TErrorCode::type expected_error = TErrorCode::OK) {
    // Reset parse_status_
    scanner_.parse_status_ = Status::OK();
    uint8_t* new_data = data;
    char slot[max<int>(sizeof(StringValue), max_len)];
    bool success = scanner_.ReadAvroChar(TYPE_CHAR, max_len, &new_data, data + data_len,
        true, slot, NULL);
    // Convert to non-inlined string value for comparison.
    StringValue value(slot, max_len);
    CheckReadResult(expected_val, expected_encoded_len, expected_error, value, success,
        new_data - data);
  }

  void TestReadAvroVarchar(int max_len, uint8_t* data, int64_t data_len,
      const StringValue& expected_val, int expected_encoded_len,
      TErrorCode::type expected_error = TErrorCode::OK) {
    // Reset parse_status_
    scanner_.parse_status_ = Status::OK();

    uint8_t* new_data = data;
    StringValue slot;
    bool success = scanner_.ReadAvroVarchar(TYPE_VARCHAR, max_len, &new_data,
        data + data_len, true, &slot, NULL);
    CheckReadResult(expected_val, expected_encoded_len, expected_error, slot, success,
        new_data - data);
  }

  void TestReadAvroString(uint8_t* data, int64_t data_len,
      const StringValue& expected_val, int expected_encoded_len,
      TErrorCode::type expected_error = TErrorCode::OK) {
    TestReadAvroType(&HdfsAvroScanner::ReadAvroString, TYPE_STRING, data, data_len,
        expected_val, expected_encoded_len, expected_error);
  }

  // A UUID always occupies UUID_BYTE_LEN bytes, so a successful read must consume exactly
  // that many. A failed read must leave the slot and the input position unchanged.
  void TestReadAvroUuid(uint8_t* data, int64_t data_len, const uint8_t* expected_bytes,
      TErrorCode::type expected_error = TErrorCode::OK) {
    scanner_.parse_status_ = Status::OK();
    uint8_t* new_data = data;
    const uint8_t sentinel = 0xa5;
    uint8_t slot[UUID_BYTE_LEN];
    memset(slot, sentinel, sizeof(slot));
    bool success = scanner_.ReadAvroUuid(TYPE_UUID, &new_data, data + data_len, true,
        slot, NULL);
    bool expect_success = expected_error == TErrorCode::OK;
    EXPECT_EQ(success, expect_success);
    if (success) {
      EXPECT_TRUE(scanner_.parse_status_.ok());
      EXPECT_EQ(0, memcmp(slot, expected_bytes, UUID_BYTE_LEN));
      EXPECT_EQ(UUID_BYTE_LEN, new_data - data);
    } else {
      EXPECT_EQ(scanner_.parse_status_.code(), expected_error);
      EXPECT_EQ(data, new_data);
      for (int i = 0; i < UUID_BYTE_LEN; ++i) EXPECT_EQ(sentinel, slot[i]) << i;
    }
  }

  // Resolves a file field with Avro schema 'file_json' against a table field with Avro
  // schema 'table_json', as ResolveSchemas() does for every field.
  Status ResolveField(const string& table_json, const string& file_json) {
    ScopedAvroSchemaElement table_field;
    ScopedAvroSchemaElement file_field;
    RETURN_IF_ERROR(ParseAvroSchema(table_json, table_field.get()));
    RETURN_IF_ERROR(ParseAvroSchema(file_json, file_field.get()));
    return scanner_.VerifyTypesMatch(*table_field.get(), *file_field.get(), "c");
  }

  static Status ParseAvroSchema(const string& json, AvroSchemaElement* element) {
    avro_schema_t schema;
    if (avro_schema_from_json_length(json.c_str(), json.size(), &schema) != 0) {
      return Status(avro_strerror());
    }
    return AvroSchemaElement::ConvertSchema(schema, element);
  }

  template<typename T>
  void TestReadAvroDecimal(uint8_t* data, int64_t data_len, DecimalValue<T> expected_val,
      int expected_encoded_len, TErrorCode::type expected_error = TErrorCode::OK) {
    TestReadAvroType(&HdfsAvroScanner::ReadAvroDecimal, sizeof(expected_val), data,
        data_len, expected_val, expected_encoded_len, expected_error);
  }

  void TestInt64Val(int64_t val) {
    uint8_t data[100];
    int len = ReadWriteUtil::PutZLong(val, data);
    DCHECK_GT(len, 0);
    TestReadAvroInt64(data, len, val, len);
    TestReadAvroInt64(data, len + 1, val, len);
    TestReadAvroInt64(data, len - 1, -1, -1, TErrorCode::SCANNER_INVALID_INT);
  }

 protected:
  HdfsAvroScanner scanner_;
};

// Tests reading a [<some type>, "null"] union.
TEST_F(HdfsAvroScannerTest, NullUnionTest) {
  uint8_t data[100];
  data[0] = 0;
  TestReadUnionType(0, data, 1, true);
  TestReadUnionType(1, data, 1, false);
  TestReadUnionType(0, data, 10, true);
  TestReadUnionType(1, data, 10, false);
  TestReadUnionType(0, data, 0, false, TErrorCode::AVRO_TRUNCATED_BLOCK);

  data[0] = 2;
  TestReadUnionType(0, data, 1, false);
  TestReadUnionType(1, data, 1, true);
  TestReadUnionType(0, data, 10, false);
  TestReadUnionType(1, data, 10, true);
  TestReadUnionType(0, data, 0, false, TErrorCode::AVRO_TRUNCATED_BLOCK);

  data[0] = 1;
  TestReadUnionType(0, data, 0, false, TErrorCode::AVRO_TRUNCATED_BLOCK);
  TestReadUnionType(0, data, 1, false, TErrorCode::AVRO_INVALID_UNION);

  data[0] = -1;
  TestReadUnionType(0, data, 1, false, TErrorCode::AVRO_INVALID_UNION);
}

TEST_F(HdfsAvroScannerTest, BooleanTest) {
  uint8_t data[100];
  data[0] = 0;
  TestReadAvroBoolean(data, 1, false);
  TestReadAvroBoolean(data, 10, false);
  TestReadAvroBoolean(data, 0, false, TErrorCode::AVRO_TRUNCATED_BLOCK);

  data[0] = 1;
  TestReadAvroBoolean(data, 1, true);
  TestReadAvroBoolean(data, 10, true);
  TestReadAvroBoolean(data, 0, false, TErrorCode::AVRO_TRUNCATED_BLOCK);

  data[0] = 2;
  TestReadAvroBoolean(data, 1, false, TErrorCode::AVRO_INVALID_BOOLEAN);
}

TEST_F(HdfsAvroScannerTest, Int32Test) {
  uint8_t data[100];
  data[0] = 1; // decodes to -1
  TestReadAvroInt32(data, 1, -1, 1);
  TestReadAvroInt32(data, 10, -1, 1);
  TestReadAvroInt32(data, 0, -1, -1, TErrorCode::SCANNER_INVALID_INT);

  data[0] = 2; // decodes to 1
  TestReadAvroInt32(data, 1, 1, 1);
  TestReadAvroInt32(data, 10, 1, 1);
  TestReadAvroInt32(data, 0, -1, -1, TErrorCode::SCANNER_INVALID_INT);

  data[0] = 0x80; // decodes to 64
  data[1] = 0x01;
  TestReadAvroInt32(data, 2, 64, 2);
  TestReadAvroInt32(data, 10, 64, 2);
  TestReadAvroInt32(data, 0, -1, -1, TErrorCode::SCANNER_INVALID_INT);
  TestReadAvroInt32(data, 1, -1, -1, TErrorCode::SCANNER_INVALID_INT);

  int len = ReadWriteUtil::PutZInt(INT_MAX, data);
  TestReadAvroInt32(data, len, INT_MAX, len);
  TestReadAvroInt32(data, len + 1, INT_MAX, len);
  TestReadAvroInt32(data, len - 1, -1, -1, TErrorCode::SCANNER_INVALID_INT);

  len = ReadWriteUtil::PutZInt(INT_MIN, data);
  TestReadAvroInt32(data, len, INT_MIN, len);
  TestReadAvroInt32(data, len + 1, INT_MIN, len);
  TestReadAvroInt32(data, len - 1, -1, -1, TErrorCode::SCANNER_INVALID_INT);

  // TODO: we don't handle invalid values (e.g. overflow) (IMPALA-3659)
}

TEST_F(HdfsAvroScannerTest, DateTest) {
  uint8_t data[100];
  data[0] = 1; // decodes to -1 which corresponds to 1969-12-31.
  TestReadAvroDate(data, 1, DateValue(1969, 12, 31), 1);
  TestReadAvroDate(data, 10, DateValue(1969, 12, 31), 1);

  // Empty data.
  DateValue invalid_dv;
  TestReadAvroDate(data, 0, invalid_dv, -1, TErrorCode::SCANNER_INVALID_INT);

  data[0] = 2; // decodes to 1 which corresponds to 1970-01-02.
  TestReadAvroDate(data, 1, DateValue(1970, 1, 2), 1);
  TestReadAvroDate(data, 10, DateValue(1970, 1, 2), 1);
  TestReadAvroDate(data, 0, invalid_dv, -1, TErrorCode::SCANNER_INVALID_INT);

  data[0] = 0x80; // decodes to 64 which corresponds to 1970-03-06.
  data[1] = 0x01;
  TestReadAvroDate(data, 2, DateValue(1970, 3, 6), 2);
  TestReadAvroDate(data, 10, DateValue(1970, 3, 6), 2);
  TestReadAvroDate(data, 0, invalid_dv, -1, TErrorCode::SCANNER_INVALID_INT);
  TestReadAvroDate(data, 1, invalid_dv, -1, TErrorCode::SCANNER_INVALID_INT);

  // Test upper limit: 9999-12-31.
  const int32_t MAX_DATE_DAYS_SINCE_EPOCH = 2932896;
  int len = ReadWriteUtil::PutZInt(MAX_DATE_DAYS_SINCE_EPOCH, data);
  TestReadAvroDate(data, len, DateValue(9999, 12 ,31), len);
  TestReadAvroDate(data, len + 1, DateValue(9999, 12, 31), len);
  TestReadAvroDate(data, len - 1, invalid_dv, -1, TErrorCode::SCANNER_INVALID_INT);

  len = ReadWriteUtil::PutZInt(MAX_DATE_DAYS_SINCE_EPOCH + 1, data);
  TestReadAvroDate(data, len, invalid_dv, -1, TErrorCode::AVRO_INVALID_DATE);
  TestReadAvroDate(data, len + 1, invalid_dv, -1, TErrorCode::AVRO_INVALID_DATE);
  TestReadAvroDate(data, len - 1, invalid_dv, -1, TErrorCode::SCANNER_INVALID_INT);

  len = ReadWriteUtil::PutZInt(INT_MAX, data);
  TestReadAvroDate(data, len, invalid_dv, -1, TErrorCode::AVRO_INVALID_DATE);
  TestReadAvroDate(data, len + 1, invalid_dv, -1, TErrorCode::AVRO_INVALID_DATE);
  TestReadAvroDate(data, len - 1, invalid_dv, -1, TErrorCode::SCANNER_INVALID_INT);

  // Test lower limit: 0001-01-01.
  const int32_t MIN_DATE_DAYS_SINCE_EPOCH = -719162;
  len = ReadWriteUtil::PutZInt(MIN_DATE_DAYS_SINCE_EPOCH, data);
  TestReadAvroDate(data, len, DateValue(1, 1, 1), len);
  TestReadAvroDate(data, len + 1, DateValue(1, 1, 1), len);
  TestReadAvroDate(data, len - 1, invalid_dv, -1, TErrorCode::SCANNER_INVALID_INT);

  len = ReadWriteUtil::PutZInt(MIN_DATE_DAYS_SINCE_EPOCH - 1, data);
  TestReadAvroDate(data, len, invalid_dv, -1, TErrorCode::AVRO_INVALID_DATE);
  TestReadAvroDate(data, len + 1, invalid_dv, -1, TErrorCode::AVRO_INVALID_DATE);
  TestReadAvroDate(data, len - 1, invalid_dv, -1, TErrorCode::SCANNER_INVALID_INT);

  len = ReadWriteUtil::PutZInt(INT_MIN, data);
  TestReadAvroDate(data, len, invalid_dv, -1, TErrorCode::AVRO_INVALID_DATE);
  TestReadAvroDate(data, len + 1, invalid_dv, -1, TErrorCode::AVRO_INVALID_DATE);
  TestReadAvroDate(data, len - 1, invalid_dv, -1, TErrorCode::SCANNER_INVALID_INT);
}

TEST_F(HdfsAvroScannerTest, Int64Test) {
  uint8_t data[100];
  data[0] = 1; // decodes to -1
  TestReadAvroInt64(data, 1, -1, 1);
  TestReadAvroInt64(data, 10, -1, 1);
  TestReadAvroInt64(data, 0, -1, -1, TErrorCode::SCANNER_INVALID_INT);

  data[0] = 2; // decodes to 1
  TestReadAvroInt64(data, 1, 1, 1);
  TestReadAvroInt64(data, 10, 1, 1);
  TestReadAvroInt64(data, 0, -1, -1, TErrorCode::SCANNER_INVALID_INT);

  data[0] = 0x80; // decodes to 64
  data[1] = 0x01;
  TestReadAvroInt64(data, 2, 64, 2);
  TestReadAvroInt64(data, 10, 64, 2);
  TestReadAvroInt64(data, 0, -1, -1, TErrorCode::SCANNER_INVALID_INT);
  TestReadAvroInt64(data, 1, -1, -1, TErrorCode::SCANNER_INVALID_INT);

  TestInt64Val(INT_MAX);
  TestInt64Val(INT_MIN);
  TestInt64Val(LLONG_MAX);
  TestInt64Val(LLONG_MIN);

  // TODO: we don't handle invalid values (e.g. overflow) (IMPALA-3659)
}

TEST_F(HdfsAvroScannerTest, FloatTest) {
  uint8_t data[100];
  float f = 1.23456789;
  memcpy(data, &f, sizeof(float));
  TestReadAvroFloat(data, 4, f);
  TestReadAvroFloat(data, 10, f);
  TestReadAvroFloat(data, 0, f, TErrorCode::AVRO_TRUNCATED_BLOCK);
  TestReadAvroFloat(data, 1, f, TErrorCode::AVRO_TRUNCATED_BLOCK);
  TestReadAvroFloat(data, 2, f, TErrorCode::AVRO_TRUNCATED_BLOCK);
  TestReadAvroFloat(data, 3, f, TErrorCode::AVRO_TRUNCATED_BLOCK);
}

TEST_F(HdfsAvroScannerTest, DoubleTest) {
  uint8_t data[100];
  double d = 1.23456789012345;
  memcpy(data, &d, sizeof(double));
  TestReadAvroDouble(data, 8, d);
  TestReadAvroDouble(data, 10, d);
  TestReadAvroDouble(data, 0, d, TErrorCode::AVRO_TRUNCATED_BLOCK);
  TestReadAvroDouble(data, 1, d, TErrorCode::AVRO_TRUNCATED_BLOCK);
  TestReadAvroDouble(data, 2, d, TErrorCode::AVRO_TRUNCATED_BLOCK);
  TestReadAvroDouble(data, 3, d, TErrorCode::AVRO_TRUNCATED_BLOCK);
  TestReadAvroDouble(data, 7, d, TErrorCode::AVRO_TRUNCATED_BLOCK);
}

TEST_F(HdfsAvroScannerTest, StringLengthOverflowTest) {
  const char* s = "hello world";
  StringValue sv(s);
  // Test handling of strings that would overflow the StringValue::len 32-bit integer.
  // Test cases with 0 and 1 in the upper bit of the 32-bit integer.
  vector<int64_t> large_string_lens({0x1FFFFFFFF, 0x0FFFFFFFF, 0x100000000});
  for (int64_t large_string_len: large_string_lens) {
    LOG(INFO) << "Testing large string of length " << large_string_len;
    vector<uint8_t> large_buffer(large_string_len + ReadWriteUtil::MAX_ZLONG_LEN);
    int64_t zlong_len = ReadWriteUtil::PutZLong(large_string_len, large_buffer.data());
    memcpy(large_buffer.data() + zlong_len, s, strlen(s));

    // CHAR and VARCHAR truncation should still work in this case.
    TestReadAvroChar(sv.Len(), large_buffer.data(), large_buffer.size(), sv,
        zlong_len + large_string_len);
    TestReadAvroVarchar(sv.Len(), large_buffer.data(), large_buffer.size(), sv,
        zlong_len + large_string_len);

    // Interpreting as a string would overflow length field.
    TestReadAvroString(large_buffer.data(), large_buffer.size(), sv, -1,
        TErrorCode::SCANNER_STRING_LENGTH_OVERFLOW);
  }
}

TEST_F(HdfsAvroScannerTest, StringTest) {
  uint8_t data[100];
  const char* s = "hello";
  DCHECK_EQ(strlen(s), 5);
  data[0] = 10; // decodes to 5
  memcpy(&data[1], s, 5);
  StringValue sv(s);
  TestReadAvroString(data, 6, sv, 6);
  TestReadAvroString(data, 10, sv, 6);
  TestReadAvroString(data, 0, sv, -1, TErrorCode::SCANNER_INVALID_INT);
  TestReadAvroString(data, 1, sv, -1, TErrorCode::AVRO_TRUNCATED_BLOCK);
  TestReadAvroString(data, 5, sv, -1, TErrorCode::AVRO_TRUNCATED_BLOCK);

  data[0] = 1; // decodes to -1
  TestReadAvroString(data, 10, sv, -1, TErrorCode::AVRO_INVALID_LENGTH);

  data[0] = 0; // decodes to 0
  sv.SetLen(0);
  TestReadAvroString(data, 1, sv, 1);
  TestReadAvroString(data, 10, sv, 1);
}

TEST_F(HdfsAvroScannerTest, UuidTest) {
  uint8_t data[100];
  const uint8_t uuid_bytes[16] = {0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78,
      0x12, 0x34, 0x56, 0x78, 0x12, 0x34, 0x56, 0x78};
  memcpy(data, uuid_bytes, sizeof(uuid_bytes));
  TestReadAvroUuid(data, UUID_BYTE_LEN, uuid_bytes);
  TestReadAvroUuid(data, UUID_BYTE_LEN + 1, uuid_bytes);
  TestReadAvroUuid(data, UUID_BYTE_LEN - 1, uuid_bytes, TErrorCode::AVRO_TRUNCATED_BLOCK);
  TestReadAvroUuid(data, 0, uuid_bytes, TErrorCode::AVRO_TRUNCATED_BLOCK);
}

TEST_F(HdfsAvroScannerTest, DecimalTest) {
  uint8_t data[100];
  Decimal4Value d4v(123);
  // Unscaled value (123) can be stored in 1 byte
  data[0] = 2; // decodes to 1
  data[1] = 123;
  TestReadAvroDecimal(data, 2, d4v, 2);
  TestReadAvroDecimal(data, 10, d4v, 2);
  TestReadAvroDecimal(data, 0, d4v, -1, TErrorCode::SCANNER_INVALID_INT);
  TestReadAvroDecimal(data, 1, d4v, -1, TErrorCode::AVRO_TRUNCATED_BLOCK);

  data[0] = 10; // decodes to 5
  TestReadAvroDecimal(data, 10, d4v, -1, TErrorCode::AVRO_INVALID_LENGTH);

  data[0] = 1; // decodes to -1
  TestReadAvroDecimal(data, 10, d4v, -1, TErrorCode::AVRO_INVALID_LENGTH);

  data[0] = 0; // decodes to 0
  TestReadAvroDecimal(data, 10, Decimal4Value(0), 1);

  data[0] = 0x80; // decodes to 64
  data[1] = 0x01;
  TestReadAvroDecimal(data, 100, d4v, -1, TErrorCode::AVRO_INVALID_LENGTH);

  d4v = Decimal4Value(123456789);
  // Unscaled value can be stored in 4 bytes
  data[0] = 8; // decodes to 4
#if __BYTE_ORDER == __LITTLE_ENDIAN
  const Decimal4Value::StorageType d4v_value = d4v.value();
  BitUtil::ByteSwap(&data[1], &d4v_value, 4);
#else
  memcpy(&data[1], &d4v.value(), 4);
#endif
  TestReadAvroDecimal(data, 5, d4v, 5);
  TestReadAvroDecimal(data, 10, d4v, 5);
  TestReadAvroDecimal(data, 0, d4v, -1, TErrorCode::SCANNER_INVALID_INT);
  TestReadAvroDecimal(data, 1, d4v, -1, TErrorCode::AVRO_TRUNCATED_BLOCK);
  TestReadAvroDecimal(data, 4, d4v, -1, TErrorCode::AVRO_TRUNCATED_BLOCK);

  Decimal8Value d8v(1);
  data[0] = 2; // decodes to 1
  data[1] = 1;
  TestReadAvroDecimal(data, 2, d8v, 2);
  TestReadAvroDecimal(data, 10, d8v, 2);
  TestReadAvroDecimal(data, 0, d8v, -1, TErrorCode::SCANNER_INVALID_INT);
  TestReadAvroDecimal(data, 1, d8v, -1, TErrorCode::AVRO_TRUNCATED_BLOCK);

  d8v = Decimal8Value(123456789012345678);
  data[0] = 16; // decodes to 8
#if __BYTE_ORDER == __LITTLE_ENDIAN
  const Decimal8Value::StorageType d8v_value = d8v.value();
  BitUtil::ByteSwap(&data[1], &d8v_value, 8);
#else
  memcpy(&data[1], &d8v.value(), 8);
#endif
  TestReadAvroDecimal(data, 9, d8v, 9);
  TestReadAvroDecimal(data, 10, d8v, 9);
  TestReadAvroDecimal(data, 0, d8v, -1, TErrorCode::SCANNER_INVALID_INT);
  TestReadAvroDecimal(data, 1, d8v, -1, TErrorCode::AVRO_TRUNCATED_BLOCK);
  TestReadAvroDecimal(data, 7, d8v, -1, TErrorCode::AVRO_TRUNCATED_BLOCK);

  Decimal16Value d16v(1234567890);
  data[0] = 10; // decodes to 5
#if __BYTE_ORDER == __LITTLE_ENDIAN
  const Decimal16Value::StorageType d16v_value = d16v.value();
  BitUtil::ByteSwap(&data[1], &d16v_value, 5);
#else
  memcpy(&data[1], &d16v.value(), 5);
#endif
  TestReadAvroDecimal(data, 6, d16v, 6);
  TestReadAvroDecimal(data, 10, d16v, 6);
  TestReadAvroDecimal(data, 0, d16v, -1, TErrorCode::SCANNER_INVALID_INT);
  TestReadAvroDecimal(data, 1, d16v, -1, TErrorCode::AVRO_TRUNCATED_BLOCK);
  TestReadAvroDecimal(data, 4, d16v, -1, TErrorCode::AVRO_TRUNCATED_BLOCK);

  /// Produce a very large decimal value.
  Decimal16Value::StorageType d16v_value2;
  memset(&d16v_value2, 0xFF, sizeof(d16v_value2));
  d16v.set_value(d16v_value2);
  data[0] = 32; // decodes to 16
#if __BYTE_ORDER == __LITTLE_ENDIAN
  BitUtil::ByteSwap(&data[1], &d16v_value2, 16);
#else
  memcpy(&data[1], &d16v_value2, 16);
#endif
  TestReadAvroDecimal(data, 17, d16v, 17);
  TestReadAvroDecimal(data, 20, d16v, 17);
  TestReadAvroDecimal(data, 0, d16v, -1, TErrorCode::SCANNER_INVALID_INT);
  TestReadAvroDecimal(data, 1, d16v, -1, TErrorCode::AVRO_TRUNCATED_BLOCK);
  TestReadAvroDecimal(data, 16, d16v, -1, TErrorCode::AVRO_TRUNCATED_BLOCK);
}

TEST_F(HdfsAvroScannerTest, ParseAvroCodecMetadata) {
  bool compressed = true;
  THdfsCompression::type type = THdfsCompression::NONE;
  EXPECT_OK(HdfsAvroScanner::ParseAvroCodecMetadata("null", &compressed, &type));
  EXPECT_FALSE(compressed);
  EXPECT_EQ(THdfsCompression::NONE, type);

  EXPECT_OK(HdfsAvroScanner::ParseAvroCodecMetadata("snappy", &compressed, &type));
  EXPECT_TRUE(compressed);
  EXPECT_EQ(THdfsCompression::SNAPPY, type);

  EXPECT_OK(HdfsAvroScanner::ParseAvroCodecMetadata("deflate", &compressed, &type));
  EXPECT_EQ(THdfsCompression::DEFLATE, type);

  // Java Avro / Trino use "zstandard" in object file metadata, not "zstd".
  EXPECT_OK(HdfsAvroScanner::ParseAvroCodecMetadata("zstandard", &compressed, &type));
  EXPECT_EQ(THdfsCompression::ZSTD, type);

  Status status = HdfsAvroScanner::ParseAvroCodecMetadata("lz4", &compressed, &type);
  EXPECT_FALSE(status.ok());
  EXPECT_STR_CONTAINS(status.GetDetail(), "Unknown Avro compression codec: lz4");
}

TEST_F(HdfsAvroScannerTest, ConvertSchemaNamedTypeReference) {
  // The second use of 'uuid_fixed' is parsed into an AVRO_LINK.
  const string json = R"({"type": "record", "name": "r", "fields": [
      {"name": "u1", "type": {"type": "fixed", "name": "uuid_fixed", "size": 16}},
      {"name": "u2", "type": ["null", "uuid_fixed"]}]})";
  avro_schema_t schema;
  ASSERT_EQ(avro_schema_from_json_length(json.c_str(), json.size(), &schema), 0)
      << avro_strerror();
  ScopedAvroSchemaElement element;
  ASSERT_OK(AvroSchemaElement::ConvertSchema(schema, element.get()));
  ASSERT_EQ(element->children.size(), 2);
  EXPECT_EQ(element->children[0].schema->type, AVRO_FIXED);
  EXPECT_FALSE(element->children[0].nullable());
  EXPECT_EQ(element->children[1].schema->type, AVRO_FIXED);
  EXPECT_TRUE(element->children[1].nullable());
}

TEST_F(HdfsAvroScannerTest, ConvertSchemaRecursiveType) {
  const string json = R"({"type": "record", "name": "node", "fields": [
      {"name": "next", "type": ["null", "node"]}]})";
  avro_schema_t schema;
  ASSERT_EQ(avro_schema_from_json_length(json.c_str(), json.size(), &schema), 0)
      << avro_strerror();
  ScopedAvroSchemaElement element;
  Status status = AvroSchemaElement::ConvertSchema(schema, element.get());
  ASSERT_FALSE(status.ok());
  EXPECT_STR_CONTAINS(status.GetDetail(), "Recursive Avro schemas are not supported");
}

TEST_F(HdfsAvroScannerTest, AvroSchemaToColumnTypeUuidFixed) {
  const string json =
      R"({"type": "fixed", "name": "u", "size": 16, "logicalType": "uuid"})";
  avro_schema_t schema;
  ASSERT_EQ(avro_schema_from_json_length(json.c_str(), json.size(), &schema), 0)
      << avro_strerror();
  ColumnType column_type;
  ASSERT_OK(AvroSchemaToColumnType(schema, "u", &column_type));
  EXPECT_TRUE(column_type.IsUuidType());
}

TEST_F(HdfsAvroScannerTest, ConvertSchemaRejectsAvroArray) {
  const string json = R"({"type": "record", "name": "r", "fields": [
      {"name": "items", "type": {"type": "array", "items": "long"}}]})";
  avro_schema_t schema;
  ASSERT_EQ(avro_schema_from_json_length(json.c_str(), json.size(), &schema), 0)
      << avro_strerror();
  ScopedAvroSchemaElement element;
  Status status = AvroSchemaElement::ConvertSchema(schema, element.get());
  ASSERT_FALSE(status.ok());
  EXPECT_STR_CONTAINS(status.GetDetail(), "Got type: array");
}

TEST_F(HdfsAvroScannerTest, ConvertSchemaRejectsFixedOtherThan16Bytes) {
  const string json = R"({"type": "record", "name": "r", "fields": [
      {"name": "f", "type": {"type": "fixed", "name": "fixed8", "size": 8}}]})";
  avro_schema_t schema;
  ASSERT_EQ(avro_schema_from_json_length(json.c_str(), json.size(), &schema), 0)
      << avro_strerror();
  ScopedAvroSchemaElement element;
  Status status = AvroSchemaElement::ConvertSchema(schema, element.get());
  ASSERT_FALSE(status.ok());
  EXPECT_STR_CONTAINS(status.GetDetail(), "Got type: fixed(8)");

  ColumnType column_type;
  status = AvroSchemaToColumnType(
      avro_schema_record_field_get_by_index(schema, 0), "f", &column_type);
  EXPECT_EQ(status.code(), TErrorCode::AVRO_UNSUPPORTED_TYPE);
}

// avro-c drops 'logicalType' from fixed schemas, so a fixed(16) file field is read as a
// UUID. The next two tests check that a fixed(16) which is not a UUID cannot be read into
// the non-UUID table column it belongs to.
TEST_F(HdfsAvroScannerTest, Fixed16WithoutUuidLogicalTypeRejectedForNonUuidColumn) {
  const string file_json = R"({"type": "fixed", "name": "fixed16", "size": 16})";
  // Reader schemas of BINARY and STRING table columns.
  EXPECT_EQ(ResolveField(R"({"type": "bytes"})", file_json).code(),
      TErrorCode::AVRO_SCHEMA_RESOLUTION_ERROR);
  EXPECT_EQ(ResolveField(R"({"type": "string"})", file_json).code(),
      TErrorCode::AVRO_SCHEMA_RESOLUTION_ERROR);
  // A UUID table column still accepts it, since the missing logicalType is not visible.
  EXPECT_OK(ResolveField(R"({"type": "fixed", "name": "uuid_fixed", "size": 16,
      "logicalType": "uuid"})", file_json));
}

TEST_F(HdfsAvroScannerTest, DecimalOnFixed16RejectedForDecimalColumn) {
  // Iceberg writes DECIMAL(38, s) to Avro as a decimal on fixed(16).
  const string file_json = R"({"type": "fixed", "name": "decimal16", "size": 16,
      "logicalType": "decimal", "precision": 38, "scale": 2})";
  // Impala's reader schema for a DECIMAL(38, 2) column is a decimal on bytes.
  const string table_json = R"({"type": "bytes", "logicalType": "decimal",
      "precision": 38, "scale": 2})";
  EXPECT_EQ(ResolveField(table_json, file_json).code(),
      TErrorCode::AVRO_SCHEMA_RESOLUTION_ERROR);
}

TEST_F(HdfsAvroScannerTest, StringFileFieldRejectedForUuidColumn) {
  const string uuid_table = R"({"type": "fixed", "name": "uuid_fixed", "size": 16,
      "logicalType": "uuid"})";
  const string string_file = R"({"type": "string"})";
  EXPECT_EQ(ResolveField(uuid_table, string_file).code(),
      TErrorCode::AVRO_SCHEMA_RESOLUTION_ERROR);
}
}
