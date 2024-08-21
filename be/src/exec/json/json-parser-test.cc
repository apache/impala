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

#include <vector>
#include <string>
#include <sstream>
#include <algorithm>

#include "exec/json/json-parser.h"
#include "testutil/gtest-util.h"
#include "util/time.h"

using namespace std;
using namespace rapidjson;

namespace impala {

class JsonParserTest : public ::testing::TestWithParam<int> {
 public:
  JsonParserTest() {
    random_shuffle(json_data_.begin(), json_data_.end());
    stringstream data_stream, result_stream;
    for (const auto& p : json_data_) {
      data_stream << p.first << '\n';
      if (p.second.empty()) continue;
      result_stream << p.second << '\n';
    }
    data_ = data_stream.str();
    result_ = result_stream.str();
  }

  virtual void SetUp() override {
    data_pos_ = 0;
    repeats_ = 0;
    stream_size_ = GetParam();
  }

  void Reset(size_t repeats = 0) {
    data_pos_ = 0;
    repeats_ = repeats;
  }

  void NextBuffer(const char** begin, const char** end) {
    EXPECT_EQ(*begin, *end);
    *begin = *end = nullptr;
    if (data_pos_ >= data_.size()) {
      if (repeats_ == 0) return;
      data_pos_ = 0;
      --repeats_;
    }
    *begin = data_.data() + data_pos_;
    size_t len = min(stream_size_, data_.size() - data_pos_);
    *end = *begin + len;
    data_pos_ += len;
  }

  const vector<string>& schema() const { return schema_; }

  const string& result() const { return result_; }

  enum JsonValueType {
    TYPE_NULL,
    TYPE_TRUE,
    TYPE_FALSE,
    TYPE_STRING,
    TYPE_NUMBER,
    TYPE_OBJECT,
    TYPE_ARRAY,
    TYPE_VALUE
  };

  void TestSkip(const char* v, JsonValueType t, ParseErrorCode e = kParseErrorNone) {
    SimpleStream ss(v);
    JsonSkipper<SimpleStream> js(ss);
    bool res;
    switch (t) {
      case TYPE_NULL: res = js.SkipNull(); break;
      case TYPE_TRUE: res = js.SkipTrue(); break;
      case TYPE_FALSE: res = js.SkipFalse(); break;
      case TYPE_STRING: res = js.SkipString(); break;
      case TYPE_NUMBER: res = js.SkipNumber(); break;
      case TYPE_OBJECT: res = js.SkipObject(); break;
      case TYPE_ARRAY: res = js.SkipArray(); break;
      case TYPE_VALUE: res = js.SkipValue(); break;
      default: ASSERT_TRUE(false);
    }
    if (e == kParseErrorNone) {
      EXPECT_TRUE(res) << v;
      EXPECT_TRUE(ss.Eos());
    } else {
      EXPECT_FALSE(res) << v;
      EXPECT_EQ(js.GetErrorCode(), e) << v;
    }
  }

 private:
  size_t repeats_ = 0;
  size_t data_pos_ = 0;
  size_t stream_size_;
  string data_;
  string result_;

  vector<string> schema_ = {"name", "bool", "score", "address"};
  vector<pair<string, string>> json_data_ = {
    // Normal Json
    {R"({"name": "Linda", "bool": true, "score": 76.3, "address": "Chicago"})",
        "Linda, true, 76.3, Chicago, "},
    {R"({"name": "Mike", "bool": null, "score": NaN, "address": "Dallas"})",
        "Mike, null, NaN, Dallas, "},
    {R"({"name": "Sara", "bool": false, "score": Inf, "address": "Seattle"})",
        "Sara, false, Inf, Seattle, "},

    // String with escape or special char.
    {R"({"name": "Joe\nJoe", "bool": null, "score": 100, "address": "{New}\t{York}"})",
        "Joe\nJoe, null, 100, {New}\t{York}, "},
    {R"({"name": "$}~{$", "bool": false, "score": 95.2, "address": "\"{Los} \\Angeles"})",
        "$}~{$, false, 95.2, \"{Los} \\Angeles, "},
    {R"({"name": "A\"}{\"A", "bool": true, "score": 79.4, "address": "[]()[{}{}]"})",
        "A\"}{\"A, true, 79.4, []()[{}{}], "},

    // Column miss or out-of-order.
    {R"({"name": "Grace", "bool": false, "score": 92.3})",
        "Grace, false, 92.3, null, "},
    {R"({"bool": false, "score": 90.5, "name": "Emily"})",
        "Emily, false, 90.5, null, "},
    {R"({"score": 87.6, "bool": false, "name": "David", "address": "Boston"})",
        "David, false, 87.6, Boston, "},

    // Column with complex type.
    {R"({"name": "Bob", "bool": true, "score": 78.9, "complex": [1, {"a2": [4, 5, 6]}]})",
        "Bob, true, 78.9, null, "},
    {R"({"name": "Peter", "object": {"array": [1, 2, 3], "object": {"empty": []}}})",
        "Peter, null, null, null, "},
    {R"({"name": "Sophia", "array": [1, 2, 3, {"test": null}], "address": "San Diego"})",
        "Sophia, null, null, San Diego, "},

    // Exposed string, number, or array
    {R"("{\"name\": \"Aisha\", \"bool\": true, \"score\": 86.1}")", ""},
    {R"(-1234.56789)", ""},
    {R"(["Pavel", 123e2, {"test": null}, {"a1": [1, 2, "{abc, [123]}"]}])", ""}
  };
};

INSTANTIATE_TEST_SUITE_P(StreamSize, JsonParserTest, ::testing::Values(1, 16, 256));

TEST_P(JsonParserTest, BasicTest) {
  SimpleJsonScanner js(schema(), [this](const char** begin, const char** end) {
    this->NextBuffer(begin, end);
  });
  constexpr int max_rows = 10;
  int num_rows = 0;
  do {
    EXPECT_OK(js.Scan(max_rows, &num_rows));
    EXPECT_GE(num_rows, 0);
    EXPECT_LE(num_rows, max_rows);
  } while (num_rows);
  EXPECT_EQ(result(), js.result());
}

TEST_P(JsonParserTest, JsonSkipperTest) {
  // positive cases
  TestSkip("null", TYPE_NULL);
  TestSkip("true", TYPE_TRUE);
  TestSkip("false", TYPE_FALSE);

  TestSkip(R"("abc")", TYPE_STRING);
  TestSkip(R"(" \n\t\r")", TYPE_STRING);
  TestSkip(R"("\0\1\2")", TYPE_STRING);
  TestSkip(R"("\u123\"\'\\")", TYPE_STRING);
  TestSkip(R"("你好🙂")", TYPE_STRING);
  TestSkip(R"("\u009f\u0099\u0082")", TYPE_STRING);

  TestSkip("1.024", TYPE_NUMBER);
  TestSkip("-9.9", TYPE_NUMBER);
  TestSkip("2e10", TYPE_NUMBER);
  TestSkip("-2e-10", TYPE_NUMBER);
  TestSkip("Inf", TYPE_NUMBER);
  TestSkip("-Infinity", TYPE_NUMBER);
  TestSkip("NaN", TYPE_NUMBER);

  TestSkip("{}", TYPE_OBJECT);
  TestSkip(R"({"a":null, "b":[1,true,false]})", TYPE_OBJECT);
  TestSkip(R"({"a":null, "b":{"c":"d"}})", TYPE_OBJECT);
  TestSkip(R"({"a":null, "b":[{"k1":"v1"}, {"k2":"v2"}]})", TYPE_OBJECT);

  TestSkip("[]", TYPE_ARRAY);
  TestSkip(R"(["",true,false])", TYPE_ARRAY);
  TestSkip(R"(["]",{"":[{},[{}]]}])", TYPE_ARRAY);
  TestSkip(R"(["",{},[[[]]],{"a":[1,2],"":""}])", TYPE_ARRAY);

  TestSkip("null", TYPE_VALUE);
  TestSkip(R"("abc")", TYPE_VALUE);
  TestSkip("1.024", TYPE_VALUE);
  TestSkip("{}", TYPE_VALUE);
  TestSkip("[]", TYPE_VALUE);

  // negative cases
  TestSkip("nuLL", TYPE_NULL, kParseErrorValueInvalid);
  TestSkip("tRue", TYPE_TRUE, kParseErrorValueInvalid);
  TestSkip("flase", TYPE_FALSE, kParseErrorValueInvalid);

  TestSkip(R"("abc\")", TYPE_STRING, kParseErrorStringMissQuotationMark);
  TestSkip(R"("你好🙂\")", TYPE_STRING, kParseErrorStringMissQuotationMark);
  TestSkip(R"("\u009f\u0099\u00\")", TYPE_STRING, kParseErrorStringMissQuotationMark);

  TestSkip("+1", TYPE_NUMBER, kParseErrorValueInvalid);
  TestSkip(".123", TYPE_NUMBER, kParseErrorValueInvalid);
  TestSkip("1.", TYPE_NUMBER, kParseErrorNumberMissFraction);
  TestSkip("2e", TYPE_NUMBER, kParseErrorNumberMissExponent);

  TestSkip("{1}", TYPE_OBJECT, kParseErrorObjectMissName);
  TestSkip(R"({"a""b"})", TYPE_OBJECT, kParseErrorObjectMissColon);
  TestSkip(R"({"a":})", TYPE_OBJECT, kParseErrorValueInvalid);
  TestSkip(R"({"a":"b")", TYPE_OBJECT, kParseErrorObjectMissCommaOrCurlyBracket);
  TestSkip(R"({"a":null, "b":{1,true,false}})", TYPE_OBJECT, kParseErrorObjectMissName);

  TestSkip("[,false]", TYPE_ARRAY, kParseErrorValueInvalid);
  TestSkip("[true,]", TYPE_ARRAY, kParseErrorValueInvalid);
  TestSkip("[true,false", TYPE_ARRAY, kParseErrorArrayMissCommaOrSquareBracket);
  TestSkip("[[1,2]", TYPE_ARRAY, kParseErrorArrayMissCommaOrSquareBracket);
  TestSkip(R"(["],"a","b"])", TYPE_ARRAY, kParseErrorArrayMissCommaOrSquareBracket);

  TestSkip("Null", TYPE_VALUE, kParseErrorValueInvalid);
  TestSkip(R"({"abc\":1})", TYPE_VALUE, kParseErrorStringMissQuotationMark);
  TestSkip("-2.e4", TYPE_VALUE, kParseErrorNumberMissFraction);
  TestSkip(R"({"a":b})", TYPE_VALUE, kParseErrorValueInvalid);
  TestSkip("[,]", TYPE_VALUE, kParseErrorValueInvalid);
}

TEST_P(JsonParserTest, CountJsonObjectsTest) {
  SimpleJsonScanner js({}, [this](const char** begin, const char** end) {
    this->NextBuffer(begin, end);
  });
  constexpr int max_rows = 1024;
  int num_rows = 0, row_count = 0;;

  int64_t scan_start_time = UnixMicros();
  Reset(1000);
  do {
    EXPECT_OK(js.Scan(max_rows, &num_rows));
  } while (num_rows);

  int64_t count_start_time = UnixMicros();
  Reset(1000);
  do {
    EXPECT_OK(js.Count(max_rows, &num_rows));
    row_count += num_rows;
  } while (num_rows);
  int64_t end_time = UnixMicros();

  EXPECT_EQ(row_count, js.row_count());
  LOG(INFO) << "JSON Scan cost time in ms: "
      << static_cast<double>(count_start_time - scan_start_time) / 1000
      << ", JSON Count cost time in ms: "
      << static_cast<double>(end_time - count_start_time) / 1000;
}

}
