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

using namespace std;

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
    stream_size_ = GetParam();
  }

  void NextBuffer(const char** begin, const char** end) {
    EXPECT_EQ(*begin, *end);
    *begin = *end = nullptr;
    if (data_pos_ > data_.size()) return;
    *begin = data_.data() + data_pos_;
    size_t len = min(stream_size_, data_.size() - data_pos_);
    *end = *begin + len;
    data_pos_ += len;
  }

  const vector<string>& schema() const { return schema_; }

  const string& result() const { return result_; }

 private:
  size_t data_pos_ = 0;
  size_t stream_size_;
  string data_;
  string result_;

  vector<string> schema_ = {"name", "bool", "score", "address"};
  vector<pair<string, string>> json_data_ = {
    // Normal Json
    {R"({"name": "Linda", "bool": true, "score": 76.3, "address": "Chicago"})",
        "Linda, true, 76.3, Chicago, "},
    {R"({"name": "Mike", "bool": null, "score": 82.1, "address": "Dallas"})",
        "Mike, null, 82.1, Dallas, "},
    {R"({"name": "Sara", "bool": false, "score": 94.8, "address": "Seattle"})",
        "Sara, false, 94.8, Seattle, "},

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

TEST_P(JsonParserTest, Basic) {
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
  EXPECT_EQ(result(), js.Result());
}

}
