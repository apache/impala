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

#include <string>

#include "testutil/gtest-util.h"
#include "util/sql-util.h"

namespace impala {

TEST(SqlUtilTest, EscapeSqlEmptyLenEqual) {
  const string str = "";
  EXPECT_EQ(str, EscapeSql(str, str.length()));
}

TEST(SqlUtilTest, EscapeSqlEmptyLenTooBig) {
  const string str = "";
  EXPECT_EQ(str, EscapeSql(str, str.length() + 1));
}

TEST(SqlUtilTest, EscapeSqlEmptyLenMissing) {
  const string str = "";
  EXPECT_EQ(str, EscapeSql(str));
}

TEST(SqlUtilTest, EscapeSqlNotEmptyLenZero) {
  const string str = "select * from my_table where field1=1";
  EXPECT_EQ("", EscapeSql(str, 0));
}

TEST(SqlUtilTest, EscapeSqlNoEscapedCharsLenEqual) {
  const string str = "select * from my_table where field1=1";
  EXPECT_EQ(str, EscapeSql(str, str.length()));
}

TEST(SqlUtilTest, EscapeSqlNoEscapedCharsLenTooBig) {
  const string str = "select * from my_table where field1=1";
  EXPECT_EQ(str, EscapeSql(str, str.length() + 1));
}

TEST(SqlUtilTest, EscapeSqlNoEscapedCharsLenSmaller) {
  const string str = "select * from my_table where field1=1";
  EXPECT_EQ("select", EscapeSql(str, 6));
}

TEST(SqlUtilTest, EscapeSqlNoEscapedCharsLenMissing) {
  const string str = "select * from my_table where field1=1";
  EXPECT_EQ(str, EscapeSql(str));
}

TEST(SqlUtilTest, EscapeSqlEscapedCharsLenEqual) {
  const string str = ";select * from \"my_table\" where field1='\\val1'\n";
  EXPECT_EQ("\\;select * from \"my_table\" where field1=\\'\\\\val1\\'\\n",
      EscapeSql(str, str.length() + 5));
}

TEST(SqlUtilTest, EscapeSqlEscapedCharsLenTooBig) {
  const string str = ";select * from \"my_table\" where field1='\\val1'\n";
  EXPECT_EQ("\\;select * from \"my_table\" where field1=\\'\\\\val1\\'\\n",
      EscapeSql(str, str.length() + 100));
}

TEST(SqlUtilTest, EscapeSqlEscapedCharsLenMissing) {
  const string str = ";select * from \"my_table\" where field1='\\val1'\n";
  EXPECT_EQ("\\;select * from \"my_table\" where field1=\\'\\\\val1\\'\\n",
      EscapeSql(str));
}

TEST(SqlUtilTest, EscapeSqlEscapedCharsLenSmallerLastCharEscaped) {
  const string str = ";select 'fld1', 'fld;2'\n from \"my_table\" where fld1='\\val1'\n";
  EXPECT_EQ("\\;select \\'fld1\\', \\'fld\\;",
      EscapeSql(str, 26));
}

TEST(SqlUtilTest, EscapeSqlEscapedCharsLenSmallerLastCharNotEscaped) {
  const string str = ";select 'fld1', 'fld;2'\n from \"my_table\" where fld1='\\val1'\n";
  EXPECT_EQ("\\;select \\'fld1",
      EscapeSql(str, 15));
}

} // namespace impala
