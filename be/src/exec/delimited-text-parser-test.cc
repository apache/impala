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

#include "exec/delimited-text-parser.inline.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

namespace impala {

void Validate(TupleDelimitedTextParser* parser, const string& data,
    int expected_offset, char tuple_delim, int expected_num_tuples,
    int expected_num_fields) {
  parser->ParserReset();
  char* data_ptr = const_cast<char*>(data.c_str());
  int remaining_len = data.size();
  int offset = parser->FindFirstInstance(data_ptr, remaining_len);

  EXPECT_EQ(offset, expected_offset) << data;
  if (offset == -1) return;

  EXPECT_GE(offset, 1) << data;
  EXPECT_LT(offset, data.size()) << data;
  if (tuple_delim != '\n') {
    EXPECT_EQ(data[offset - 1], tuple_delim) << data;
  } else {
    EXPECT_TRUE(data[offset - 1] == '\n' || data[offset - 1] == '\r')
        << data[offset - 1] << endl << data;
    if (data[offset - 1] == '\r' && offset < data.size()) {
      EXPECT_NE(data[offset], '\n');
    }
  }

  data_ptr += offset;
  remaining_len -= offset;

  char* row_end_locs[100];
  vector<FieldLocation> field_locations(100);
  int num_tuples = 0;
  int num_fields = 0;
  char* next_column_start;
  Status status =
      parser->ParseFieldLocations(100, remaining_len, &data_ptr, &row_end_locs[0],
          field_locations.data(), &num_tuples, &num_fields, &next_column_start);
  EXPECT_EQ(num_tuples, expected_num_tuples) << data;
  EXPECT_EQ(num_fields, expected_num_fields) << data;
}

TEST(DelimitedTextParser, Basic) {
  const char TUPLE_DELIM = '|';
  const char FIELD_DELIM = ',';
  const char COLLECTION_DELIM = ',';
  const char ESCAPE_CHAR = '@';

  const int NUM_COLS = 1;

  // Test without escape
  bool is_materialized_col[NUM_COLS];
  for (int i = 0; i < NUM_COLS; ++i) is_materialized_col[i] = true;

  TupleDelimitedTextParser no_escape_parser(NUM_COLS, 0, is_materialized_col,
                                            TUPLE_DELIM, FIELD_DELIM, COLLECTION_DELIM);
  // Note that only complete tuples "count"
  Validate(&no_escape_parser, "no_delims", -1, TUPLE_DELIM, 0, 0);
  Validate(&no_escape_parser, "abc||abc", 4, TUPLE_DELIM, 1, 1);
  Validate(&no_escape_parser, "|abcd", 1, TUPLE_DELIM, 0, 0);
  Validate(&no_escape_parser, "a|bcd", 2, TUPLE_DELIM, 0, 0);

  // Test with escape char
  TupleDelimitedTextParser escape_parser(NUM_COLS, 0, is_materialized_col,
                                         TUPLE_DELIM, FIELD_DELIM, COLLECTION_DELIM,
                                         ESCAPE_CHAR);
  Validate(&escape_parser, "a@|a|bcd", 5, TUPLE_DELIM, 0, 0);
  Validate(&escape_parser, "a@@|a|bcd", 4, TUPLE_DELIM, 1, 1);
  Validate(&escape_parser, "a@@@|a|bcd", 7, TUPLE_DELIM, 0, 0);
  Validate(&escape_parser, "a@@@@|a|bcd", 6, TUPLE_DELIM, 1, 1);
  Validate(&escape_parser, "a|@@@|a|bcd", 2, TUPLE_DELIM, 1, 1);

  // // The parser doesn't support this case.
  // // TODO: update test when it is fixed
  // Validate(&escape_parser, "@|no_delims", -1, TUPLE_DELIM);

  // Test null characters
  const string str1("\0no_delims", 10);
  const string str2("ab\0||abc", 8);
  const string str3("\0|\0|\0", 5);
  const string str4("abc|\0a|abc", 10);
  const string str5("\0|aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 32);
  Validate(&no_escape_parser, str1, -1, TUPLE_DELIM, 0, 0);
  Validate(&no_escape_parser, str2, 4, TUPLE_DELIM, 1, 1);
  Validate(&no_escape_parser, str3, 2, TUPLE_DELIM, 1, 1);
  Validate(&no_escape_parser, str4, 4, TUPLE_DELIM, 1, 1);
  Validate(&no_escape_parser, str5, 2, TUPLE_DELIM, 0, 0);

  const string str6("\0@|\0|\0", 6);
  const string str7("\0@@|\0|\0", 6);
  const string str8("\0@\0@|\0|\0", 8);
  const string str9("\0@||aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 34);
  Validate(&escape_parser, str6, 5, TUPLE_DELIM, 0, 0);
  Validate(&escape_parser, str7, 4, TUPLE_DELIM, 1, 1);
  Validate(&escape_parser, str8, 7, TUPLE_DELIM, 0, 0);
  Validate(&escape_parser, str9, 4, TUPLE_DELIM, 0, 0);
}

TEST(DelimitedTextParser, Fields) {
  const char TUPLE_DELIM = '|';
  const char FIELD_DELIM = ',';
  const char COLLECTION_DELIM = ',';
  const char ESCAPE_CHAR = '@';

  const int NUM_COLS = 2;

  bool is_materialized_col[NUM_COLS];
  for (int i = 0; i < NUM_COLS; ++i) is_materialized_col[i] = true;

  TupleDelimitedTextParser no_escape_parser(NUM_COLS, 0, is_materialized_col,
                                            TUPLE_DELIM, FIELD_DELIM, COLLECTION_DELIM);

  Validate(&no_escape_parser, "a,b|c,d|e,f", 4, TUPLE_DELIM, 1, 3);
  Validate(&no_escape_parser, "b|c,d|e,f", 2, TUPLE_DELIM, 1, 3);
  Validate(&no_escape_parser, "a,|c,d|", 3, TUPLE_DELIM, 1, 2);
  Validate(&no_escape_parser, "a,|c|e", 3, TUPLE_DELIM, 1, 2);
  const string str10("a,\0|c,d|e", 9);
  Validate(&no_escape_parser, str10, 4, TUPLE_DELIM, 1, 2);

  TupleDelimitedTextParser escape_parser(NUM_COLS, 0, is_materialized_col,
                                         TUPLE_DELIM, FIELD_DELIM, COLLECTION_DELIM,
                                         ESCAPE_CHAR);

  Validate(&escape_parser, "a,b|c,d|e,f", 4, TUPLE_DELIM, 1, 3);
  Validate(&escape_parser, "a,@|c|e,f", 6, TUPLE_DELIM, 0, 1);
  Validate(&escape_parser, "a|b,c|d@,e", 2, TUPLE_DELIM, 1, 2);
}

TEST(DelimitedTextParser, SpecialDelimiters) {
  const char TUPLE_DELIM = '\n'; // implies '\r' and "\r\n" are also delimiters
  const char NUL_DELIM = '\0';
  const int NUM_COLS = 1;
  const int MAX_COLS = 2;

  bool is_materialized_col[MAX_COLS];
  for (int i = 0; i < MAX_COLS; ++i) is_materialized_col[i] = true;

  TupleDelimitedTextParser tuple_delim_parser(NUM_COLS, 0, is_materialized_col,
      TUPLE_DELIM);

  TupleDelimitedTextParser nul_tuple_parser(NUM_COLS, 0, is_materialized_col, NUL_DELIM);

  TupleDelimitedTextParser nul_field_parser(MAX_COLS, 0, is_materialized_col,
                                            TUPLE_DELIM, NUL_DELIM);

  // Non-SSE case
  Validate(&tuple_delim_parser, "A\r\nB", 3, TUPLE_DELIM, 0, 0);
  Validate(&tuple_delim_parser, "A\rB", 2, TUPLE_DELIM, 0, 0);
  Validate(&tuple_delim_parser, "A\nB", 2, TUPLE_DELIM, 0, 0);
  Validate(&tuple_delim_parser, "A\nB\r\nC", 2, TUPLE_DELIM, 1, 1);
  Validate(&tuple_delim_parser, "A\r\nB\r\nC", 3, TUPLE_DELIM, 1, 1);
  Validate(&tuple_delim_parser, "A\rB\nC\r\nD", 2, TUPLE_DELIM, 2, 2);
  Validate(&tuple_delim_parser, "\r\r\n\n", 1, TUPLE_DELIM, 2, 2);

  // NUL tuple delimiter; no field delimiter
  const string nul1("\0\0\0", 3);
  const string nul2("AAA\0BBB\0", 8);
  const string nul3("\n\0\r\0\r\n\0", 7);
  const string nul4("\n\0\r\0\r\n", 6);
  Validate(&nul_tuple_parser, nul1, 1, NUL_DELIM, 2, 2);
  Validate(&nul_tuple_parser, nul2, 4, NUL_DELIM, 1, 1);
  Validate(&nul_tuple_parser, nul3, 2, NUL_DELIM, 2, 2);
  Validate(&nul_tuple_parser, nul4, 2, NUL_DELIM, 1, 1);

  // SSE case
  string data = "\rAAAAAAAAAAAAAAA";
  DCHECK_EQ(data.size(), SSEUtil::CHARS_PER_128_BIT_REGISTER);
  Validate(&tuple_delim_parser, data, 1, TUPLE_DELIM, 0, 0);
  data = "\nAAAAAAAAAAAAAAA";
  DCHECK_EQ(data.size(), SSEUtil::CHARS_PER_128_BIT_REGISTER);
  Validate(&tuple_delim_parser, data, 1, TUPLE_DELIM, 0, 0);
  data = "\r\nAAAAAAAAAAAAAA";
  DCHECK_EQ(data.size(), SSEUtil::CHARS_PER_128_BIT_REGISTER);
  Validate(&tuple_delim_parser, data, 2, TUPLE_DELIM, 0, 0);
  data = "\r\nAAA\n\r\r\nAAAAAAA";
  DCHECK_EQ(data.size(), SSEUtil::CHARS_PER_128_BIT_REGISTER);
  Validate(&tuple_delim_parser, data, 2, TUPLE_DELIM, 3, 3);

  // NUL SSE case
  const string nulsse1("AAAAA\0AAAAAAAAAAA\0AAAAAAAAAAAA\0\0", 32);
  const string nulsse2("AAAAA\0AAAAAAAAAAA\0AAAAAAAAAAAA\0A", 32);
  const string nulsse3("AAA\0BBBbbbbbbbbbbbbbbbbbbb\0cccc,ddd\0", 36);
  const string nulsse4("AAA\0BBBbbbbbbbbbbbbbbbbbbb\0cccc,dddd", 36);
  Validate(&nul_tuple_parser, nulsse1, 6, NUL_DELIM, 3, 3);
  Validate(&nul_tuple_parser, nulsse2, 6, NUL_DELIM, 2, 2);
  Validate(&nul_tuple_parser, nulsse3, 4, NUL_DELIM, 2, 2);
  Validate(&nul_tuple_parser, nulsse4, 4, NUL_DELIM, 1, 1);

  // NUL Field delimiters
  const string field1("\na\0b\0c\n", 7);
  const string field2("aaaa\na\0b\0c\naaaaa\0b\na\0b\0c\n", 25);
  Validate(&nul_field_parser, field1, 1, TUPLE_DELIM, 1, 2);
  Validate(&nul_field_parser, field2, 5, TUPLE_DELIM, 3, 6);
}

// TODO: expand test for other delimited text parser functions/cases.
// Not all of them work without creating a HdfsScanNode but we can expand
// these tests quite a bit more.

}

