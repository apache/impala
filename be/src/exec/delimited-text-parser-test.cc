// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <string>
#include <gtest/gtest.h>

#include "exec/delimited-text-parser.inline.h"
#include "util/cpu-info.h"

using namespace std;

namespace impala {

void ValidateTupleStart(DelimitedTextParser* parser, const char* data, 
    int expected_offset, char tuple_delim) {
  int offset = parser->FindFirstInstance(data, strlen(data));
  
  EXPECT_EQ(offset, expected_offset);
  if (offset != -1) {
    EXPECT_GE(offset, 1);
    EXPECT_LT(offset, strlen(data));
    EXPECT_EQ(data[offset - 1], tuple_delim);
  }
}

// Test finding first tuple delim with escape characters
TEST(DelimitedTextParser, Escapes) {
  const char TUPLE_DELIM = '|';
  const char FIELD_DELIM = ',';
  const char COLLECTION_DELIM = ',';
  const char ESCAPE_CHAR = '@';

  // Test without escape
  DelimitedTextParser no_escape_parser(NULL, TUPLE_DELIM, FIELD_DELIM, 
      COLLECTION_DELIM);
  ValidateTupleStart(&no_escape_parser, "no_delims", -1, TUPLE_DELIM);
  ValidateTupleStart(&no_escape_parser, "abc||abc", 4, TUPLE_DELIM);
  ValidateTupleStart(&no_escape_parser, "|abcd", 1, TUPLE_DELIM);
  ValidateTupleStart(&no_escape_parser, "a|bcd", 2, TUPLE_DELIM);
  
  // Test with escape char
  DelimitedTextParser escape_parser(NULL, TUPLE_DELIM, FIELD_DELIM, 
      COLLECTION_DELIM, ESCAPE_CHAR);

  ValidateTupleStart(&escape_parser, "a@|a|bcd", 5, TUPLE_DELIM);
  ValidateTupleStart(&escape_parser, "a@@|a|bcd", 4, TUPLE_DELIM);
  ValidateTupleStart(&escape_parser, "a@@@|a|bcd", 7, TUPLE_DELIM);
  ValidateTupleStart(&escape_parser, "a@@@@|a|bcd", 6, TUPLE_DELIM);
  ValidateTupleStart(&escape_parser, "a|@@@|a|bcd", 2, TUPLE_DELIM);

  // The parser doesn't support this case.  
  // TODO: update test when it is fixed
  ValidateTupleStart(&escape_parser, "@|no_delims", -1, TUPLE_DELIM);
}

// TODO: expand test for other delimited text parser functions/cases.
// Not all of them work without creating a HdfsScanNode but we can expand
// these tests quite a bit more.

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::CpuInfo::Init();
  return RUN_ALL_TESTS();
}

