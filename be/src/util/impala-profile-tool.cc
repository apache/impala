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

#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <gflags/gflags.h>

#include "common/object-pool.h"
#include "util/runtime-profile.h"

#include "common/names.h"

// Utility to decode an Impala profile log from standard input.
// The profile log is consumed from standard input and each successfully parsed entry
// is pretty-printed to standard output.
//
// Example usage:
//   impala-profile-tool < impala_profile_log_1.1-1607057366897
//
// The following options are supported:
// --query_id=<query id>: given an impala query ID, only process profiles with this
//      query id
// --min_timestamp=<integer timestamp>: only process profiles at or after this timestamp
// --max_timestamp=<integer timestamp>: only process profiles at or before this timestamp
//
// --gen_experimental_profile: if set to true, generates full output for the new
//      experimental profile.
DEFINE_string(query_id, "", "Query ID to output profiles for");
DEFINE_int64(min_timestamp, -1, "Minimum timestamp (inclusive) to output profiles for");
DEFINE_int64(max_timestamp, -1, "Maximum timestamp (inclusive) to output profiles for");

using namespace impala;

using std::cerr;
using std::cin;
using std::cout;
using std::istringstream;

int main(int argc, char** argv) {
  google::ParseCommandLineFlags(&argc, &argv, true);

  int errors = 0;
  string line;
  int lineno = 1;
  // Read profile log lines from stdin.
  for (; getline(cin, line); ++lineno) {
    // Parse out fields from the line.
    istringstream liness(line);
    int64_t timestamp;
    string query_id, encoded_profile;
    liness >> timestamp >> query_id >> encoded_profile;
    if (liness.fail()) {
      cerr << "Error parsing line " << lineno << ": '" << line << "'\n";
      ++errors;
      continue;
    }

    // Skip decoding entries that don't match our parameters.
    if ((FLAGS_query_id != "" && FLAGS_query_id != query_id) ||
        (FLAGS_min_timestamp != -1 && timestamp < FLAGS_min_timestamp) ||
        (FLAGS_max_timestamp != -1 && timestamp > FLAGS_max_timestamp)) {
      continue;
    }

    ObjectPool pool;
    RuntimeProfile* profile;
    Status status = RuntimeProfile::CreateFromArchiveString(
        encoded_profile, &pool, &profile);
    if (!status.ok()) {
      cerr << "Error parsing entry " << lineno << ": " << status.GetDetail() << "\n";
      ++errors;
      continue;
    }
    profile->PrettyPrint(&cout);
  }
  if (cin.bad()) {
    cerr << "Error reading line " << lineno << "\n";
    ++errors;
  }

  if (errors > 0) {
    cerr << "Encountered " << errors << " parse errors" << "\n";
    return 1;
  }
  return 0;
}
