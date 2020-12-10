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

#include <cstdint>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include <boost/algorithm/string/case_conv.hpp>
#include <gflags/gflags.h>

#include "common/object-pool.h"
#include "util/runtime-profile.h"

#include "common/names.h"

static const char* USAGE =
    "Utility to decode an Impala profile log from standard input.\n"
    "\n"
    "The profile log is consumed from standard input and each successfully parsed entry"
    " is pretty-printed to standard output.\n"
    "\n"
    "Usage:"
    "  impala-profile-tool < impala_profile_log_1.1-1607057366897\n"
    "\n"
    "The following options are supported:\n"
    "Output options:\n"
    "--profile_format={text,json,prettyjson}: controls\n"
    "   text (default): pretty-print in the standard human readable format\n"
    "   json: output as JSON with one profile per line. Compatible with jsonlines.org\n"
    "   prettyjson: output as pretty-printed JSON array with one element per object\n"
    "--profile_verbosity={0,1,2,3,4,minimal,legacy,default,extended,full}: control"
    " verbosity of profile output. If not set, picks based on profile version\n"
    "\n"
    "Filtering options:\n"
    "--query_id=<query id>: given an impala query ID, only process profiles with this"
    " query id\n"
    "--min_timestamp=<integer timestamp>: only process profiles at or after this"
    " timestamp\n"
    "--max_timestamp=<integer timestamp>: only process profiles at or before this"
    " timestamp\n";

DEFINE_string(
    profile_format, "text", "Profile format to output: either text, json or prettyjson");
DEFINE_string(profile_verbosity, "", "Verbosity of profile output. Must be one of "
    "{0,1,2,3,4,minimal,legacy,default,extended,full}. If not set, picks based on "
    "version of each input profile.");
DEFINE_string(query_id, "", "Query ID to output profiles for");
DEFINE_int64(min_timestamp, -1, "Minimum timestamp (inclusive) to output profiles for");
DEFINE_int64(max_timestamp, -1, "Maximum timestamp (inclusive) to output profiles for");

using namespace impala;

using boost::algorithm::to_lower_copy;
using google::DescribeOneFlag;
using google::GetCommandLineFlagInfoOrDie;
using std::cerr;
using std::cin;
using std::cout;
using std::istringstream;

int main(int argc, char** argv) {
  google::SetUsageMessage(USAGE);
  google::ParseCommandLineFlags(&argc, &argv, true);

  string profile_format = to_lower_copy(FLAGS_profile_format);
  if (profile_format != "text" && profile_format != "json"
      && profile_format != "prettyjson") {
    cerr << "Invalid --profile_format value: '" << profile_format << "'\n\n"
         << DescribeOneFlag(GetCommandLineFlagInfoOrDie("profile_format"));
    return 1;
  }
  RuntimeProfileBase::Verbosity configured_verbosity =
      RuntimeProfileBase::Verbosity::DEFAULT;
  if (FLAGS_profile_verbosity != ""
      && !RuntimeProfileBase::ParseVerbosity(
             FLAGS_profile_verbosity, &configured_verbosity)) {
    cerr << "Invalid --profile_verbosity value: '" << FLAGS_profile_verbosity << "'\n\n"
         << DescribeOneFlag(GetCommandLineFlagInfoOrDie("profile_verbosity"));
    return 1;
  }

  if (profile_format == "prettyjson") cout << "[\n";
  int errors = 0;
  int profiles_emitted = 0;
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
    int32_t profile_version;
    Status status = RuntimeProfile::CreateFromArchiveString(
        encoded_profile, &pool, &profile, &profile_version);
    if (!status.ok()) {
      cerr << "Error parsing entry " << lineno << ": " << status.GetDetail() << "\n";
      ++errors;
      continue;
    }

    // Default verbosity depends on version - preserve legacy output for V1 profiles.
    RuntimeProfileBase::Verbosity verbosity = configured_verbosity;
    if (FLAGS_profile_verbosity == "") {
      verbosity = profile_version < 2 ? RuntimeProfileBase::Verbosity::LEGACY :
                                        RuntimeProfileBase::Verbosity::DEFAULT;
    }

    if (profile_format == "text") {
      profile->PrettyPrint(verbosity, &cout);
    } else if (profile_format == "json") {
      CHECK_EQ("json", profile_format);
      rapidjson::Document json_profile(rapidjson::kObjectType);
      profile->ToJson(verbosity, &json_profile);
      RuntimeProfile::JsonProfileToString(json_profile, /*pretty=*/false, &cout);
      cout << "\n"; // Each JSON document gets a separate line.
    } else {
      CHECK_EQ("prettyjson", profile_format);
      rapidjson::Document json_profile(rapidjson::kObjectType);
      profile->ToJson(verbosity, &json_profile);
      if (profiles_emitted > 0) cout << ",\n";
      RuntimeProfile::JsonProfileToString(json_profile, /*pretty=*/true, &cout);
      cout << "\n"; // Each JSON document starts on a new line.
    }
    ++profiles_emitted;
  }
  if (profile_format == "prettyjson") cout << "]\n";
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
