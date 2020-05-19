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

#include <regex>

#include "exec/acid-metadata-utils.h"

#include "common/logging.h"
#include "gen-cpp/CatalogObjects_types.h"

#include "common/names.h"

namespace impala {

/// Unnamed namespace for auxiliary functions.
namespace {

const string BASE_PREFIX = "base_";
const string DELTA_PREFIX = "delta_";
const string DELETE_DELTA_PREFIX = "delete_delta_";

string GetParentDirName(const string& filepath) {
  int slash_before_file = filepath.rfind('/');
  if (slash_before_file == string::npos) return "";
  int slash_before_dirname = filepath.rfind('/', slash_before_file - 1);
  if (slash_before_dirname == string::npos) return "";
  return filepath.substr(
      slash_before_dirname + 1, slash_before_file - slash_before_dirname - 1);
}

inline string GetFileName(const string& filepath) {
  std::size_t slash_before_file = filepath.rfind('/');
  if (slash_before_file == string::npos) return filepath;
  return filepath.substr(slash_before_file + 1);
}

inline bool StrStartsWith(const string& str, const string& prefix) {
  return str.rfind(prefix, 0) == 0;
}

} // unnamed namespace

ValidWriteIdList::ValidWriteIdList(const TValidWriteIdList& valid_write_ids) {
  InitFrom(valid_write_ids);
}

std::pair<int64_t, int64_t> ValidWriteIdList::GetWriteIdRange(const string& file_path) {
  string dir_name = GetParentDirName(file_path);
  if (!(StrStartsWith(dir_name, DELTA_PREFIX) ||
        StrStartsWith(dir_name, DELETE_DELTA_PREFIX) ||
        StrStartsWith(dir_name, BASE_PREFIX))) {
    // Write ids of original files are 0.
    return {0, 0};
  }
  int min_write_id_pos = 0;
  if (StrStartsWith(dir_name, DELTA_PREFIX)) {
    min_write_id_pos = DELTA_PREFIX.size();
  }
  else if (StrStartsWith(dir_name, DELETE_DELTA_PREFIX)) {
    min_write_id_pos = DELETE_DELTA_PREFIX.size();
  } else {
    StrStartsWith(dir_name, BASE_PREFIX);
    int write_id_pos = BASE_PREFIX.size();
    int64_t write_id = std::atoll(dir_name.c_str() + write_id_pos);
    return {write_id, write_id};
  }
  int max_write_id_pos = dir_name.find('_', min_write_id_pos) + 1;
  return {std::atoll(dir_name.c_str() + min_write_id_pos),
          std::atoll(dir_name.c_str() + max_write_id_pos)};
}

int ValidWriteIdList::GetStatementId(const std::string& file_path) {
  string dir_name = GetParentDirName(file_path);
  // Only delta directories can have a statement id.
  if (StrStartsWith(dir_name, BASE_PREFIX)) return 0;
  // Expected number of '_' if statement id is present.
  int expected_underscores = 0;
  if (StrStartsWith(dir_name, DELTA_PREFIX)) {
    expected_underscores = 3;
  } else if (StrStartsWith(dir_name, DELETE_DELTA_PREFIX)) {
    expected_underscores = 4;
  } else {
    return 0;
  }
  int count_underscores = std::count(dir_name.begin(), dir_name.end(), '_');
  if (count_underscores != expected_underscores || dir_name.find("_v") != string::npos) {
    return 0;
  }
  int last_underscore_pos = dir_name.rfind('_');
  return std::atoi(dir_name.c_str() + last_underscore_pos + 1);
}

int ValidWriteIdList::GetBucketProperty(const std::string& file_path) {
  static const std::regex ORIGINAL_PATTERN("[0-9]+_[0-9]+(_copy_[0-9]+)?");
  static const std::regex BUCKET_PATTERN("bucket_([0-9]+)(_[0-9]+)?");

  string filename = GetFileName(file_path);
  int bucket_id = 0;
  if (std::regex_match(filename, ORIGINAL_PATTERN)) {
    bucket_id = std::atoi(filename.c_str());
  } else if (std::regex_match(filename, BUCKET_PATTERN)) {
    bucket_id = std::atoi(filename.c_str() + sizeof("bucket_"));
  } else {
    return -1;
  }
  int statement_id = GetStatementId(file_path);

  constexpr int BUCKET_CODEC_VERSION = 1;
  constexpr int NUM_BUCKET_ID_BITS = 12;
  constexpr int NUM_STATEMENT_ID_BITS = 12;
  return BUCKET_CODEC_VERSION << (1 + NUM_BUCKET_ID_BITS + 4 + NUM_STATEMENT_ID_BITS) |
         bucket_id << (4 + NUM_STATEMENT_ID_BITS) |
         statement_id;
}

void ValidWriteIdList::InitFrom(const TValidWriteIdList& valid_write_ids) {
  if (valid_write_ids.__isset.high_watermark) {
    high_water_mark_ = valid_write_ids.high_watermark;
  } else {
    high_water_mark_ = std::numeric_limits<int64_t>::max();
  }
  invalid_write_ids_.clear();
  for (int64_t invalid_write_id : valid_write_ids.invalid_write_ids) {
    invalid_write_ids_.insert(invalid_write_id);
  }
}

bool ValidWriteIdList::IsWriteIdValid(int64_t write_id) const {
  if (write_id > high_water_mark_) {
    return false;
  }
  return invalid_write_ids_.find(write_id) == invalid_write_ids_.end();
}

ValidWriteIdList::RangeResponse ValidWriteIdList::IsWriteIdRangeValid(
    int64_t min_write_id, int64_t max_write_id) const {
  if (max_write_id <= high_water_mark_ && invalid_write_ids_.empty()) return ALL;

  bool found_valid = false;
  bool found_invalid = false;
  for (int64_t i = min_write_id; i <= max_write_id; ++i) {
    if (IsWriteIdValid(i)) {
      found_valid = true;
    } else {
      found_invalid = true;
    }
    if (found_valid && found_invalid) return SOME;
  }
  if (found_invalid) return NONE;
  return ALL;
}

ValidWriteIdList::RangeResponse ValidWriteIdList::IsFileRangeValid(
    const std::string& file_path) const {
  std::pair<int64_t, int64_t> write_id_range = GetWriteIdRange(file_path);
  // In base and original directories everything is valid.
  if (write_id_range == std::make_pair<int64_t, int64_t>(0, 0)) return ALL;
  return IsWriteIdRangeValid(write_id_range.first, write_id_range.second);
}

bool ValidWriteIdList::IsCompacted(const std::string& file_path) {
  string dir_name = GetParentDirName(file_path);
  if (!StrStartsWith(dir_name, BASE_PREFIX) &&
      !StrStartsWith(dir_name, DELTA_PREFIX) &&
      !StrStartsWith(dir_name, DELETE_DELTA_PREFIX)) return false;
  return dir_name.find("_v") != string::npos;
}

} // namespace impala
