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
  if (slash_before_file <= 0) return "";
  int slash_before_dirname = filepath.rfind('/', slash_before_file - 1);
  if (slash_before_dirname <= 0) return "";
  return filepath.substr(
      slash_before_dirname + 1, slash_before_file - slash_before_dirname - 1);
}

inline bool StrStartsWith(const string& str, const string& prefix) {
  return str.rfind(prefix, 0) == 0;
}

std::pair<int64_t, int64_t> GetWriteIdRangeOfDeltaDir(const string& delta_dir) {
  int min_write_id_pos = 0;
  if (StrStartsWith(delta_dir, DELTA_PREFIX)) {
    min_write_id_pos = DELTA_PREFIX.size();
  }
  else if (StrStartsWith(delta_dir, DELETE_DELTA_PREFIX)) {
    min_write_id_pos = DELETE_DELTA_PREFIX.size();
  } else {
    DCHECK(false) << delta_dir + " is not a delta directory";
  }
  int max_write_id_pos = delta_dir.find('_', min_write_id_pos) + 1;
  return {std::atoll(delta_dir.c_str() + min_write_id_pos),
          std::atoll(delta_dir.c_str() + max_write_id_pos)};
}

} // unnamed namespace

ValidWriteIdList::ValidWriteIdList(const TValidWriteIdList& valid_write_ids) {
  InitFrom(valid_write_ids);
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
  string dir_name = GetParentDirName(file_path);
  if (!(StrStartsWith(dir_name, DELTA_PREFIX) ||
        StrStartsWith(dir_name, DELETE_DELTA_PREFIX))) {
    return ALL;
  }
  std::pair<int64_t, int64_t> write_id_range = GetWriteIdRangeOfDeltaDir(dir_name);
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
