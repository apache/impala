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

#pragma once

#include <limits>
#include <string>
#include <unordered_set>

namespace impala {

class TValidWriteIdList;

class ValidWriteIdList {
public:
  enum RangeResponse {
    NONE, SOME, ALL
  };

  static bool IsCompacted(const std::string& file_path);
  static std::pair<int64_t, int64_t> GetWriteIdRange(const std::string& file_path);
  static int GetBucketProperty(const std::string& file_path);
  static int GetStatementId(const std::string& file_path);

  ValidWriteIdList() {}
  ValidWriteIdList(const TValidWriteIdList& valid_write_ids);

  void InitFrom(const TValidWriteIdList& valid_write_ids);

  bool IsWriteIdValid(int64_t write_id) const;
  RangeResponse IsWriteIdRangeValid(int64_t min_write_id, int64_t max_write_id) const;
  RangeResponse IsFileRangeValid(const std::string& file_path) const;
private:
  void AddInvalidWriteIds(const std::string& invalid_ids_str);
  int64_t high_water_mark_ = std::numeric_limits<int64_t>::max();
  std::unordered_set<int64_t> invalid_write_ids_;
};

}
