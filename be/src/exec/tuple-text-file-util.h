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

#include "common/status.h"

namespace impala {

/// Utility class for verifying the tuple text files.
class TupleTextFileUtil {
 public:
  /// Verify whether two debug dump caches are the same.
  /// If the files are with different sizes or not able to open the files, will return
  /// an error status to stop the query.
  /// If the files content are different, return status Ok but set the passed as false
  /// to allow further comparison.
  /// If files are the same or there is no need for a comparison, like verification is
  /// off or cache doesn't exist, return status Ok and set the passed as true.
  static Status VerifyDebugDumpCache(
      const std::string& file_name, const std::string& ref_file_name, bool* passed);

  /// Slow path to varify the debug dump caches row by row.
  static Status VerifyRows(const string& cmp_file_path, const string& ref_file_path);
};
} // namespace impala
