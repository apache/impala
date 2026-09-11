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

#include <map>
#include <string>

#include "common/status.h"

namespace impala {

/// Process-global S3 credentials from the --s3a_access_key_cmd / --s3a_secret_key_cmd
/// startup flags (each flag names a shell command whose stdout is the credential value).
///
/// Init() is called once at daemon startup.  Get() returns the resulting config map
/// for use as the HdfsFsCache::GetConnection() fallback credential when no per-query
/// vended credential covers the requested path.
class S3ConnCredentials {
 public:
  /// Runs both commands (if set) and stores the resulting fs.s3a.access.key /
  /// fs.s3a.secret.key in global_config_.  No-op when neither flag is set, when only
  /// one is set, or when a command prints nothing (the latter two log a warning and
  /// leave the Hadoop client configuration in charge); error if a command fails.
  static Status Init();

  /// Returns the process-global S3 credential config (empty when no flags were set).
  /// Valid after Init().  The returned reference is stable for the lifetime of the
  /// process (written once at startup, never modified again).
  static const std::map<std::string, std::string>& Get() { return global_config_; }

 private:
  static std::map<std::string, std::string> global_config_;
};

} // namespace impala
