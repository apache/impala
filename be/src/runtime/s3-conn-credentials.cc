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

#include "runtime/s3-conn-credentials.h"

#include <map>
#include <string>

#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "util/os-util.h"

#include "common/names.h"

using namespace strings;

namespace impala {

DEFINE_string(s3a_access_key_cmd, "", "A Unix command whose output returns the "
    "access key to S3, i.e. \"fs.s3a.access.key\".");

DEFINE_string(s3a_secret_key_cmd, "", "A Unix command whose output returns the "
    "secret key to S3, i.e. \"fs.s3a.secret.key\".");

map<string, string> S3ConnCredentials::global_config_;

Status S3ConnCredentials::Init() {
  // Both commands must be provided together, or neither.
  if (FLAGS_s3a_access_key_cmd.empty() && FLAGS_s3a_secret_key_cmd.empty()) {
    return Status::OK();
  }
  if (FLAGS_s3a_access_key_cmd.empty() || FLAGS_s3a_secret_key_cmd.empty()) {
    LOG(WARNING) << "Only one of --s3a_access_key_cmd and --s3a_secret_key_cmd is set; "
                 << "both are required. Ignoring them and using the Hadoop client "
                 << "configuration for S3 credentials.";
    return Status::OK();
  }

  string access_key;
  if (!RunShellProcess(FLAGS_s3a_access_key_cmd, &access_key, true,
      {"JAVA_TOOL_OPTIONS"})) {
    return Status(Substitute("Could not run command '$0' to retrieve S3 Access Key. "
        "Impala will not be able to access S3.", FLAGS_s3a_access_key_cmd));
  }
  LOG(INFO) << "S3 Access Key retrieval command '" << FLAGS_s3a_access_key_cmd
            << "' executed successfully.";

  string secret_key;
  if (!RunShellProcess(FLAGS_s3a_secret_key_cmd, &secret_key, true,
      {"JAVA_TOOL_OPTIONS"})) {
    return Status(Substitute("Could not run command '$0' to retrieve S3 Secret Key. "
        "Impala will not be able to access S3.", FLAGS_s3a_secret_key_cmd));
  }
  LOG(INFO) << "S3 Secret Key retrieval command '" << FLAGS_s3a_secret_key_cmd
            << "' executed successfully.";

  // A command that succeeds but prints nothing must not override the credentials in the
  // Hadoop client configuration with empty ones, which S3A rejects outright.
  if (access_key.empty() || secret_key.empty()) {
    LOG(WARNING) << "The S3 " << (access_key.empty() ? "Access" : "Secret")
                 << " Key retrieval command returned an empty value. Ignoring both "
                 << "--s3a_access_key_cmd and --s3a_secret_key_cmd and using the Hadoop "
                 << "client configuration for S3 credentials.";
    return Status::OK();
  }

  global_config_ = {
      {"fs.s3a.access.key", access_key},
      {"fs.s3a.secret.key", secret_key}
  };
  return Status::OK();
}

} // namespace impala
