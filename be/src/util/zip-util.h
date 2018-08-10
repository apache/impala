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

#ifndef IMPALA_UTIL_ZIP_UTIL_H
#define IMPALA_UTIL_ZIP_UTIL_H

#include <string>

#include <jni.h>

#include "common/status.h"

namespace impala {

class ZipUtil {
 public:
  /// Loads the JNI helper method to extract files from a Zip archive.
  static void InitJvm();

  /// Extract files from a zip archive to a destination directory in local filesystem.
  static Status ExtractFiles(const std::string& archive_file,
      const std::string& destination_dir) WARN_UNUSED_RESULT;

 private:
  static jclass zip_util_class_;
  static jmethodID extract_files_method;
};

}
#endif
