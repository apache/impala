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

#ifndef IMPALA_UTIL_FILESYSTEM_UTIL_H
#define IMPALA_UTIL_FILESYSTEM_UTIL_H

#include "common/status.h"

namespace impala {

/// Utility class for common local file system operations such as file creation and
/// deletion. This class should NOT be used to read or write data (DiskIoMgr is used
/// for that). Errors are indicated by the status code RUNTIME_ERROR, and are not
/// handled via exceptions.
class FileSystemUtil {
 public:
  /// Create the specified directory and any ancestor directories that do not exist yet.
  /// The directory and its contents are destroyed if it already exists.
  /// Returns Status::OK if successful, or a runtime error with a message otherwise.
  static Status CreateDirectory(const std::string& directory);

  /// Create a file at the specified path.
  static Status CreateFile(const std::string& file_path);

  /// Resize a file to a specified length - uses unistd truncate().
  static Status ResizeFile(const std::string& file_path, int64_t trunc_len);

  /// Remove the specified paths and their enclosing files/directories.
  static Status RemovePaths(const std::vector<std::string>& directories);

  /// Verify that the specified path is an existing directory.
  /// Returns Status::OK if it is, or a runtime error with a message otherwise.
  static Status VerifyIsDirectory(const std::string& directory_path);

  /// Returns the space available on the file system containing 'directory_path'
  /// in 'available_bytes'
  static Status GetSpaceAvailable(const std::string& directory_path,
      uint64_t* available_bytes);

  /// Returns the currently allowed maximum of possible file descriptors. In case of an
  /// error returns 0.
  static uint64_t MaxNumFileHandles();
};

}

#endif
