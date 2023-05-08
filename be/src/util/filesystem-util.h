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

#ifndef IMPALA_UTIL_FILESYSTEM_UTIL_H
#define IMPALA_UTIL_FILESYSTEM_UTIL_H

#include <dirent.h>
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
  static Status RemoveAndCreateDirectory(const std::string& directory) WARN_UNUSED_RESULT;

  /// Create a file at the specified path.
  static Status CreateFile(const std::string& file_path) WARN_UNUSED_RESULT;

  /// Remove the specified paths and their enclosing files/directories.
  static Status RemovePaths(
      const std::vector<std::string>& directories) WARN_UNUSED_RESULT;

  /// Verify that the specified path is an existing directory.
  /// Returns Status::OK if it is, or a runtime error with a message otherwise.
  static Status VerifyIsDirectory(const std::string& directory_path) WARN_UNUSED_RESULT;

  /// Check if a path exists. Returns an error if this could not be determined.
  static Status PathExists(const std::string& path, bool* exists);

  /// Returns the space available on the file system containing 'directory_path'
  /// in 'available_bytes'
  static Status GetSpaceAvailable(
      const std::string& directory_path, uint64_t* available_bytes) WARN_UNUSED_RESULT;

  /// Returns the currently allowed maximum of possible file descriptors. In case of an
  /// error returns 0.
  static uint64_t MaxNumFileHandles();

  /// Finds the canonicalized absolute pathname for 'file_path' and returns it in
  /// *canonical_path.
  static Status GetCanonicalPath(
      const std::string& file_path, std::string* canonical_path) WARN_UNUSED_RESULT;

  /// Checks if 'file_path' is a symbolic link. If it is, 'is_symbolic_link' is set to
  /// 'true' and *canonical_path is set to the resolved canonicalized path.
  static Status IsSymbolicLink(const std::string& file_path, bool* is_symbolic_link,
      std::string* canonical_path) WARN_UNUSED_RESULT;

  /// Returns 'true' iff 'path' is a canonicalized path. 'path' doesn't have to be an
  /// existing path.
  /// Always returns 'true' for the *canonical_path returned by GetCanonicalPath() and
  /// IsSymbolicLink().
  static bool IsCanonicalPath(const std::string& path);

  /// Returns 'true' iff path 'prefix' is a non-empty prefix of path 'path'.
  /// This is a string computation: the filesystem is not accessed to confirm the
  /// existance of 'path' or 'prefix'. It is assumed that 'prefix' and 'path' are both
  /// canonicalized paths.
  static bool IsPrefixPath(const std::string& prefix, const std::string& path);

  /// - If 'start' is a prefix of 'path', it constructs relative filepath to 'path' from
  /// the 'start' directory and sets 'relpath' to the resulting path. 'true' is returned.
  /// - Otherwise, 'relpath' is left intact  and 'false' is returned.
  /// This is a string computation: the filesystem is not accessed to confirm the
  /// existance of 'path' or 'start'. It is assumed that 'path' and 'start' are both
  /// canonicalized paths.
  static bool GetRelativePath(const std::string& path, const std::string& start,
      std::string* relpath);

  /// Finds the first file matching the supplied 'regex' in 'path', or returns empty
  /// string; matches the behavior of paths specified in Java CLASSPATH for JARs.
  /// If 'path' is a file, returns 'path' if 'regex' matches its filename.
  /// If 'path' is a directory, returns the absolute path for a match if 'regex' matches
  /// any files in that directory.
  /// If 'path' ends with a wildcard - as in "/lib/*" - it will treat path as a directory
  /// excluding the wildcard.
  static std::string FindFileInPath(string path, const std::string& regex);

  /// Ext filesystem on certain kernel versions may result in inconsistent metadata after
  /// punching holes in files. The filesystem may require fsck repair on next reboot.
  /// See KUDU-1508 for details. This function checks if the filesystem at 'path' resides
  /// in a ext filesystem and the kernel version is affected by KUDU-1058. If so, return
  /// error status; Returns OK otherwise.
  static Status CheckForBuggyExtFS(const std::string& path);

  /// Checks if the filesystem at the directory 'path' supports hole punching (i.e.
  /// calling fallocate with FALLOC_FL_PUNCH_HOLE).
  ///
  /// Return error status if:
  /// - 'path' resides in a ext filesystem and the kernel version is vulnerable to
  ///    KUDU-1508.
  /// - creating a test file at 'path' failed.
  /// - punching holes in test file failed.
  /// - reading the test file's size failed.
  ///
  /// Returns OK otherwise.
  static Status CheckHolePunch(const std::string& path);

  /// Return the approximate file size of 'path' into output argument 'file_size', based
  /// on what file system sees.
  static Status ApproximateFileSize(const std::string& path, uintmax_t& file_size);

  class Directory {
   public:
    // Different types of entry in the directory
    enum EntryType {
      DIR_ENTRY_ANY = 0,
      DIR_ENTRY_REG, // regular file (DT_REG in readdir() result)
      DIR_ENTRY_DIR, // directory    (DT_DIR in readdir() result)
      DIR_ENTRY_NUM_TYPES
    };

    /// Opens 'path' directory for iteration. Directory entries "." and ".." will be
    /// skipped while iterating through the entries.
    Directory(const string& path);

    /// Closes the directory.
    ~Directory();

    /// Reads the next directory entry and sets 'entry_name' to the entry name.
    /// Returns false if an error occured or no more entries were found in the directory.
    /// If 'type' is specified and filesystem supports returning the types of directory
    /// entries, only entries of 'type' will be included. Otherwise, it may return
    /// entries of all types. Return 'true' on success.
    bool GetNextEntryName(std::string* entry_name, EntryType type = DIR_ENTRY_ANY);

    /// Returns the status of the previous directory operation.
    const Status& GetLastStatus() const { return status_; }

    /// Reads no more than 'max_result_size' directory entries from 'path' and returns
    /// their names in 'entry_names' vector. If 'max_result_size' <= 0, every directory
    /// entry is returned. Directory entries "." and ".." will be skipped. If 'type' is
    /// specified and filesystem of 'path' supports returning type of directory entries,
    /// only entries of 'type' will be included in 'entry_names'. Otherwise, it will
    /// include entries of all types. Optionally, a 'regex' can be specified. If the
    /// regex is not empty, only entries that match the regex are returned. See
    /// std::regex for the regex pattern syntax.
    static Status GetEntryNames(const string& path, std::vector<std::string>* entry_names,
        int max_result_size = 0, EntryType type = DIR_ENTRY_ANY,
        const std::string& regex = "");

   private:
    DIR* dir_stream_;
    std::string dir_path_;
    Status status_;

    // Do not allow making copies.
    Directory(const Directory&);
    Directory& operator=(const Directory&);
  };

 private:

  /// This function returns true iff the kernel version Impala is running on
  /// is affected by KUDU-1508.
  static bool IsBuggyEl6Kernel();
};

}

#endif
