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

#include <fcntl.h>
#include <sys/stat.h>
#include <boost/filesystem.hpp>
#include <gutil/strings/substitute.h>

#include "util/filesystem-util.h"
#include "util/error-util.h"

using namespace std;
using namespace strings;
// boost::filesystem functions, which may throw exceptions, are used with their
// fully-qualified names to explicitly distinguish them from unix system
// functions that do not throw exceptions.

namespace impala {

Status FileSystemUtil::CreateDirectories(const vector<string>& directories) {
  for (int i = 0; i < directories.size(); ++i) {
    // Remove the directory and it's contents if it exists. Ignore the error
    // that occurs if the directory doesn't exist. Only report the error from
    // create directory.
    try {
      boost::filesystem::remove_all(directories[i]);
    } catch (exception& e) {
    }
    try {
      boost::filesystem::create_directory(directories[i]);
    } catch (exception& e) {
      return Status(TStatusCode::RUNTIME_ERROR, Substitute(
          "Encountered error creating directory $0: $1", directories[i], e.what()));
    }
  }

  return Status::OK;
}

Status FileSystemUtil::RemovePaths(const vector<string>& directories) {
  for (int i = 0; i < directories.size(); ++i) {
    try {
        boost::filesystem::remove_all(directories[i]);
    } catch (exception& e) {
      return Status(TStatusCode::RUNTIME_ERROR, Substitute(
          "Encountered error removing directory $0: $1", directories[i], e.what()));
    }
  }

  return Status::OK;
}

Status FileSystemUtil::CreateFile(const string& file_path) {
  int fd = creat(file_path.c_str(), S_IRUSR | S_IWUSR);

  if (fd < 0) {
    return Status(TStatusCode::RUNTIME_ERROR,
        Substitute("Create file $0 failed with errno=$1 description=$2",
            file_path.c_str(), errno, GetStrErrMsg()));
  }

  int success = close(fd);
  if (success < 0) {
    return Status(TStatusCode::RUNTIME_ERROR,
        Substitute("Close file $0 failed with errno=$1 description=$2",
            file_path.c_str(), errno, GetStrErrMsg()));
  }

  return Status::OK;
}

Status FileSystemUtil::ResizeFile(const string& file_path, int64_t trunc_len) {
  int success = truncate(file_path.c_str(), trunc_len);
  if (success != 0) {
    return Status(TStatusCode::RUNTIME_ERROR, Substitute(
        "Truncate file $0 to length $1 failed with errno $2 ($3)",
        file_path, trunc_len, errno, GetStrErrMsg()));
  }

  return Status::OK;
}

Status FileSystemUtil::VerifyIsDirectory(const string& directory_path) {
  try {
    if (!boost::filesystem::exists(directory_path)) {
      return Status(TStatusCode::RUNTIME_ERROR, Substitute(
          "Directory path $0 does not exist", directory_path));
    }
  } catch (exception& e) {
    return Status(TStatusCode::RUNTIME_ERROR, Substitute(
        "Encountered exception while verifying existence of directory path $0: $1",
        directory_path, e.what()));
  }
  if (!boost::filesystem::is_directory(directory_path)) {
    return Status(TStatusCode::RUNTIME_ERROR, Substitute(
        "Path $0 is not a directory", directory_path));
  }
  return Status::OK;
}

Status FileSystemUtil::GetSpaceAvailable(const string& directory_path,
    uint64_t* available_bytes) {
  try {
    boost::filesystem::space_info info = boost::filesystem::space(directory_path);
    *available_bytes = info.available;
  } catch (exception& e) {
    return Status(TStatusCode::RUNTIME_ERROR, Substitute(
        "Encountered exception while checking available space for path $0: $1",
        directory_path, e.what()));
  }

  return Status::OK;
}

} // namespace impala
