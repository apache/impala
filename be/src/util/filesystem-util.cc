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
#include <sys/resource.h>
#include <sys/stat.h>
#include <boost/filesystem.hpp>
#include <gutil/strings/substitute.h>

#include "util/filesystem-util.h"
#include "util/error-util.h"

#include "common/names.h"

namespace errc = boost::system::errc;
namespace filesystem = boost::filesystem;

using boost::system::error_code;
using std::exception;
using namespace strings;

// boost::filesystem functions must be given an errcode parameter to avoid the variants
// of those functions that throw exceptions.
namespace impala {

Status FileSystemUtil::CreateDirectory(const string& directory) {
  error_code errcode;
  bool exists = filesystem::exists(directory, errcode);
  // Need to check for no_such_file_or_directory error case - Boost's exists() sometimes
  // returns an error when it should simply return false.
  if (errcode != errc::success &&
      errcode != errc::no_such_file_or_directory) {
    return Status(ErrorMsg(TErrorCode::RUNTIME_ERROR, Substitute(
        "Encountered error checking existence of directory '0': $1", directory,
        errcode.message())));
  }
  if (exists) {
    // Attempt to remove the directory and its contents so that we can create a fresh
    // empty directory that we will have permissions for. There is an open window between
    // the check for existence above and the removal here. If the directory is removed in
    // this window, we may get "no_such_file_or_directory" error which is fine.
    //
    // There is a bug in boost library (as of version 1.6) which may lead to unexpected
    // exceptions even though we are using the no-exceptions interface. See IMPALA-2846.
    try {
      filesystem::remove_all(directory, errcode);
    } catch (filesystem::filesystem_error& e) {
      errcode = e.code();
    }
    if (errcode != errc::success &&
        errcode != errc::no_such_file_or_directory) {
      return Status(ErrorMsg(TErrorCode::RUNTIME_ERROR, Substitute("Encountered error "
          "removing directory '$0': $1", directory, errcode.message())));
    }
  }
  filesystem::create_directories(directory, errcode);
  if (errcode != errc::success) {
    return Status(ErrorMsg(TErrorCode::RUNTIME_ERROR, Substitute(
        "Encountered error creating directory '$0': $1", directory, errcode.message())));
  }
  return Status::OK();
}

Status FileSystemUtil::RemovePaths(const vector<string>& directories) {
  for (int i = 0; i < directories.size(); ++i) {
    error_code errcode;
    // There is a bug in boost library (as of version 1.6) which may lead to unexpected
    // exceptions even though we are using the no-exceptions interface. See IMPALA-2846.
    try {
      filesystem::remove_all(directories[i], errcode);
    } catch (filesystem::filesystem_error& e) {
      errcode = e.code();
    }
    if (errcode != errc::success) {
      return Status(ErrorMsg(TErrorCode::RUNTIME_ERROR, Substitute(
          "Encountered error removing directory $0: $1", directories[i],
          errcode.message())));
    }
  }

  return Status::OK();
}

Status FileSystemUtil::CreateFile(const string& file_path) {
  int fd = creat(file_path.c_str(), S_IRUSR | S_IWUSR);

  if (fd < 0) {
    return Status(ErrorMsg(TErrorCode::RUNTIME_ERROR,
        Substitute("Create file $0 failed with errno=$1 description=$2",
            file_path.c_str(), errno, GetStrErrMsg())));
  }

  int success = close(fd);
  if (success < 0) {
    return Status(ErrorMsg(TErrorCode::RUNTIME_ERROR,
        Substitute("Close file $0 failed with errno=$1 description=$2",
            file_path.c_str(), errno, GetStrErrMsg())));
  }

  return Status::OK();
}

Status FileSystemUtil::ResizeFile(const string& file_path, int64_t trunc_len) {
  int success = truncate(file_path.c_str(), trunc_len);
  if (success != 0) {
    return Status(ErrorMsg(TErrorCode::RUNTIME_ERROR, Substitute(
        "Truncate file $0 to length $1 failed with errno $2 ($3)",
        file_path, trunc_len, errno, GetStrErrMsg())));
  }

  return Status::OK();
}

Status FileSystemUtil::VerifyIsDirectory(const string& directory_path) {
  error_code errcode;
  bool exists = filesystem::exists(directory_path, errcode);
  if (errcode != errc::success) {
    return Status(ErrorMsg(TErrorCode::RUNTIME_ERROR, Substitute(
        "Encountered exception while verifying existence of directory path $0: $1",
        directory_path, errcode.message())));
  }
  if (!exists) {
    return Status(ErrorMsg(TErrorCode::RUNTIME_ERROR, Substitute(
        "Directory path $0 does not exist", directory_path)));
  }
  bool is_dir = filesystem::is_directory(directory_path, errcode);
  if (errcode != errc::success) {
    return Status(ErrorMsg(TErrorCode::RUNTIME_ERROR, Substitute(
        "Encountered exception while verifying existence of directory path $0: $1",
        directory_path, errcode.message())));
  }
  if (!is_dir) {
    return Status(ErrorMsg(TErrorCode::RUNTIME_ERROR, Substitute(
        "Path $0 is not a directory", directory_path)));
  }
  return Status::OK();
}

Status FileSystemUtil::GetSpaceAvailable(const string& directory_path,
    uint64_t* available_bytes) {
  error_code errcode;
  filesystem::space_info info = filesystem::space(directory_path, errcode);
  if (errcode != errc::success) {
    return Status(ErrorMsg(TErrorCode::RUNTIME_ERROR, Substitute(
        "Encountered exception while checking available space for path $0: $1",
        directory_path, errcode.message())));
  }
  *available_bytes = info.available;
  return Status::OK();
}

uint64_t FileSystemUtil::MaxNumFileHandles() {
  struct rlimit data;
  if (getrlimit(RLIMIT_NOFILE, &data) == 0) return static_cast<uint64_t>(data.rlim_cur);
  return 0ul;
}

} // namespace impala
