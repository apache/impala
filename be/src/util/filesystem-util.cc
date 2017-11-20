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

#include <fcntl.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <boost/filesystem.hpp>
#include <gutil/strings/substitute.h>

#include "runtime/io/error-converter.h"
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

Status FileSystemUtil::RemoveAndCreateDirectory(const string& directory) {
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

Status FileSystemUtil::GetCanonicalPath(const string& file_path, string* canonical_path) {
  DCHECK(canonical_path != nullptr);
  char rp[PATH_MAX];
  if (realpath(file_path.c_str(), rp) == nullptr) {
    return Status(ErrorMsg(TErrorCode::RUNTIME_ERROR,
        Substitute("Resolving path for $0 failed with errno=$1 description=$2",
            file_path.c_str(), errno, GetStrErrMsg())));
  }
  *canonical_path = rp;
  return Status::OK();
}

bool FileSystemUtil::IsCanonicalPath(const string& path) {
  return !path.empty()
      && path.front() == '/'
      && (path.length() == 1 || path.back() != '/')
      && (path.length() == 1 || strcmp(path.c_str() + path.length() - 2, "/.") != 0)
      && (path.length() <= 2 || strcmp(path.c_str() + path.length() - 3, "/..") != 0)
      && strstr(path.c_str(), "//") == nullptr
      && strstr(path.c_str(), "/./") == nullptr
      && strstr(path.c_str(), "/../") == nullptr;
}

Status FileSystemUtil::IsSymbolicLink(const string& file_path, bool* is_symbolic_link,
    string* canonical_path) {
  DCHECK(is_symbolic_link != nullptr);
  DCHECK(canonical_path != nullptr);
  struct stat sb;
  if (lstat(file_path.c_str(), &sb) == -1) {
    return Status(ErrorMsg(TErrorCode::RUNTIME_ERROR,
        Substitute("Getting file status for $0 failed with errno=$1 description=$2",
            file_path.c_str(), errno, GetStrErrMsg())));
  }

  *is_symbolic_link = S_ISLNK(sb.st_mode) && sb.st_size > 0;
  if (*is_symbolic_link) {
    return GetCanonicalPath(file_path, canonical_path);
  }
  return Status::OK();
}

bool FileSystemUtil::IsPrefixPath(const string& prefix, const string& path) {
  DCHECK(IsCanonicalPath(prefix));
  DCHECK(IsCanonicalPath(path));
  return !prefix.empty()
      && (prefix == path
          || (prefix.length() < path.length()
              && strncmp(prefix.c_str(), path.c_str(), prefix.length()) == 0
              && (prefix.back() == '/' || path[prefix.length()] == '/')));
}

bool FileSystemUtil::GetRelativePath(const string& path, const string& start,
    string* relpath) {
  DCHECK(IsCanonicalPath(path));
  DCHECK(IsCanonicalPath(start));
  DCHECK(relpath != nullptr);
  if (IsPrefixPath(start, path)) {
    *relpath = path.substr(start.length());
    if (!relpath->empty() && relpath->front() == '/') *relpath = relpath->substr(1);
    return true;
  }
  return false;
}


FileSystemUtil::Directory::Directory(const string& path)
    : dir_path_(path),
      status_(Status::OK()) {
  dir_stream_ = opendir(dir_path_.c_str());
  if (dir_stream_ == nullptr) {
    status_ = ErrorConverter::GetErrorStatusFromErrno("opendir()", dir_path_, errno);
  }
}

FileSystemUtil::Directory::~Directory() {
  if (dir_stream_ != nullptr) (void)closedir(dir_stream_);
}

bool FileSystemUtil::Directory::GetNextEntryName(string* entry_name) {
  DCHECK(entry_name != nullptr);

  if (status_.ok()) {
    DCHECK(dir_stream_ != nullptr);

    errno = 0;
    const dirent* dir_entry = nullptr;
    while ((dir_entry = readdir(dir_stream_)) != nullptr) {
      if (dir_entry->d_name[0] == 0 || strcmp(dir_entry->d_name, ".") == 0
          || strcmp(dir_entry->d_name, "..") == 0) {
        continue;
      }
      *entry_name = dir_entry->d_name;
      return true;
    }

    // readdir() returned nullptr:
    // Either readdir() failed or no more entries were found in 'dir_stream'.
    if (errno != 0) {
      status_ = ErrorConverter::GetErrorStatusFromErrno(
          "readdir()", dir_path_, errno);
    }
  }
  return false;
}

Status FileSystemUtil::Directory::GetEntryNames(const string& path,
    vector<string>* entry_names, int max_result_size) {
  DCHECK(entry_names != nullptr);

  Directory dir(path);
  entry_names->clear();
  string entry_name;
  while ((max_result_size <= 0 || entry_names->size() < max_result_size)
      && dir.GetNextEntryName(&entry_name)) {
    entry_names->push_back(entry_name);
  }

  return dir.GetLastStatus();
}

} // namespace impala
