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

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/resource.h>
#include <sys/stat.h>

#include <memory>
#include <regex>
#include <string>
#include <vector>

#include <boost/algorithm/string/trim.hpp>
#include <boost/filesystem.hpp>

#include "common/status.h"
#include "gen-cpp/ErrorCodes_types.h"
#include "gutil/macros.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/substitute.h"
#include "gutil/strings/util.h"
#include "kudu/util/env.h"
#include "kudu/util/path_util.h"
#include "kudu/util/slice.h"
#include "runtime/io/error-converter.h"
#include "util/debug-util.h"
#include "util/error-util.h"
#include "util/filesystem-util.h"
#include "util/kudu-status-util.h"
#include "util/scope-exit-trigger.h"
#include "util/uid-util.h"

#ifndef FALLOC_FL_PUNCH_HOLE
#include <linux/falloc.h>
#endif

#include "common/names.h"

namespace errc = boost::system::errc;
namespace filesystem = boost::filesystem;

using boost::system::error_code;
using kudu::Env;
using kudu::JoinPathSegments;
using kudu::RWFile;
using kudu::Slice;
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
    // Since unexpected exceptions from the no-exceptions interface (Ticket #7307) was
    // fixed in boost 1.63.0, revert the code change made by IMPALA-2846 + IMPALA-9571
    // when upgrading boost to 1.74.0.
    filesystem::remove_all(directory, errcode);
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
    filesystem::remove_all(directories[i], errcode);
    if (errcode != errc::success) {
      return Status(ErrorMsg(TErrorCode::RUNTIME_ERROR, Substitute(
          "Encountered error removing directory $0: $1", directories[i],
          errcode.message())));
    }
  }

  return Status::OK();
}

Status FileSystemUtil::CreateFile(const string& file_path) {
  int fd;
  RETRY_ON_EINTR(fd, creat(file_path.c_str(), S_IRUSR | S_IWUSR));

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

Status FileSystemUtil::PathExists(const std::string& path, bool* exists) {
  error_code errcode;
  *exists = filesystem::exists(path, errcode);
  if (errcode != errc::success) {
    // Need to check for no_such_file_or_directory error case - Boost's exists() sometimes
    // returns an error when it should simply return false.
    if (errcode == errc::no_such_file_or_directory) {
      *exists = false;
      return Status::OK();
    }
    return Status(ErrorMsg(TErrorCode::RUNTIME_ERROR, Substitute(
        "Encountered exception while checking existence of path $0: $1",
        path, errcode.message())));
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

std::string FileSystemUtil::FindFileInPath(string path, const std::string& regex) {
  // Wildcard at the end matches any file, so trim and treat as a directory.
  boost::algorithm::trim_right_if(path, boost::algorithm::is_any_of("*"));
  if (path.empty()) return "";
  if (!filesystem::exists(path)) return "";
  std::regex pattern{regex};
  if (filesystem::is_directory(path)) {
    for (filesystem::directory_entry& child : filesystem::directory_iterator(path)) {
      // If child matches pattern, use it
      if (std::regex_match(child.path().filename().string(), pattern)) {
        return child.path().string();
      }
    }
  } else if (std::regex_match(filesystem::path(path).filename().string(), pattern)) {
    return path;
  }
  return "";
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

bool FileSystemUtil::Directory::GetNextEntryName(string* entry_name, EntryType type) {
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
      if (type != DIR_ENTRY_ANY && dir_entry->d_type != DT_UNKNOWN) {
        if (type == DIR_ENTRY_REG && dir_entry->d_type != DT_REG) {
          continue;
        }
        if (type == DIR_ENTRY_DIR && dir_entry->d_type != DT_DIR) {
          continue;
        }
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
    vector<string>* entry_names, int max_result_size, EntryType type,
    const string& regex) {
  DCHECK(entry_names != nullptr);

  Directory dir(path);
  entry_names->clear();
  std::regex entry_regex(regex);
  string entry_name;
  while ((max_result_size <= 0 || entry_names->size() < max_result_size) &&
         dir.GetNextEntryName(&entry_name, type)) {
    // If there is a regex and the regex doesn't match, skip this entry
    if (regex.size() != 0 && !std::regex_match(entry_name, entry_regex)) continue;
    entry_names->push_back(entry_name);
  }

  return dir.GetLastStatus();
}

// Copied and pasted from Kudu source: src/kudu/fs/log_block_manager.cc
bool FileSystemUtil::IsBuggyEl6Kernel() {
  const string& kernel_release = kudu::Env::Default()->GetKernelRelease();
  autodigit_less lt;

  // Only el6 is buggy.
  if (kernel_release.find("el6") == string::npos) return false;

  // Kernels in the 6.8 update stream (2.6.32-642.a.b) are fixed
  // for a >= 15.
  //
  // https://rhn.redhat.com/errata/RHSA-2017-0307.html
  if (MatchPattern(kernel_release, "2.6.32-642.*.el6.*") &&
      lt("2.6.32-642.15.0", kernel_release)) {
    return false;
  }

  // If the kernel older is than 2.6.32-674 (el6.9), it's buggy.
  return lt(kernel_release, "2.6.32-674");
}

Status FileSystemUtil::CheckForBuggyExtFS(const string& path) {
  bool is_on_ext;
  KUDU_RETURN_IF_ERROR(kudu::Env::Default()->IsOnExtFilesystem(path, &is_on_ext),
      Substitute("Failed to check filesystem type at $0", path));
  if (is_on_ext && IsBuggyEl6Kernel()) {
    return Status(Substitute("Data dir $0 is on an ext filesystem which is affected by "
        "KUDU-1508.", path));
  }
  return Status::OK();
}

Status FileSystemUtil::CheckHolePunch(const string& path) {
  // Check if the filesystem of 'path' is affected by KUDU-1508.
  RETURN_IF_ERROR(CheckForBuggyExtFS(path));

  // Open the test file.
  string filename = JoinPathSegments(path, PrintId(GenerateUUID()));
  unique_ptr<RWFile> test_file;
  KUDU_RETURN_IF_ERROR(kudu::Env::Default()->NewRWFile(filename, &test_file),
      Substitute("Failed to create file $0", filename));

  // Delete file on exit from the function.
  auto delete_file = MakeScopeExitTrigger([&filename]() {
      kudu::Env::Default()->DeleteFile(filename);
  });

  const int buffer_size = 4096 * 4;
  unique_ptr<uint8_t[]> buffer(new uint8_t[buffer_size]);
  memset(buffer.get(), 0xaa, buffer_size);
  for (int i = 0; i < 4; ++i) {
    KUDU_RETURN_IF_ERROR(test_file->Write(i * buffer_size,
        Slice(buffer.get(), buffer_size)),
        Substitute("Failed to write to file $0", path));
  }

  const off_t init_file_size = buffer_size * 4;
  uint64_t sz;
  KUDU_RETURN_IF_ERROR(kudu::Env::Default()->GetFileSizeOnDisk(filename, &sz),
      "Failed to get pre-punch file size");
  if (sz != init_file_size) {
    return Status(Substitute("Unexpected pre-punch file size for $0: expected $1 but "
        "got $2", filename, init_file_size, sz));
  }

  // Punch the hole, testing the file's size again.
  const off_t hole_offset = buffer_size;
  const off_t hole_size = buffer_size * 2;
  KUDU_RETURN_IF_ERROR(test_file->PunchHole(hole_offset, hole_size),
      "Failed to punch hole");

  const int final_file_size = init_file_size - hole_size;
  KUDU_RETURN_IF_ERROR(kudu::Env::Default()->GetFileSizeOnDisk(filename, &sz),
      "Failed to get post-punch file size");
  if (sz != final_file_size) {
    return Status(Substitute("Unexpected post-punch file size for $0: expected $1 but "
        "got $2", filename, final_file_size, sz));
  }

  int offset = 0;
  unique_ptr<uint8_t[]> tmp_buffer(new uint8_t[buffer_size]);
  memset(tmp_buffer.get(), 0, buffer_size);
  KUDU_RETURN_IF_ERROR(test_file->Read(offset, Slice(tmp_buffer.get(), buffer_size)),
      Substitute("Failed to read file $0", path));
  if (memcmp(tmp_buffer.get(), buffer.get(), buffer_size) != 0) {
    return Status(Substitute("Mismatched file content $0 at offset 0", filename));
  }

  offset = hole_offset + hole_size;
  memset(tmp_buffer.get(), 0, buffer_size);
  KUDU_RETURN_IF_ERROR(test_file->Read(offset, Slice(tmp_buffer.get(), buffer_size)),
      Substitute("Failed to read file $0", path));
  if (memcmp(tmp_buffer.get(), buffer.get(), buffer_size) != 0) {
    return Status(Substitute("Mismatched file content $0 at offset $1", filename,
        offset));
  }

  return Status::OK();
}

Status FileSystemUtil::ApproximateFileSize(
    const std::string& path, uintmax_t& file_size) {
  bool exist = false;
  RETURN_IF_ERROR(PathExists(path, &exist));
  if (!exist) {
    return Status(
        ErrorMsg(TErrorCode::RUNTIME_ERROR, Substitute("Path $0 does not exist!", path)));
  } else {
    error_code errcode;
    file_size = filesystem::file_size(path, errcode);
    if (errcode != errc::success) {
      return Status(ErrorMsg(TErrorCode::RUNTIME_ERROR,
          Substitute("Encountered exception while checking file size of path $0: $1",
              path, errcode.message())));
    }
  }
  return Status::OK();
}

} // namespace impala
