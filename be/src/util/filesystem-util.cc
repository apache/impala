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
#include <sstream>

#include "util/filesystem-util.h"
#include "util/error-util.h"

using namespace std;
using namespace boost::filesystem;
using namespace strings;

namespace impala {

Status FileSystemUtil::CreateDirectories(const vector<string>& directories) {
  for (int i = 0; i < directories.size(); ++i) {
    // Remove the directory and it's contents if it exists. Ignore the error
    // that occurs if the directory doesn't exist. Only report the error from
    // create directory.
    try {
      remove_all(directories[i]);
    } catch (exception& e) {
    }
    try {
      create_directory(directories[i]);
    } catch (exception& e) {
      stringstream err_msg;
      err_msg << "Error creating directory " << directories[i] << " " << e.what();
      LOG(ERROR) << err_msg;
      return Status(TStatusCode::RUNTIME_ERROR, err_msg.str());
    }
  }

  return Status::OK;
}

Status FileSystemUtil::RemovePaths(const vector<string>& directories) {
  try {
    for (int i = 0; i < directories.size(); ++i) {
      remove_all(directories[i]);
    }
  } catch (exception& e) {
    stringstream err_msg;
    err_msg << "Error removing directory: " << e.what();
    LOG(ERROR) << err_msg;
    return Status(TStatusCode::RUNTIME_ERROR, err_msg.str());
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
    stringstream err_msg;
    err_msg << "Truncate file " << file_path
            << " to length==" << trunc_len
            << " failed with errno=" << errno
            << " description=" << GetStrErrMsg();
    LOG (ERROR) << err_msg.str();
    return Status(TStatusCode::RUNTIME_ERROR, err_msg.str());
  }

  return Status::OK;
}
} // namespace impala
