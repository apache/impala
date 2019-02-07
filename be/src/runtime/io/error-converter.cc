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

#include "runtime/io/error-converter.h"

#include "gutil/strings/substitute.h"
#include "util/debug-util.h"
#include "util/error-util.h"

#include "common/names.h"

namespace impala {

using std::unordered_map;

unordered_map<int, string> ErrorConverter::errno_to_error_text_map_(
    {{EACCES, "Access denied for the process' user"},
     {EINTR,   "Internal error occured."},
     {EINVAL,  "Invalid inputs."},
     {EMFILE,  "Process level opened file descriptor count is reached."},
     {ENAMETOOLONG,
         "Either the path length or a path component exceeds the maximum length."},
     {ENFILE,  "OS level opened file descriptor count is reached."},
     {ENOENT,  "The given path doesn't exist."},
     {ENOSPC,  "No space left on device."},
     {ENOTDIR, "It is not a directory."},
     {EOVERFLOW, "File size can't be represented."},
     {EROFS,   "The file system is read only."},
     {EAGAIN,  "Resource temporarily unavailable."},
     {EBADF,   "The given file descriptor is invalid."},
     {ENOMEM,  "Not enough memory."},
     {EFBIG,   "Maximum file size reached."},
     {EIO,     "Disk level I/O error occured."},
     {ENXIO,   "Device doesn't exist."}});

Status ErrorConverter::GetErrorStatusFromErrno(const string& function_name,
    const string& file_path, int err_no, const Params& params) {
  return Status(ErrorMsg(TErrorCode::DISK_IO_ERROR, GetBackendString(),
      GetErrorText(function_name, file_path, err_no, params)));
}

string ErrorConverter::GetErrorText(const string& function_name,
    const string& file_path, int err_no, Params params) {
  const string* error_text_body = GetErrorTextBody(err_no);
  if (error_text_body != nullptr) {
    params["errno"] = SimpleItoa(err_no);
    return Substitute("$0 failed for $1. $2 $3", function_name, file_path,
        *error_text_body, GetParamsString(params), err_no);
  }
  return Substitute("$0 failed for $1. errno=$2, description=$3", function_name,
      file_path, err_no, GetStrErrMsg(err_no));
}

string ErrorConverter::GetParamsString(const Params& params) {
  string result = "";
  bool first = true;
  for (const auto& item : params) {
    if (!first) result.append(", ");
    result.append(item.first).append("=").append(item.second);
    first = false;
  }
  return result;
}

const string* ErrorConverter::GetErrorTextBody(int err_no) {
  auto error_mapping_it = errno_to_error_text_map_.find(err_no);
  if (error_mapping_it != errno_to_error_text_map_.end()) {
    return &error_mapping_it->second;
  }
  return nullptr;
}

}
