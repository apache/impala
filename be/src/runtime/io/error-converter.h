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

#ifndef IMPALA_RUNTIME_IO_ERROR_CONVERTER_H
#define IMPALA_RUNTIME_IO_ERROR_CONVERTER_H

#include <string>
#include <unordered_map>

#include "common/status.h"

namespace impala {

/// This class translates 'errno' values set by disk I/O related functions to Status
/// objects with DISK_IO_ERROR error code alongside with an error text corresponding to
/// 'errno'. Instead of using GetStrErrMsg() this class provides richer, custom error
/// messages so that the root cause of a disk I/O issue can be identified easier.
/// If an internal mapping is not found for the errno then we fall back to
/// GetStrErrMsg().
///
/// A sample error text is:
/// fseek() failed for <file_path>. Not enough memory. errno=12
class ErrorConverter {
public:
  typedef std::unordered_map<std::string, std::string> Params;

  /// Given the name of the function that set errno and the file's path that
  /// was being manipulated this function returns a Status object that contains
  /// DISK_IO_ERROR error code and an error text corresponding to 'err_no'. The error
  /// text is provided by the errno_to_error_text_map_ member. The key-value pairs in
  /// 'params' provide a way for the user to extend the error text with additional
  /// information in the format of "key1=value1,key2=value2".
  static Status GetErrorStatusFromErrno(const string& function_name,
      const std::string& file_path, int err_no, const Params& params = Params());

  /// Return true if the 'err_no' matches any of the 'blacklistable' error code.
  static bool IsBlacklistableError(int err_no);
  /// Parse error text to get 'err_no' for thr given status with error code as
  /// DISK_IO_ERROR. Return true if the 'err_no' matches any of the 'blacklistable'
  /// error code.
  static bool IsBlacklistableError(const Status& status);

 private:
  /// Maps errno to error text
  static std::unordered_map<int, std::string> errno_to_error_text_map_;

  /// This function is a helper for GetErrorStatusFromErrno() and returns the error text
  /// corresponding to 'err_no'.
  static std::string GetErrorText(const string& function_name, const std::string& file_path,
      int err_no, Params params);

  /// Looks up 'err_no' in 'errno_to_error_text_map_' and returns a pointer to the mapped
  /// error text. Returns nullptr if 'err_no' is not found.
  static const std::string* GetErrorTextBody(int err_no);

  /// Returns a string formatted as "key1=value1, key2=value2" where key-value pairs are
  /// taken from the 'params' map.
  static std::string GetParamsString(const Params& params);
};

}

#endif
