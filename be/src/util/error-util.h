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


#ifndef IMPALA_UTIL_ERROR_UTIL_H
#define IMPALA_UTIL_ERROR_UTIL_H

#include <string>
#include <vector>

#include "common/logging.h"
#include "gen-cpp/CatalogObjects_types.h"
#include "gen-cpp/ErrorCodes_constants.h"
#include "gen-cpp/ErrorCodes_types.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gen-cpp/TCLIService_types.h"
#include "gutil/strings/substitute.h"

namespace impala {

/// Returns the error message for errno. We should not use strerror directly
/// as that is not thread safe.
/// Returns empty string if errno is 0.
std::string GetStrErrMsg();

// This version of the function receives errno as a parameter instead of reading it
// itself.
std::string GetStrErrMsg(int err_no);

/// Returns an error message warning that the given table names are missing relevant
/// table/and or column statistics.
std::string GetTablesMissingStatsWarning(
    const std::vector<TTableName>& tables_missing_stats);


/// Class that holds a formatted error message and potentially a set of detail
/// messages. Error messages are intended to be user facing. Error details can be attached
/// as strings to the message. These details should only be accessed internally.
class ErrorMsg {
 public:
  static constexpr int MAX_ERROR_MESSAGE_LEN = 128 * 1024; // 128kb

  typedef strings::internal::SubstituteArg ArgType;

  /// Trivial constructor.
  ErrorMsg() : error_(TErrorCode::OK) {}

  /// Below are a set of overloaded constructors taking all possible number of arguments
  /// that can be passed to Substitute. The reason is to try to avoid forcing the compiler
  /// putting all arguments for Substitute() on the stack whenver this is called and thus
  /// polute the instruction cache.
  explicit ErrorMsg(TErrorCode::type error);
  ErrorMsg(TErrorCode::type error, const ArgType& arg0);
  ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1);
  ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2);
  ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3);
  ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3, const ArgType& arg4);
  ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
      const ArgType& arg5);
  ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
      const ArgType& arg5, const ArgType& arg6);
  ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
      const ArgType& arg5, const ArgType& arg6, const ArgType& arg7);
  ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
      const ArgType& arg5, const ArgType& arg6, const ArgType& arg7,
      const ArgType& arg8);
  ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
      const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
      const ArgType& arg5, const ArgType& arg6, const ArgType& arg7,
      const ArgType& arg8, const ArgType& arg9);

  /// Static initializer that is needed to avoid issues with static initialization order
  /// and the point in time when the string list generated via thrift becomes
  /// available. This method should not be used if no static initialization is needed as
  /// the cost of this method is proportional to the number of entries in the global error
  /// message list.
  /// WARNING: DO NOT CALL THIS METHOD IN A NON STATIC CONTEXT
  static ErrorMsg Init(TErrorCode::type error, const ArgType& arg0 = ArgType::kNoArg,
      const ArgType& arg1 = ArgType::kNoArg,
      const ArgType& arg2 = ArgType::kNoArg,
      const ArgType& arg3 = ArgType::kNoArg,
      const ArgType& arg4 = ArgType::kNoArg,
      const ArgType& arg5 = ArgType::kNoArg,
      const ArgType& arg6 = ArgType::kNoArg,
      const ArgType& arg7 = ArgType::kNoArg,
      const ArgType& arg8 = ArgType::kNoArg,
      const ArgType& arg9 = ArgType::kNoArg);

  TErrorCode::type error() const { return error_; }

  /// Add detail string message.
  void AddDetail(const std::string& d) {
    details_.push_back(d);
  }

  /// Set a specific error code.
  void SetErrorCode(TErrorCode::type e) {
    error_ = e;
  }

  /// Return the formatted error string.
  const std::string& msg() const {
    return message_;
  }

  const std::vector<std::string>& details() const {
    return details_;
  }

  /// Set a specific error message. Truncate the message if the length is longer than
  /// MAX_ERROR_MESSAGE_LEN.
  void SetErrorMsg(const std::string& msg);

  /// Produce a string representation of the error message that includes the formatted
  /// message of the original error and the attached detail strings.
  std::string GetFullMessageDetails() const;

private:
  TErrorCode::type error_;
  std::string message_;
  std::vector<std::string> details_;
};

/// Maps the HS2 TStatusCode types to the corresponding TErrorCode.
TErrorCode::type HS2TStatusCodeToTErrorCode(
    const apache::hive::service::cli::thrift::TStatusCode::type& hs2Code);
}

#endif
