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

#include "util/error-util.h"

#include <errno.h>
#include <string.h>
#include <sstream>

#include "common/logging.h"
#include "common/names.h"

namespace impala {

string GetStrErrMsg() {
  // Save errno. "<<" could reset it.
  int e = errno;
  if (e == 0) return "";
  stringstream ss;
  char buf[1024];
  ss << "Error(" << e << "): " << strerror_r(e, buf, 1024);
  return ss.str();
}

string GetTablesMissingStatsWarning(const vector<TTableName>& tables_missing_stats) {
  stringstream ss;
  if (tables_missing_stats.empty()) return string("");
  ss << "WARNING: The following tables are missing relevant table and/or column "
     << "statistics.\n";
  for (int i = 0; i < tables_missing_stats.size(); ++i) {
    const TTableName& table_name = tables_missing_stats[i];
    if (i != 0) ss << ",";
    ss << table_name.db_name << "." << table_name.table_name;
  }
  return ss.str();
}

ErrorMsg::ErrorMsg(TErrorCode::type error)
    : error_(error),
      message_(strings::Substitute(g_ErrorCodes_constants.TErrorMessage[error_])) {}

ErrorMsg::ErrorMsg(TErrorCode::type error, const ArgType& arg0)
    : error_(error),
      message_(strings::Substitute(g_ErrorCodes_constants.TErrorMessage[error_],
              arg0)) {}

ErrorMsg::ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1)
    : error_(error),
      message_(strings::Substitute(g_ErrorCodes_constants.TErrorMessage[error_],
              arg0, arg1)) {}

ErrorMsg::ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
    const ArgType& arg2)
    : error_(error),
      message_(strings::Substitute(g_ErrorCodes_constants.TErrorMessage[error_],
              arg0, arg1, arg2)) {}

ErrorMsg::ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
    const ArgType& arg2, const ArgType& arg3)
    : error_(error),
      message_(strings::Substitute(g_ErrorCodes_constants.TErrorMessage[error_],
              arg0, arg1, arg2, arg3)) {}

ErrorMsg::ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
    const ArgType& arg2, const ArgType& arg3, const ArgType& arg4)
    : error_(error),
      message_(strings::Substitute(g_ErrorCodes_constants.TErrorMessage[error_],
              arg0, arg1, arg2, arg3, arg4)) {}

ErrorMsg::ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
    const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
    const ArgType& arg5)
    : error_(error),
      message_(strings::Substitute(g_ErrorCodes_constants.TErrorMessage[error_],
              arg0, arg1, arg2, arg3, arg4, arg5)) {}

ErrorMsg::ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
    const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
    const ArgType& arg5, const ArgType& arg6)
    : error_(error),
      message_(strings::Substitute(g_ErrorCodes_constants.TErrorMessage[error_],
              arg0, arg1, arg2, arg3, arg4, arg5, arg6)) {}

ErrorMsg::ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
    const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
    const ArgType& arg5, const ArgType& arg6, const ArgType& arg7)
    : error_(error),
      message_(strings::Substitute(g_ErrorCodes_constants.TErrorMessage[error_],
              arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7)) {}

ErrorMsg::ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
    const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
    const ArgType& arg5, const ArgType& arg6, const ArgType& arg7,
    const ArgType& arg8)
    : error_(error),
      message_(strings::Substitute(g_ErrorCodes_constants.TErrorMessage[error_],
              arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8)) {}

ErrorMsg::ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
    const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
    const ArgType& arg5, const ArgType& arg6, const ArgType& arg7,
    const ArgType& arg8, const ArgType& arg9)
    : error_(error),
      message_(strings::Substitute(g_ErrorCodes_constants.TErrorMessage[error_],
              arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9)) {}

ErrorMsg ErrorMsg::Init(TErrorCode::type error, const ArgType& arg0,
    const ArgType& arg1, const ArgType& arg2, const ArgType& arg3,
    const ArgType& arg4, const ArgType& arg5, const ArgType& arg6, const ArgType& arg7,
    const ArgType& arg8, const ArgType& arg9) {

  ErrorCodesConstants error_strings;
  ErrorMsg m;
  m.error_ = error;
  m.message_ = strings::Substitute(error_strings.TErrorMessage[m.error_],
      arg0, arg1, arg2, arg3, arg4, arg5,
      arg6, arg7, arg8, arg9);
  return m;
}

void PrintErrorMap(ostream* stream, const ErrorLogMap& errors) {
  for (const ErrorLogMap::value_type& v: errors) {
    const TErrorLogEntry& log_entry = v.second;
    if (v.first == TErrorCode::GENERAL) {
      DCHECK_EQ(log_entry.count, 0);
      for (const string& s: log_entry.messages) {
        *stream << s << "\n";
      }
    } else if (!log_entry.messages.empty()) {
      DCHECK_GT(log_entry.count, 0);
      DCHECK_EQ(log_entry.messages.size(), 1);
      *stream << log_entry.messages.front();
      if (log_entry.count == 1) {
        *stream << "\n";
      } else {
        *stream << " (1 of " << log_entry.count << " similar)\n";
      }
    }
  }
}

string PrintErrorMapToString(const ErrorLogMap& errors) {
  stringstream stream;
  PrintErrorMap(&stream, errors);
  return stream.str();
}

void MergeErrorMaps(const ErrorLogMap& m1, ErrorLogMap* m2) {
  for (const ErrorLogMap::value_type& v: m1) {
    TErrorLogEntry& target = (*m2)[v.first];
    const TErrorLogEntry& source = v.second;
    // Append generic message, append specific codes or increment count if exists
    if (v.first == TErrorCode::GENERAL) {
      DCHECK_EQ(v.second.count, 0);
      target.messages.insert(
          target.messages.end(), source.messages.begin(), source.messages.end());
    } else {
      DCHECK_EQ(source.messages.empty(), source.count == 0);
      if (target.messages.empty()) {
        target.messages = source.messages;
      }
      target.count += source.count;
    }
  }
}

void AppendError(ErrorLogMap* map, const ErrorMsg& e) {
  TErrorLogEntry& target = (*map)[e.error()];
  if (e.error() == TErrorCode::GENERAL) {
    target.messages.push_back(e.msg());
  } else {
    if (target.messages.empty()) {
      target.messages.push_back(e.msg());
    }
    ++target.count;
  }
}

void ClearErrorMap(ErrorLogMap& errors) {
  for (auto iter = errors.begin(); iter != errors.end(); ++iter) {
    iter->second.messages.clear();
    iter->second.count = 0;
  }
}

size_t ErrorCount(const ErrorLogMap& errors) {
  ErrorLogMap::const_iterator cit = errors.find(TErrorCode::GENERAL);
  if (cit == errors.end()) return errors.size();
  return errors.size() + cit->second.messages.size() - 1;
}

string ErrorMsg::GetFullMessageDetails() const {
  stringstream ss;
  ss << message_ << "\n";
  for(size_t i = 0, end = details_.size(); i < end; ++i) {
    ss << details_[i] << "\n";
  }
  return ss.str();
}

}
