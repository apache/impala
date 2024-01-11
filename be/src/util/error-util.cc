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

#include "util/error-util-internal.h"
#include "util/debug-util.h"
#include "util/string-util.h"

#include <errno.h>
#include <string.h>
#include <sstream>

#include "common/logging.h"
#include "common/names.h"

using apache::hive::service::cli::thrift::TStatusCode;

namespace impala {

string GetStrErrMsg() {
  // Save errno. "<<" could reset it.
  int e = errno;
  return GetStrErrMsg(e);
}

string GetStrErrMsg(int err_no) {
  if (err_no == 0) return "";
  stringstream ss;
  char buf[1024];
  ss << "Error(" << err_no << "): " << strerror_r(err_no, buf, 1024);
  return ss.str();
}

string GetTablesMissingStatsWarning(const vector<TTableName>& tables_missing_stats) {
  stringstream ss;
  if (tables_missing_stats.empty()) return string("");
  ss << "WARNING: The following tables are missing relevant table and/or column "
     << "statistics.\n" << PrintTableList(tables_missing_stats);
  return ss.str();
}

ErrorMsg::ErrorMsg(TErrorCode::type error) : error_(error) {
  SetErrorMsg(strings::Substitute(g_ErrorCodes_constants.TErrorMessage[error_]));
}

ErrorMsg::ErrorMsg(TErrorCode::type error, const ArgType& arg0) : error_(error) {
  SetErrorMsg(strings::Substitute(g_ErrorCodes_constants.TErrorMessage[error_], arg0));
}

ErrorMsg::ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1)
  : error_(error) {
  SetErrorMsg(
      strings::Substitute(g_ErrorCodes_constants.TErrorMessage[error_], arg0, arg1));
}

ErrorMsg::ErrorMsg(
    TErrorCode::type error, const ArgType& arg0, const ArgType& arg1, const ArgType& arg2)
  : error_(error) {
  SetErrorMsg(strings::Substitute(
      g_ErrorCodes_constants.TErrorMessage[error_], arg0, arg1, arg2));
}

ErrorMsg::ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
    const ArgType& arg2, const ArgType& arg3)
  : error_(error) {
  SetErrorMsg(strings::Substitute(
      g_ErrorCodes_constants.TErrorMessage[error_], arg0, arg1, arg2, arg3));
}

ErrorMsg::ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
    const ArgType& arg2, const ArgType& arg3, const ArgType& arg4)
  : error_(error) {
  SetErrorMsg(strings::Substitute(
      g_ErrorCodes_constants.TErrorMessage[error_], arg0, arg1, arg2, arg3, arg4));
}

ErrorMsg::ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
    const ArgType& arg2, const ArgType& arg3, const ArgType& arg4, const ArgType& arg5)
  : error_(error) {
  SetErrorMsg(strings::Substitute(
      g_ErrorCodes_constants.TErrorMessage[error_], arg0, arg1, arg2, arg3, arg4, arg5));
}

ErrorMsg::ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
    const ArgType& arg2, const ArgType& arg3, const ArgType& arg4, const ArgType& arg5,
    const ArgType& arg6)
  : error_(error) {
  SetErrorMsg(strings::Substitute(g_ErrorCodes_constants.TErrorMessage[error_], arg0,
      arg1, arg2, arg3, arg4, arg5, arg6));
}

ErrorMsg::ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
    const ArgType& arg2, const ArgType& arg3, const ArgType& arg4, const ArgType& arg5,
    const ArgType& arg6, const ArgType& arg7)
  : error_(error) {
  SetErrorMsg(strings::Substitute(g_ErrorCodes_constants.TErrorMessage[error_], arg0,
      arg1, arg2, arg3, arg4, arg5, arg6, arg7));
}

ErrorMsg::ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
    const ArgType& arg2, const ArgType& arg3, const ArgType& arg4, const ArgType& arg5,
    const ArgType& arg6, const ArgType& arg7, const ArgType& arg8)
  : error_(error) {
  SetErrorMsg(strings::Substitute(g_ErrorCodes_constants.TErrorMessage[error_], arg0,
      arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8));
}

ErrorMsg::ErrorMsg(TErrorCode::type error, const ArgType& arg0, const ArgType& arg1,
    const ArgType& arg2, const ArgType& arg3, const ArgType& arg4, const ArgType& arg5,
    const ArgType& arg6, const ArgType& arg7, const ArgType& arg8, const ArgType& arg9)
  : error_(error) {
  SetErrorMsg(strings::Substitute(g_ErrorCodes_constants.TErrorMessage[error_], arg0,
      arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9));
}

ErrorMsg ErrorMsg::Init(TErrorCode::type error, const ArgType& arg0,
    const ArgType& arg1, const ArgType& arg2, const ArgType& arg3,
    const ArgType& arg4, const ArgType& arg5, const ArgType& arg6, const ArgType& arg7,
    const ArgType& arg8, const ArgType& arg9) {

  ErrorCodesConstants error_strings;
  ErrorMsg m;
  m.error_ = error;
  m.SetErrorMsg(strings::Substitute(error_strings.TErrorMessage[m.error_], arg0, arg1,
      arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9));
  return m;
}

void PrintErrorMap(ostream* stream, const ErrorLogMap& errors) {
  for (const ErrorLogMap::value_type& v: errors) {
    const ErrorLogEntryPB& entry = v.second;
    if (v.first == TErrorCode::GENERAL) {
      DCHECK_EQ(entry.count(), 0);
      for (auto& msg : entry.messages()) {
        *stream << msg << "\n";
      }
    } else if (entry.messages_size() > 0) {
      DCHECK_GT(entry.count(), 0);
      DCHECK_EQ(entry.messages_size(), 1);
      *stream << *(entry.messages().begin());
      if (entry.count() == 1) {
        *stream << "\n";
      } else {
        *stream << " (1 of " << entry.count() << " similar)\n";
      }
    }
  }
}

string PrintErrorMapToString(const ErrorLogMap& errors) {
  stringstream stream;
  PrintErrorMap(&stream, errors);
  return stream.str();
}

void MergeErrorLogEntry(TErrorCode::type error_code, const ErrorLogEntryPB& entry,
    ErrorLogMap* target_map) {
  ErrorLogEntryPB* target = &(*target_map)[error_code];

  // Append generic message, append specific codes or increment count if exists.
  if (error_code == TErrorCode::GENERAL || target->messages_size() == 0) {
    for (auto& msg : entry.messages()) {
      target->add_messages(msg);
    }
  }
  if (error_code != TErrorCode::GENERAL) {
    target->set_count(target->count() + entry.count());
  }
}

void MergeErrorMaps(const ErrorLogMapPB& m1, ErrorLogMap* m2) {
  for (const ErrorLogMapPB::value_type& v : m1) {
    MergeErrorLogEntry(static_cast<TErrorCode::type>(v.first), v.second, m2);
  }
}

void MergeErrorMaps(const ErrorLogMap& m1, ErrorLogMap* m2) {
  for (const ErrorLogMap::value_type& v : m1) {
    MergeErrorLogEntry(v.first, v.second, m2);
  }
}

void AppendError(ErrorLogMap* map, const ErrorMsg& e) {
  ErrorLogEntryPB* target = &(*map)[e.error()];
  if (e.error() == TErrorCode::GENERAL) {
    target->add_messages(e.msg());
  } else {
    if (target->messages_size() == 0) {
      target->add_messages(e.msg());
    }
    target->set_count(target->count() + 1);
  }
}

void ClearErrorMap(ErrorLogMap& errors) {
  for (auto& err : errors) {
    err.second.mutable_messages()->Clear();
    err.second.set_count(0);
  }
}

size_t ErrorCount(const ErrorLogMap& errors) {
  ErrorLogMap::const_iterator cit = errors.find(TErrorCode::GENERAL);
  if (cit == errors.end()) return errors.size();
  return errors.size() + cit->second.messages_size() - 1;
}

void ErrorMsg::SetErrorMsg(const std::string& msg) {
  Status status = TruncateDown(msg, ErrorMsg::MAX_ERROR_MESSAGE_LEN, &message_);
  DCHECK_OK(status);
}

string ErrorMsg::GetFullMessageDetails() const {
  stringstream ss;
  ss << message_ << "\n";
  for(size_t i = 0, end = details_.size(); i < end; ++i) {
    ss << details_[i] << "\n";
  }
  return ss.str();
}

TErrorCode::type HS2TStatusCodeToTErrorCode(const TStatusCode::type& hs2Code) {
  // There is no one-one mapping between HS2 error codes and TStatusCode types.
  // So we return a "GENERAL" error type for ERROR_STATUS code. This lets the callers
  // pick their own error message for substitution.
  switch (hs2Code) {
    case TStatusCode::ERROR_STATUS:
      return TErrorCode::GENERAL;
    default:
      DCHECK(false) << "Unexpected hs2Code: " << hs2Code;
  }
  LOG(ERROR) << "Unexpected hs2Code encountered: " << hs2Code;
  return TErrorCode::UNUSED;
}

}
