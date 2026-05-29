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

#include <algorithm>
#include <sstream>

#include "common/names.h"

namespace impala {

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

void ErrorMsg::SetErrorMsg(const std::string& msg) {
  message_ = msg.substr(
      0, std::min(msg.size(), static_cast<size_t>(ErrorMsg::MAX_ERROR_MESSAGE_LEN)));
}

string ErrorMsg::GetFullMessageDetails() const {
  stringstream ss;
  ss << message_ << "\n";
  for (size_t i = 0, end = details_.size(); i < end; ++i) {
    ss << details_[i] << "\n";
  }
  return ss.str();
}

}
