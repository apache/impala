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

#pragma once

#include "common/status.h"
#include "gutil/strings/substitute.h"
#include "kudu/util/status.h"

/// Takes a Kudu status and returns an impala one, if it's not OK.
/// Evaluates the prepend argument only if the status is not OK.
#define KUDU_RETURN_IF_ERROR(expr, prepend)                        \
  do {                                                             \
    const kudu::Status& _s = (expr);                               \
    if (UNLIKELY(!_s.ok())) {                                      \
      return impala::Status(strings::Substitute(                   \
          "$0: $1", prepend, _s.ToString()));                      \
    }                                                              \
  } while (0)

#define KUDU_ASSERT_OK(status)                                     \
  do {                                                             \
    const impala::Status& _s = FromKuduStatus(status);             \
    ASSERT_TRUE(_s.ok()) << "Error: " << _s.GetDetail();           \
  } while (0)


namespace impala {

/// Utility function for creating an Impala Status object based on a kudu::Status object.
/// 'k_status' is the kudu::Status object.
/// 'prepend' is a string to be prepended to details of 'k_status' when creating the
/// Impala Status object.
/// Note that we don't translate the kudu::Status error code to Impala error code
/// so the returned status' type is always of TErrorCode::GENERAL.
inline Status FromKuduStatus(
    const kudu::Status& k_status, const std::string prepend = "") {
  if (LIKELY(k_status.ok())) return Status::OK();
  const std::string& err_msg = prepend.empty() ? k_status.ToString() :
      strings::Substitute("$0: $1", prepend, k_status.ToString());
  VLOG(1) << err_msg;
  return Status::Expected(err_msg);
}

} // namespace impala
