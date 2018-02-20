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

#ifndef IMPALA_UTIL_KUDU_UTIL_H_
#define IMPALA_UTIL_KUDU_UTIL_H_

// TODO: Remove when toolchain callbacks.h properly defines ::tm.
struct tm;

#include <kudu/client/callbacks.h>
#include <kudu/client/client.h>
#include <kudu/client/value.h>

#include "common/status.h"
#include "runtime/string-value.h"
#include "runtime/types.h"

namespace impala {

/// Takes a Kudu status and returns an impala one, if it's not OK.
#define KUDU_RETURN_IF_ERROR(expr, prepend) \
  do { \
    kudu::Status _s = (expr); \
    if (UNLIKELY(!_s.ok())) {                                      \
      return Status(strings::Substitute("$0: $1", prepend, _s.ToString())); \
    } \
  } while (0)

#define KUDU_ASSERT_OK(status)                                     \
  do {                                                             \
    const Status& _s = FromKuduStatus(status);                     \
    ASSERT_TRUE(_s.ok()) << "Error: " << _s.GetDetail();           \
  } while (0)

class TimestampValue;

/// Returns false when running on an operating system that Kudu doesn't support. If this
/// check fails, there is no way Kudu should be expected to work. Exposed for testing.
bool KuduClientIsSupported();

/// Returns OK if Kudu is available or an error status containing the reason Kudu is not
/// available. Kudu may not be available if no Kudu client is available for the platform
/// or if Kudu was disabled by the startup flag --disable_kudu.
Status CheckKuduAvailability() WARN_UNUSED_RESULT;

/// Convenience function for the bool equivalent of CheckKuduAvailability().
bool KuduIsAvailable();

/// Creates a new KuduClient using the specified master adresses. If any error occurs,
/// 'client' is not set and an error status is returned.
Status CreateKuduClient(const std::vector<std::string>& master_addrs,
    kudu::client::sp::shared_ptr<kudu::client::KuduClient>* client) WARN_UNUSED_RESULT;

/// Initializes Kudu's logging by binding a callback that logs back to Impala's glog. This
/// also sets Kudu's verbose logging to whatever level is set in Impala.
void InitKuduLogging();

// This is the callback mentioned above. When the Kudu client logs a message it gets
// redirected here and forwarded to Impala's glog.
// This method is not supposed to be used directly.
void LogKuduMessage(kudu::client::KuduLogSeverity severity, const char* filename,
    int line_number, const struct ::tm* time, const char* message, size_t message_len);

/// Casts 'value' according to the column type in 'col_type' and writes it into 'row'
/// at position 'col'. If the column type's primitive type is STRING or VARCHAR,
/// 'copy_strings' determines if 'value' will be copied into memory owned by the row.
/// If false, string data must remain valid while the row is being used.
Status WriteKuduValue(int col, const ColumnType& col_type, const void* value,
    bool copy_strings, kudu::KuduPartialRow* row) WARN_UNUSED_RESULT;

/// Casts 'value' according to the column type in 'col_type' and create a
/// new KuduValue containing 'value' which is returned in 'out'.
Status CreateKuduValue(const ColumnType& col_type, void* value,
    kudu::client::KuduValue** out) WARN_UNUSED_RESULT;

/// Takes a Kudu client DataType and KuduColumnTypeAttributes and
/// returns the corresponding Impala ColumnType.
ColumnType KuduDataTypeToColumnType(kudu::client::KuduColumnSchema::DataType type,
    const kudu::client::KuduColumnTypeAttributes& type_attributes);

/// Utility function for creating an Impala Status object based on a kudu::Status object.
/// 'k_status' is the kudu::Status object.
/// 'prepend' is a string to be prepended to details of 'k_status' when creating the
/// Impala Status object.
/// Note that we don't translate the kudu::Status error code to Impala error code
/// so the returned status' type is always of TErrorCode::GENERAL.
inline Status FromKuduStatus(
    const kudu::Status& k_status, const std::string prepend = "") {
  if (LIKELY(k_status.ok())) return Status::OK();
  const string& err_msg = prepend.empty() ? k_status.ToString() :
      strings::Substitute("$0: $1", prepend, k_status.ToString());
  VLOG(1) << err_msg;
  return Status::Expected(err_msg);
}

} /// namespace impala
#endif
