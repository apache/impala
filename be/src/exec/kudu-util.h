// Copyright 2015 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http:///www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef IMPALA_UTIL_KUDU_UTIL_H_
#define IMPALA_UTIL_KUDU_UTIL_H_

#include <kudu/client/client.h>

namespace impala {

class Status;
class ColumnType;
class TupleDescriptor;

Status ImpalaToKuduType(const ColumnType& impala_type,
    kudu::client::KuduColumnSchema::DataType* kudu_type);

Status KuduToImpalaType(const kudu::client::KuduColumnSchema::DataType& kudu_type,
    ColumnType* impala_type);

/// Builds a KuduSchema from a TupleDescriptor.
Status KuduSchemaFromTupleDescriptor(const TupleDescriptor& tuple_desc,
    kudu::client::KuduSchema* schema);

/// Initializes Kudu'd logging by binding a callback that logs back to Impala's Glog. This
/// also sets Kudu's verbose logging to whatever level is set in Impala.
void InitKuduLogging();

// This is the callback mentioned above. When the Kudu client logs a message it gets
// redirected here and forwarded to impala's glog.
// This method is not supposed to be used directly.
void LogKuduMessage(kudu::KuduLogSeverity severity, const char* filename,
    int line_number, const struct ::tm* time, const char* message, size_t message_len);

/// Takes a Kudu status and returns an impala one, if it's not OK.
#define KUDU_RETURN_IF_ERROR(expr, prepend) \
  do { \
    kudu::Status _s = (expr); \
    if (UNLIKELY(!_s.ok())) {                                      \
      return Status(strings::Substitute("$0: $1", prepend, _s.ToString())); \
    } \
  } while (0)

#define KUDU_DCHECK_OK(to_call) do { \
  ::kudu::Status _s = (to_call); \
  DCHECK(_s.ok()) << "Bad Kudu Status: " << _s.ToString(); \
  } while (0);

} /// namespace impala
#endif
