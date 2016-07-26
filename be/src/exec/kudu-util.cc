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

#include "exec/kudu-util.h"

#include <algorithm>

#include <boost/algorithm/string.hpp>
#include <boost/unordered_set.hpp>
#include <kudu/client/callbacks.h>
#include <kudu/client/schema.h>

#include "runtime/descriptors.h"
#include "gutil/strings/substitute.h"

#include "common/names.h"

using boost::algorithm::to_lower_copy;
using kudu::client::KuduSchema;
using kudu::client::KuduColumnSchema;

DECLARE_bool(disable_kudu);

namespace impala {

bool KuduClientIsSupported() {
  // The value below means the client is actually a stubbed client. This should mean
  // that no official client exists for the underlying OS. The value below should match
  // the value generated in bin/bootstrap_toolchain.py.
  return kudu::client::GetShortVersionString() != "__IMPALA_KUDU_STUB__";
}

bool KuduIsAvailable() { return CheckKuduAvailability().ok(); }

Status CheckKuduAvailability() {
  if (KuduClientIsSupported()) {
    if (FLAGS_disable_kudu) {
      return Status(TErrorCode::KUDU_NOT_ENABLED);
    } else{
      return Status::OK();
    }
  }
  return Status(TErrorCode::KUDU_NOT_SUPPORTED_ON_OS);
}

Status ImpalaToKuduType(const ColumnType& impala_type,
    kudu::client::KuduColumnSchema::DataType* kudu_type) {
  using kudu::client::KuduColumnSchema;

  switch (impala_type.type) {
    case TYPE_VARCHAR:
    case TYPE_STRING:
      *kudu_type = KuduColumnSchema::STRING;
      break;
    case TYPE_TINYINT:
      *kudu_type = KuduColumnSchema::INT8;
      break;
    case TYPE_SMALLINT:
      *kudu_type = KuduColumnSchema::INT16;
      break;
    case TYPE_INT:
      *kudu_type = KuduColumnSchema::INT32;
      break;
    case TYPE_BIGINT:
      *kudu_type = KuduColumnSchema::INT64;
      break;
    case TYPE_FLOAT:
      *kudu_type = KuduColumnSchema::FLOAT;
      break;
    case TYPE_DOUBLE:
      *kudu_type = KuduColumnSchema::DOUBLE;
      break;
    case TYPE_BOOLEAN:
      *kudu_type = KuduColumnSchema::BOOL;
      break;
    default:
      return Status(TErrorCode::IMPALA_KUDU_TYPE_MISSING, TypeToString(impala_type.type));
  }
  return Status::OK();
}

Status KuduToImpalaType(const kudu::client::KuduColumnSchema::DataType& kudu_type,
    ColumnType* impala_type) {
  using kudu::client::KuduColumnSchema;

  switch (kudu_type) {
    case KuduColumnSchema::STRING:
      *impala_type = TYPE_STRING;
      break;
    case KuduColumnSchema::INT8:
      *impala_type = TYPE_TINYINT;
      break;
    case KuduColumnSchema::INT16:
      *impala_type = TYPE_SMALLINT;
      break;
    case KuduColumnSchema::INT32:
      *impala_type = TYPE_INT;
      break;
    case KuduColumnSchema::INT64:
      *impala_type = TYPE_BIGINT;
      break;
    case KuduColumnSchema::FLOAT:
      *impala_type = TYPE_FLOAT;
      break;
    case KuduColumnSchema::DOUBLE:
      *impala_type = TYPE_DOUBLE;
      break;
    default:
      return Status(TErrorCode::KUDU_IMPALA_TYPE_MISSING,
                    KuduColumnSchema::DataTypeToString(kudu_type));
  }
  return Status::OK();
}

/// Returns a map of lower case column names to column indexes in 'map'.
/// Returns an error Status if 'schema' had more than one column with the same lower
/// case name.
Status MapLowercaseKuduColumnNamesToIndexes(const kudu::client::KuduSchema& schema,
    IdxByLowerCaseColName* map) {
  DCHECK(map != NULL);
  for(size_t i = 0; i < schema.num_columns(); ++i) {
    string lower_case_col_name = to_lower_copy(schema.Column(i).name());
    if (map->find(lower_case_col_name) != map->end()) {
      return Status(strings::Substitute("There was already a column with name: '$0' "
          "in the schema", lower_case_col_name));
    }
    (*map)[lower_case_col_name] = i;
  }
  return Status::OK();
}

Status ProjectedColumnsFromTupleDescriptor(const TupleDescriptor& tuple_desc,
    vector<string>* projected_columns, const KuduSchema& schema) {
  DCHECK(projected_columns != NULL);
  projected_columns->clear();

  IdxByLowerCaseColName idx_by_lc_name;
  RETURN_IF_ERROR(MapLowercaseKuduColumnNamesToIndexes(schema, &idx_by_lc_name));

  // In debug mode try a dynamic cast. If it fails it means that the
  // TableDescriptor is not an instance of KuduTableDescriptor.
  DCHECK(dynamic_cast<const KuduTableDescriptor*>(tuple_desc.table_desc()) != NULL)
      << "TableDescriptor must be an instance KuduTableDescriptor.";

  const KuduTableDescriptor* table_desc =
      static_cast<const KuduTableDescriptor*>(tuple_desc.table_desc());
  LOG(INFO) << "Table desc for schema: " << table_desc->DebugString();

  const std::vector<SlotDescriptor*>& slots = tuple_desc.slots();
  for (int i = 0; i < slots.size(); ++i) {
    int col_idx = slots[i]->col_pos();
    string impala_col_name = to_lower_copy(table_desc->col_descs()[col_idx].name());
    IdxByLowerCaseColName::const_iterator iter = idx_by_lc_name.find(impala_col_name);
    if (iter == idx_by_lc_name.end()) {
      return Status(strings::Substitute("Could not find column: $0 in the Kudu schema.",
         impala_col_name));
    }
    projected_columns->push_back(schema.Column((*iter).second).name());
  }

  return Status::OK();
}

void LogKuduMessage(void* unused, kudu::client::KuduLogSeverity severity,
    const char* filename, int line_number, const struct ::tm* time, const char* message,
    size_t message_len) {

  // Note: we use raw ints instead of the nice LogSeverity typedef
  // that can be found in glog/log_severity.h as it has an import
  // conflict with gutil/logging-inl.h (indirectly imported).
  int glog_severity;

  switch (severity) {
    case kudu::client::SEVERITY_INFO: glog_severity = 0; break;
    case kudu::client::SEVERITY_WARNING: glog_severity = 1; break;
    case kudu::client::SEVERITY_ERROR: glog_severity = 2; break;
    case kudu::client::SEVERITY_FATAL: glog_severity = 3; break;
    default : DCHECK(false) << "Unexpected severity type: " << severity;
  }

  google::LogMessage log_entry(filename, line_number, glog_severity);
  string msg(message, message_len);
  log_entry.stream() << msg;
}

void InitKuduLogging() {
  DCHECK(KuduIsAvailable());
  static kudu::client::KuduLoggingFunctionCallback<void*> log_cb(&LogKuduMessage, NULL);
  kudu::client::InstallLoggingCallback(&log_cb);
  kudu::client::SetVerboseLogLevel(FLAGS_v);
}

}  // namespace impala
