// Copyright 2015 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/kudu-util.h"

#include <boost/unordered_set.hpp>
#include <kudu/client/schema.h>
#include <kudu/gutil/bind.h>
#include <kudu/util/logging_callback.h>

#include "runtime/descriptors.h"

#include "common/names.h"

namespace impala {

Status ImpalaToKuduType(const ColumnType& impala_type,
    kudu::client::KuduColumnSchema::DataType* kudu_type) {
  using kudu::client::KuduColumnSchema;

  switch (impala_type.type) {
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
    default:
      DCHECK(false) << "Impala type unsupported in Kudu: "
          << TypeToString(impala_type.type);
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
      DCHECK(false) << "Kudu type unsupported in impala: "
          << KuduColumnSchema::DataTypeToString(kudu_type);
      return Status(TErrorCode::KUDU_IMPALA_TYPE_MISSING,
                    KuduColumnSchema::DataTypeToString(kudu_type));
  }
  return Status::OK();
}

Status KuduSchemaFromTupleDescriptor(const TupleDescriptor& tuple_desc,
    kudu::client::KuduSchema* schema) {
#ifndef NDEBUG
  // In debug mode try a dynamic cast. If it fails it means that the
  // TableDescriptor is not an instance of KuduTableDescriptor.
  DCHECK(dynamic_cast<const KuduTableDescriptor*>(tuple_desc.table_desc()))
      << "TableDescriptor must be an instance KuduTableDescriptor.";
#endif

  using kudu::client::KuduColumnSchema;

  DCHECK_NOTNULL(schema);

  const KuduTableDescriptor* table_desc =
      static_cast<const KuduTableDescriptor*>(tuple_desc.table_desc());
  LOG(INFO) << "Table desc for schema: " << table_desc->DebugString();

  unordered_set<string> key_cols(table_desc->key_columns().begin(),
      table_desc->key_columns().end());

  vector<KuduColumnSchema> kudu_cols;
  const std::vector<SlotDescriptor*>& slots = tuple_desc.slots();
  for (int i = 0; i < slots.size(); ++i) {
    if (!slots[i]->is_materialized()) continue;
    int col_idx = slots[i]->col_pos();
    const string& col_name = table_desc->col_names()[col_idx];
    // Initialize the DataType to avoid an uninitialized warning.
    KuduColumnSchema::DataType kt = KuduColumnSchema::INT8;
    RETURN_IF_ERROR(ImpalaToKuduType(slots[i]->type(), &kt));

    // Key columns are not nullable, all others are for now.
    bool nullable = key_cols.find(col_name) == key_cols.end();
    kudu_cols.push_back(KuduColumnSchema(col_name, kt, nullable));
  }

  schema->Reset(kudu_cols, key_cols.size());
  return Status::OK;
}

void LogKuduMessage(kudu::KuduLogSeverity severity, const char* filename,
    int line_number, const struct ::tm* time, const char* message, size_t message_len) {

  // Note: we use raw ints instead of the nice LogSeverity typedef
  // that can be found in glog/log_severity.h as it has an import
  // conflict with gutil/logging-inl.h (indirectly imported).
  int glog_severity;

  switch(severity) {
    case kudu::SEVERITY_INFO: glog_severity = 0; break;
    case kudu::SEVERITY_WARNING: glog_severity = 1; break;
    case kudu::SEVERITY_ERROR: glog_severity = 2; break;
    case kudu::SEVERITY_FATAL: glog_severity = 3; break;
    default : CHECK(false) << "Unexpected severity type: " << severity;
  }

  google::LogMessage log_entry(filename, line_number, glog_severity);
  string msg(message, message_len);
  log_entry.stream() << msg;
}

void InitKuduLogging() {
  kudu::client::InstallLoggingCallback(kudu::Bind(&LogKuduMessage));
  kudu::client::SetVerboseLogLevel(FLAGS_v);
}

}  // namespace impala
