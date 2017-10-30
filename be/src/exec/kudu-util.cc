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
#include <string>
#include <sstream>

#include <kudu/client/callbacks.h>
#include <kudu/client/schema.h>
#include <kudu/common/partial_row.h>

#include "common/logging.h"
#include "common/names.h"
#include "common/status.h"
#include "runtime/timestamp-value.h"
#include "runtime/timestamp-value.inline.h"

using kudu::client::KuduSchema;
using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using DataType = kudu::client::KuduColumnSchema::DataType;

DECLARE_bool(disable_kudu);
DECLARE_int32(kudu_operation_timeout_ms);

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

Status CreateKuduClient(const vector<string>& master_addrs,
    kudu::client::sp::shared_ptr<KuduClient>* client) {
  kudu::client::KuduClientBuilder b;
  for (const string& address: master_addrs) b.add_master_server_addr(address);
  KUDU_RETURN_IF_ERROR(b.Build(client), "Unable to create Kudu client");
  return Status::OK();
}

string KuduSchemaDebugString(const KuduSchema& schema) {
  stringstream ss;
  for (int i = 0; i < schema.num_columns(); ++i) {
    const KuduColumnSchema& col = schema.Column(i);
    ss << col.name() << " " << KuduColumnSchema::DataTypeToString(col.type()) << "\n";
  }
  return ss.str();
}

void LogKuduMessage(void* unused, kudu::client::KuduLogSeverity severity,
    const char* filename, int line_number, const struct ::tm* time, const char* message,
    size_t message_len) {

  // Note: use raw ints instead of the nice LogSeverity typedef
  // that can be found in glog/log_severity.h as it has an import
  // conflict with gutil/logging-inl.h (indirectly imported).
  int glog_severity = 0;

  switch (severity) {
    case kudu::client::SEVERITY_INFO: glog_severity = 0; break;
    // Log Kudu WARNING messages at the INFO level to avoid contention created by glog
    // locking while flushing WARNING messages.
    case kudu::client::SEVERITY_WARNING: glog_severity = 0; break;
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
  // Kudu client logging is more noisy than Impala logging, log at v-1.
  kudu::client::SetVerboseLogLevel(std::max(0, FLAGS_v - 1));
}

Status WriteKuduTimestampValue(int col, const TimestampValue* tv,
    kudu::KuduPartialRow* row) {
  int64_t ts_micros;
  bool success = tv->UtcToUnixTimeMicros(&ts_micros);
  DCHECK(success); // If the value was invalid the slot should've been null.
  if (UNLIKELY(!success)) {
    return Status(TErrorCode::RUNTIME_ERROR,
        "Invalid TimestampValue: " + tv->ToString());
  }
  KUDU_RETURN_IF_ERROR(row->SetUnixTimeMicros(col, ts_micros),
      "Could not add Kudu WriteOp.");
  return Status::OK();
}

Status WriteKuduValue(int col, PrimitiveType type, const void* value,
    bool copy_strings, kudu::KuduPartialRow* row) {
  // TODO: codegen this to eliminate braching on type.
  switch (type) {
    case TYPE_VARCHAR:
    case TYPE_STRING: {
      const StringValue* sv = reinterpret_cast<const StringValue*>(value);
      kudu::Slice slice(reinterpret_cast<uint8_t*>(sv->ptr), sv->len);
      if (copy_strings) {
        KUDU_RETURN_IF_ERROR(row->SetString(col, slice), "Could not set Kudu row value.");
      } else {
        KUDU_RETURN_IF_ERROR(
            row->SetStringNoCopy(col, slice), "Could not set Kudu row value.");
      }
      break;
    }
    case TYPE_FLOAT:
      KUDU_RETURN_IF_ERROR(row->SetFloat(col, *reinterpret_cast<const float*>(value)),
          "Could not set Kudu row value.");
      break;
    case TYPE_DOUBLE:
      KUDU_RETURN_IF_ERROR(row->SetDouble(col, *reinterpret_cast<const double*>(value)),
          "Could not set Kudu row value.");
      break;
    case TYPE_BOOLEAN:
      KUDU_RETURN_IF_ERROR(row->SetBool(col, *reinterpret_cast<const bool*>(value)),
          "Could not set Kudu row value.");
      break;
    case TYPE_TINYINT:
      KUDU_RETURN_IF_ERROR(row->SetInt8(col, *reinterpret_cast<const int8_t*>(value)),
          "Could not set Kudu row value.");
      break;
    case TYPE_SMALLINT:
      KUDU_RETURN_IF_ERROR(row->SetInt16(col, *reinterpret_cast<const int16_t*>(value)),
          "Could not set Kudu row value.");
      break;
    case TYPE_INT:
      KUDU_RETURN_IF_ERROR(row->SetInt32(col, *reinterpret_cast<const int32_t*>(value)),
          "Could not set Kudu row value.");
      break;
    case TYPE_BIGINT:
      KUDU_RETURN_IF_ERROR(row->SetInt64(col, *reinterpret_cast<const int64_t*>(value)),
          "Could not set Kudu row value.");
      break;
    case TYPE_TIMESTAMP:
      RETURN_IF_ERROR(WriteKuduTimestampValue(col,
          reinterpret_cast<const TimestampValue*>(value), row));
      break;
    default:
      return Status(TErrorCode::IMPALA_KUDU_TYPE_MISSING, TypeToString(type));
  }

  return Status::OK();
}

ColumnType KuduDataTypeToColumnType(DataType type) {
  switch (type) {
    case DataType::INT8: return ColumnType(PrimitiveType::TYPE_TINYINT);
    case DataType::INT16: return ColumnType(PrimitiveType::TYPE_SMALLINT);
    case DataType::INT32: return ColumnType(PrimitiveType::TYPE_INT);
    case DataType::INT64: return ColumnType(PrimitiveType::TYPE_BIGINT);
    case DataType::STRING: return ColumnType(PrimitiveType::TYPE_STRING);
    case DataType::BOOL: return ColumnType(PrimitiveType::TYPE_BOOLEAN);
    case DataType::FLOAT: return ColumnType(PrimitiveType::TYPE_FLOAT);
    case DataType::DOUBLE: return ColumnType(PrimitiveType::TYPE_DOUBLE);
    case DataType::BINARY: return ColumnType(PrimitiveType::TYPE_BINARY);
    case DataType::UNIXTIME_MICROS: return ColumnType(PrimitiveType::TYPE_TIMESTAMP);
  }
  return ColumnType(PrimitiveType::INVALID_TYPE);
}

}  // namespace impala
