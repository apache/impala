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

#include "exec/kudu/kudu-table-sink.h"

#include <sstream>

#include <boost/bind.hpp>
#include <kudu/client/client.h>
#include <kudu/client/resource_metrics.h>
#include <kudu/client/write_op.h>
#include <thrift/protocol/TDebugProtocol.h>

#include "exec/kudu/kudu-util.h"
#include "exprs/scalar-expr-evaluator.h"
#include "exprs/scalar-expr.h"
#include "gen-cpp/ImpalaInternalService_constants.h"
#include "gutil/gscoped_ptr.h"
#include "runtime/descriptors.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "runtime/row-batch.h"
#include "util/debug-util.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

#define DEFAULT_KUDU_MUTATION_BUFFER_SIZE 10 * 1024 * 1024
#define DEFAULT_KUDU_ERROR_BUFFER_SIZE 10 * 1024 * 1024

DEFINE_int32(kudu_mutation_buffer_size, DEFAULT_KUDU_MUTATION_BUFFER_SIZE,
    "The size (bytes) of the Kudu client buffer for mutations.");

// We estimate that 10MB is enough memory, and that seems to provide acceptable results in
// testing. This is still exposed as a flag for now though, because it may be possible
// that in some cases this is always too high (in which case tracked mem >> RSS and the
// memory is underutilized), or this may not be high enough (e.g. we underestimate the
// size of error strings, and queries are failed).
DEFINE_int32(kudu_error_buffer_size, DEFAULT_KUDU_ERROR_BUFFER_SIZE,
    "The size (bytes) of the Kudu client buffer for returning errors, with a min of 1KB."
    "If the actual errors exceed this size the query will fail.");

// IMPALA-10465: This change the behavior of KuduTableSink to use the IGNORE variant of
// Kudu API rather than using the non-IGNORE and discarding the Kudu error messages
// manually. Set 'false' to return to the old behavior.
DEFINE_bool(kudu_ignore_conflicts, true,
    "Control whether Impala should ignore Kudu conflict error on duplicate and absent "
    "primary keys during write operations. If this flag is set to true and Kudu cluster "
    "supports ignore operations, Impala will use {INSERT,UPDATE,DELETE}_IGNORE "
    "operations of Kudu API. Otherwise, Impala will use regular {INSERT,UPDATE,DELETE} "
    "operations and logs any occurrences of such conflict error. "
    "See also kudu_ignore_conflicts_in_transaction flag.");

DEFINE_bool(kudu_ignore_conflicts_in_transaction, false,
    "Control whether Kudu transaction should ignore conflict error on duplicate and "
    "absent primary keys during write operations. If set to true, the Kudu transaction "
    "will not be aborted for hitting such a conflict error. This flag is only "
    "considered if kudu_ignore_conflicts flag is true.");

DECLARE_int32(kudu_operation_timeout_ms);

using kudu::client::KuduColumnSchema;
using kudu::client::KuduSchema;
using kudu::client::KuduClient;
using kudu::client::KuduRowResult;
using kudu::client::KuduTable;
using kudu::client::KuduTransaction;
using kudu::client::KuduInsert;
using kudu::client::KuduUpdate;
using kudu::client::KuduError;
using kudu::client::ResourceMetrics;

namespace impala {

// Send 7MB buffers to Kudu, matching a hard-coded size in Kudu (KUDU-1693).
const static int INDIVIDUAL_BUFFER_SIZE = 7 * 1024 * 1024;

DataSink* KuduTableSinkConfig::CreateSink(RuntimeState* state) const {
  TDataSinkId sink_id = state->fragment().idx;
  return state->obj_pool()->Add(
      new KuduTableSink(sink_id, *this, tsink_->table_sink, state));
}

KuduTableSink::KuduTableSink(TDataSinkId sink_id, const DataSinkConfig& sink_config,
      const TTableSink& table_sink, RuntimeState* state)
  : DataSink(sink_id, sink_config, "KuduTableSink", state),
    table_id_(table_sink.target_table_id),
    sink_action_(table_sink.action),
    kudu_table_sink_(table_sink.kudu_table_sink),
    client_tracked_bytes_(0) {
  DCHECK(KuduIsAvailable());
}

Status KuduTableSink::Prepare(RuntimeState* state, MemTracker* parent_mem_tracker) {
  RETURN_IF_ERROR(DataSink::Prepare(state, parent_mem_tracker));
  SCOPED_TIMER(profile()->total_time_counter());

  // Get the kudu table descriptor.
  TableDescriptor* table_desc = state->desc_tbl().GetTableDescriptor(table_id_);
  DCHECK(table_desc != nullptr);

  // In debug mode try a dynamic cast. If it fails it means that the
  // TableDescriptor is not an instance of KuduTableDescriptor.
  DCHECK(dynamic_cast<const KuduTableDescriptor*>(table_desc))
      << "TableDescriptor must be an instance KuduTableDescriptor.";
  table_desc_ = static_cast<const KuduTableDescriptor*>(table_desc);

  state->dml_exec_state()->InitForKuduDml();

  // Add counters
  total_rows_ = ADD_COUNTER(profile(), "TotalNumRows", TUnit::UNIT);
  num_row_errors_ = ADD_COUNTER(profile(), "NumRowErrors", TUnit::UNIT);
  kudu_apply_timer_ = ADD_TIMER(profile(), "KuduApplyTimer");
  rows_processed_rate_ = profile()->AddDerivedCounter(
      "RowsProcessedRate", TUnit::UNIT_PER_SECOND,
      bind<int64_t>(&RuntimeProfile::UnitsPerSecond, total_rows_,
      profile()->total_time_counter()));

  return Status::OK();
}

Status KuduTableSink::Open(RuntimeState* state) {
  RETURN_IF_ERROR(DataSink::Open(state));

  // Account for the memory used by the Kudu client. This is necessary because the
  // KuduClient allocates non-trivial amounts of untracked memory,
  // TODO: Handle DML w/ small or known resource requirements (e.g. VALUES specified or
  // query has LIMIT) specially to avoid over-consumption.
  int64_t error_buffer_size = max<int64_t>(1024, FLAGS_kudu_error_buffer_size);
  int64_t required_mem = FLAGS_kudu_mutation_buffer_size + error_buffer_size;
  if (!mem_tracker_->TryConsume(required_mem)) {
    return mem_tracker_->MemLimitExceeded(state,
        "Could not allocate memory for KuduTableSink", required_mem);
  }
  client_tracked_bytes_ = required_mem;

  RETURN_IF_ERROR(ExecEnv::GetInstance()->GetKuduClient(
      table_desc_->kudu_master_addresses(), &client_));
  KUDU_RETURN_IF_ERROR(client_->OpenTable(table_desc_->table_name(), &table_),
      "Unable to open Kudu table");

  // Verify the KuduTable's schema is what we expect, in case it was modified since
  // analysis. If the underlying schema is changed after this point but before the write
  // completes, the KuduTable's schema will stay the same and we'll get an error back from
  // the Kudu server.
  for (int i = 0; i < output_expr_evals_.size(); ++i) {
    int col_idx = kudu_table_sink_.referenced_columns.empty() ?
        i : kudu_table_sink_.referenced_columns[i];
    if (col_idx >= table_->schema().num_columns()) {
      return Status(strings::Substitute(
          "Table $0 has fewer columns than expected.", table_desc_->name()));
    }
    const KuduColumnSchema& kudu_col = table_->schema().Column(col_idx);
    const ColumnType& type =
        KuduDataTypeToColumnType(kudu_col.type(), kudu_col.type_attributes());
    if (type != output_expr_evals_[i]->root().type()) {
      return Status(strings::Substitute("Column $0 has unexpected type. ($1 vs. $2)",
          table_->schema().Column(col_idx).name(), type.DebugString(),
          output_expr_evals_[i]->root().type().DebugString()));
    }
  }

  // Cache Kudu column nullabilities to avoid non-inlined slow calls to the Kudu client
  // Schema accessor.
  for (int i = 0; i < table_->schema().num_columns(); i++) {
    kudu_column_nullabilities_.push_back(table_->schema().Column(i).is_nullable());
  }

  // Inject failure or sleep time before creating Kudu session.
  RETURN_IF_ERROR(
      DebugAction(state->query_options(), "FIS_KUDU_TABLE_SINK_CREATE_SESSION"));

  if (kudu_table_sink_.__isset.kudu_txn_token
      && !kudu_table_sink_.kudu_txn_token.empty()) {
    // Deserialize the transaction token and create a session in the context of
    // transaction.
    KUDU_RETURN_IF_ERROR(
        KuduTransaction::Deserialize(client_, kudu_table_sink_.kudu_txn_token, &txn_),
        "Couldn't deserialize metadata of Kudu transaction");
    KUDU_RETURN_IF_ERROR(txn_->CreateSession(&session_),
        "Couldn't create session in the context of transaction");
    is_transactional_ = true;
  } else {
    session_ = client_->NewSession();
  }

  ignore_conflicts_ =
      FLAGS_kudu_ignore_conflicts && kudu_table_sink_.ignore_not_found_or_duplicate;
  if (!ignore_conflicts_) profile()->AppendExecOption("Log Kudu Conflicts");

  session_->SetTimeoutMillis(FLAGS_kudu_operation_timeout_ms);

  // KuduSession Set* methods here and below return a status for API compatibility.
  // As long as the Kudu client is statically linked, these shouldn't fail and thus these
  // calls could also DCHECK status is OK for debug builds (while still returning errors
  // for release).
  KUDU_RETURN_IF_ERROR(session_->SetFlushMode(
      kudu::client::KuduSession::AUTO_FLUSH_BACKGROUND), "Unable to set flush mode");

  const int32_t buf_size = FLAGS_kudu_mutation_buffer_size;
  if (buf_size < 1024 * 1024) {
    return Status(strings::Substitute(
        "Invalid kudu_mutation_buffer_size: '$0'. Must be greater than 1MB.", buf_size));
  }
  KUDU_RETURN_IF_ERROR(session_->SetMutationBufferSpace(buf_size),
      "Couldn't set mutation buffer size");

  // Configure client memory used for buffering.
  // Internally, the Kudu client keeps one or more buffers for writing operations. When a
  // single buffer is flushed, it is locked (that space cannot be reused) until all
  // operations within it complete, so it is important to have a number of buffers. In
  // our testing, we found that allowing a total of 10MB of buffer space to provide good
  // results; this is the default.  Then, because of some existing 8MB limits in Kudu, we
  // want to have that total space broken up into 7MB buffers (INDIVIDUAL_BUFFER_SIZE).
  // The mutation flush watermark is set to flush every INDIVIDUAL_BUFFER_SIZE.
  // TODO: simplify/remove this logic when Kudu simplifies the API (KUDU-1808).
  int num_buffers = FLAGS_kudu_mutation_buffer_size / INDIVIDUAL_BUFFER_SIZE;
  if (num_buffers == 0) num_buffers = 1;
  KUDU_RETURN_IF_ERROR(session_->SetMutationBufferFlushWatermark(1.0 / num_buffers),
      "Couldn't set mutation buffer watermark");

  // No limit on the buffer count since the settings above imply a max number of buffers.
  // Note that the Kudu client API has a few too many knobs for configuring the size and
  // number of these buffers; there are a few ways to accomplish similar behaviors.
  KUDU_RETURN_IF_ERROR(session_->SetMutationBufferMaxNum(0),
      "Couldn't set mutation buffer count");

  KUDU_RETURN_IF_ERROR(session_->SetErrorBufferSpace(error_buffer_size),
      "Failed to set error buffer space");

  return Status::OK();
}

kudu::client::KuduWriteOperation* KuduTableSink::NewWriteOp() {
  if (sink_action_ == TSinkAction::INSERT) {
    return table_->NewInsert();
  } else if (sink_action_ == TSinkAction::UPDATE) {
    return table_->NewUpdate();
  } else if (sink_action_ == TSinkAction::UPSERT) {
    return table_->NewUpsert();
  } else {
    DCHECK(sink_action_ == TSinkAction::DELETE) << "Sink type not supported: "
        << sink_action_;
    return table_->NewDelete();
  }
}

kudu::client::KuduWriteOperation* KuduTableSink::NewWriteIgnoreOp() {
  if (sink_action_ == TSinkAction::INSERT) {
    return table_->NewInsertIgnore();
  } else if (sink_action_ == TSinkAction::UPDATE) {
    return table_->NewUpdateIgnore();
  } else if (sink_action_ == TSinkAction::UPSERT) {
    return table_->NewUpsert();
  } else {
    DCHECK(sink_action_ == TSinkAction::DELETE)
        << "Sink type not supported: " << sink_action_;
    return table_->NewDeleteIgnore();
  }
}

Status KuduTableSink::Send(RuntimeState* state, RowBatch* batch) {
  SCOPED_TIMER(profile()->total_time_counter());
  expr_results_pool_->Clear();
  RETURN_IF_ERROR(state->CheckQueryState());

  // Collect all write operations and apply them together so the time in Apply() can be
  // easily timed.
  vector<unique_ptr<kudu::client::KuduWriteOperation>> write_ops;

  // Count the number of rows with nulls in non-nullable columns, i.e. null constraint
  // violations.
  int num_null_violations = 0;

  RETURN_IF_ERROR(
      DebugAction(state->query_options(), "FIS_KUDU_TABLE_SINK_WRITE_BEGIN"));

  // Since everything is set up just forward everything to the writer.
  for (int i = 0; i < batch->num_rows(); ++i) {
    TupleRow* current_row = batch->GetRow(i);
    unique_ptr<kudu::client::KuduWriteOperation> write(
        ignore_conflicts_ ? NewWriteIgnoreOp() : NewWriteOp());
    bool add_row = true;

    for (int j = 0; j < output_expr_evals_.size(); ++j) {
      // output_expr_evals_ only contains the columns that the op
      // applies to, i.e. columns explicitly mentioned in the query, and
      // referenced_columns is then used to map to actual column positions.
      int col = kudu_table_sink_.referenced_columns.empty() ?
          j : kudu_table_sink_.referenced_columns[j];

      void* value = output_expr_evals_[j]->GetValue(current_row);
      if (value == nullptr) {
        if (kudu_column_nullabilities_[col]) {
          KUDU_RETURN_IF_ERROR(write->mutable_row()->SetNull(col),
              "Could not add Kudu WriteOp.");
          continue;
        } else {
          // This row violates the nullability constraints of the column, do not attempt
          // to write this row because it is already known to be an error and the Kudu
          // error will be difficult to interpret later (error code isn't specific).
          ++num_null_violations;
          state->LogError(ErrorMsg::Init(TErrorCode::KUDU_NULL_CONSTRAINT_VIOLATION,
              table_desc_->table_name()));
          add_row = false;
          break; // skip remaining columns for this row
        }
      }

      const ColumnType& type = output_expr_evals_[j]->root().type();
      Status s = WriteKuduValue(col, type, value, true, write->mutable_row());
      // This can only fail if we set a col to an incorrect type, which would be a bug in
      // planning, so we can DCHECK.
      DCHECK(s.ok()) << "WriteKuduValue failed for col '"
                     << table_->schema().Column(col).name()
                     << "' of type '" << type << "': "
                     << s.GetDetail();
      RETURN_IF_ERROR(s);
    }
    if (add_row) write_ops.push_back(move(write));
  }

  {
    SCOPED_TIMER(kudu_apply_timer_);
    for (auto&& write: write_ops) {
      KUDU_RETURN_IF_ERROR(session_->Apply(write.release()), "Error applying Kudu Op.");
      // Inject failure when writing partial of row batch.
      RETURN_IF_ERROR(
          DebugAction(state->query_options(), "FIS_KUDU_TABLE_SINK_WRITE_PARTIAL_ROW"));
    }
  }
  // Inject failure or sleep time in the end of writing row batch.
  RETURN_IF_ERROR(DebugAction(state->query_options(), "FIS_KUDU_TABLE_SINK_WRITE_BATCH"));

  // Increment for all rows received by the sink, including errors.
  COUNTER_ADD(total_rows_, batch->num_rows());
  // Add the number of null constraint violations to the number of row errors, which
  // isn't reported by Kudu in CheckForErrors() because those rows were never
  // successfully added to the KuduSession.
  COUNTER_ADD(num_row_errors_, num_null_violations);
  RETURN_IF_ERROR(CheckForErrors(state));
  return Status::OK();
}

Status KuduTableSink::CheckForErrors(RuntimeState* state) {
  RETURN_IF_ERROR(state->CheckQueryState());

  if (ignore_conflicts_) {
    const ResourceMetrics& metrics = session_->GetWriteOpMetrics();
    int64_t ignored_errors = metrics.GetMetric("insert_ignore_errors")
        + metrics.GetMetric("update_ignore_errors")
        + metrics.GetMetric("delete_ignore_errors");
    DCHECK(ignored_errors >= total_ignored_errors_);
    if (ignored_errors > total_ignored_errors_) {
      total_ignored_errors_ = ignored_errors;
      COUNTER_SET(num_row_errors_, total_ignored_errors_);
      if (is_transactional_ && !FLAGS_kudu_ignore_conflicts_in_transaction) {
        // Return general status error to abort transaction.
        return Status("Kudu reported write operation errors during transaction.");
      }
    }
  }

  // Regardless of ignore_conflicts_ value, we still need to check for pending errors
  // existence for the following possible scenarios:
  // - sink_action_ is UPSERT.
  // - Received other Kudu error such as timeouts or column constraint violations.
  if (session_->CountPendingErrors() == 0) return Status::OK();

  vector<KuduError*> errors;
  Status status = Status::OK();

  // Get the pending errors from the Kudu session. If errors overflowed the error buffer
  // we can't be sure all errors can be ignored, so an error status will be reported.
  bool error_overflow = false;
  session_->GetPendingErrors(&errors, &error_overflow);
  if (is_transactional_) {
    // Return the first error so that the transaction will be aborted.
    status = Status(
        strings::Substitute("Kudu reported error: $0", errors[0]->status().ToString()));
  } else if (UNLIKELY(error_overflow)) {
    status = Status("Error overflow in Kudu session.");
  }

  // The memory for the errors is manually managed. Iterate over all errors and delete
  // them accordingly.
  for (int i = 0; i < errors.size(); ++i) {
    kudu::Status e = errors[i]->status();
    if (e.IsNotFound()) {
      // Kudu does not yet have a way to programmatically differentiate between 'row not
      // found' and 'tablet not found' (e.g. PK in a non-covered range) and both have the
      // IsNotFound error code.
      state->LogError(ErrorMsg::Init(TErrorCode::KUDU_NOT_FOUND,
          table_desc_->table_name(), e.ToString()), 2);
    } else if (e.IsAlreadyPresent()) {
      state->LogError(ErrorMsg::Init(TErrorCode::KUDU_KEY_ALREADY_PRESENT,
          table_desc_->table_name()), 2);
    } else {
      if (status.ok()) {
        status = Status(strings::Substitute(
            "Kudu error(s) reported, first error: $0", e.ToString()));
      }
      state->LogError(ErrorMsg::Init(TErrorCode::KUDU_SESSION_ERROR,
          table_desc_->table_name(), e.ToString()), 2);
    }
    delete errors[i];
  }

  COUNTER_ADD(num_row_errors_, errors.size());
  return status;
}

Status KuduTableSink::FlushFinal(RuntimeState* state) {
  kudu::Status flush_status = session_->Flush();

  // Flush() may return an error status but any errors will also be reported by
  // CheckForErrors(), so it's safe to ignore and always call CheckForErrors.
  if (!flush_status.ok()) {
    VLOG_RPC << "Ignoring Flush() error status: " << flush_status.ToString();
  }
  Status status = CheckForErrors(state);
  state->dml_exec_state()->SetKuduDmlStats(
      total_rows_->value() - num_row_errors_->value(), num_row_errors_->value(),
      client_->GetLatestObservedTimestamp());
  return status;
}

void KuduTableSink::Close(RuntimeState* state) {
  if (closed_) return;
  session_.reset();
  total_ignored_errors_ = 0;
  mem_tracker_->Release(client_tracked_bytes_);
  txn_.reset();
  client_.reset();
  SCOPED_TIMER(profile()->total_time_counter());
  DataSink::Close(state);
  closed_ = true;
}

}  // namespace impala
