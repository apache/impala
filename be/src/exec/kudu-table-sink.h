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

#ifndef IMPALA_EXEC_KUDU_TABLE_SINK_H
#define IMPALA_EXEC_KUDU_TABLE_SINK_H

#include <boost/scoped_ptr.hpp>
#include <kudu/client/client.h>

#include "gen-cpp/ImpalaInternalService_constants.h"
#include "common/status.h"
#include "exec/kudu-util.h"
#include "exec/data-sink.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"

namespace impala {

/// Sink that takes RowBatches and writes them into a Kudu table.
///
/// The data is added to Kudu in Send(). The Kudu client is configured to automatically
/// flush records when enough data has been written (AUTO_FLUSH_BACKGROUND). This
/// requires specifying a mutation buffer size and a buffer flush watermark percentage in
/// the Kudu client. The mutation buffer needs to be large enough to buffer rows sent to
/// all destination nodes because the buffer accounting is not specified per-tablet
/// server (KUDU-1693). Tests showed that 10MB was a good default, and this is
/// configurable via the gflag --kudu_mutation_buffer_size. The buffer flush watermark
/// percentage is set to a value that results in Kudu flushing after 7MB is in a
/// buffer for a particular destination (of the 10MB of the total mutation buffer space)
/// because Kudu currently has some 8MB buffer limits.
///
/// Kudu doesn't have transactions yet, so some rows may fail to write while others are
/// successful. The Kudu client reports errors, some of which are treated as warnings and
/// will not fail the query: PK already exists on INSERT, key not found on UPDATE/DELETE,
/// NULL in a non-nullable column, and PK specifying rows in an uncovered range.
/// The number of rows that cannot be modified due to these errors is reported in the
/// TInsertPartitionStatus report sent by the DataSink to the coordinator.
/// Any other kind of error reported by Kudu results in the sink returning an error
/// status. All reported errors (ignored or not) will be logged via the RuntimeState.
class KuduTableSink : public DataSink {
 public:
  KuduTableSink(TDataSinkId sink_id, const RowDescriptor* row_desc,
      const TDataSink& tsink, RuntimeState* state);

  /// Prepares the expressions to be applied and creates a KuduSchema based on the
  /// expressions and KuduTableDescriptor.
  virtual Status Prepare(RuntimeState* state, MemTracker* parent_mem_tracker);

  /// Connects to Kudu and creates the KuduSession to be used for the writes.
  virtual Status Open(RuntimeState* state);

  /// Transforms 'batch' into Kudu writes and sends them to Kudu.
  /// The KuduSession is flushed on each row batch.
  virtual Status Send(RuntimeState* state, RowBatch* batch);

  /// Forces any remaining buffered operations to be flushed to Kudu.
  virtual Status FlushFinal(RuntimeState* state);

  /// Closes the KuduSession and the expressions.
  virtual void Close(RuntimeState* state);

 private:
  /// Create a new write operation according to the sink type.
  kudu::client::KuduWriteOperation* NewWriteOp();

  /// Checks for any errors buffered in the Kudu session, and increments
  /// appropriate counters for ignored errors.
  //
  /// Returns a bad Status if there are non-ignorable errors.
  Status CheckForErrors(RuntimeState* state) WARN_UNUSED_RESULT;

  /// Used to get the KuduTableDescriptor from the RuntimeState
  TableId table_id_;

  /// The descriptor of the KuduTable being written to. Set on Prepare().
  const KuduTableDescriptor* table_desc_;

  /// The Kudu client, owned by the ExecEnv.
  kudu::client::KuduClient* client_ = nullptr;
  /// The Kudu table and session.
  kudu::client::sp::shared_ptr<kudu::client::KuduTable> table_;
  kudu::client::sp::shared_ptr<kudu::client::KuduSession> session_;

  /// A cache of the nullability of each Kudu column. The Kudu schema accessor
  /// is not inlined and actually creates a copy (see IMPALA-8284).
  ///
  /// Initialized in Open().
  std::vector<bool> kudu_column_nullabilities_;

  /// Used to specify the type of write operation (INSERT/UPDATE/DELETE).
  TSinkAction::type sink_action_;

  /// Captures parameters passed down from the frontend
  TKuduTableSink kudu_table_sink_;

  /// The amount consumed from 'mem_tracker_' to account for the mem used by 'client_'.
  int64_t client_tracked_bytes_;

  /// Time spent applying Kudu operations. In normal circumstances, Apply() should be
  /// negligible because it is asynchronous with AUTO_FLUSH_BACKGROUND enabled.
  /// Significant time spent in Apply() may indicate that Kudu cannot buffer and send
  /// rows as fast as the sink can write them.
  RuntimeProfile::Counter* kudu_apply_timer_;

  /// Total number of rows processed, i.e. rows written to Kudu and also rows with
  /// errors.
  RuntimeProfile::Counter* total_rows_ = nullptr;

  /// The number of rows with errors.
  RuntimeProfile::Counter* num_row_errors_ = nullptr;

  /// Rate at which the sink consumes and processes rows, i.e. writing rows to Kudu or
  /// skipping rows that are known to violate nullability constraints.
  RuntimeProfile::Counter* rows_processed_rate_ = nullptr;
};

}  // namespace impala

#endif // IMPALA_EXEC_KUDU_TABLE_SINK_H
