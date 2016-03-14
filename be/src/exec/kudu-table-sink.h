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

#ifndef IMPALA_EXEC_KUDU_TABLE_SINK_H
#define IMPALA_EXEC_KUDU_TABLE_SINK_H

#include <boost/scoped_ptr.hpp>
#ifdef USE_KUDU
#include <kudu/client/client.h>
#endif

#include "gen-cpp/ImpalaInternalService_constants.h"
#include "common/status.h"
#ifdef USE_KUDU
#include "exec/kudu-util.h"
#endif
#include "exec/data-sink.h"
#include "exprs/expr-context.h"
#include "exprs/expr.h"

namespace impala {

#ifdef USE_KUDU
/// Sink that takes RowBatches and writes them into Kudu.
/// Currently the data is sent to Kudu on Send(), i.e. the data is batched on the
/// KuduSession until all the rows in a RowBatch are applied and then the session
/// is flushed.
///
/// Kudu doesn't have transactions (yet!) so some rows may fail to write while
/// others are successful. This sink will return an error if any of the rows fails
/// to be written.
///
/// TODO Once Kudu actually implements AUTOFLUSH_BACKGROUND flush mode we should
/// change the flushing behavior as it will likely make writes more efficient.
class KuduTableSink : public DataSink {
 public:
  KuduTableSink(const RowDescriptor& row_desc,
      const std::vector<TExpr>& select_list_texprs, const TDataSink& tsink);

  /// Prepares the expressions to be applied and creates a KuduSchema based on the
  /// expressions and KuduTableDescriptor.
  virtual Status Prepare(RuntimeState* state);

  /// Connects to Kudu and creates the KuduSession to be used for the writes.
  virtual Status Open(RuntimeState* state);

  /// Transforms 'batch' into Kudu writes and sends them to Kudu.
  /// The KuduSession is flushed on each row batch.
  virtual Status Send(RuntimeState* state, RowBatch* batch, bool eos);

  /// Does nothing. We currently flush on each Send() call.
  virtual Status FlushFinal(RuntimeState* state);

  /// Closes the KuduSession and the expressions.
  virtual void Close(RuntimeState* state);

  virtual RuntimeProfile* profile() { return runtime_profile_; }

 private:
  /// Turn thrift TExpr into Expr and prepare them to run
  Status PrepareExprs(RuntimeState* state);

  /// Create a new write operation according to the sink type.
  kudu::client::KuduWriteOperation* NewWriteOp();

  /// Flushes the Kudu session, making sure all previous operations were committed, and handles
  /// errors returned from Kudu. Passes the number of errors during the flush operations as an
  /// out parameter.
  /// Returns a non-OK status if there was an unrecoverable error. This might return an OK
  /// status even if 'error_count' is > 0, as some errors might be ignored.
  Status Flush(int64_t* error_count);

  /// Used to get the KuduTableDescriptor from the RuntimeState
  TableId table_id_;

  /// Row descriptor of row batches passed in Send(). Set in c'tor.
  const RowDescriptor& row_desc_;
  /// The descriptor of the KuduTable being written to. Set on Prepare().
  const KuduTableDescriptor* table_desc_;

  /// The expression descriptors and the prepared expressions. The latter are built
  /// on Prepare().
  const std::vector<TExpr>& select_list_texprs_;
  std::vector<ExprContext*> output_expr_ctxs_;

  /// The Kudu client, table and session.
  /// This uses 'std::tr1::shared_ptr' as that is the type expected by Kudu.
  std::tr1::shared_ptr<kudu::client::KuduClient> client_;
  std::tr1::shared_ptr<kudu::client::KuduTable> table_;
  std::tr1::shared_ptr<kudu::client::KuduSession> session_;

  /// Allocated from runtime state's pool.
  RuntimeProfile* runtime_profile_;

  /// Used to specify the type of write operation (INSERT/UPDATE/DELETE).
  TSinkAction::type sink_action_;

  /// Captures parameters passed down from the frontend
  TKuduTableSink kudu_table_sink_;

  /// Counts the number of calls to KuduSession::flush().
  RuntimeProfile::Counter* kudu_flush_counter_;

  /// Aggregates the times spent in KuduSession:flush().
  RuntimeProfile::Counter* kudu_flush_timer_;

  /// Total number of errors returned from Kudu.
  RuntimeProfile::Counter* kudu_error_counter_;

  /// Total number of rows written including errors.
  RuntimeProfile::Counter* rows_written_;
  RuntimeProfile::Counter* rows_written_rate_;

};

#else  // No Kudu

class KuduTableSink : public DataSink {
 public:
  KuduTableSink(const RowDescriptor& row_desc,
      const std::vector<TExpr>& select_list_texprs, const TDataSink& tsink) {}

  virtual Status Prepare(RuntimeState* state) {
    return Status(TErrorCode::KUDU_NOT_SUPPORTED_ON_OS);
  }

  virtual Status Open(RuntimeState* state) {
    return Status(TErrorCode::KUDU_NOT_SUPPORTED_ON_OS);
  }

  virtual Status Send(RuntimeState* state, RowBatch* batch, bool eos) {
    return Status(TErrorCode::KUDU_NOT_SUPPORTED_ON_OS);
  }

  virtual void Close(RuntimeState* state) {}

  virtual RuntimeProfile* profile() { return NULL; }
};

#endif
}  // namespace impala

#endif // IMPALA_EXEC_KUDU_TABLE_SINK_H
