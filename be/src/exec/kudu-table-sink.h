// Copyright 2012 Cloudera Inc.
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
#include <kudu/client/client.h>

#include "common/status.h"
#include "exec/data-sink.h"

namespace impala {

// Sink that takes RowBatches and inserts them into Kudu.
// Currently the data is sent to Kudu on Send(), i.e. the data is batched on the
// KuduSession until all the rows in a RowBatch are applied and then the session
// is flushed.
//
// Kudu doesn't have transactions (yet!) so some rows may fail to insert while
// others are successful. This sink will return an error if any of the rows fails
// to insert.
//
// TODO Once Kudu actually implements AUTOFLUSH_BACKGROUND flush mode we should
// change the flushing behavior as it will likely make inserts more efficient.
class KuduTableSink : public DataSink {
 public:
  KuduTableSink(const RowDescriptor& row_desc,
      const std::vector<TExpr>& select_list_texprs, const TDataSink& tsink);

  // Prepares the expressions to be applied and creates a KuduSchema based on the
  // expressions and KuduTableDescriptor.
  Status Prepare(RuntimeState* state);

  // Connects to Kudu and creates the KuduSession to be used for the inserts.
  Status Open(RuntimeState* state);

  // Transforms 'batch' into Kudu inserts and sends them to Kudu.
  // The session is flushed on each Kudu batch.
  Status Send(RuntimeState* state, RowBatch* batch, bool eos);

  // Closes the KuduSession and the expressions.
  void Close(RuntimeState* state);

  RuntimeProfile* profile() { return runtime_profile_; }

 private:
  // Turn thrift TExpr into Expr and prepare them to run
  Status PrepareExprs(RuntimeState* state);

  /// Used to get the KuduTableDescriptor from the RuntimeState
  TableId table_id_;

  // Owned by the RuntimeState.
  const RowDescriptor& row_desc_;
  const KuduTableDescriptor* table_desc_;

  // The expression descriptors and the prepared expressions. The latter are built
  // on Prepare().
  // Owned by the RuntimeState.
  const std::vector<TExpr>& select_list_texprs_;
  std::vector<ExprContext*> output_expr_ctxs_;

  /// The schema of the materialized slots (i.e projection)
  kudu::client::KuduSchema schema_;

  /// The Kudu client, table and session.
  std::tr1::shared_ptr<kudu::client::KuduClient> client_;
  scoped_refptr<kudu::client::KuduTable> table_;
  std::tr1::shared_ptr<kudu::client::KuduSession> session_;

  // Allocated from runtime state's pool.
  RuntimeProfile* runtime_profile_;
};

}  // namespace impala

#endif // IMPALA_EXEC_KUDU_TABLE_SINK_H
