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

#ifndef IMPALA_EXEC_HBASE_TABLE_SINK_H
#define IMPALA_EXEC_HBASE_TABLE_SINK_H

#include <vector>

#include "common/status.h"
#include "runtime/runtime-state.h"
#include "runtime/row-batch.h"
#include "runtime/descriptors.h"
#include "exec/data-sink.h"
#include "exec/hbase-table-writer.h"
#include "gen-cpp/Data_types.h"
#include "gen-cpp/Exprs_types.h"

namespace impala {

/// Class to take row batches and send them to the HBaseTableWriter to
/// eventually be written into an HBase table.
class HBaseTableSink : public DataSink {
 public:
  HBaseTableSink(const RowDescriptor& row_desc,
                 const std::vector<TExpr>& select_list_texprs,
                 const TDataSink& tsink);
  virtual Status Prepare(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
  virtual Status Send(RuntimeState* state, RowBatch* batch, bool eos);
  virtual Status FlushFinal(RuntimeState* state);
  virtual void Close(RuntimeState* state);
  virtual RuntimeProfile* profile() { return runtime_profile_; }

 private:
  /// Turn thrift TExpr into Expr and prepare them to run
  Status PrepareExprs(RuntimeState* state);

  /// Used to get the HBaseTableDescriptor from the RuntimeState
  TableId table_id_;

  /// The description of the table.  Used for table name and column mapping.
  HBaseTableDescriptor* table_desc_;

  /// The object that this sink uses to write to hbase.
  /// hbase_table_writer is owned by this sink and should be closed
  /// when this is Close'd.
  boost::scoped_ptr<HBaseTableWriter> hbase_table_writer_;

  /// Owned by the RuntimeState.
  const RowDescriptor& row_desc_;

  /// Owned by the RuntimeState.
  const std::vector<TExpr>& select_list_texprs_;
  std::vector<ExprContext*> output_expr_ctxs_;

  /// Allocated from runtime state's pool.
  RuntimeProfile* runtime_profile_;
};

}  // namespace impala

#endif  // IMPALA_EXEC_HBASE_TABLE_SINK_H
