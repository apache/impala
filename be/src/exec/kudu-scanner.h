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

#ifndef IMPALA_EXEC_KUDU_SCANNER_H_
#define IMPALA_EXEC_KUDU_SCANNER_H_

#include <boost/scoped_ptr.hpp>
#include <kudu/client/client.h>

#include "exec/kudu-scan-node.h"
#include "runtime/descriptors.h"

namespace impala {

class MemPool;
class RowBatch;
class RuntimeState;
class Tuple;

/// Executes scans in Kudu based on the provided set of scan ranges.
class KuduScanner {
 public:
  KuduScanner(KuduScanNode* scan_node, RuntimeState* state);

  /// Prepares this scanner for execution.
  /// Does not actually open a kudu::client::KuduScanner.
  Status Open();

  /// Opens a new kudu::client::KuduScanner to scan 'range'.
  Status OpenNextRange(const TKuduKeyRange& range);

  /// Fetches the next batch from the current kudu::client::KuduScanner.
  Status GetNext(RowBatch* row_batch, bool* eos);

  /// Sends a "Ping" to the Kudu TabletServer servicing the current scan, if there is one.
  /// This serves the purpose of making the TabletServer keep the server side scanner alive
  /// if the batch queue is full and no batches can be queued.
  Status KeepKuduScannerAlive();

  /// Closes this scanner.
  void Close();

 private:
  /// Handles the case where the projection is empty (e.g. count(*)).
  /// Does this by adding sets of rows to 'row_batch' instead of adding one-by-one.
  Status HandleEmptyProjection(RowBatch* row_batch, bool* batch_done);

  /// Set 'slot' to Null in 'tuple'.
  void SetSlotToNull(Tuple* tuple, const SlotDescriptor& slot);

  /// Returns true if 'slot' is Null in 'tuple'.
  bool IsSlotNull(Tuple* tuple, const SlotDescriptor& slot);

  /// Returns true if the current block hasn't been fully scanned.
  bool CurrentBlockHasMoreRows() {
    return rows_scanned_current_block_ < cur_kudu_batch_.NumRows();
  }

  /// Decodes rows previously fetched from kudu, now in 'cur_rows_' into a RowBatch.
  ///  - 'batch' is the batch that will point to the new tuples.
  ///  - *tuple_mem should be the location to output tuples.
  ///  - Sets 'batch_done' to true to indicate that the batch was filled to capacity or the limit
  ///    was reached.
  Status DecodeRowsIntoRowBatch(RowBatch* batch, Tuple** tuple_mem, bool* batch_done);

  /// Returns true of the current kudu::client::KuduScanner has more rows.
  bool CurrentRangeHasMoreBlocks() {
    return scanner_->HasMoreRows();
  }

  /// Fetches the next block of the current kudu::client::KuduScanner.
  Status GetNextBlock();

  /// Closes the current kudu::client::KuduScanner.
  void CloseCurrentRange();

  /// Given a tuple, copies the values of those columns that require additional memory from memory
  /// owned by the kudu::client::KuduScanner into memory owned by the RowBatch. Assumes that the
  /// other columns are already materialized.
  Status RelocateValuesFromKudu(Tuple* tuple, MemPool* mem_pool);

  /// Transforms a kudu row into an Impala row. Columns that don't require auxiliary
  /// memory are copied to the tuple directly. String columns are stored as a reference to
  /// the memory of the RowPtr and need to be relocated later.
  Status KuduRowToImpalaTuple(const kudu::client::KuduScanBatch::RowPtr& row,
      RowBatch* row_batch, Tuple* tuple);

  inline Tuple* next_tuple(Tuple* t) const {
    uint8_t* mem = reinterpret_cast<uint8_t*>(t);
    return reinterpret_cast<Tuple*>(mem + scan_node_->tuple_desc()->byte_size());
  }

  /// Returns the tuple idx into the row for this scan node to output to.
  /// Currently this is always 0.
  int tuple_idx() const { return 0; }

  KuduScanNode* scan_node_;
  RuntimeState* state_;

  /// The set of key ranges being serviced by this scanner.
  const vector<TKuduKeyRange> scan_ranges_;

  /// The kudu::client::KuduScanner for the current range.
  /// One such scanner is required per range as per-range start/stop keys can only be set once.
  boost::scoped_ptr<kudu::client::KuduScanner> scanner_;

  /// The current batch of retrieved rows.
  kudu::client::KuduScanBatch cur_kudu_batch_;
  int rows_scanned_current_block_;

  /// The last time a keepalive request or successful RPC was sent.
  int64_t last_alive_time_micros_;

  /// The scanner's cloned copy of the conjuncts to apply.
  vector<ExprContext*> conjunct_ctxs_;

  std::vector<std::string> projected_columns_;

  /// Size of the materialized tuple in the row batch.
  int tuple_byte_size_;

  /// Number of bytes needed to represent the null bits in the tuple.
  int tuple_num_null_bytes_;

  /// List of string slots that need relocation for their auxiliary memory.
  std::vector<SlotDescriptor*> string_slots_;
};

} /// namespace impala

#endif /// IMPALA_EXEC_KUDU_SCANNER_H_
