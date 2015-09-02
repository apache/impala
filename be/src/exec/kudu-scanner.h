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

  /// Opens the first scanner into the provided table, with the provided
  /// client.
  Status Open(const std::tr1::shared_ptr<kudu::client::KuduClient>& client,
      const std::tr1::shared_ptr<kudu::client::KuduTable>& table);

  /// Opens the next scanner to read range.
  Status OpenNextScanner(const TKuduKeyRange& range);

  /// Fetches the next batch from Kudu.
  /// TODO make the scanner manages its own batches (like the HDFS scanner
  /// does) for now there is no point since there is a single instance.
  Status GetNext(RowBatch* row_batch, bool* eos);

  /// Closes the Kudu scanner.
  void Close();

 private:
  /// Set 'tuple' slot 'mat_slot_idx' to null.
  void SetSlotToNull(Tuple* tuple, const SlotDescriptor& slot);

  /// Returns true if the slot is null.
  bool IsSlotNull(Tuple* tuple, const SlotDescriptor& slot);

  /// Returns true if the current block hasn't been fully scanned.
  bool CurrentBlockHasMoreRows() {
    return rows_scanned_current_block_ < cur_rows_.size();
  }

  /// Decodes rows previously fetched from kudu, now in 'cur_rows_' into a RowBatch.
  ///  - 'batch' is the batch that will point to the new tuples.
  ///  - *tuple_mem should be the location to output tuples.
  ///  - 'batch_done' on return, indicates whether the batch was filled to capacity.
  Status DecodeRowsIntoRowBatch(RowBatch* batch, Tuple** tuple_mem, bool* batch_done);

  /// Returns true of the current range has more rows.
  bool CurrentScannerHasMoreBlocks() {
    return scanner_->HasMoreRows();
  }

  /// Fetches the next block of the current scanner.
  Status GetNextBlock();

  /// Closes the current scanner.
  void CloseCurrentScanner();

  /// Given a tuple, relocates the values of those columns that require additional memory
  /// from their current location into memory owned by the RowBatch. Assumes that the
  /// other columns are already materialized.
  Status RelocateValuesFromKudu(Tuple* tuple, MemPool* mem_pool);

  /// Transforms a kudu row into an Impala row. Columns that don't require auxiliary
  /// memory are copied to the tuple directly. String columns are stored as a reference to
  /// the memory of the KuduRowResult and need to be relocated later.
  Status KuduRowToImpalaTuple(const kudu::client::KuduRowResult& row,
      RowBatch* row_batch, Tuple* tuple);

  inline Tuple* next_tuple(Tuple* t) const {
    uint8_t* mem = reinterpret_cast<uint8_t*>(t);
    return reinterpret_cast<Tuple*>(mem + scan_node_->tuple_desc()->byte_size());
  }

  KuduScanNode* scan_node_;
  RuntimeState* state_;

  /// The set of key ranges being serviced by this scanner.
  const vector<TKuduKeyRange> scan_ranges_;

  /// References to the client and table being used.
  std::tr1::shared_ptr<kudu::client::KuduClient> client_;
  std::tr1::shared_ptr<kudu::client::KuduTable> table_;

  /// The current Kudu scanner.
  boost::scoped_ptr<kudu::client::KuduScanner> scanner_;

  /// The current set of retrieved rows.
  std::vector<kudu::client::KuduRowResult> cur_rows_;
  size_t rows_scanned_current_block_;

  /// The scanner's cloned copy of the conjuncts to apply.
  vector<ExprContext*> conjunct_ctxs_;

  std::vector<SlotDescriptor*> materialized_slots_;
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
