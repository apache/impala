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

/// Executes scans in Kudu based on the provided set of scan ranges.
class KuduScanner {
 public:
  KuduScanner(KuduScanNode* scan_node, RuntimeState* state,
      const vector<TKuduKeyRange>& scan_ranges);

  /// Opens the first scanner into the provided table, with the provided
  /// client.
  Status Open(const std::tr1::shared_ptr<kudu::client::KuduClient>& client,
      const std::tr1::shared_ptr<kudu::client::KuduTable>& table);

  /// Fetches the next batch from Kudu.
  /// TODO make the scanner manages its own batches (like the HDFS scanner
  /// does) for now there is no point since there is a single instance.
  Status GetNext(RowBatch* row_batch, bool* eos);

  /// Closes the Kudu scanner.
  Status Close();

 private:
  // Set 'tuple' slot 'mat_slot_idx' to null.
  void SetSlotToNull(Tuple* tuple, int mat_slot_idx);

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

  /// Returns whether there are more scan ranges to scan.
  bool HasMoreScanners() {
    return cur_scan_range_idx_ < scan_ranges_.size();
  }

  /// Opens the next scanner, requires there are more ranges to scan.
  Status GetNextScanner();

  /// Transforms a kudu row in an impala row. For non string values data is copied
  /// directly to the right slot. For string values data is copied to the row batch's
  /// tuple data pool and a prt/len is set on the slot.
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

  /// The index into scan_range_params_ for the range currently being serviced.
  int cur_scan_range_idx_;

  /// The current set of retrieved rows.
  std::vector<kudu::client::KuduRowResult> cur_rows_;
  size_t rows_scanned_current_block_;
};

} /// namespace impala

#endif /// IMPALA_EXEC_KUDU_SCANNER_H_
