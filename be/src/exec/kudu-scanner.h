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

#ifndef IMPALA_EXEC_KUDU_SCANNER_H_
#define IMPALA_EXEC_KUDU_SCANNER_H_

#include <boost/scoped_ptr.hpp>
#include <kudu/client/client.h>

#include "common/object-pool.h"
#include "exec/kudu-scan-node-base.h"
#include "runtime/descriptors.h"

namespace impala {

class MemPool;
class RowBatch;
class RuntimeState;
class Tuple;

/// Wraps a Kudu client scanner to fetch row batches from Kudu. The Kudu client scanner
/// is created from a scan token in OpenNextScanToken(), which then provides rows fetched
/// by GetNext() until it reaches eos, and the caller may open another scan token.
class KuduScanner {
 public:
  KuduScanner(KuduScanNodeBase* scan_node, RuntimeState* state);

  /// Prepares this scanner for execution.
  /// Does not actually open a kudu::client::KuduScanner.
  Status Open();

  /// Opens a new kudu::client::KuduScanner using 'scan_token'.
  Status OpenNextScanToken(const std::string& scan_token);

  /// Fetches the next batch from the current kudu::client::KuduScanner.
  Status GetNext(RowBatch* row_batch, bool* eos);

  /// Sends a "Ping" to the Kudu TabletServer servicing the current scan, if there is
  /// one. This serves the purpose of making the TabletServer keep the server side
  /// scanner alive if the batch queue is full and no batches can be queued. If there are
  /// any errors, they are ignored here, since we assume that we will just fail the next
  /// time we try to read a batch.
  void KeepKuduScannerAlive();

  /// Closes this scanner.
  void Close();

 private:
  /// Handles the case where the projection is empty (e.g. count(*)).
  /// Does this by adding sets of rows to 'row_batch' instead of adding one-by-one.
  Status HandleEmptyProjection(RowBatch* row_batch);

  /// Decodes rows previously fetched from kudu, now in 'cur_rows_' into a RowBatch.
  ///  - 'batch' is the batch that will point to the new tuples.
  ///  - *tuple_mem should be the location to output tuples.
  /// Returns OK when one of the following conditions occur:
  ///  - cur_kudu_batch_ is fully consumed
  ///  - batch is full
  ///  - scan_node_ limit has been reached
  Status DecodeRowsIntoRowBatch(RowBatch* batch, Tuple** tuple_mem);

  /// Fetches the next batch of rows from the current kudu::client::KuduScanner.
  Status GetNextScannerBatch();

  /// Closes the current kudu::client::KuduScanner.
  void CloseCurrentClientScanner();

  inline Tuple* next_tuple(Tuple* t) const {
    uint8_t* mem = reinterpret_cast<uint8_t*>(t);
    return reinterpret_cast<Tuple*>(mem + scan_node_->tuple_desc()->byte_size());
  }

  KuduScanNodeBase* scan_node_;
  RuntimeState* state_;

  /// For objects which have the same life time as the scanner.
  ObjectPool obj_pool_;

  /// MemPool used for expr-managed allocations in expression evaluators in this scanner.
  /// Need to be local to each scanner as MemPool is not thread safe.
  boost::scoped_ptr<MemPool> expr_perm_pool_;

  /// MemPool used for allocations by expression evaluators in this scanner that hold
  /// results of expression evaluation. Need to be local to each scanner as MemPool is
  /// not thread safe.
  boost::scoped_ptr<MemPool> expr_results_pool_;

  /// The kudu::client::KuduScanner for the current scan token. A new KuduScanner is
  /// created for each scan token using KuduScanToken::DeserializeIntoScanner().
  boost::scoped_ptr<kudu::client::KuduScanner> scanner_;

  /// The current batch of retrieved rows.
  kudu::client::KuduScanBatch cur_kudu_batch_;

  /// The number of rows already read from cur_kudu_batch_.
  int cur_kudu_batch_num_read_;

  /// The last time a keepalive request or successful RPC was sent.
  int64_t last_alive_time_micros_;

  /// The scanner's cloned copy of the conjuncts to apply.
  vector<ScalarExprEvaluator*> conjunct_evals_;

  /// Timestamp slots in the tuple descriptor of the scan node. Used to convert Kudu
  /// UNIXTIME_MICRO values inline.
  vector<const SlotDescriptor*> timestamp_slots_;
};

} /// namespace impala

#endif /// IMPALA_EXEC_KUDU_SCANNER_H_
