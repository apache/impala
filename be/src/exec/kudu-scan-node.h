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

#ifndef IMPALA_EXEC_KUDU_SCAN_NODE_H_
#define IMPALA_EXEC_KUDU_SCAN_NODE_H_

#include <boost/scoped_ptr.hpp>
#include <gtest/gtest.h>
#include <kudu/client/client.h>

#include "exec/scan-node.h"
#include "runtime/descriptors.h"
#include "runtime/thread-resource-mgr.h"
#include "gutil/gscoped_ptr.h"
#include "util/thread.h"

namespace impala {

class KuduScanner;
class Tuple;

/// A scan node that scans Kudu TabletServers.
///
/// This scan node takes a set of ranges and uses a Kudu client to retrieve the data
/// belonging to those ranges from Kudu. The client's schema is rebuilt from the
/// TupleDescriptors forwarded by the frontend so that we're sure all the scan nodes
/// use the same schema, for the same scan.
class KuduScanNode : public ScanNode {
 public:
  KuduScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  ~KuduScanNode();

  /// Create Kudu schema and columns to slots mapping.
  virtual Status Prepare(RuntimeState* state);

  /// Start Kudu scan.
  virtual Status Open(RuntimeState* state);

  /// Fill the next row batch by fetching more data from the KuduScanner.
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);

  /// Close connections to Kudu.
  virtual void Close(RuntimeState* state);

 protected:
  /// Write debug string of this into out.
  virtual void DebugString(int indentation_level, std::stringstream* out) const;

 private:
  FRIEND_TEST(KuduScanNodeTest, TestPushIntGEPredicateOnKey);
  FRIEND_TEST(KuduScanNodeTest, TestPushIntEQPredicateOn2ndColumn);
  FRIEND_TEST(KuduScanNodeTest, TestPushStringLEPredicateOn3rdColumn);
  FRIEND_TEST(KuduScanNodeTest, TestPushTwoPredicatesOnNonMaterializedColumn);
  friend class KuduScanner;

  kudu::client::KuduClient* kudu_client() {
    return client_.get();
  }

  kudu::client::KuduTable* kudu_table() {
    return table_.get();
  }

  /// Tuple id resolved in Prepare() to set tuple_desc_.
  const TupleId tuple_id_;

  RuntimeState* runtime_state_;

  /// Descriptor of tuples read from Kudu table.
  const TupleDescriptor* tuple_desc_;

  /// The list of Kudu columns to project for the scan, extracted from the TupleDescriptor
  /// and translated into Kudu format, i.e. matching the case of the Kudu schema.
  std::vector<std::string> projected_columns_;

  /// The Kudu client and table. Scanners share these instances.
  std::tr1::shared_ptr<kudu::client::KuduClient> client_;
  std::tr1::shared_ptr<kudu::client::KuduTable> table_;

  /// Set of ranges to be scanned.
  std::vector<TKuduKeyRange> key_ranges_;

  /// The next index in 'key_ranges_' to be assigned to a scanner.
  int next_scan_range_idx_;

  // Outgoing row batches queue. Row batches are produced asynchronously by the scanner
  // threads and consumed by the main thread.
  boost::scoped_ptr<RowBatchQueue> materialized_row_batches_;

  /// Protects access to state accessed by scanner threads, such as 'status_' or
  /// 'num_active_scanners_'.
  boost::mutex lock_;

  /// The current status of the scan, set to non-OK if any problems occur, e.g. if an error
  /// occurs in a scanner.
  /// Protected by lock_
  Status status_;

  /// Number of active running scanner threads.
  /// Protected by lock_
  int num_active_scanners_;

  /// Set to true when the scan is complete (either because all ranges were scanned, the limit
  /// was reached or some error occurred).
  /// Protected by lock_
  volatile bool done_;

  /// Maximum size of materialized_row_batches_.
  int max_materialized_row_batches_;

  /// Thread group for all scanner worker threads
  ThreadGroup scanner_threads_;

  RuntimeProfile::Counter* kudu_read_timer_;
  RuntimeProfile::Counter* kudu_round_trips_;
  static const std::string KUDU_READ_TIMER;
  static const std::string KUDU_ROUND_TRIPS;

  /// The function names of the supported predicates.
  static const std::string GE_FN;
  static const std::string LE_FN;
  static const std::string EQ_FN;

  /// The set of conjuncts, in TExpr form, to be pushed to Kudu.
  /// Received in TPlanNode::kudu_scan_node.
  const std::vector<TExpr> pushable_conjuncts_;

  /// The id of the callback added to the thread resource manager when thread token
  /// is available. Used to remove the callback before this scan node is destroyed.
  /// -1 if no callback is registered.
  int thread_avail_cb_id_;

  /// The set of conjuncts, in KuduPredicate form, to be pushed to Kudu.
  /// Derived from 'pushable_conjuncts_'.
  std::vector<kudu::client::KuduPredicate*> kudu_predicates_;

  /// Returns a KuduValue with 'type' built from a literal in 'node'.
  /// Expects that 'node' is a literal value.
  Status GetExprLiteralBound(const TExprNode& node,
      kudu::client::KuduColumnSchema::DataType type, kudu::client::KuduValue** value);

  /// Returns a string with the name of the column that 'node' refers to.
  void GetSlotRefColumnName(const TExprNode& node, string* col_name);

  /// Transforms 'pushable_conjuncts_' received from the frontend into 'kudu_predicates_' that will
  /// be set in all scanners.
  Status TransformPushableConjunctsToRangePredicates();

  /// Called when scanner threads are available for this scan node. This will
  /// try to spin up as many scanner threads as the quota allows.
  void ThreadTokenAvailableCb(ThreadResourceMgr::ResourcePool* pool);

  /// Main function for scanner thread which executes a KuduScanner. Begins by processing
  /// 'initial_range', and continues processing ranges returned by 'GetNextKeyRange()'
  /// until there are no more ranges, an error occurs, or the limit is reached.
  void ScannerThread(const string& name, const TKuduKeyRange* initial_range);

  /// Processes a single scan range. Row batches are fetched using 'scanner' and enqueued
  /// in 'materialized_row_batches_' until the scanner reports eos for 'key_range', an
  /// error occurs, or the limit is reached.
  Status ProcessRange(KuduScanner* scanner, const TKuduKeyRange* key_range);

  /// Returns the next partition key range to read. Thread safe. Returns NULL if there are
  /// no more ranges.
  TKuduKeyRange* GetNextKeyRange();

  const std::vector<std::string>& projected_columns() const { return projected_columns_; }

  const TupleDescriptor* tuple_desc() const { return tuple_desc_; }

  // Returns a cloned copy of the scan node's conjuncts. Requires that the expressions
  // have been open previously.
  Status GetConjunctCtxs(vector<ExprContext*>* ctxs);

  // Clones the set of predicates to be set on scanners.
  void ClonePredicates(vector<kudu::client::KuduPredicate*>* predicates);

  RuntimeProfile::Counter* kudu_read_timer() const { return kudu_read_timer_; }
  RuntimeProfile::Counter* kudu_round_trips() const { return kudu_round_trips_; }
};

}

#endif
