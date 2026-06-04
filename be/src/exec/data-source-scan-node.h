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


#ifndef IMPALA_EXEC_DATA_SOURCE_SCAN_NODE_H_
#define IMPALA_EXEC_DATA_SOURCE_SCAN_NODE_H_

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <string>
#include <vector>
#include <boost/scoped_ptr.hpp>

#include "common/global-types.h"
#include "common/status.h"
#include "exec/external-data-source-executor.h"
#include "exec/scan-node.h"
#include "runtime/descriptors.h"
#include "runtime/mem-pool.h"

#include "gen-cpp/ExternalDataSource_types.h"

namespace impala {

class DataSourceScanNode;
class Tuple;

/// Shared JDBC connection used by all DataSourceScanNode instances within the same
/// fragment. A single ExternalDataSourceExecutor is opened once and then called
/// concurrently by N MT_DOP scanner threads. Fetch serialization is provided by the
/// Java-side fetchLock in JdbcRecordIterator; once a thread returns from FetchBatch()
/// it materializes its batch in parallel with other threads.
class SharedJdbcConnection {
 public:
  SharedJdbcConnection() {}

  /// Opens the JDBC connection. Thread-safe: the first caller initializes the executor
  /// and opens the Java data source; all subsequent callers block until the first caller
  /// finishes and then reuse the same connection.
  Status Open(const std::string& jar_path, const std::string& class_name,
      const std::string& api_version, const std::string& init_string,
      const extdatasource::TOpenParams& params, extdatasource::TOpenResult* result);

  /// Fetches the next batch from the shared JDBC connection. Multiple threads may
  /// call this concurrently; serialization is handled by the Java-side fetchLock.
  /// Short-circuits immediately if eos() is already true.
  Status FetchBatch(extdatasource::TGetNextResult* result);

  /// Decrements the open reference count. The last caller (ref_count_ reaches 0)
  /// closes the Java data source.
  Status Close(const extdatasource::TCloseParams& params,
      extdatasource::TCloseResult* result);

  const std::string& scan_handle() const { return scan_handle_; }

  /// Returns true once the JDBC stream has been fully consumed. All scanner threads
  /// check this before calling FetchBatch() to avoid redundant JNI calls.
  bool eos() const { return eos_.load(std::memory_order_acquire); }

 private:
  ExternalDataSourceExecutor executor_;
  std::string scan_handle_;

  /// Guards ref_count_ / open_done_ / open_status_: used in Open() for one-time
  /// initialization and in Close() to check whether executor_.Close() should be called.
  std::mutex lock_;
  bool open_done_ = false;
  Status open_status_;

  /// Reference count: incremented in Open(), decremented in Close(). When it reaches
  /// zero, the last DSSN instance calls executor_.Close().
  int ref_count_ = 0;

  /// Set to true when the JDBC stream is exhausted. Threads check this before
  /// crossing the JNI bridge to avoid unnecessary calls after EOS.
  std::atomic<bool> eos_{false};
};

/// Per-fragment plan node for data source scans. Created once per fragment (shared
/// across all DataSourceScanNode instances), it owns the SharedJdbcConnection so that
/// all N MT_DOP instances share a single JDBC connection with parallel batch conversion.
class DataSourceScanPlanNode : public ScanPlanNode {
 public:
  virtual Status CreateExecNode(RuntimeState* state, ExecNode** node) const override;

  /// Owned by the plan node; shared by all DataSourceScanNode instances in the fragment.
  /// Mutable so that Open() (called from exec nodes) can initialize it on first call.
  mutable SharedJdbcConnection shared_conn;
};

/// Scan node for external data sources. The external data source jar is loaded
/// in Prepare() (via SharedJdbcConnection), and then the data source is called to
/// receive row batches when necessary. N MT_DOP instances of this node share a single
/// SharedJdbcConnection; fetch serialization is handled by the Java-side fetchLock
/// while batch conversion/materialization runs in parallel across all N threads.
class DataSourceScanNode : public ScanNode {
 public:
  DataSourceScanNode(ObjectPool* pool, const DataSourceScanPlanNode& pnode,
      const DescriptorTbl& descs);

  ~DataSourceScanNode();

  /// Initialize the shared JDBC connection (once per fragment) and profile counters.
  virtual Status Prepare(RuntimeState* state) override;

  /// Opens the data source via the shared connection and fetches the first batch.
  virtual Status Open(RuntimeState* state) override;

  /// Fill the next row batch from the external scanner.
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) override;

  /// NYI
  virtual Status Reset(RuntimeState* state, RowBatch* row_batch) override;

  /// Close the scanner via the shared connection.
  virtual void Close(RuntimeState* state) override;

 protected:
  /// Write debug string of this into out.
  virtual void DebugString(int indentation_level, std::stringstream* out) const override;

 private:
  /// Reference to the per-fragment plan node; owns shared_conn.
  const DataSourceScanPlanNode& plan_node_;

  /// Non-owning pointer to the plan node's shared JDBC connection.
  SharedJdbcConnection* shared_conn_ = nullptr;

  /// Thrift structure describing the data source scan node.
  const TDataSourceScanNode data_src_node_;

  /// Descriptor of tuples read
  const TupleDescriptor* tuple_desc_;

  /// Tuple index in tuple row.
  int tuple_idx_;

  /// The opaque handle returned by the data source for the scan (shared across all
  /// instances — all call getNext() on the same Java object using the same handle).
  std::string scan_handle_;

  /// The current result from calling GetNext() on the data source. Contains the
  /// thrift representation of the rows.
  boost::scoped_ptr<extdatasource::TGetNextResult> input_batch_;

  /// The number of rows in input_batch_->rows. The data source should have set
  /// TRowBatch.num_rows, but we compute it just in case they haven't.
  int num_rows_;

  /// The index of the next row in input_batch_,
  /// i.e. the index into TColumnData.is_null.
  size_t next_row_idx_;

  /// The indexes of the next non-null value in the row batch, per column. Should always
  /// contain tuple_desc_->slots().size() integers. All values are reset to 0 when getting
  /// the next row batch.
  std::vector<int> cols_next_val_idx_;

  /// The total number of calls to ExternalDataSource::GetNext().
  RuntimeProfile::Counter* num_ext_data_source_get_next_;

  /// Wall-clock time this thread spent blocked in the full JNI/Java JDBC round-trip.
  RuntimeProfile::Counter* jdbc_jni_wait_timer_ = nullptr;

  /// Time this thread held the Java-side fetchLock while iterating the JDBC cursor,
  /// as reported by the Java layer. Reflects pure JDBC cursor utilisation.
  RuntimeProfile::Counter* jdbc_cursor_fetch_timer_ = nullptr;

  /// Time this thread waited to acquire the Java-side fetchLock, as reported by the
  /// Java layer. High values indicate cursor contention across scanner threads.
  RuntimeProfile::Counter* jdbc_lock_wait_timer_ = nullptr;

  /// Number of JNI GetNext() calls issued by this scan node.
  RuntimeProfile::Counter* jdbc_jni_call_count_ = nullptr;

  /// Materializes the next row (next_row_idx_) into tuple. 'local_tz' is used as the
  /// local time-zone for materializing 'TYPE_TIMESTAMP' slots.
  Status MaterializeNextRow(const Timezone* local_tz, MemPool* mem_pool, Tuple* tuple);

  /// Fetches the next batch from the data source via the shared JDBC connection.
  Status GetNextInputBatch();

  /// Validate row_batch_ contains the correct number of columns and that columns
  /// contain the same number of rows.
  Status ValidateRowBatchSize();

  /// True if input_batch_ has more rows.
  bool InputBatchHasNext() {
    if (!input_batch_->__isset.rows) return false;
    return next_row_idx_ < num_rows_;
  }
};

}

#endif
