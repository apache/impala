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

#pragma once

#include "common/global-types.h"
#include "exec/paimon/paimon-jni-row-reader.h"
#include "exec/paimon/paimon-jni-scanner.h"
#include "exec/scan-node.h"
#include "runtime/descriptors.h"

#include <jni.h>
#include <arrow/record_batch.h>

namespace impala {

class ExecNode;
class PaimonJniRowReader;
class RuntimeState;
class Status;

/// Scan node for an Paimon table.
/// Since Paimon API can be used to scan both paimon data tables and metadata tables.
/// The current jni scanner works as a generic solution to scan both data table and
/// metadata tables.
/// For scanning these paimon tables this scanner calls into the JVM and creates an
/// 'PaimonJniScanner' object that does the scanning. Once the Paimon scan is done,
/// To minimize the jni overhead, the PaimonJniScanner will first write batch of
/// InternalRows to the arrow BatchRecord into the offheap, and the offheap memory
/// pointer will pass to the native side to directly read the batch record and
/// convert the arrow format into native impala RowBatch. The benchmark shows 2.x
/// better performance than pure jni implementation.
///
/// The flow of scanning is:
/// 1. Backend:  Get the splits/table obj from plan node, generate thrift
///    encoded param and passes to the JNI scanner.
/// 2. Backend:  Creates an PaimonJniScanner object on the Java heap.
/// 3. Backend:  Triggers a table scan on the Frontend
/// 4. Frontend: Executes the scan
/// 5. Backend:  Calls GetNext that calls PaimonJniScanner's GetNext
/// 6. Frontend: PaimonJniScanner's GetNextBatchDirect will return the offheap pointer,
///    as well as consumed offheap bytes size to the arrow row batch.
/// 7. Backend:  Consume the arrow RecordBatch into impala RowBatch.
///
/// Note:
///   This scan node can be executed on any executor.

class PaimonScanPlanNode;
class PaimonJniScanNode : public ScanNode {
 public:
  PaimonJniScanNode(
      ObjectPool* pool, const PaimonScanPlanNode& pnode, const DescriptorTbl& descs);

  /// Initializes counters, executes Paimon table scan and initializes accessors.
  Status Prepare(RuntimeState* state) override;

  /// Creates the Paimon row reader.
  Status Open(RuntimeState* state) override;

  /// Fills the next rowbatch with the results returned by the Paimon scan.
  Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) override;

  /// Finalize and close this operator.
  void Close(RuntimeState* state) override;

 protected:
  Status CollectProjectionFieldIds(
      const TupleDescriptor* tuple_desc, vector<int32_t>& projection);
  Status OffheapTrackAllocation(long bytes);
  void OffheapTrackFree();

 private:
  /// Adapter that helps preparing the table and executes an Paimon table scan
  /// on Java side. Allows the ScanNode to fetch the Arrow RecordBatch from the
  /// Java Heap.
  std::unique_ptr<PaimonJniScanner> jni_scanner_;

  /// Helper class to transform Paimon rows to Impala tuples.
  std::unique_ptr<PaimonJniRowReader> paimon_row_reader_;

  /// Get the next arrow row record batch from the jni scanner.
  /// returns false if no record is available anymore, true if valid row is returned.
  Status GetNextBatchIfNeeded(bool* is_empty_batch);

  /// The TupleId and TupleDescriptor of the tuple that this scan node will populate.
  const TupleId tuple_id_;
  const TupleDescriptor* tuple_desc_ = nullptr;

  /// The table name.
  const std::string table_name_;

  // Paimon scan param.
  TPaimonJniScanParam paimon_jni_scan_param_;
  /// Indicate whether the splits is empty.
  /// It is used to control whether the
  /// jni scanner should be created. If splits is empty,
  /// it will bypass the JNI operation, in other words,
  /// all jni related operation will be skipped,directly
  /// set eof flag to true, and return empty scan
  /// result.
  bool splits_empty_;
  /// MemTracker for tracing arrow used JVM offheap memory.
  /// Initialized in Prepare(). Owned by RuntimeState.
  std::unique_ptr<MemTracker> arrow_batch_mem_tracker_;
  /// last consumed offheap bytes for arrow batch
  long paimon_last_arrow_record_batch_consumed_bytes_;
  /// Thrift serialized paimon scan param
  std::string paimon_jni_scan_param_serialized_;
  /// current unconsumed arrow record batch.
  std::shared_ptr<arrow::RecordBatch> paimon_arrow_record_batch_holder_;
  /// current row_count of the arrow record batch.
  long arrow_record_batch_row_count_;
  /// current row_index of the arrow record batch.
  long arrow_record_batch_row_index_;
  /// Paimon scan specific counters.
  RuntimeProfile::Counter* scan_open_timer_;
  RuntimeProfile::Counter* paimon_api_scan_timer_;
};

inline Status PaimonJniScanNode::OffheapTrackAllocation(long consumed_bytes) {
  if (consumed_bytes > 0) {
    if (arrow_batch_mem_tracker_->TryConsume(consumed_bytes)) {
      paimon_last_arrow_record_batch_consumed_bytes_ = consumed_bytes;
      return Status::OK();
    } else {
      return Status::MemLimitExceeded("Arrow batch size exceed the mem limit.");
    }
  } else {
    return Status::OK();
  }
}

inline void PaimonJniScanNode::OffheapTrackFree() {
  if (paimon_last_arrow_record_batch_consumed_bytes_ > 0) {
    arrow_batch_mem_tracker_->Release(paimon_last_arrow_record_batch_consumed_bytes_);
    paimon_last_arrow_record_batch_consumed_bytes_ = 0;
  }
}

} // namespace impala
