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

#include "exec/hdfs-scanner.h"

#include <boost/scoped_ptr.hpp>

namespace impala {

class HdfsScanNodeBase;
class HdfsScanPlanNode;
class RowBatch;
class RuntimeState;
struct ScratchTupleBatch;

/// Parent class for scanners that read values into a scratch batch before applying
/// conjuncts and runtime filters.
class HdfsColumnarScanner : public HdfsScanner {
 public:
  HdfsColumnarScanner(HdfsScanNodeBase* scan_node, RuntimeState* state);
  virtual ~HdfsColumnarScanner();

  virtual Status Open(ScannerContext* context) override WARN_UNUSED_RESULT;

  /// Codegen ProcessScratchBatch(). Stores the resulting function in
  /// 'process_scratch_batch_fn' if codegen was successful or NULL otherwise.
  static Status Codegen(HdfsScanPlanNode* node, FragmentState* state,
      llvm::Function** process_scratch_batch_fn);

  /// Add sync read related counters.
  void AddSyncReadBytesCounter(int64_t total_bytes);

  /// Add async read related counters.
  void AddAsyncReadBytesCounter(int64_t total_bytes);

  /// Add skipped bytes related counters.
  void AddSkippedReadBytesCounter(int64_t total_bytes);

  /// Class name in LLVM IR.
  static const char* LLVM_CLASS_NAME;

 protected:
  /// Column readers will write slot values into this scratch batch for
  /// top-level tuples. See AssembleRows() in the derived classes.
  boost::scoped_ptr<ScratchTupleBatch> scratch_batch_;

  /// Indicate whether this is a footer scanner or not.
  /// Assigned in HdfsColumnarScanner::Open().
  bool is_footer_scanner_ = false;

  /// Scan range for the metadata.
  const io::ScanRange* metadata_range_ = nullptr;

  typedef int (*ProcessScratchBatchFn)(HdfsColumnarScanner*, RowBatch*);
  /// The codegen'd version of ProcessScratchBatch() if available, NULL otherwise.
  /// Function type: ProcessScratchBatchFn
  const CodegenFnPtrBase* codegend_process_scratch_batch_fn_ = nullptr;

  /// Filters out tuples from 'scratch_batch_' and adds the surviving tuples
  /// to the given batch. Finalizing transfer of batch is not done here.
  /// Returns the number of tuples that should be committed to the given batch.
  int FilterScratchBatch(RowBatch* row_batch);

  /// Get filename of the scan range.
  const char* filename() const { return metadata_range_->file(); }

  /// Evaluates runtime filters and conjuncts (if any) against the tuples in
  /// 'scratch_batch_', and adds the surviving tuples to the given batch.
  /// Transfers the ownership of tuple memory to the target batch when the
  /// scratch batch is exhausted.
  /// Returns the number of rows that should be committed to the given batch.
  int TransferScratchTuples(RowBatch* row_batch);

  /// Processes a single row batch for TransferScratchTuples, looping over scratch_batch_
  /// until it is exhausted or the output is full. Called for the case when there are
  /// materialized tuples. This is a separate function so it can be codegened.
  int ProcessScratchBatch(RowBatch* dst_batch);

  /// List of pair of (column index, reservation allocated).
  typedef std::vector<std::pair<int, int64_t>> ColumnReservations;
  /// List of column range lengths.
  typedef std::vector<int64_t> ColumnRangeLengths;

  /// Decides how to divide stream_->reservation() between the columns. May increase
  /// the reservation if more reservation would enable more efficient I/O for the
  /// current columns being scanned. Sets the reservation on each corresponding reader
  /// in 'column_readers'.
  Status DivideReservationBetweenColumns(const ColumnRangeLengths& col_range_lengths,
      ColumnReservations& reservation_per_column);

  /// Helper for DivideReservationBetweenColumns(). Implements the core algorithm for
  /// dividing a reservation of 'reservation_to_distribute' bytes between columns with
  /// scan range lengths 'col_range_lengths' given a min and max buffer size. Returns
  /// a vector with an entry per column with the index into 'col_range_lengths' and the
  /// amount of reservation in bytes to give to that column.
  static ColumnReservations DivideReservationBetweenColumnsHelper(int64_t min_buffer_size,
      int64_t max_buffer_size, const ColumnRangeLengths& col_range_lengths,
      int64_t reservation_to_distribute);

  /// Compute the ideal reservation to scan a file with scan range lengths
  /// 'col_range_lengths' given the min and max buffer size of the singleton DiskIoMgr
  /// in ExecEnv.
  static int64_t ComputeIdealReservation(const ColumnRangeLengths& col_range_lengths);

  /// Number of columns that need to be read.
  RuntimeProfile::Counter* num_cols_counter_;

  /// Number of scanners that end up doing no reads because their splits don't overlap
  /// with the midpoint of any row-group/stripe in the file.
  RuntimeProfile::Counter* num_scanners_with_no_reads_counter_;

  /// Average and min/max time spent processing the footer by each split.
  RuntimeProfile::SummaryStatsCounter* process_footer_timer_stats_;

  /// Average and min/max memory reservation for a scanning a row group (parquet) or
  /// stripe (orc), both ideal (calculated based on min and max buffer size) and actual.
  RuntimeProfile::SummaryStatsCounter* columnar_scanner_ideal_reservation_counter_;
  RuntimeProfile::SummaryStatsCounter* columnar_scanner_actual_reservation_counter_;

  /// Number of stream read request done in in sync and async manners and the total.
  RuntimeProfile::Counter* io_sync_request_;
  RuntimeProfile::Counter* io_async_request_;
  RuntimeProfile::Counter* io_total_request_;

  /// Number of stream read bytes done in in sync and async manners and the total.
  RuntimeProfile::Counter* io_sync_bytes_;
  RuntimeProfile::Counter* io_async_bytes_;
  RuntimeProfile::Counter* io_total_bytes_;

  /// Total number of bytes skipped during stream reading.
  RuntimeProfile::Counter* io_skipped_bytes_;

  /// Total file metadata reads done.
  /// Incremented when serving query from metadata instead of iterating rows or
  /// row groups / stripes.
  RuntimeProfile::Counter* num_file_metadata_read_;

 private:
  int ProcessScratchBatchCodegenOrInterpret(RowBatch* dst_batch);
};

}
