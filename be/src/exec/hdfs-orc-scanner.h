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


#ifndef IMPALA_EXEC_HDFS_ORC_SCANNER_H
#define IMPALA_EXEC_HDFS_ORC_SCANNER_H

#include <orc/OrcFile.hh>

#include "runtime/exec-env.h"
#include "runtime/io/disk-io-mgr.h"
#include "runtime/runtime-state.h"
#include "exec/hdfs-scanner.h"
#include "exec/hdfs-scan-node.h"
#include "util/runtime-profile-counters.h"

namespace impala {

struct HdfsFileDesc;

/// This scanner leverage the ORC library to parse ORC files located in HDFS. Data is
/// transformed into Impala in-memory representation, i.e. Tuples, RowBatches.
///
/// For the file format spec, see https://orc.apache.org/docs/spec-intro.html
class HdfsOrcScanner : public HdfsScanner {
 public:
  /// Exception throws from the orc scanner to stop the orc::RowReader. It's used in
  /// IO errors (e.g. cancellation) or memory errors (e.g. mem_limit exceeded). The
  /// exact error message will be recorded in parse_status_.
  class ResourceError : public std::runtime_error {
   public:
    explicit ResourceError(const Status& status)
      : runtime_error(status.msg().msg()), status_(status) {}
    virtual ~ResourceError() {}
    Status& GetStatus() { return status_; }

   private:
    Status status_;
  };

  class OrcMemPool : public orc::MemoryPool {
   public:
    OrcMemPool(HdfsOrcScanner* scanner);
    virtual ~OrcMemPool();

    char* malloc(uint64_t size) override;
    void free(char* p) override;

    void FreeAll();
   private:

    HdfsOrcScanner* scanner_;
    MemTracker* mem_tracker_;
    boost::unordered_map<char*, uint64_t> chunk_sizes_;
  };

  class ScanRangeInputStream : public orc::InputStream {
   public:
    ScanRangeInputStream(HdfsOrcScanner* scanner) {
      this->scanner_ = scanner;
      this->filename_ = scanner->filename();
      this->file_desc_ = scanner->scan_node_->GetFileDesc(
          scanner->context_->partition_descriptor()->id(), filename_);
    }

    uint64_t getLength() const {
      return file_desc_->file_length;
    }

    uint64_t getNaturalReadSize() const {
      return ExecEnv::GetInstance()->disk_io_mgr()->max_buffer_size();
    }

    void read(void* buf, uint64_t length, uint64_t offset);

    const std::string& getName() const {
      return filename_;
    }

  private:
    HdfsOrcScanner* scanner_;
    HdfsFileDesc* file_desc_;
    std::string filename_;
  };

  HdfsOrcScanner(HdfsScanNodeBase* scan_node, RuntimeState* state);
  virtual ~HdfsOrcScanner();

  /// Issue just the footer range for each file.  We'll then parse the footer and pick
  /// out the columns we want.
  static Status IssueInitialRanges(HdfsScanNodeBase* scan_node,
      const std::vector<HdfsFileDesc*>& files) WARN_UNUSED_RESULT;

  virtual Status Open(ScannerContext* context) override WARN_UNUSED_RESULT;
  virtual Status ProcessSplit() override WARN_UNUSED_RESULT;
  virtual void Close(RowBatch* row_batch) override;

 private:
  friend class HdfsOrcScannerTest;

  /// Memory guard of the tuple_mem_
  uint8_t* tuple_mem_end_ = nullptr;

  /// Index of the current stripe being processed. Initialized to -1 which indicates
  /// that we have not started processing the first stripe yet (GetNext() has not yet
  /// been called).
  int32_t stripe_idx_ = -1;

  /// Counts the number of rows processed for the current stripe.
  int64_t stripe_rows_read_ = 0;

  /// Indicates whether we should advance to the next stripe in the next GetNext().
  /// Starts out as true to move to the very first stripe.
  bool advance_stripe_ = true;

  /// Indicates whether we are at the end of a stripe.
  bool end_of_stripe_ = true;

  /// Number of scratch batches processed so far.
  int64_t row_batches_produced_ = 0;

  /// Mem pool used in orc readers.
  boost::scoped_ptr<OrcMemPool> reader_mem_pool_;

  /// orc::Reader's responsibility is to read the footer and metadata from an ORC file.
  /// It creates orc::RowReader for further materialization. orc::RowReader is used for
  /// reading rows from the file.
  std::unique_ptr<orc::Reader> reader_ = nullptr;
  std::unique_ptr<orc::RowReader> row_reader_ = nullptr;

  /// Orc reader will write slot values into this scratch batch for top-level tuples.
  /// See AssembleRows().
  std::unique_ptr<orc::ColumnVectorBatch> scratch_batch_;
  int scratch_batch_tuple_idx_ = 0;

  /// ReaderOptions used to create orc::Reader.
  orc::ReaderOptions reader_options_;

  /// RowReaderOptions used to create orc::RowReader.
  orc::RowReaderOptions row_reader_options;

  /// Column id is the pre order id in orc::Type tree.
  /// Map from column id to slot descriptor.
  boost::unordered_map<int, const SlotDescriptor*> col_id_slot_map_;

  /// Scan range for the metadata.
  const io::ScanRange* metadata_range_ = nullptr;

  /// Timer for materializing rows. This ignores time getting the next buffer.
  ScopedTimer<MonotonicStopWatch> assemble_rows_timer_;

  /// Average and min/max time spent processing the footer by each split.
  RuntimeProfile::SummaryStatsCounter* process_footer_timer_stats_ = nullptr;

  /// Number of columns that need to be read.
  RuntimeProfile::Counter* num_cols_counter_ = nullptr;

  /// Number of stripes that need to be read.
  RuntimeProfile::Counter* num_stripes_counter_ = nullptr;

  /// Number of scanners that end up doing no reads because their splits don't overlap
  /// with the midpoint of any stripe in the file.
  RuntimeProfile::Counter* num_scanners_with_no_reads_counter_ = nullptr;

  const char *filename() const { return metadata_range_->file(); }

  virtual Status GetNextInternal(RowBatch* row_batch) override WARN_UNUSED_RESULT;

  /// Advances 'stripe_idx_' to the next non-empty stripe and initializes
  /// row_reader_ to scan it.
  Status NextStripe() WARN_UNUSED_RESULT;

  /// Reads data using orc-reader to materialize instances of 'tuple_desc'.
  /// Returns a non-OK status if a non-recoverable error was encountered and execution
  /// of this query should be terminated immediately.
  Status AssembleRows(RowBatch* row_batch) WARN_UNUSED_RESULT;

  /// Function used by TransferScratchTuples() to read a single row from scratch_batch_
  /// into 'tuple'.
  Status ReadRow(const orc::StructVectorBatch& batch, int row_idx,
      const orc::Type* orc_type, Tuple* tuple, RowBatch* dst_batch) WARN_UNUSED_RESULT;

  /// Evaluates runtime filters and conjuncts (if any) against the tuples in
  /// 'scratch_batch_', and adds the surviving tuples to the given batch.
  /// Returns the number of rows that should be committed to the given batch.
  Status TransferScratchTuples(RowBatch* dst_batch) WARN_UNUSED_RESULT;

  /// Process the file footer and parse file_metadata_.  This should be called with the
  /// last FOOTER_SIZE bytes in context_.
  Status ProcessFileTail() WARN_UNUSED_RESULT;

  /// Update reader options used in orc reader by the given tuple descriptor.
  Status SelectColumns(const TupleDescriptor* tuple_desc) WARN_UNUSED_RESULT;

  /// Validate whether the ColumnType is compatible with the orc type
  Status ValidateType(const ColumnType& type, const orc::Type& orc_type)
      WARN_UNUSED_RESULT;

  /// Part of the HdfsScanner interface, not used in Orc.
  Status InitNewRange() override WARN_UNUSED_RESULT { return Status::OK(); }

  THdfsCompression::type TranslateCompressionKind(orc::CompressionKind kind);

  inline bool ScratchBatchNotEmpty();

  inline Status AllocateTupleMem(RowBatch* row_batch) WARN_UNUSED_RESULT;

};

} // namespace impala

#endif
