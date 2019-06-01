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

#include <stack>

#include <orc/OrcFile.hh>

#include "runtime/exec-env.h"
#include "runtime/io/disk-io-mgr.h"
#include "runtime/runtime-state.h"
#include "exec/hdfs-scanner.h"
#include "exec/hdfs-scan-node.h"
#include "exec/orc-metadata-utils.h"
#include "util/runtime-profile-counters.h"

namespace impala {

struct HdfsFileDesc;
class OrcStructReader;
class OrcComplexColumnReader;

/// This scanner leverage the ORC library to parse ORC files located in HDFS. Data is
/// transformed into Impala in-memory representation (i.e. Tuples, RowBatches) by
/// different kinds of OrcColumnReaders.
///
/// Steps of how we create orc::Reader, orc::RowReader and OrcColumnReaders:
///   * 'ProcessFileTail' to create orc::Reader with OrcMemPool and ScanRangeInputStream
///   * Resolve TupleDescriptors to get a list of mapped orc::Types (a.k.a column/node).
///     Init 'row_reader_options_' with these selected type ids.
///   * Build a map 'col_id_path_map_' from each orc::Type id to a SchemaPath. Will be
///     used in creating OrcColumnReaders.
///   * Create temporary orc::RowReader with 'row_reader_options_' to get the selected
///     subset of the schema (a tree of the selected orc::Types, i.e. the local variable
///     'root_type' in 'HdfsOrcScanner::Open').
///   * Create OrcColumnReaders recursively with 'root_type', 'col_id_path_map_' and
///     TupleDescriptors (HdfsOrcScanner::Open)
///   * At the begining of processing a Stripe, we update 'row_reader_options_' to have
///     the range of the Stripe boundaries. Then create a orc::RowReader for this Stripe
///     (HdfsOrcScanner::NextStripe)
///
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

  /// A wrapper of std::malloc and std::free to track the memory usage of the ORC lib.
  /// Without this the ORC lib will use std::malloc and std::free directly.
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

  /// A wrapper of DiskIoMgr to be used by the ORC lib.
  class ScanRangeInputStream : public orc::InputStream {
   public:
    ScanRangeInputStream(HdfsOrcScanner* scanner) {
      this->scanner_ = scanner;
      this->filename_ = scanner->filename();
      this->file_desc_ = scanner->scan_node_->GetFileDesc(
          scanner->context_->partition_descriptor()->id(), filename_);
    }

    uint64_t getLength() const override {
      return file_desc_->file_length;
    }

    uint64_t getNaturalReadSize() const override {
      return ExecEnv::GetInstance()->disk_io_mgr()->max_buffer_size();
    }

    /// Read 'length' bytes from the file starting at 'offset' into the buffer starting
    /// at 'buf'.
    void read(void* buf, uint64_t length, uint64_t offset) override;

    const std::string& getName() const override {
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
  friend class OrcColumnReader;
  friend class OrcStringColumnReader;
  friend class OrcTimestampReader;
  friend class OrcComplexColumnReader;
  friend class OrcCollectionReader;
  friend class OrcStructReader;
  friend class OrcListReader;
  friend class OrcMapReader;
  friend class HdfsOrcScannerTest;

  /// Memory guard of the tuple_mem_
  uint8_t* tuple_mem_end_ = nullptr;

  /// Index of the current stripe being processed. Stripe in ORC is equivalent to
  /// RowGroup in Parquet. Initialized to -1 which indicates that we have not started
  /// processing the first stripe yet (GetNext() has not yet been called).
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

  std::unique_ptr<OrcSchemaResolver> schema_resolver_ = nullptr;

  /// orc::Reader's responsibility is to read the footer and metadata from an ORC file.
  /// It creates orc::RowReader for further materialization. orc::RowReader is used for
  /// reading rows from the file.
  std::unique_ptr<orc::Reader> reader_ = nullptr;
  std::unique_ptr<orc::RowReader> row_reader_ = nullptr;
  /// ReaderOptions used to create orc::Reader.
  orc::ReaderOptions reader_options_;
  /// RowReaderOptions used to create orc::RowReader.
  orc::RowReaderOptions row_reader_options_;

  /// Scratch batch updated in place by 'row_reader_' (reader from the ORC lib). Will be
  /// consumed by 'orc_root_reader_' (column reader implemented by ourselves). See more
  /// in 'AssembleRows'
  std::unique_ptr<orc::ColumnVectorBatch> orc_root_batch_;

  /// The root column reader to transfer orc values into impala RowBatch. The root of
  /// the ORC file schema is always in STRUCT type so we use OrcStructReader here.
  /// Instead of using std::unique_ptr, this object is tracked in 'obj_pool_' to be
  /// together with children readers.
  OrcStructReader* orc_root_reader_ = nullptr;

  /// Slot descriptors that don't match any columns of the ORC file. We'll set NULL in
  /// these slots.
  std::unordered_set<const SlotDescriptor*> missing_field_slots_;

  /// Selected column(type) ids of the ORC file. Use list instead of vector here since
  /// orc::RowReaderOptions.includeTypes() expects a list
  std::list<uint64_t> selected_type_ids_;

  /// Map from orc::Type id to SchemaPath. See more in descriptors.h for the definition
  /// of SchemaPath. The map is used in the constructors of OrcColumnReaders where we
  /// resolve SchemaPaths of the descriptors into ORC columns. Built in 'Open'.
  std::vector<SchemaPath> col_id_path_map_;

  /// Scan range for the metadata (file tail).
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

  /// Number of collection items read in current row batch. It is a scanner-local counter
  /// used to reduce the frequency of updating HdfsScanNode counter. It is updated by the
  /// callees of AssembleRows() and is merged into the HdfsScanNode counter at the end of
  /// AssembleRows() and then is reset to 0.
  int64_t coll_items_read_counter_;

  const char *filename() const { return metadata_range_->file(); }

  Status GetNextInternal(RowBatch* row_batch) override WARN_UNUSED_RESULT;

  /// Advances 'stripe_idx_' to the next non-empty stripe and initializes
  /// row_reader_ to scan it.
  Status NextStripe() WARN_UNUSED_RESULT;

  /// Reads data to materialize instances of 'tuple_desc'.
  /// Returns a non-OK status if a non-recoverable error was encountered and execution
  /// of this query should be terminated immediately.
  Status AssembleRows(RowBatch* row_batch) WARN_UNUSED_RESULT;

  /// Materialize collection(list/map) tuples belong to the 'row_idx'-th row of
  /// coll_reader's ORC batch. Each column reader will hold an ORC batch until its values
  /// are drained.
  Status AssembleCollection(const OrcComplexColumnReader& coll_reader, int row_idx,
      CollectionValueBuilder* coll_value_builder) WARN_UNUSED_RESULT;

  /// Transfer rows in 'orc_root_batch_' into tuples in 'dst_batch'. Evaluates runtime
  /// filters and conjuncts (if any) against the tuples. Only surviving tuples are added
  /// to the given batch. Returns if either 'orc_root_batch_' is drained or 'dst_batch'
  /// is full.
  Status TransferTuples(OrcComplexColumnReader* column_reader,
      RowBatch* dst_batch) WARN_UNUSED_RESULT;

  /// Process the file footer and parse file_metadata_.  This should be called with the
  /// last FOOTER_SIZE bytes in context_.
  Status ProcessFileTail() WARN_UNUSED_RESULT;

  /// Resolve SchemaPath in TupleDescriptors and translate them to ORC type ids into
  /// 'selected_nodes'. Track the position slots by pre-order traversal in the
  /// descriptors and push them to a stack as 'pos_slots'.
  Status ResolveColumns(const TupleDescriptor& tuple_desc,
      std::list<const orc::Type*>* selected_nodes,
      std::stack<const SlotDescriptor*>* pos_slots);

  /// Resolve 'tuple_desc' to get selected columns. Update 'row_reader_options' with the
  /// selected type ids.
  Status SelectColumns(const TupleDescriptor& tuple_desc) WARN_UNUSED_RESULT;

  /// Part of the HdfsScanner interface, not used in Orc.
  Status InitNewRange() override WARN_UNUSED_RESULT { return Status::OK(); }

  THdfsCompression::type TranslateCompressionKind(orc::CompressionKind kind);

  inline Status AllocateTupleMem(RowBatch* row_batch) WARN_UNUSED_RESULT;

  bool IsPartitionKeySlot(const SlotDescriptor* slot);

  bool IsMissingField(const SlotDescriptor* slot);
};

} // namespace impala

#endif
