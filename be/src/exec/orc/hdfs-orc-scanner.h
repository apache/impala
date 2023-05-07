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
#include "exec/acid-metadata-utils.h"
#include "exec/hdfs-columnar-scanner.h"
#include "exec/hdfs-scan-node.h"
#include "exec/orc/orc-metadata-utils.h"
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
///   * Create temporary orc::RowReader with 'row_reader_options_' to get the selected
///     subset of the schema (a tree of the selected orc::Types, i.e. the local variable
///     'root_type' in 'HdfsOrcScanner::Open').
///   * Create OrcColumnReaders recursively with 'root_type' and TupleDescriptors
///     (HdfsOrcScanner::Open)
///   * At the begining of processing a Stripe, we update 'row_reader_options_' to have
///     the range of the Stripe boundaries. Then create a orc::RowReader for this Stripe
///     (HdfsOrcScanner::NextStripe)
///
class HdfsOrcScanner : public HdfsColumnarScanner {
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

  struct ColumnRange {
    uint64_t offset_;
    uint64_t length_;
    uint64_t current_position_;
    orc::StreamKind kind_;
    uint64_t type_id_;
    HdfsOrcScanner* scanner_;
    ScannerContext::Stream* stream_ = nullptr;
    int io_reservation = 0;

    ColumnRange(uint64_t length, uint64_t offset, orc::StreamKind kind, uint64_t type_id,
        HdfsOrcScanner* scanner)
      : offset_(offset),
        length_(length),
        current_position_(offset),
        kind_(kind),
        type_id_(type_id),
        scanner_(scanner) {}

    bool operator<(const ColumnRange& other) const { return offset_ < other.offset_; }

    /// Read 'length' bytes from the stream_ starting at 'offset' into 'buf'.
    Status read(void* buf, uint64_t length, uint64_t offset);

    std::string debug() {
      return strings::Substitute(
          "colrange_offset: $0 colrange_length: $1 colrange_pos: $2 "
          "typeId: $3 kind: $4 filename: $5",
          offset_, length_, current_position_, type_id_, orc::streamKindToString(kind_),
          scanner_->filename());
    }
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

    /// Get the total length of the file in bytes.
    uint64_t getLength() const override {
      return file_desc_->file_length;
    }

    /// Get the natural size for reads.
    /// Return 0 to let ORC lib decide what is the best buffer size depending on
    /// compression method that is being used.
    uint64_t getNaturalReadSize() const override { return 0; }

    /// Read 'length' bytes from the file starting at 'offset' into the buffer starting
    /// at 'buf'.
    void read(void* buf, uint64_t length, uint64_t offset) override;

    /// Get the name of the stream for error messages.
    const std::string& getName() const override {
      return filename_;
    }

   private:
    HdfsOrcScanner* scanner_;
    const HdfsFileDesc* file_desc_;
    std::string filename_;

    /// Default read implementation for non async IO.
    Status readRandom(void* buf, uint64_t length, uint64_t offset);
  };

  HdfsOrcScanner(HdfsScanNodeBase* scan_node, RuntimeState* state);
  virtual ~HdfsOrcScanner();

  /// Issue just the footer range for each file.  We'll then parse the footer and pick
  /// out the columns we want.
  static Status IssueInitialRanges(HdfsScanNodeBase* scan_node,
      const std::vector<HdfsFileDesc*>& files) WARN_UNUSED_RESULT;

  /// Size of the file footer for ORC.
  /// This is a guess, matched with orc::DIRECTORY_SIZE_GUESS from ORC lib.
  /// If this value is too little, we will need to issue another read.
  static const int64_t ORC_FOOTER_SIZE = 1024 * 16;
  static_assert(ORC_FOOTER_SIZE <= READ_SIZE_MIN_VALUE,
      "ORC_FOOTER_SIZE can not be greater than READ_SIZE_MIN_VALUE.\n"
      "You can increase ORC_FOOTER_SIZE if you want, "
      "just don't forget to increase READ_SIZE_MIN_VALUE as well.");

  virtual Status Open(ScannerContext* context) override WARN_UNUSED_RESULT;
  virtual Status ProcessSplit() override WARN_UNUSED_RESULT;
  virtual void Close(RowBatch* row_batch) override;

  THdfsFileFormat::type file_format() const override {
    return THdfsFileFormat::ORC;
  }

 private:
  friend class OrcColumnReader;
  friend class OrcDateColumnReader;
  friend class OrcStringColumnReader;
  friend class OrcTimestampReader;
  friend class OrcComplexColumnReader;
  friend class OrcCollectionReader;
  friend class OrcStructReader;
  friend class OrcListReader;
  friend class OrcMapReader;

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

  /// Pool to copy dictionary buffer into.
  /// This pool is shared across all the batches in a stripe.
  boost::scoped_ptr<MemPool> dictionary_pool_;
  /// Pool to copy non-dictionary buffer into. This pool is responsible for handling
  /// vector batches that do not necessarily fit into one row batch.
  boost::scoped_ptr<MemPool> data_batch_pool_;
  /// Pool to copy values into when building search arguments. Freed on Close().
  boost::scoped_ptr<MemPool> search_args_pool_;

  /// Clone of statistics conjunct evaluators. Has the same lifetime as the scanner.
  /// Stored in 'obj_pool_'.
  vector<ScalarExprEvaluator*> stats_conjunct_evals_;

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

  /// Mappings from slot/tuple descriptors to ORC type id. We save this information during
  /// column resolution and use it during column reader creation.
  std::unordered_map<const SlotDescriptor*, uint64_t> slot_to_col_id_;
  std::unordered_map<const TupleDescriptor*, uint64_t> tuple_to_col_id_;

  /// With the help of it we can check the validity of ACID write ids.
  ValidWriteIdList valid_write_ids_;

  /// The write id range for ACID files.
  std::pair<int64_t, int64_t> acid_write_id_range_;

  /// Non-ACID file in full ACID table.
  bool acid_original_file_ = false;

  /// Slot descriptor for file position (a.k.a. row ID) for two different purposes: It can
  /// be used for the synthetic rowid column in original files of a full ACID table, or
  /// for the FILE__POSITION virtual column.
  const SlotDescriptor* file_position_ = nullptr;

  /// True if we need to validate the row batches against the valid write id list. This
  /// only needs to be done for Hive Streaming Ingestion. The 'write id' will be the same
  /// within a stripe, but we still need to read the row batches for validation because
  /// there are no statistics written.
  /// For files not written by Streaming Ingestion we can assume that every row is valid.
  bool row_batches_need_validation_ = false;

  /// Scan range for column streams.
  /// StartColumnReading() guarantees that columnRanges_ is sorted by the element's
  /// offset, and there are no two overlapping range.
  vector<ColumnRange> columnRanges_;

  /// Timer for materializing rows. This ignores time getting the next buffer.
  ScopedTimer<MonotonicStopWatch> assemble_rows_timer_;

  /// Number of stripes that need to be read.
  RuntimeProfile::Counter* num_stripes_counter_ = nullptr;

  /// Number of predicates that are pushed down to the ORC reader.
  RuntimeProfile::Counter* num_pushed_down_predicates_counter_ = nullptr;

  /// Number of runtime filters that are pushed down to the ORC reader.
  RuntimeProfile::Counter* num_pushed_down_runtime_filters_counter_ = nullptr;

  /// Number of arrived runtime IN-list filters that can be pushed down.
  /// Used in ShouldUpdateSearchArgument(). Init to -1 so the check can pass at first.
  int num_pushable_in_list_filters_ = -1;

  /// Number of collection items read in current row batch. It is a scanner-local counter
  /// used to reduce the frequency of updating HdfsScanNode counter. It is updated by the
  /// callees of AssembleRows() and is merged into the HdfsScanNode counter at the end of
  /// AssembleRows() and then is reset to 0.
  int64_t coll_items_read_counter_ = 0;

  const char *filename() const { return metadata_range_->file(); }

  Status GetNextInternal(RowBatch* row_batch) override WARN_UNUSED_RESULT;

  /// Advances 'stripe_idx_' to the next non-empty stripe and initializes
  /// row_reader_ to scan it.
  Status NextStripe() WARN_UNUSED_RESULT;

  /// Begin reading columns of given stripe.
  Status StartColumnReading(const orc::StripeInformation& stripe);

  /// Find scan ColumnRange that where range [offset, offset+length) lies in.
  /// Return nullptr if such range does not exist.
  inline ColumnRange* FindColumnRange(uint64_t length, uint64_t offset) {
    auto in_range = [length, offset](ColumnRange c) {
      return (offset >= c.offset_) && (offset + length <= c.offset_ + c.length_);
    };
    auto range = std::find_if(columnRanges_.begin(), columnRanges_.end(), in_range);
    return range != columnRanges_.end() ? &(*range) : nullptr;
  }

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
  Status TransferTuples(RowBatch* dst_batch) WARN_UNUSED_RESULT;

  /// Process the file footer and parse file_metadata_.  This should be called with the
  /// last ORC_FOOTER_SIZE bytes in context_.
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

  void SetSyntheticAcidFieldForOriginalFile(const SlotDescriptor* slot_desc,
      Tuple* template_tuple);

  bool PrepareBinaryPredicate(const string& fn_name, uint64_t orc_column_id,
      const ColumnType& type, ScalarExprEvaluator* eval,
      orc::SearchArgumentBuilder* sarg);
  bool PrepareInListPredicate(uint64_t orc_column_id, const ColumnType& type,
      ScalarExprEvaluator* eval, orc::SearchArgumentBuilder* sarg);
  bool PrepareInListPredicate(uint64_t orc_column_id, const ColumnType& type,
      const std::vector<orc::Literal>& in_list, orc::SearchArgumentBuilder* sarg);
  void PrepareIsNullPredicate(bool is_not_null, uint64_t orc_column_id,
      const ColumnType& type, orc::SearchArgumentBuilder* sarg);

  /// Builds ORC search arguments from the conjuncts and arrived runtime filters.
  /// The search arguments will be re-built each time we start reading a new stripe,
  /// because we may have new runtime filters arrive.
  Status PrepareSearchArguments() WARN_UNUSED_RESULT;

  /// Helper function for GetLiteralSearchArguments. The template parameter T is the
  /// type of val, and U is the destination type that the constructor of orc::Literal
  /// accepts. The conversion is required here, since otherwise multiple implicit
  /// conversions would be possible.
  template<typename T, typename U>
  orc::Literal GetOrcPrimitiveLiteral(orc::PredicateDataType predicate_type, void* val);

  /// Helper function for mapping ColumnType to orc::PredicateDataType.
  static orc::PredicateDataType GetOrcPredicateDataType(const ColumnType& type);

  /// Returns the literal specified by 'child_idx' from the stats conjuct 'eval',
  /// with the assumption that the specifit child is a literal.
  orc::Literal GetSearchArgumentLiteral(ScalarExprEvaluator* eval, int child_idx,
      const ColumnType& dst_type, orc::PredicateDataType* predicate_type);

  /// Return true if [offset, offset+length) is within the remaining part of the footer
  /// stream (initial scan range issued by IssueInitialRanges).
  inline bool IsInFooterRange(uint64_t offset, uint64_t length) const {
    DCHECK(stream_ != nullptr);
    return offset >= stream_->file_offset() && length <= stream_->bytes_left();
  }

  Status ReadFooterStream(void* buf, uint64_t length, uint64_t offset);

  /// Updates the SearchArgument based on arrived runtime filters.
  /// Returns true if any filter is applied.
  bool UpdateSearchArgumentWithFilters(orc::SearchArgumentBuilder* sarg);

  /// Decides whether we should rebuild the SearchArgument. It returns true at the first
  /// call and whenever a new and usable IN-list filter arrives.
  bool ShouldUpdateSearchArgument();

  /// Checks whether the runtime filter is a usable IN-list filter that can be pushed
  /// down.
  bool IsPushableInListFilter(const RuntimeFilter* filter);
};

} // namespace impala

#endif
