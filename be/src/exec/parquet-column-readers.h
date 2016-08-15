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

#ifndef IMPALA_PARQUET_COLUMN_READERS_H
#define IMPALA_PARQUET_COLUMN_READERS_H

#include <boost/scoped_ptr.hpp>

#include "exec/hdfs-parquet-scanner.h"
#include "util/codec.h"
#include "util/dict-encoding.h"
#include "util/rle-encoding.h"

namespace impala {

class Tuple;
class MemPool;

/// Decoder for all supported Parquet level encodings. Optionally reads, decodes, and
/// caches level values in batches.
/// Level values are unsigned 8-bit integers because we support a maximum nesting
/// depth of 100, as enforced by the FE. Using a small type saves memory and speeds up
/// populating the level cache (e.g., with RLE we can memset() repeated values).
///
/// Inherits from RleDecoder instead of containing one for performance reasons.
/// The containment design would require two BitReaders per column reader. The extra
/// BitReader causes enough bloat for a column reader to require another cache line.
/// TODO: It is not clear whether the inheritance vs. containment choice still makes
/// sense with column-wise materialization. The containment design seems cleaner and
/// we should revisit.
class ParquetLevelDecoder : public RleDecoder {
 public:
  ParquetLevelDecoder(bool is_def_level_decoder)
    : cached_levels_(NULL),
      num_cached_levels_(0),
      cached_level_idx_(0),
      encoding_(parquet::Encoding::PLAIN),
      max_level_(0),
      cache_size_(0),
      num_buffered_values_(0),
      decoding_error_code_(is_def_level_decoder ?
          TErrorCode::PARQUET_DEF_LEVEL_ERROR : TErrorCode::PARQUET_REP_LEVEL_ERROR) {
  }

  /// Initialize the LevelDecoder. Reads and advances the provided data buffer if the
  /// encoding requires reading metadata from the page header.
  Status Init(const string& filename, parquet::Encoding::type encoding,
      MemPool* cache_pool, int cache_size, int max_level, int num_buffered_values,
      uint8_t** data, int* data_size);

  /// Returns the next level or INVALID_LEVEL if there was an error.
  inline int16_t ReadLevel();

  /// Decodes and caches the next batch of levels. Resets members associated with the
  /// cache. Returns a non-ok status if there was a problem decoding a level, or if a
  /// level was encountered with a value greater than max_level_.
  Status CacheNextBatch(int batch_size);

  /// Functions for working with the level cache.
  inline bool CacheHasNext() const { return cached_level_idx_ < num_cached_levels_; }
  inline uint8_t CacheGetNext() {
    DCHECK_LT(cached_level_idx_, num_cached_levels_);
    return cached_levels_[cached_level_idx_++];
  }
  inline void CacheSkipLevels(int num_levels) {
    DCHECK_LE(cached_level_idx_ + num_levels, num_cached_levels_);
    cached_level_idx_ += num_levels;
  }
  inline int CacheSize() const { return num_cached_levels_; }
  inline int CacheRemaining() const { return num_cached_levels_ - cached_level_idx_; }
  inline int CacheCurrIdx() const { return cached_level_idx_; }

 private:
  /// Initializes members associated with the level cache. Allocates memory for
  /// the cache from pool, if necessary.
  Status InitCache(MemPool* pool, int cache_size);

  /// Decodes and writes a batch of levels into the cache. Sets the number of
  /// values written to the cache in *num_cached_levels. Returns false if there was
  /// an error decoding a level or if there was a level value greater than max_level_.
  bool FillCache(int batch_size, int* num_cached_levels);

  /// Buffer for a batch of levels. The memory is allocated and owned by a pool in
  /// passed in Init().
  uint8_t* cached_levels_;
  /// Number of valid level values in the cache.
  int num_cached_levels_;
  /// Current index into cached_levels_.
  int cached_level_idx_;
  parquet::Encoding::type encoding_;

  /// For error checking and reporting.
  int max_level_;
  /// Number of level values cached_levels_ has memory allocated for.
  int cache_size_;
  /// Number of remaining data values in the current data page.
  int num_buffered_values_;
  string filename_;
  TErrorCode::type decoding_error_code_;
};

/// Base class for reading a Parquet column. Reads a logical column, not necessarily a
/// column materialized in the file (e.g. collections). The two subclasses are
/// BaseScalarColumnReader and CollectionColumnReader. Column readers read one def and rep
/// level pair at a time. The current def and rep level are exposed to the user, and the
/// corresponding value (if defined) can optionally be copied into a slot via
/// ReadValue(). Can also write position slots.
class ParquetColumnReader {
 public:
  /// Creates a column reader for 'node' and associates it with the given parent scanner.
  /// Adds the new column reader to the parent's object pool.
  /// 'slot_desc' may be NULL, in which case the returned column reader can only be used
  /// to read def/rep levels.
  /// 'is_collection_field' should be set to true if the returned reader is reading a
  /// collection. This cannot be determined purely by 'node' because a repeated scalar
  /// node represents both an array and the array's items (in this case
  /// 'is_collection_field' should be true if the reader reads one value per array, and
  /// false if it reads one value per item).  The reader is added to the runtime state's
  /// object pool. Does not create child readers for collection readers; these must be
  /// added by the caller.
  static ParquetColumnReader* Create(const SchemaNode& node, bool is_collection_field,
      const SlotDescriptor* slot_desc, HdfsParquetScanner* parent);

  virtual ~ParquetColumnReader() { }

  int def_level() const { return def_level_; }
  int rep_level() const { return rep_level_; }

  const SlotDescriptor* slot_desc() const { return slot_desc_; }
  const parquet::SchemaElement& schema_element() const { return *node_.element; }
  int16_t max_def_level() const { return max_def_level_; }
  int16_t max_rep_level() const { return max_rep_level_; }
  int def_level_of_immediate_repeated_ancestor() const {
    return node_.def_level_of_immediate_repeated_ancestor;
  }
  const SlotDescriptor* pos_slot_desc() const { return pos_slot_desc_; }
  void set_pos_slot_desc(const SlotDescriptor* pos_slot_desc) {
    DCHECK(pos_slot_desc_ == NULL);
    pos_slot_desc_ = pos_slot_desc;
  }

  /// Returns true if this reader materializes collections (i.e. CollectionValues).
  virtual bool IsCollectionReader() const { return false; }

  const char* filename() const { return parent_->filename(); };

  /// Read the current value (or null) into 'tuple' for this column. This should only be
  /// called when a value is defined, i.e., def_level() >=
  /// def_level_of_immediate_repeated_ancestor() (since empty or NULL collections produce
  /// no output values), otherwise NextLevels() should be called instead.
  ///
  /// Advances this column reader to the next value (i.e. NextLevels() doesn't need to be
  /// called after calling ReadValue()).
  ///
  /// Returns false if execution should be aborted for some reason, e.g. parse_error_ is
  /// set, the query is cancelled, or the scan node limit was reached. Otherwise returns
  /// true.
  ///
  /// NextLevels() must be called on this reader before calling ReadValue() for the first
  /// time. This is to initialize the current value that ReadValue() will read.
  ///
  /// TODO: this is the function that needs to be codegen'd (e.g. CodegenReadValue())
  /// The codegened functions from all the materialized cols will then be combined
  /// into one function.
  /// TODO: another option is to materialize col by col for the entire row batch in
  /// one call.  e.g. MaterializeCol would write out 1024 values.  Our row batches
  /// are currently dense so we'll need to figure out something there.
  virtual bool ReadValue(MemPool* pool, Tuple* tuple) = 0;

  /// Same as ReadValue() but does not advance repetition level. Only valid for columns
  /// not in collections.
  virtual bool ReadNonRepeatedValue(MemPool* pool, Tuple* tuple) = 0;

  /// Returns true if this reader needs to be seeded with NextLevels() before
  /// calling ReadValueBatch() or ReadNonRepeatedValueBatch().
  /// Note that all readers need to be seeded before calling the non-batched ReadValue().
  virtual bool NeedsSeedingForBatchedReading() const { return true; }

  /// Batched version of ReadValue() that reads up to max_values at once and materializes
  /// them into tuples in tuple_mem. Returns the number of values actually materialized
  /// in *num_values. The return value, error behavior and state changes are generally
  /// the same as in ReadValue(). For example, if an error occurs in the middle of
  /// materializing a batch then false is returned, and num_values, tuple_mem, as well as
  /// this column reader are left in an undefined state, assuming that the caller will
  /// immediately abort execution.
  virtual bool ReadValueBatch(MemPool* pool, int max_values, int tuple_size,
      uint8_t* tuple_mem, int* num_values);

  /// Batched version of ReadNonRepeatedValue() that reads up to max_values at once and
  /// materializes them into tuples in tuple_mem.
  /// The return value and error behavior are the same as in ReadValueBatch().
  virtual bool ReadNonRepeatedValueBatch(MemPool* pool, int max_values, int tuple_size,
      uint8_t* tuple_mem, int* num_values);

  /// Advances this column reader's def and rep levels to the next logical value, i.e. to
  /// the next scalar value or the beginning of the next collection, without attempting to
  /// read the value. This is used to skip past def/rep levels that don't materialize a
  /// value, such as the def/rep levels corresponding to an empty containing collection.
  ///
  /// NextLevels() must be called on this reader before calling ReadValue() for the first
  /// time. This is to initialize the current value that ReadValue() will read.
  ///
  /// Returns false if execution should be aborted for some reason, e.g. parse_error_ is
  /// set, the query is cancelled, or the scan node limit was reached. Otherwise returns
  /// true.
  virtual bool NextLevels() = 0;

  /// Should only be called if pos_slot_desc_ is non-NULL. Writes pos_current_value_ to
  /// 'tuple' (i.e. "reads" the synthetic position field of the parent collection into
  /// 'tuple') and increments pos_current_value_.
  void ReadPosition(Tuple* tuple);

  /// Returns true if this column reader has reached the end of the row group.
  inline bool RowGroupAtEnd() { return rep_level_ == HdfsParquetScanner::ROW_GROUP_END; }

  /// Transfers the remaining resources backing tuples to the given row batch,
  /// and frees up other resources.
  virtual void Close(RowBatch* row_batch) = 0;

 protected:
  HdfsParquetScanner* parent_;
  const SchemaNode& node_;
  const SlotDescriptor* slot_desc_;

  /// The slot descriptor for the position field of the tuple, if there is one. NULL if
  /// there's not. Only one column reader for a given tuple desc will have this set.
  const SlotDescriptor* pos_slot_desc_;

  /// The next value to write into the position slot, if there is one. 64-bit int because
  /// the pos slot is always a BIGINT Set to -1 when this column reader does not have a
  /// current rep and def level (i.e. before the first NextLevels() call or after the last
  /// value in the column has been read).
  int64_t pos_current_value_;

  /// The current repetition and definition levels of this reader. Advanced via
  /// ReadValue() and NextLevels(). Set to -1 when this column reader does not have a
  /// current rep and def level (i.e. before the first NextLevels() call or after the last
  /// value in the column has been read). If this is not inside a collection, rep_level_ is
  /// always 0.
  /// int16_t is large enough to hold the valid levels 0-255 and sentinel value -1.
  /// The maximum values are cached here because they are accessed in inner loops.
  int16_t rep_level_;
  int16_t max_rep_level_;
  int16_t def_level_;
  int16_t max_def_level_;

  // Cache frequently accessed members of slot_desc_ for perf.

  /// slot_desc_->tuple_offset(). -1 if slot_desc_ is NULL.
  int tuple_offset_;

  /// slot_desc_->null_indicator_offset(). Invalid if slot_desc_ is NULL.
  NullIndicatorOffset null_indicator_offset_;

  ParquetColumnReader(HdfsParquetScanner* parent, const SchemaNode& node,
      const SlotDescriptor* slot_desc)
    : parent_(parent),
      node_(node),
      slot_desc_(slot_desc),
      pos_slot_desc_(NULL),
      pos_current_value_(HdfsParquetScanner::INVALID_POS),
      rep_level_(HdfsParquetScanner::INVALID_LEVEL),
      max_rep_level_(node_.max_rep_level),
      def_level_(HdfsParquetScanner::INVALID_LEVEL),
      max_def_level_(node_.max_def_level),
      tuple_offset_(slot_desc == NULL ? -1 : slot_desc->tuple_offset()),
      null_indicator_offset_(slot_desc == NULL ? NullIndicatorOffset(-1, -1) :
          slot_desc->null_indicator_offset()) {
    DCHECK_GE(node_.max_rep_level, 0);
    DCHECK_LE(node_.max_rep_level, std::numeric_limits<int16_t>::max());
    DCHECK_GE(node_.max_def_level, 0);
    DCHECK_LE(node_.max_def_level, std::numeric_limits<int16_t>::max());
    // rep_level_ is always valid and equal to 0 if col not in collection.
    if (max_rep_level() == 0) rep_level_ = 0;
  }

  /// Trigger debug action. Returns false if the debug action deems that the
  /// parquet column reader should halt execution. In which case, 'parse_status_'
  /// is also updated.
  bool TriggerDebugAction();
};

/// Reader for a single column from the parquet file.  It's associated with a
/// ScannerContext::Stream and is responsible for decoding the data.  Super class for
/// per-type column readers. This contains most of the logic, the type specific functions
/// must be implemented in the subclass.
class BaseScalarColumnReader : public ParquetColumnReader {
 public:
  BaseScalarColumnReader(HdfsParquetScanner* parent, const SchemaNode& node,
      const SlotDescriptor* slot_desc)
    : ParquetColumnReader(parent, node, slot_desc),
      data_(NULL),
      data_end_(NULL),
      def_levels_(true),
      rep_levels_(false),
      page_encoding_(parquet::Encoding::PLAIN_DICTIONARY),
      num_buffered_values_(0),
      num_values_read_(0),
      metadata_(NULL),
      stream_(NULL),
      decompressed_data_pool_(new MemPool(parent->scan_node_->mem_tracker())) {
    DCHECK_GE(node_.col_idx, 0) << node_.DebugString();
  }

  virtual ~BaseScalarColumnReader() { }

  /// This is called once for each row group in the file.
  Status Reset(const parquet::ColumnMetaData* metadata, ScannerContext::Stream* stream) {
    DCHECK(stream != NULL);
    DCHECK(metadata != NULL);

    num_buffered_values_ = 0;
    data_ = NULL;
    data_end_ = NULL;
    stream_ = stream;
    metadata_ = metadata;
    num_values_read_ = 0;
    def_level_ = -1;
    // See ColumnReader constructor.
    rep_level_ = max_rep_level() == 0 ? 0 : -1;
    pos_current_value_ = -1;

    if (metadata_->codec != parquet::CompressionCodec::UNCOMPRESSED) {
      RETURN_IF_ERROR(Codec::CreateDecompressor(
          NULL, false, PARQUET_TO_IMPALA_CODEC[metadata_->codec], &decompressor_));
    }
    ClearDictionaryDecoder();
    return Status::OK();
  }

  virtual void Close(RowBatch* row_batch) {
    if (decompressed_data_pool_.get() != NULL) {
      row_batch->tuple_data_pool()->AcquireData(decompressed_data_pool_.get(), false);
    }
    if (decompressor_.get() != NULL) decompressor_->Close();
  }

  int64_t total_len() const { return metadata_->total_compressed_size; }
  int col_idx() const { return node_.col_idx; }
  THdfsCompression::type codec() const {
    if (metadata_ == NULL) return THdfsCompression::NONE;
    return PARQUET_TO_IMPALA_CODEC[metadata_->codec];
  }
  MemPool* decompressed_data_pool() const { return decompressed_data_pool_.get(); }

  /// Reads the next definition and repetition levels for this column. Initializes the
  /// next data page if necessary.
  virtual bool NextLevels() { return NextLevels<true>(); }

  // TODO: Some encodings might benefit a lot from a SkipValues(int num_rows) if
  // we know this row can be skipped. This could be very useful with stats and big
  // sections can be skipped. Implement that when we can benefit from it.

 protected:
  // Friend parent scanner so it can perform validation (e.g. ValidateEndOfRowGroup())
  friend class HdfsParquetScanner;

  // Class members that are accessed for every column should be included up here so they
  // fit in as few cache lines as possible.

  /// Pointer to start of next value in data page
  uint8_t* data_;

  /// End of the data page.
  const uint8_t* data_end_;

  /// Decoder for definition levels.
  ParquetLevelDecoder def_levels_;

  /// Decoder for repetition levels.
  ParquetLevelDecoder rep_levels_;

  /// Page encoding for values. Cached here for perf.
  parquet::Encoding::type page_encoding_;

  /// Num values remaining in the current data page
  int num_buffered_values_;

  // Less frequently used members that are not accessed in inner loop should go below
  // here so they do not occupy precious cache line space.

  /// The number of values seen so far. Updated per data page.
  int64_t num_values_read_;

  const parquet::ColumnMetaData* metadata_;
  boost::scoped_ptr<Codec> decompressor_;
  ScannerContext::Stream* stream_;

  /// Pool to allocate decompression buffers from.
  boost::scoped_ptr<MemPool> decompressed_data_pool_;

  /// Header for current data page.
  parquet::PageHeader current_page_header_;

  /// Read the next data page. If a dictionary page is encountered, that will be read and
  /// this function will continue reading the next data page.
  Status ReadDataPage();

  /// Try to move the the next page and buffer more values. Return false and sets rep_level_,
  /// def_level_ and pos_current_value_ to -1 if no more pages or an error encountered.
  bool NextPage();

  /// Implementation for NextLevels().
  template <bool ADVANCE_REP_LEVEL>
  bool NextLevels();

  /// Creates a dictionary decoder from values/size. 'decoder' is set to point to a
  /// dictionary decoder stored in this object. Subclass must implement this. Returns
  /// an error status if the dictionary values could not be decoded successfully.
  virtual Status CreateDictionaryDecoder(uint8_t* values, int size,
      DictDecoderBase** decoder) = 0;

  /// Return true if the column has an initialized dictionary decoder. Subclass must
  /// implement this.
  virtual bool HasDictionaryDecoder() = 0;

  /// Clear the dictionary decoder so HasDictionaryDecoder() will return false. Subclass
  /// must implement this.
  virtual void ClearDictionaryDecoder() = 0;

  /// Initializes the reader with the data contents. This is the content for the entire
  /// decompressed data page. Decoders can initialize state from here. The caller must
  /// validate the input such that 'size' is non-negative and that 'data' has at least
  /// 'size' bytes remaining.
  virtual Status InitDataPage(uint8_t* data, int size) = 0;

 private:
  /// Writes the next value into *slot using pool if necessary. Also advances rep_level_
  /// and def_level_ via NextLevels().
  ///
  /// Returns false if execution should be aborted for some reason, e.g. parse_error_ is
  /// set, the query is cancelled, or the scan node limit was reached. Otherwise returns
  /// true.
  template <bool IN_COLLECTION>
  inline bool ReadSlot(void* slot, MemPool* pool);
};

/// Collections are not materialized directly in parquet files; only scalar values appear
/// in the file. CollectionColumnReader uses the definition and repetition levels of child
/// column readers to figure out the boundaries of each collection in this column.
class CollectionColumnReader : public ParquetColumnReader {
 public:
  CollectionColumnReader(HdfsParquetScanner* parent, const SchemaNode& node,
      const SlotDescriptor* slot_desc)
    : ParquetColumnReader(parent, node, slot_desc) {
    DCHECK(node_.is_repeated());
    if (slot_desc != NULL) DCHECK(slot_desc->type().IsCollectionType());
  }

  virtual ~CollectionColumnReader() { }

  vector<ParquetColumnReader*>* children() { return &children_; }

  virtual bool IsCollectionReader() const { return true; }

  /// The repetition level indicating that the current value is the first in a new
  /// collection (meaning the last value read was the final item in the previous
  /// collection).
  int new_collection_rep_level() const { return max_rep_level() - 1; }

  /// Materializes CollectionValue into tuple slot (if materializing) and advances to next
  /// value.
  virtual bool ReadValue(MemPool* pool, Tuple* tuple);

  /// Same as ReadValue but does not advance repetition level. Only valid for columns not
  /// in collections.
  virtual bool ReadNonRepeatedValue(MemPool* pool, Tuple* tuple);

  /// Advances all child readers to the beginning of the next collection and updates this
  /// reader's state.
  virtual bool NextLevels();

  /// This is called once for each row group in the file.
  void Reset() {
    def_level_ = -1;
    rep_level_ = -1;
    pos_current_value_ = -1;
  }

  virtual void Close(RowBatch* row_batch) {
    for (ParquetColumnReader* child_reader: children_) {
      child_reader->Close(row_batch);
    }
  }

 private:
  /// Column readers of fields contained within this collection. There is at least one
  /// child reader per collection reader. Child readers either materialize slots in the
  /// collection item tuples, or there is a single child reader that does not materialize
  /// any slot and is only used by this reader to read def and rep levels.
  vector<ParquetColumnReader*> children_;

  /// Updates this reader's def_level_, rep_level_, and pos_current_value_ based on child
  /// reader's state.
  void UpdateDerivedState();

  /// Recursively reads from children_ to assemble a single CollectionValue into
  /// *slot. Also advances rep_level_ and def_level_ via NextLevels().
  ///
  /// Returns false if execution should be aborted for some reason, e.g. parse_error_ is
  /// set, the query is cancelled, or the scan node limit was reached. Otherwise returns
  /// true.
  inline bool ReadSlot(void* slot, MemPool* pool);
};

}

#endif
