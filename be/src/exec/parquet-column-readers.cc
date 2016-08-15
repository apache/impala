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

#include "parquet-column-readers.h"

#include <boost/scoped_ptr.hpp>
#include <string>
#include <sstream>
#include <gflags/gflags.h>
#include <gutil/strings/substitute.h>

#include "exec/hdfs-parquet-scanner.h"
#include "exec/parquet-metadata-utils.h"
#include "exec/parquet-scratch-tuple-batch.h"
#include "exec/read-write-util.h"
#include "gutil/bits.h"
#include "rpc/thrift-util.h"
#include "runtime/collection-value-builder.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "runtime/runtime-state.h"
#include "runtime/mem-pool.h"
#include "util/codec.h"
#include "util/debug-util.h"
#include "util/dict-encoding.h"
#include "util/rle-encoding.h"

#include "common/names.h"

using strings::Substitute;

// Provide a workaround for IMPALA-1658.
DEFINE_bool(convert_legacy_hive_parquet_utc_timestamps, false,
    "When true, TIMESTAMPs read from files written by Parquet-MR (used by Hive) will "
    "be converted from UTC to local time. Writes are unaffected.");

// Max data page header size in bytes. This is an estimate and only needs to be an upper
// bound. It is theoretically possible to have a page header of any size due to string
// value statistics, but in practice we'll have trouble reading string values this large.
// Also, this limit is in place to prevent impala from reading corrupt parquet files.
DEFINE_int32(max_page_header_size, 8*1024*1024, "max parquet page header size in bytes");

// Trigger debug action on every 128 tuples produced. This is useful in exercising the
// failure or cancellation path.
#ifndef NDEBUG
#define DEBUG_ACTION_TRIGGER (127)
#define SHOULD_TRIGGER_DEBUG_ACTION(x) ((x & DEBUG_ACTION_TRIGGER) == 0)
#else
#define SHOULD_TRIGGER_DEBUG_ACTION(x) (false)
#endif

namespace impala {

const string PARQUET_MEM_LIMIT_EXCEEDED = "HdfsParquetScanner::$0() failed to allocate "
    "$1 bytes for $2.";

Status ParquetLevelDecoder::Init(const string& filename,
    parquet::Encoding::type encoding, MemPool* cache_pool, int cache_size,
    int max_level, int num_buffered_values, uint8_t** data, int* data_size) {
  DCHECK_GE(num_buffered_values, 0);
  encoding_ = encoding;
  max_level_ = max_level;
  num_buffered_values_ = num_buffered_values;
  filename_ = filename;
  RETURN_IF_ERROR(InitCache(cache_pool, cache_size));

  // Return because there is no level data to read, e.g., required field.
  if (max_level == 0) return Status::OK();

  int32_t num_bytes = 0;
  switch (encoding) {
    case parquet::Encoding::RLE: {
      Status status;
      if (!ReadWriteUtil::Read(data, data_size, &num_bytes, &status)) {
        return status;
      }
      if (num_bytes < 0) {
        return Status(TErrorCode::PARQUET_CORRUPT_RLE_BYTES, filename, num_bytes);
      }
      int bit_width = Bits::Log2Ceiling64(max_level + 1);
      Reset(*data, num_bytes, bit_width);
      break;
    }
    case parquet::Encoding::BIT_PACKED:
      num_bytes = BitUtil::Ceil(num_buffered_values, 8);
      bit_reader_.Reset(*data, num_bytes);
      break;
    default: {
      stringstream ss;
      ss << "Unsupported encoding: " << encoding;
      return Status(ss.str());
    }
  }
  if (UNLIKELY(num_bytes < 0 || num_bytes > *data_size)) {
    return Status(Substitute("Corrupt Parquet file '$0': $1 bytes of encoded levels but "
        "only $2 bytes left in page", filename, num_bytes, data_size));
  }
  *data += num_bytes;
  *data_size -= num_bytes;
  return Status::OK();
}

Status ParquetLevelDecoder::InitCache(MemPool* pool, int cache_size) {
  num_cached_levels_ = 0;
  cached_level_idx_ = 0;
  // Memory has already been allocated.
  if (cached_levels_ != NULL) {
    DCHECK_EQ(cache_size_, cache_size);
    return Status::OK();
  }

  cached_levels_ = reinterpret_cast<uint8_t*>(pool->TryAllocate(cache_size));
  if (cached_levels_ == NULL) {
    return pool->mem_tracker()->MemLimitExceeded(
        NULL, "Definition level cache", cache_size);
  }
  memset(cached_levels_, 0, cache_size);
  cache_size_ = cache_size;
  return Status::OK();
}

inline int16_t ParquetLevelDecoder::ReadLevel() {
  bool valid;
  uint8_t level;
  if (encoding_ == parquet::Encoding::RLE) {
    valid = Get(&level);
  } else {
    DCHECK_EQ(encoding_, parquet::Encoding::BIT_PACKED);
    valid = bit_reader_.GetValue(1, &level);
  }
  return LIKELY(valid) ? level : HdfsParquetScanner::INVALID_LEVEL;
}

Status ParquetLevelDecoder::CacheNextBatch(int batch_size) {
  DCHECK_LE(batch_size, cache_size_);
  cached_level_idx_ = 0;
  if (max_level_ > 0) {
    if (UNLIKELY(!FillCache(batch_size, &num_cached_levels_))) {
      return Status(decoding_error_code_, num_buffered_values_, filename_);
    }
  } else {
    // No levels to read, e.g., because the field is required. The cache was
    // already initialized with all zeros, so we can hand out those values.
    DCHECK_EQ(max_level_, 0);
    num_cached_levels_ = batch_size;
  }
  return Status::OK();
}

bool ParquetLevelDecoder::FillCache(int batch_size,
    int* num_cached_levels) {
  DCHECK(num_cached_levels != NULL);
  int num_values = 0;
  if (encoding_ == parquet::Encoding::RLE) {
    while (true) {
      // Add RLE encoded values by repeating the current value this number of times.
      uint32_t num_repeats_to_set =
          min<uint32_t>(repeat_count_, batch_size - num_values);
      memset(cached_levels_ + num_values, current_value_, num_repeats_to_set);
      num_values += num_repeats_to_set;
      repeat_count_ -= num_repeats_to_set;

      // Add remaining literal values, if any.
      uint32_t num_literals_to_set =
          min<uint32_t>(literal_count_, batch_size - num_values);
      int num_values_end = min<uint32_t>(num_values + literal_count_, batch_size);
      for (; num_values < num_values_end; ++num_values) {
        bool valid = bit_reader_.GetValue(bit_width_, &cached_levels_[num_values]);
        if (UNLIKELY(!valid || cached_levels_[num_values] > max_level_)) return false;
      }
      literal_count_ -= num_literals_to_set;

      if (num_values == batch_size) break;
      if (UNLIKELY(!NextCounts<int16_t>())) return false;
      if (repeat_count_ > 0 && current_value_ > max_level_) return false;
    }
  } else {
    DCHECK_EQ(encoding_, parquet::Encoding::BIT_PACKED);
    for (; num_values < batch_size; ++num_values) {
      bool valid = bit_reader_.GetValue(1, &cached_levels_[num_values]);
      if (UNLIKELY(!valid || cached_levels_[num_values] > max_level_)) return false;
    }
  }
  *num_cached_levels = num_values;
  return true;
}

/// Per column type reader. If MATERIALIZED is true, the column values are materialized
/// into the slot described by slot_desc. If MATERIALIZED is false, the column values
/// are not materialized, but the position can be accessed.
template<typename T, bool MATERIALIZED>
class ScalarColumnReader : public BaseScalarColumnReader {
 public:
  ScalarColumnReader(HdfsParquetScanner* parent, const SchemaNode& node,
      const SlotDescriptor* slot_desc)
    : BaseScalarColumnReader(parent, node, slot_desc),
      dict_decoder_init_(false) {
    if (!MATERIALIZED) {
      // We're not materializing any values, just counting them. No need (or ability) to
      // initialize state used to materialize values.
      DCHECK(slot_desc_ == NULL);
      return;
    }

    DCHECK(slot_desc_ != NULL);
    DCHECK_NE(slot_desc_->type().type, TYPE_BOOLEAN);
    if (slot_desc_->type().type == TYPE_DECIMAL) {
      fixed_len_size_ = ParquetPlainEncoder::DecimalSize(slot_desc_->type());
    } else if (slot_desc_->type().type == TYPE_VARCHAR) {
      fixed_len_size_ = slot_desc_->type().len;
    } else {
      fixed_len_size_ = -1;
    }
    needs_conversion_ = slot_desc_->type().type == TYPE_CHAR ||
        // TODO: Add logic to detect file versions that have unconverted TIMESTAMP
        // values. Currently all versions have converted values.
        (FLAGS_convert_legacy_hive_parquet_utc_timestamps &&
        slot_desc_->type().type == TYPE_TIMESTAMP &&
        parent->file_version_.application == "parquet-mr");
  }

  virtual ~ScalarColumnReader() { }

  virtual bool ReadValue(MemPool* pool, Tuple* tuple) {
    return ReadValue<true>(pool, tuple);
  }

  virtual bool ReadNonRepeatedValue(MemPool* pool, Tuple* tuple) {
    return ReadValue<false>(pool, tuple);
  }

  virtual bool NeedsSeedingForBatchedReading() const { return false; }

  virtual bool ReadValueBatch(MemPool* pool, int max_values, int tuple_size,
      uint8_t* tuple_mem, int* num_values) {
    return ReadValueBatch<true>(pool, max_values, tuple_size, tuple_mem, num_values);
  }

  virtual bool ReadNonRepeatedValueBatch(MemPool* pool, int max_values, int tuple_size,
      uint8_t* tuple_mem, int* num_values) {
    return ReadValueBatch<false>(pool, max_values, tuple_size, tuple_mem, num_values);
  }

 protected:
  template <bool IN_COLLECTION>
  inline bool ReadValue(MemPool* pool, Tuple* tuple) {
    // NextLevels() should have already been called and def and rep levels should be in
    // valid range.
    DCHECK_GE(rep_level_, 0);
    DCHECK_LE(rep_level_, max_rep_level());
    DCHECK_GE(def_level_, 0);
    DCHECK_LE(def_level_, max_def_level());
    DCHECK_GE(def_level_, def_level_of_immediate_repeated_ancestor()) <<
        "Caller should have called NextLevels() until we are ready to read a value";

    if (MATERIALIZED) {
      if (def_level_ >= max_def_level()) {
        if (page_encoding_ == parquet::Encoding::PLAIN_DICTIONARY) {
          if (!ReadSlot<true>(tuple->GetSlot(tuple_offset_), pool)) return false;
        } else {
          if (!ReadSlot<false>(tuple->GetSlot(tuple_offset_), pool)) return false;
        }
      } else {
        tuple->SetNull(null_indicator_offset_);
      }
    }
    return NextLevels<IN_COLLECTION>();
  }

  /// Implementation of the ReadValueBatch() functions specialized for this
  /// column reader type. This function drives the reading of data pages and
  /// caching of rep/def levels. Once a data page and cached levels are available,
  /// it calls into a more specialized MaterializeValueBatch() for doing the actual
  /// value materialization using the level caches.
  template<bool IN_COLLECTION>
  bool ReadValueBatch(MemPool* pool, int max_values, int tuple_size,
      uint8_t* tuple_mem, int* num_values) {
    // Repetition level is only present if this column is nested in a collection type.
    if (!IN_COLLECTION) DCHECK_EQ(max_rep_level(), 0) << slot_desc()->DebugString();
    if (IN_COLLECTION) DCHECK_GT(max_rep_level(), 0) << slot_desc()->DebugString();

    int val_count = 0;
    bool continue_execution = true;
    while (val_count < max_values && !RowGroupAtEnd() && continue_execution) {
      // Read next page if necessary.
      if (num_buffered_values_ == 0) {
        if (!NextPage()) {
          continue_execution = parent_->parse_status_.ok();
          continue;
        }
      }

      // Fill def/rep level caches if they are empty.
      int level_batch_size = min(parent_->state_->batch_size(), num_buffered_values_);
      if (!def_levels_.CacheHasNext()) {
        parent_->parse_status_.MergeStatus(def_levels_.CacheNextBatch(level_batch_size));
      }
      // We only need the repetition levels for populating the position slot since we
      // are only populating top-level tuples.
      if (IN_COLLECTION && pos_slot_desc_ != NULL && !rep_levels_.CacheHasNext()) {
        parent_->parse_status_.MergeStatus(rep_levels_.CacheNextBatch(level_batch_size));
      }
      if (UNLIKELY(!parent_->parse_status_.ok())) return false;

      // This special case is most efficiently handled here directly.
      if (!MATERIALIZED && !IN_COLLECTION) {
        int vals_to_add = min(def_levels_.CacheRemaining(), max_values - val_count);
        val_count += vals_to_add;
        def_levels_.CacheSkipLevels(vals_to_add);
        num_buffered_values_ -= vals_to_add;
        continue;
      }

      // Read data page and cached levels to materialize values.
      int cache_start_idx = def_levels_.CacheCurrIdx();
      uint8_t* next_tuple = tuple_mem + val_count * tuple_size;
      int remaining_val_capacity = max_values - val_count;
      int ret_val_count = 0;
      if (page_encoding_ == parquet::Encoding::PLAIN_DICTIONARY) {
        continue_execution = MaterializeValueBatch<IN_COLLECTION, true>(
            pool, remaining_val_capacity, tuple_size, next_tuple, &ret_val_count);
      } else {
        continue_execution = MaterializeValueBatch<IN_COLLECTION, false>(
            pool, remaining_val_capacity, tuple_size, next_tuple, &ret_val_count);
      }
      val_count += ret_val_count;
      num_buffered_values_ -= (def_levels_.CacheCurrIdx() - cache_start_idx);
      if (SHOULD_TRIGGER_DEBUG_ACTION(val_count)) {
        continue_execution &= TriggerDebugAction();
      }
    }
    *num_values = val_count;
    return continue_execution;
  }

  /// Helper function for ReadValueBatch() above that performs value materialization.
  /// It assumes a data page with remaining values is available, and that the def/rep
  /// level caches have been populated.
  /// For efficiency, the simple special case of !MATERIALIZED && !IN_COLLECTION is not
  /// handled in this function.
  template<bool IN_COLLECTION, bool IS_DICT_ENCODED>
  bool MaterializeValueBatch(MemPool* pool, int max_values, int tuple_size,
      uint8_t* tuple_mem, int* num_values) {
    DCHECK(MATERIALIZED || IN_COLLECTION);
    DCHECK_GT(num_buffered_values_, 0);
    DCHECK(def_levels_.CacheHasNext());
    if (IN_COLLECTION && pos_slot_desc_ != NULL) DCHECK(rep_levels_.CacheHasNext());

    uint8_t* curr_tuple = tuple_mem;
    int val_count = 0;
    while (def_levels_.CacheHasNext()) {
      Tuple* tuple = reinterpret_cast<Tuple*>(curr_tuple);
      int def_level = def_levels_.CacheGetNext();

      if (IN_COLLECTION) {
        if (def_level < def_level_of_immediate_repeated_ancestor()) {
          // A containing repeated field is empty or NULL. Skip the value but
          // move to the next repetition level if necessary.
          if (pos_slot_desc_ != NULL) rep_levels_.CacheGetNext();
          continue;
        }
        if (pos_slot_desc_ != NULL) {
          int rep_level = rep_levels_.CacheGetNext();
          // Reset position counter if we are at the start of a new parent collection.
          if (rep_level <= max_rep_level() - 1) pos_current_value_ = 0;
          void* pos_slot = tuple->GetSlot(pos_slot_desc()->tuple_offset());
          *reinterpret_cast<int64_t*>(pos_slot) = pos_current_value_++;
        }
      }

      if (MATERIALIZED) {
        if (def_level >= max_def_level()) {
          bool continue_execution =
              ReadSlot<IS_DICT_ENCODED>(tuple->GetSlot(tuple_offset_), pool);
          if (UNLIKELY(!continue_execution)) return false;
        } else {
          tuple->SetNull(null_indicator_offset_);
        }
      }

      curr_tuple += tuple_size;
      ++val_count;
      if (UNLIKELY(val_count == max_values)) break;
    }
    *num_values = val_count;
    return true;
  }

  virtual Status CreateDictionaryDecoder(uint8_t* values, int size,
      DictDecoderBase** decoder) {
    if (!dict_decoder_.Reset(values, size, fixed_len_size_)) {
        return Status(TErrorCode::PARQUET_CORRUPT_DICTIONARY, filename(),
            slot_desc_->type().DebugString(), "could not decode dictionary");
    }
    dict_decoder_init_ = true;
    *decoder = &dict_decoder_;
    return Status::OK();
  }

  virtual bool HasDictionaryDecoder() {
    return dict_decoder_init_;
  }

  virtual void ClearDictionaryDecoder() {
    dict_decoder_init_ = false;
  }

  virtual Status InitDataPage(uint8_t* data, int size) {
    // Data can be empty if the column contains all NULLs
    DCHECK_GE(size, 0);
    page_encoding_ = current_page_header_.data_page_header.encoding;
    if (page_encoding_ != parquet::Encoding::PLAIN_DICTIONARY &&
        page_encoding_ != parquet::Encoding::PLAIN) {
      stringstream ss;
      ss << "File '" << filename() << "' is corrupt: unexpected encoding: "
         << PrintEncoding(page_encoding_) << " for data page of column '"
         << schema_element().name << "'.";
      return Status(ss.str());
    }

    // If slot_desc_ is NULL, dict_decoder_ is uninitialized
    if (page_encoding_ == parquet::Encoding::PLAIN_DICTIONARY && slot_desc_ != NULL) {
      if (!dict_decoder_init_) {
        return Status("File corrupt. Missing dictionary page.");
      }
      RETURN_IF_ERROR(dict_decoder_.SetData(data, size));
    }

    // TODO: Perform filter selectivity checks here.
    return Status::OK();
  }

 private:
  /// Writes the next value into *slot using pool if necessary.
  ///
  /// Returns false if execution should be aborted for some reason, e.g. parse_error_ is
  /// set, the query is cancelled, or the scan node limit was reached. Otherwise returns
  /// true.
  template<bool IS_DICT_ENCODED>
  inline bool ReadSlot(void* slot, MemPool* pool) {
    T val;
    T* val_ptr = NeedsConversion() ? &val : reinterpret_cast<T*>(slot);
    if (IS_DICT_ENCODED) {
      DCHECK_EQ(page_encoding_, parquet::Encoding::PLAIN_DICTIONARY);
      if (UNLIKELY(!dict_decoder_.GetValue(val_ptr))) {
        SetDictDecodeError();
        return false;
      }
    } else {
      DCHECK_EQ(page_encoding_, parquet::Encoding::PLAIN);
      int encoded_len =
          ParquetPlainEncoder::Decode<T>(data_, data_end_, fixed_len_size_, val_ptr);
      if (UNLIKELY(encoded_len < 0)) {
        SetPlainDecodeError();
        return false;
      }
      data_ += encoded_len;
    }
    if (UNLIKELY(NeedsConversion() &&
            !ConvertSlot(&val, reinterpret_cast<T*>(slot), pool))) {
      return false;
    }
    return true;
  }

  /// Most column readers never require conversion, so we can avoid branches by
  /// returning constant false. Column readers for types that require conversion
  /// must specialize this function.
  inline bool NeedsConversion() const {
    DCHECK(!needs_conversion_);
    return false;
  }

  /// Converts and writes src into dst based on desc_->type()
  bool ConvertSlot(const T* src, T* dst, MemPool* pool) {
    DCHECK(false);
    return false;
  }

  /// Pull out slow-path Status construction code from ReadRepetitionLevel()/
  /// ReadDefinitionLevel() for performance.
  void __attribute__((noinline)) SetDictDecodeError() {
    parent_->parse_status_ = Status(TErrorCode::PARQUET_DICT_DECODE_FAILURE, filename(),
        slot_desc_->type().DebugString(), stream_->file_offset());
  }
  void __attribute__((noinline)) SetPlainDecodeError() {
    parent_->parse_status_ = Status(TErrorCode::PARQUET_CORRUPT_PLAIN_VALUE, filename(),
        slot_desc_->type().DebugString(), stream_->file_offset());
  }

  /// Dictionary decoder for decoding column values.
  DictDecoder<T> dict_decoder_;

  /// True if dict_decoder_ has been initialized with a dictionary page.
  bool dict_decoder_init_;

  /// true if decoded values must be converted before being written to an output tuple.
  bool needs_conversion_;

  /// The size of this column with plain encoding for FIXED_LEN_BYTE_ARRAY, or
  /// the max length for VARCHAR columns. Unused otherwise.
  int fixed_len_size_;
};

template<>
inline bool ScalarColumnReader<StringValue, true>::NeedsConversion() const {
  return needs_conversion_;
}

template<>
bool ScalarColumnReader<StringValue, true>::ConvertSlot(
    const StringValue* src, StringValue* dst, MemPool* pool) {
  DCHECK(slot_desc() != NULL);
  DCHECK(slot_desc()->type().type == TYPE_CHAR);
  int len = slot_desc()->type().len;
  StringValue sv;
  sv.len = len;
  if (slot_desc()->type().IsVarLenStringType()) {
    sv.ptr = reinterpret_cast<char*>(pool->TryAllocate(len));
    if (UNLIKELY(sv.ptr == NULL)) {
      string details = Substitute(PARQUET_MEM_LIMIT_EXCEEDED, "ConvertSlot",
          len, "StringValue");
      parent_->parse_status_ =
          pool->mem_tracker()->MemLimitExceeded(parent_->state_, details, len);
      return false;
    }
  } else {
    sv.ptr = reinterpret_cast<char*>(dst);
  }
  int unpadded_len = min(len, src->len);
  memcpy(sv.ptr, src->ptr, unpadded_len);
  StringValue::PadWithSpaces(sv.ptr, len, unpadded_len);

  if (slot_desc()->type().IsVarLenStringType()) *dst = sv;
  return true;
}

template<>
inline bool ScalarColumnReader<TimestampValue, true>::NeedsConversion() const {
  return needs_conversion_;
}

template<>
bool ScalarColumnReader<TimestampValue, true>::ConvertSlot(
    const TimestampValue* src, TimestampValue* dst, MemPool* pool) {
  // Conversion should only happen when this flag is enabled.
  DCHECK(FLAGS_convert_legacy_hive_parquet_utc_timestamps);
  *dst = *src;
  if (dst->HasDateAndTime()) dst->UtcToLocal();
  return true;
}

class BoolColumnReader : public BaseScalarColumnReader {
 public:
  BoolColumnReader(HdfsParquetScanner* parent, const SchemaNode& node,
      const SlotDescriptor* slot_desc)
    : BaseScalarColumnReader(parent, node, slot_desc) {
    if (slot_desc_ != NULL) DCHECK_EQ(slot_desc_->type().type, TYPE_BOOLEAN);
  }

  virtual ~BoolColumnReader() { }

  virtual bool ReadValue(MemPool* pool, Tuple* tuple) {
    return ReadValue<true>(pool, tuple);
  }

  virtual bool ReadNonRepeatedValue(MemPool* pool, Tuple* tuple) {
    return ReadValue<false>(pool, tuple);
  }

 protected:
  virtual Status CreateDictionaryDecoder(uint8_t* values, int size,
      DictDecoderBase** decoder) {
    DCHECK(false) << "Dictionary encoding is not supported for bools. Should never "
                  << "have gotten this far.";
    return Status::OK();
  }

  virtual bool HasDictionaryDecoder() {
    // Decoder should never be created for bools.
    return false;
  }

  virtual void ClearDictionaryDecoder() { }

  virtual Status InitDataPage(uint8_t* data, int size) {
    // Initialize bool decoder
    bool_values_ = BitReader(data, size);
    return Status::OK();
  }

 private:
  template<bool IN_COLLECTION>
  inline bool ReadValue(MemPool* pool, Tuple* tuple) {
    DCHECK(slot_desc_ != NULL);
    // Def and rep levels should be in valid range.
    DCHECK_GE(rep_level_, 0);
    DCHECK_LE(rep_level_, max_rep_level());
    DCHECK_GE(def_level_, 0);
    DCHECK_LE(def_level_, max_def_level());
    DCHECK_GE(def_level_, def_level_of_immediate_repeated_ancestor()) <<
        "Caller should have called NextLevels() until we are ready to read a value";

    if (def_level_ >= max_def_level()) {
      return ReadSlot<IN_COLLECTION>(tuple->GetSlot(tuple_offset_), pool);
    } else {
      // Null value
      tuple->SetNull(null_indicator_offset_);
      return NextLevels<IN_COLLECTION>();
    }
  }

  /// Writes the next value into *slot using pool if necessary. Also advances def_level_
  /// and rep_level_ via NextLevels().
  ///
  /// Returns false if execution should be aborted for some reason, e.g. parse_error_ is
  /// set, the query is cancelled, or the scan node limit was reached. Otherwise returns
  /// true.
  template <bool IN_COLLECTION>
  inline bool ReadSlot(void* slot, MemPool* pool)  {
    if (!bool_values_.GetValue(1, reinterpret_cast<bool*>(slot))) {
      parent_->parse_status_ = Status("Invalid bool column.");
      return false;
    }
    return NextLevels<IN_COLLECTION>();
  }

  BitReader bool_values_;
};

bool ParquetColumnReader::TriggerDebugAction() {
  Status status = parent_->TriggerDebugAction();
  if (!status.ok()) {
    if (!status.IsCancelled()) parent_->parse_status_.MergeStatus(status);
    return false;
  }
  return true;
}

bool ParquetColumnReader::ReadValueBatch(MemPool* pool, int max_values,
    int tuple_size, uint8_t* tuple_mem, int* num_values) {
  int val_count = 0;
  bool continue_execution = true;
  while (val_count < max_values && !RowGroupAtEnd() && continue_execution) {
    Tuple* tuple = reinterpret_cast<Tuple*>(tuple_mem + val_count * tuple_size);
    if (def_level_ < def_level_of_immediate_repeated_ancestor()) {
      // A containing repeated field is empty or NULL
      continue_execution = NextLevels();
      continue;
    }
    // Fill in position slot if applicable
    if (pos_slot_desc_ != NULL) ReadPosition(tuple);
    continue_execution = ReadValue(pool, tuple);
    ++val_count;
    if (SHOULD_TRIGGER_DEBUG_ACTION(val_count)) {
      continue_execution &= TriggerDebugAction();
    }
  }
  *num_values = val_count;
  return continue_execution;
}

bool ParquetColumnReader::ReadNonRepeatedValueBatch(MemPool* pool,
    int max_values, int tuple_size, uint8_t* tuple_mem, int* num_values) {
  int val_count = 0;
  bool continue_execution = true;
  while (val_count < max_values && !RowGroupAtEnd() && continue_execution) {
    Tuple* tuple = reinterpret_cast<Tuple*>(tuple_mem + val_count * tuple_size);
    continue_execution = ReadNonRepeatedValue(pool, tuple);
    ++val_count;
    if (SHOULD_TRIGGER_DEBUG_ACTION(val_count)) {
      continue_execution &= TriggerDebugAction();
    }
  }
  *num_values = val_count;
  return continue_execution;
}

void ParquetColumnReader::ReadPosition(Tuple* tuple) {
  DCHECK(pos_slot_desc() != NULL);
  // NextLevels() should have already been called
  DCHECK_GE(rep_level_, 0);
  DCHECK_GE(def_level_, 0);
  DCHECK_GE(pos_current_value_, 0);
  DCHECK_GE(def_level_, def_level_of_immediate_repeated_ancestor()) <<
      "Caller should have called NextLevels() until we are ready to read a value";

  void* slot = tuple->GetSlot(pos_slot_desc()->tuple_offset());
  *reinterpret_cast<int64_t*>(slot) = pos_current_value_++;
}

// In 1.1, we had a bug where the dictionary page metadata was not set. Returns true
// if this matches those versions and compatibility workarounds need to be used.
static bool RequiresSkippedDictionaryHeaderCheck(
    const ParquetFileVersion& v) {
  if (v.application != "impala") return false;
  return v.VersionEq(1,1,0) || (v.VersionEq(1,2,0) && v.is_impala_internal);
}

Status BaseScalarColumnReader::ReadDataPage() {
  Status status;
  uint8_t* buffer;

  // We're about to move to the next data page.  The previous data page is
  // now complete, pass along the memory allocated for it.
  parent_->scratch_batch_->mem_pool()->AcquireData(decompressed_data_pool_.get(), false);

  // Read the next data page, skipping page types we don't care about.
  // We break out of this loop on the non-error case (a data page was found or we read all
  // the pages).
  while (true) {
    DCHECK_EQ(num_buffered_values_, 0);
    if (num_values_read_ == metadata_->num_values) {
      // No more pages to read
      // TODO: should we check for stream_->eosr()?
      break;
    } else if (num_values_read_ > metadata_->num_values) {
      ErrorMsg msg(TErrorCode::PARQUET_COLUMN_METADATA_INVALID,
          metadata_->num_values, num_values_read_, node_.element->name, filename());
      RETURN_IF_ERROR(parent_->state_->LogOrReturnError(msg));
      return Status::OK();
    }

    int64_t buffer_size;
    RETURN_IF_ERROR(stream_->GetBuffer(true, &buffer, &buffer_size));
    if (buffer_size == 0) {
      // The data pages contain fewer values than stated in the column metadata.
      DCHECK(stream_->eosr());
      DCHECK_LT(num_values_read_, metadata_->num_values);
      // TODO for 2.3: node_.element->name isn't necessarily useful
      ErrorMsg msg(TErrorCode::PARQUET_COLUMN_METADATA_INVALID,
          metadata_->num_values, num_values_read_, node_.element->name, filename());
      RETURN_IF_ERROR(parent_->state_->LogOrReturnError(msg));
      return Status::OK();
    }

    // We don't know the actual header size until the thrift object is deserialized.  Loop
    // until we successfully deserialize the header or exceed the maximum header size.
    uint32_t header_size;
    while (true) {
      header_size = buffer_size;
      status = DeserializeThriftMsg(
          buffer, &header_size, true, &current_page_header_);
      if (status.ok()) break;

      if (buffer_size >= FLAGS_max_page_header_size) {
        stringstream ss;
        ss << "ParquetScanner: could not read data page because page header exceeded "
           << "maximum size of "
           << PrettyPrinter::Print(FLAGS_max_page_header_size, TUnit::BYTES);
        status.AddDetail(ss.str());
        return status;
      }

      // Didn't read entire header, increase buffer size and try again
      Status status;
      int64_t new_buffer_size = max<int64_t>(buffer_size * 2, 1024);
      bool success = stream_->GetBytes(
          new_buffer_size, &buffer, &new_buffer_size, &status, /* peek */ true);
      if (!success) {
        DCHECK(!status.ok());
        return status;
      }
      DCHECK(status.ok());

      if (buffer_size == new_buffer_size) {
        DCHECK_NE(new_buffer_size, 0);
        return Status(TErrorCode::PARQUET_HEADER_EOF, filename());
      }
      DCHECK_GT(new_buffer_size, buffer_size);
      buffer_size = new_buffer_size;
    }

    // Successfully deserialized current_page_header_
    if (!stream_->SkipBytes(header_size, &status)) return status;

    int data_size = current_page_header_.compressed_page_size;
    int uncompressed_size = current_page_header_.uncompressed_page_size;
    if (UNLIKELY(data_size < 0)) {
      return Status(Substitute("Corrupt Parquet file '$0': negative page size $1 for "
          "column '$2'", filename(), data_size, schema_element().name));
    }
    if (UNLIKELY(uncompressed_size < 0)) {
      return Status(Substitute("Corrupt Parquet file '$0': negative uncompressed page "
          "size $1 for column '$2'", filename(), uncompressed_size,
          schema_element().name));
    }

    if (current_page_header_.type == parquet::PageType::DICTIONARY_PAGE) {
      if (slot_desc_ == NULL) {
        // Skip processing the dictionary page if we don't need to decode any values. In
        // addition to being unnecessary, we are likely unable to successfully decode the
        // dictionary values because we don't necessarily create the right type of scalar
        // reader if there's no slot to read into (see CreateReader()).
        if (!stream_->ReadBytes(data_size, &data_, &status)) return status;
        continue;
      }

      if (HasDictionaryDecoder()) {
        return Status("Column chunk should not contain two dictionary pages.");
      }
      if (node_.element->type == parquet::Type::BOOLEAN) {
        return Status("Unexpected dictionary page. Dictionary page is not"
            " supported for booleans.");
      }
      const parquet::DictionaryPageHeader* dict_header = NULL;
      if (current_page_header_.__isset.dictionary_page_header) {
        dict_header = &current_page_header_.dictionary_page_header;
      } else {
        if (!RequiresSkippedDictionaryHeaderCheck(parent_->file_version_)) {
          return Status("Dictionary page does not have dictionary header set.");
        }
      }
      if (dict_header != NULL &&
          dict_header->encoding != parquet::Encoding::PLAIN &&
          dict_header->encoding != parquet::Encoding::PLAIN_DICTIONARY) {
        return Status("Only PLAIN and PLAIN_DICTIONARY encodings are supported "
            "for dictionary pages.");
      }

      if (!stream_->ReadBytes(data_size, &data_, &status)) return status;
      data_end_ = data_ + data_size;

      uint8_t* dict_values = NULL;
      if (decompressor_.get() != NULL) {
        dict_values = parent_->dictionary_pool_->TryAllocate(uncompressed_size);
        if (UNLIKELY(dict_values == NULL)) {
          string details = Substitute(PARQUET_MEM_LIMIT_EXCEEDED, "ReadDataPage",
              uncompressed_size, "dictionary");
          return parent_->dictionary_pool_->mem_tracker()->MemLimitExceeded(
              parent_->state_, details, uncompressed_size);
        }
        RETURN_IF_ERROR(decompressor_->ProcessBlock32(true, data_size, data_,
            &uncompressed_size, &dict_values));
        VLOG_FILE << "Decompressed " << data_size << " to " << uncompressed_size;
        if (current_page_header_.uncompressed_page_size != uncompressed_size) {
          return Status(Substitute("Error decompressing dictionary page in file '$0'. "
              "Expected $1 uncompressed bytes but got $2", filename(),
              current_page_header_.uncompressed_page_size, uncompressed_size));
        }
        data_size = uncompressed_size;
      } else {
        if (current_page_header_.uncompressed_page_size != data_size) {
          return Status(Substitute("Error reading dictionary page in file '$0'. "
              "Expected $1 bytes but got $2", filename(),
              current_page_header_.uncompressed_page_size, data_size));
        }
        // Copy dictionary from io buffer (which will be recycled as we read
        // more data) to a new buffer
        dict_values = parent_->dictionary_pool_->TryAllocate(data_size);
        if (UNLIKELY(dict_values == NULL)) {
          string details = Substitute(PARQUET_MEM_LIMIT_EXCEEDED, "ReadDataPage",
              data_size, "dictionary");
          return parent_->dictionary_pool_->mem_tracker()->MemLimitExceeded(
              parent_->state_, details, data_size);
        }
        memcpy(dict_values, data_, data_size);
      }

      DictDecoderBase* dict_decoder;
      RETURN_IF_ERROR(CreateDictionaryDecoder(dict_values, data_size, &dict_decoder));
      if (dict_header != NULL &&
          dict_header->num_values != dict_decoder->num_entries()) {
        return Status(TErrorCode::PARQUET_CORRUPT_DICTIONARY, filename(),
            slot_desc_->type().DebugString(),
            Substitute("Expected $0 entries but data contained $1 entries",
            dict_header->num_values, dict_decoder->num_entries()));
      }
      // Done with dictionary page, read next page
      continue;
    }

    if (current_page_header_.type != parquet::PageType::DATA_PAGE) {
      // We can safely skip non-data pages
      if (!stream_->SkipBytes(data_size, &status)) return status;
      continue;
    }

    // Read Data Page
    // TODO: when we start using page statistics, we will need to ignore certain corrupt
    // statistics. See IMPALA-2208 and PARQUET-251.
    if (!stream_->ReadBytes(data_size, &data_, &status)) return status;
    data_end_ = data_ + data_size;
    int num_values = current_page_header_.data_page_header.num_values;
    if (num_values < 0) {
      return Status(Substitute("Error reading data page in Parquet file '$0'. "
          "Invalid number of values in metadata: $1", filename(), num_values));
    }
    num_buffered_values_ = num_values;
    num_values_read_ += num_buffered_values_;

    if (decompressor_.get() != NULL) {
      SCOPED_TIMER(parent_->decompress_timer_);
      uint8_t* decompressed_buffer =
          decompressed_data_pool_->TryAllocate(uncompressed_size);
      if (UNLIKELY(decompressed_buffer == NULL)) {
        string details = Substitute(PARQUET_MEM_LIMIT_EXCEEDED, "ReadDataPage",
            uncompressed_size, "decompressed data");
        return decompressed_data_pool_->mem_tracker()->MemLimitExceeded(
            parent_->state_, details, uncompressed_size);
      }
      RETURN_IF_ERROR(decompressor_->ProcessBlock32(true,
          current_page_header_.compressed_page_size, data_, &uncompressed_size,
          &decompressed_buffer));
      VLOG_FILE << "Decompressed " << current_page_header_.compressed_page_size
                << " to " << uncompressed_size;
      if (current_page_header_.uncompressed_page_size != uncompressed_size) {
        return Status(Substitute("Error decompressing data page in file '$0'. "
            "Expected $1 uncompressed bytes but got $2", filename(),
            current_page_header_.uncompressed_page_size, uncompressed_size));
      }
      data_ = decompressed_buffer;
      data_size = current_page_header_.uncompressed_page_size;
      data_end_ = data_ + data_size;
    } else {
      DCHECK_EQ(metadata_->codec, parquet::CompressionCodec::UNCOMPRESSED);
      if (current_page_header_.compressed_page_size != uncompressed_size) {
        return Status(Substitute("Error reading data page in file '$0'. "
            "Expected $1 bytes but got $2", filename(),
            current_page_header_.compressed_page_size, uncompressed_size));
      }
    }

    // Initialize the repetition level data
    RETURN_IF_ERROR(rep_levels_.Init(filename(),
        current_page_header_.data_page_header.repetition_level_encoding,
        parent_->level_cache_pool_.get(), parent_->state_->batch_size(),
        max_rep_level(), num_buffered_values_,
        &data_, &data_size));

    // Initialize the definition level data
    RETURN_IF_ERROR(def_levels_.Init(filename(),
        current_page_header_.data_page_header.definition_level_encoding,
        parent_->level_cache_pool_.get(), parent_->state_->batch_size(),
        max_def_level(), num_buffered_values_, &data_, &data_size));

    // Data can be empty if the column contains all NULLs
    RETURN_IF_ERROR(InitDataPage(data_, data_size));
    break;
  }

  return Status::OK();
}

template <bool ADVANCE_REP_LEVEL>
bool BaseScalarColumnReader::NextLevels() {
  if (!ADVANCE_REP_LEVEL) DCHECK_EQ(max_rep_level(), 0) << slot_desc()->DebugString();

  if (UNLIKELY(num_buffered_values_ == 0)) {
    if (!NextPage()) return parent_->parse_status_.ok();
  }
  --num_buffered_values_;

  // Definition level is not present if column and any containing structs are required.
  def_level_ = max_def_level() == 0 ? 0 : def_levels_.ReadLevel();
  // The compiler can optimize these two conditions into a single branch by treating
  // def_level_ as unsigned.
  if (UNLIKELY(def_level_ < 0 || def_level_ > max_def_level())) {
    parent_->parse_status_.MergeStatus(Status(Substitute("Corrupt Parquet file '$0': "
        "invalid def level $1 > max def level $2 for column '$3'", filename(),
        def_level_, max_def_level(), schema_element().name)));
    return false;
  }

  if (ADVANCE_REP_LEVEL && max_rep_level() > 0) {
    // Repetition level is only present if this column is nested in any collection type.
    rep_level_ = rep_levels_.ReadLevel();
    // Reset position counter if we are at the start of a new parent collection.
    if (rep_level_ <= max_rep_level() - 1) pos_current_value_ = 0;
  }

  return parent_->parse_status_.ok();
}

bool BaseScalarColumnReader::NextPage() {
  parent_->assemble_rows_timer_.Stop();
  parent_->parse_status_ = ReadDataPage();
  if (UNLIKELY(!parent_->parse_status_.ok())) return false;
  if (num_buffered_values_ == 0) {
    rep_level_ = HdfsParquetScanner::ROW_GROUP_END;
    def_level_ = HdfsParquetScanner::INVALID_LEVEL;
    pos_current_value_ = HdfsParquetScanner::INVALID_POS;
    return false;
  }
  parent_->assemble_rows_timer_.Start();
  return true;
}

bool CollectionColumnReader::NextLevels() {
  DCHECK(!children_.empty());
  DCHECK_LE(rep_level_, new_collection_rep_level());
  for (int c = 0; c < children_.size(); ++c) {
    do {
      // TODO(skye): verify somewhere that all column readers are at end
      if (!children_[c]->NextLevels()) return false;
    } while (children_[c]->rep_level() > new_collection_rep_level());
  }
  UpdateDerivedState();
  return true;
}

bool CollectionColumnReader::ReadValue(MemPool* pool, Tuple* tuple) {
  DCHECK_GE(rep_level_, 0);
  DCHECK_GE(def_level_, 0);
  DCHECK_GE(def_level_, def_level_of_immediate_repeated_ancestor()) <<
      "Caller should have called NextLevels() until we are ready to read a value";

  if (tuple_offset_ == -1) {
    return CollectionColumnReader::NextLevels();
  } else if (def_level_ >= max_def_level()) {
    return ReadSlot(tuple->GetSlot(tuple_offset_), pool);
  } else {
    // Null value
    tuple->SetNull(null_indicator_offset_);
    return CollectionColumnReader::NextLevels();
  }
}

bool CollectionColumnReader::ReadNonRepeatedValue(
    MemPool* pool, Tuple* tuple) {
  return CollectionColumnReader::ReadValue(pool, tuple);
}

bool CollectionColumnReader::ReadSlot(void* slot, MemPool* pool) {
  DCHECK(!children_.empty());
  DCHECK_LE(rep_level_, new_collection_rep_level());

  // Recursively read the collection into a new CollectionValue.
  CollectionValue* coll_slot = reinterpret_cast<CollectionValue*>(slot);
  *coll_slot = CollectionValue();
  CollectionValueBuilder builder(
      coll_slot, *slot_desc_->collection_item_descriptor(), pool, parent_->state_);
  bool continue_execution = parent_->AssembleCollection(
      children_, new_collection_rep_level(), &builder);
  if (!continue_execution) return false;

  // AssembleCollection() advances child readers, so we don't need to call NextLevels()
  UpdateDerivedState();
  return true;
}

void CollectionColumnReader::UpdateDerivedState() {
  // We don't need to cap our def_level_ at max_def_level(). We always check def_level_
  // >= max_def_level() to check if the collection is defined.
  // TODO(skye): consider capping def_level_ at max_def_level()
  def_level_ = children_[0]->def_level();
  rep_level_ = children_[0]->rep_level();

  // All children should have been advanced to the beginning of the next collection
  for (int i = 0; i < children_.size(); ++i) {
    DCHECK_EQ(children_[i]->rep_level(), rep_level_);
    if (def_level_ < max_def_level()) {
      // Collection not defined
      FILE_CHECK_EQ(children_[i]->def_level(), def_level_);
    } else {
      // Collection is defined
      FILE_CHECK_GE(children_[i]->def_level(), max_def_level());
    }
  }

  if (RowGroupAtEnd()) {
    // No more values
    pos_current_value_ = HdfsParquetScanner::INVALID_POS;
  } else if (rep_level_ <= max_rep_level() - 2) {
    // Reset position counter if we are at the start of a new parent collection (i.e.,
    // the current collection is the first item in a new parent collection).
    pos_current_value_ = 0;
  }
}

ParquetColumnReader* ParquetColumnReader::Create(const SchemaNode& node,
    bool is_collection_field, const SlotDescriptor* slot_desc, HdfsParquetScanner* parent) {
  ParquetColumnReader* reader = NULL;
  if (is_collection_field) {
    // Create collection reader (note this handles both NULL and non-NULL 'slot_desc')
    reader = new CollectionColumnReader(parent, node, slot_desc);
  } else if (slot_desc != NULL) {
    // Create the appropriate ScalarColumnReader type to read values into 'slot_desc'
    switch (slot_desc->type().type) {
      case TYPE_BOOLEAN:
        reader = new BoolColumnReader(parent, node, slot_desc);
        break;
      case TYPE_TINYINT:
        reader = new ScalarColumnReader<int8_t, true>(parent, node, slot_desc);
        break;
      case TYPE_SMALLINT:
        reader = new ScalarColumnReader<int16_t, true>(parent, node, slot_desc);
        break;
      case TYPE_INT:
        reader = new ScalarColumnReader<int32_t, true>(parent, node, slot_desc);
        break;
      case TYPE_BIGINT:
        reader = new ScalarColumnReader<int64_t, true>(parent, node, slot_desc);
        break;
      case TYPE_FLOAT:
        reader = new ScalarColumnReader<float, true>(parent, node, slot_desc);
        break;
      case TYPE_DOUBLE:
        reader = new ScalarColumnReader<double, true>(parent, node, slot_desc);
        break;
      case TYPE_TIMESTAMP:
        reader = new ScalarColumnReader<TimestampValue, true>(parent, node, slot_desc);
        break;
      case TYPE_STRING:
      case TYPE_VARCHAR:
      case TYPE_CHAR:
        reader = new ScalarColumnReader<StringValue, true>(parent, node, slot_desc);
        break;
      case TYPE_DECIMAL:
        switch (slot_desc->type().GetByteSize()) {
          case 4:
            reader = new ScalarColumnReader<Decimal4Value, true>(
                parent, node, slot_desc);
            break;
          case 8:
            reader = new ScalarColumnReader<Decimal8Value, true>(
                parent, node, slot_desc);
            break;
          case 16:
            reader = new ScalarColumnReader<Decimal16Value, true>(
                parent, node, slot_desc);
            break;
        }
        break;
      default:
        DCHECK(false) << slot_desc->type().DebugString();
    }
  } else {
    // Special case for counting scalar values (e.g. count(*), no materialized columns in
    // the file, only materializing a position slot). We won't actually read any values,
    // only the rep and def levels, so it doesn't matter what kind of reader we make.
    reader = new ScalarColumnReader<int8_t, false>(parent, node, slot_desc);
  }
  return parent->obj_pool_.Add(reader);
}

}
