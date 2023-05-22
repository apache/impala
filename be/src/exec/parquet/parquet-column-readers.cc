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

#include <string>
#include <gutil/strings/substitute.h>

#include "exec/parquet/hdfs-parquet-scanner.h"
#include "exec/parquet/parquet-bool-decoder.h"
#include "exec/parquet/parquet-data-converter.h"
#include "exec/parquet/parquet-level-decoder.h"
#include "exec/parquet/parquet-metadata-utils.h"
#include "exec/parquet/parquet-struct-column-reader.h"
#include "exec/scratch-tuple-batch.h"
#include "parquet-collection-column-reader.h"
#include "runtime/runtime-state.h"
#include "runtime/scoped-buffer.h"
#include "runtime/string-value.inline.h"
#include "runtime/tuple.h"
#include "util/debug-util.h"
#include "util/dict-encoding.h"
#include "util/rle-encoding.h"

#include "common/names.h"

using namespace impala::io;

using parquet::Encoding;

namespace impala {

// Definition of variable declared in header for use of the
// SHOULD_TRIGGER_COL_READER_DEBUG_ACTION macro.
AtomicInt32 parquet_column_reader_debug_count;

/// Per column type reader. InternalType is the datatype that Impala uses internally to
/// store tuple data and PARQUET_TYPE is the corresponding primitive datatype (as defined
/// in the parquet spec) that is used to store column values in parquet files.
/// If MATERIALIZED is true, the column values are materialized into the slot described
/// by slot_desc. If MATERIALIZED is false, the column values are not materialized, but
/// the position can be accessed.
template <typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
class ScalarColumnReader : public BaseScalarColumnReader {
 public:
  ScalarColumnReader(HdfsParquetScanner* parent, const SchemaNode& node,
      const SlotDescriptor* slot_desc);
  virtual ~ScalarColumnReader() { }

  virtual bool ReadValue(MemPool* pool, Tuple* tuple) override {
    return ReadValue<true>(tuple);
  }

  virtual bool ReadNonRepeatedValue(MemPool* pool, Tuple* tuple) override {
    return ReadValue<false>(tuple);
  }

  virtual bool ReadValueBatch(MemPool* pool, int max_values, int tuple_size,
      uint8_t* tuple_mem, int* num_values) override {
    return ReadValueBatch<true>(max_values, tuple_size, tuple_mem, num_values);
  }

  virtual bool ReadNonRepeatedValueBatch(MemPool* pool, int max_values, int tuple_size,
      uint8_t* tuple_mem, int* num_values) override {
    return ReadValueBatch<false>(max_values, tuple_size, tuple_mem, num_values);
  }

  virtual DictDecoderBase* GetDictionaryDecoder() override {
    return HasDictionaryDecoder() ? &dict_decoder_ : nullptr;
  }

  virtual bool NeedsConversion() override { return NeedsConversionInline(); }
  virtual bool NeedsValidation() override { return NeedsValidationInline(); }

  template <bool IN_COLLECTION>
  inline bool ReadValue(Tuple* tuple);

 protected:
  /// Implementation of the ReadValueBatch() functions specialized for this
  /// column reader type. This function drives the reading of data pages and
  /// caching of rep/def levels. Once a data page and cached levels are available,
  /// it calls into a more specialized MaterializeValueBatch() for doing the actual
  /// value materialization using the level caches.
  /// Use RESTRICT so that the compiler knows that it is safe to cache member
  /// variables in registers or on the stack (otherwise gcc's alias analysis
  /// conservatively assumes that buffers like 'tuple_mem', 'num_values' or the
  /// 'def_levels_' 'rep_levels_' buffers may alias 'this', especially with
  /// -fno-strict-alias).
  template <bool IN_COLLECTION>
  bool ReadValueBatch(int max_values, int tuple_size, uint8_t* RESTRICT tuple_mem,
      int* RESTRICT num_values) RESTRICT;

  /// Helper function for ReadValueBatch() above that performs value materialization.
  /// It assumes a data page with remaining values is available, and that the def/rep
  /// level caches have been populated. Materializes values into 'tuple_mem' with a
  /// stride of 'tuple_size' and updates 'num_buffered_values_'. Returns the number of
  /// values materialized in 'num_values'.
  /// For efficiency, the simple special case of !MATERIALIZED && !IN_COLLECTION is not
  /// handled in this function.
  /// Use RESTRICT so that the compiler knows that it is safe to cache member
  /// variables in registers or on the stack (otherwise gcc's alias analysis
  /// conservatively assumes that buffers like 'tuple_mem', 'num_values' or the
  /// 'def_levels_' 'rep_levels_' buffers may alias 'this', especially with
  /// -fno-strict-alias).
  template <bool IN_COLLECTION, Encoding::type ENCODING, bool NEEDS_CONVERSION>
  bool MaterializeValueBatch(int max_values, int tuple_size, uint8_t* RESTRICT tuple_mem,
      int* RESTRICT num_values) RESTRICT;

  /// Same as above, but dispatches to the appropriate templated implementation of
  /// MaterializeValueBatch() based on 'page_encoding_' and NeedsConversionInline().
  template <bool IN_COLLECTION>
  bool MaterializeValueBatch(int max_values, int tuple_size, uint8_t* RESTRICT tuple_mem,
      int* RESTRICT num_values) RESTRICT;

  /// Fast path for MaterializeValueBatch() that materializes values for a run of
  /// repeated definition levels. Read up to 'max_values' values into 'tuple_mem',
  /// returning the number of values materialised in 'num_values'.
  bool MaterializeValueBatchRepeatedDefLevel(int max_values, int tuple_size,
      uint8_t* RESTRICT tuple_mem, int* RESTRICT num_values) RESTRICT;

  /// Read 'num_to_read' values into a batch of tuples starting at 'tuple_mem'.
  bool ReadSlots(
      int64_t num_to_read, int tuple_size, uint8_t* RESTRICT tuple_mem) RESTRICT;

  /// Read 'num_to_read' values into a batch of tuples starting at 'tuple_mem', when
  /// conversion is needed.
  bool ReadAndConvertSlots(
      int64_t num_to_read, int tuple_size, uint8_t* RESTRICT tuple_mem) RESTRICT;

  /// Read 'num_to_read' values into a batch of tuples starting at 'tuple_mem', when
  /// conversion is not needed.
  bool ReadSlotsNoConversion(
      int64_t num_to_read, int tuple_size, uint8_t* RESTRICT tuple_mem) RESTRICT;

  /// Read 'num_to_read' position values into a batch of tuples starting at 'tuple_mem'.
  void ReadPositions(
      int64_t num_to_read, int tuple_size, uint8_t* RESTRICT tuple_mem) RESTRICT;
  void ReadItemPositions(
      int64_t num_to_read, int tuple_size, uint8_t* RESTRICT tuple_mem) RESTRICT;
  void ReadFilePositions(
      int64_t num_to_read, int tuple_size, uint8_t* RESTRICT tuple_mem) RESTRICT;

  /// Reads file position into 'file_pos' based on 'rep_level'.
  /// It updates 'current_row_' when 'rep_level' is 0.
  inline ALWAYS_INLINE void ReadFilePositionBatched(int16_t rep_level, int64_t* file_pos);

  virtual Status CreateDictionaryDecoder(
      uint8_t* values, int size, DictDecoderBase** decoder) override {
    DCHECK(slot_desc_->type().type != TYPE_BOOLEAN)
        << "Dictionary encoding is not supported for bools. Should never have gotten "
        << "this far.";
    if (!dict_decoder_.template Reset<PARQUET_TYPE>(values, size, fixed_len_size_)) {
      return Status(TErrorCode::PARQUET_CORRUPT_DICTIONARY, filename(),
          slot_desc_->type().DebugString(), "could not decode dictionary");
    }
    dict_decoder_init_ = true;
    *decoder = &dict_decoder_;
    return Status::OK();
  }

  virtual bool HasDictionaryDecoder() override {
    return dict_decoder_init_;
  }

  virtual void ClearDictionaryDecoder() override {
    dict_decoder_init_ = false;
  }

  virtual Status InitDataDecoder(uint8_t* data, int size) override;

  virtual bool SkipEncodedValuesInPage(int64_t num_values) override;

 private:
  /// Writes the next value into the appropriate destination slot in 'tuple'. Returns
  /// false if execution should be aborted for some reason, e.g. parse_error_ is set, the
  /// query is cancelled, or the scan node limit was reached. Otherwise returns true.
  ///
  /// Force inlining - GCC does not always inline this into hot loops.
  template <Encoding::type ENCODING, bool NEEDS_CONVERSION>
  inline ALWAYS_INLINE bool ReadSlot(Tuple* tuple);

  /// Decode one value from *data into 'val' and advance *data. 'data_end' is one byte
  /// past the end of the buffer. Return false and set 'parse_error_' if there is an
  /// error decoding the value.
  template <Encoding::type ENCODING>
  inline ALWAYS_INLINE bool DecodeValue(
      uint8_t** data, const uint8_t* data_end, InternalType* RESTRICT val) RESTRICT;

  /// Decode multiple values into 'out_vals' with a stride of 'stride' bytes. Return
  /// false and set 'parse_error_' if there is an error decoding any value.
  inline ALWAYS_INLINE bool DecodeValues(
      int64_t stride, int64_t count, InternalType* RESTRICT out_vals) RESTRICT;
  /// Specialisation of DecodeValues for a particular encoding, to allow overriding
  /// specific instances via template specialisation.
  template <Encoding::type ENCODING>
  inline ALWAYS_INLINE bool DecodeValues(
      int64_t stride, int64_t count, InternalType* RESTRICT out_vals) RESTRICT;

  /// Most column readers never require conversion, so we can avoid branches by
  /// returning constant false. Column readers for types that require conversion
  /// must specialize this function.
  inline bool NeedsConversionInline() const;

  /// Similar to NeedsConversion(), most column readers do not require validation,
  /// so to avoid branches, we return constant false. In general, types where not
  /// all possible bit representations of the data type are valid should be
  /// validated.
  inline bool NeedsValidationInline() const {
    return false;
  }

  /// Converts and writes 'src' into 'slot' based on desc_->type()
  bool ConvertSlot(const InternalType* src, void* slot) {
    return data_converter_.ConvertSlot(src, slot);
  }

  /// Checks if 'val' is invalid, e.g. due to being out of the valid value range. If it
  /// is invalid, logs the error and returns false. If the error should stop execution,
  /// sets 'parent_->parse_status_'.
  bool ValidateValue(InternalType* val) const {
    DCHECK(false);
    return false;
  }

  /// Pull out slow-path Status construction code
  void __attribute__((noinline)) SetDictDecodeError() {
    parent_->parse_status_ = Status(TErrorCode::PARQUET_DICT_DECODE_FAILURE, filename(),
        slot_desc_->type().DebugString(), col_chunk_reader_.stream()->file_offset());
  }

  void __attribute__((noinline)) SetPlainDecodeError() {
    parent_->parse_status_ = Status(TErrorCode::PARQUET_CORRUPT_PLAIN_VALUE, filename(),
        slot_desc_->type().DebugString(), col_chunk_reader_.stream()->file_offset());
  }

  void __attribute__((noinline)) SetBoolDecodeError() {
    parent_->parse_status_ = Status(TErrorCode::PARQUET_CORRUPT_BOOL_VALUE, filename(),
        PrintValue(page_encoding_), col_chunk_reader_.stream()->file_offset());
  }

  ParquetTimestampDecoder& GetTimestampDecoder() {
    return data_converter_.timestamp_decoder();
  }

  /// Dictionary decoder for decoding column values.
  DictDecoder<InternalType> dict_decoder_;

  /// True if dict_decoder_ has been initialized with a dictionary page.
  bool dict_decoder_init_ = false;

  /// The size of this column with plain encoding for FIXED_LEN_BYTE_ARRAY, or
  /// the max length for VARCHAR columns. Unused otherwise.
  int fixed_len_size_;

  /// Converts values if needed.
  ParquetDataConverter<InternalType, MATERIALIZED> data_converter_;

  /// Contains extra state required to decode boolean values. Only initialised for
  /// BOOLEAN columns.
  unique_ptr<ParquetBoolDecoder> bool_decoder_;

  /// Allocated from parent_->perm_pool_ if NeedsConversion() is true and null otherwise.
  uint8_t* conversion_buffer_ = nullptr;
};

template <typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
ScalarColumnReader<InternalType, PARQUET_TYPE, MATERIALIZED>::ScalarColumnReader(
    HdfsParquetScanner* parent, const SchemaNode& node, const SlotDescriptor* slot_desc)
  : BaseScalarColumnReader(parent, node, slot_desc),
    dict_decoder_(parent->scan_node_->mem_tracker()),
    data_converter_(node.element,
                    MATERIALIZED ? &slot_desc->type() : nullptr) {
  if (!MATERIALIZED) {
    // We're not materializing any values, just counting them. No need (or ability) to
    // initialize state used to materialize values.
    DCHECK(slot_desc_ == nullptr);
    return;
  }

  DCHECK(slot_desc_ != nullptr);
  if (slot_desc_->type().type == TYPE_DECIMAL
      && PARQUET_TYPE == parquet::Type::FIXED_LEN_BYTE_ARRAY) {
    fixed_len_size_ = node.element->type_length;
  } else if (slot_desc_->type().type == TYPE_VARCHAR) {
    fixed_len_size_ = slot_desc_->type().len;
  } else {
    fixed_len_size_ = -1;
  }

  if (slot_desc_->type().type == TYPE_TIMESTAMP) {
    data_converter_.SetTimestampDecoder(parent->CreateTimestampDecoder(*node.element));
    dict_decoder_.SetTimestampHelper(GetTimestampDecoder());
  }
  if (slot_desc_->type().type == TYPE_BOOLEAN) {
    bool_decoder_ = make_unique<ParquetBoolDecoder>();
  }
}

template <typename T>
struct IsDecimalValue {
  static constexpr bool value =
    std::is_same<T, Decimal4Value>::value ||
    std::is_same<T, Decimal8Value>::value ||
    std::is_same<T, Decimal16Value>::value;
};

template <typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
inline bool ScalarColumnReader<InternalType, PARQUET_TYPE, MATERIALIZED>
::NeedsConversionInline() const {
  //TODO: use constexpr ifs when we switch to C++17.
  if /* constexpr */ (MATERIALIZED) {
    if /* constexpr */ (IsDecimalValue<InternalType>::value) {
      return data_converter_.NeedsConversion();
    }
    if /* constexpr */ (std::is_same<InternalType, TimestampValue>::value) {
      return data_converter_.NeedsConversion();
    }
    if /* constexpr */ (std::is_same<InternalType, StringValue>::value &&
                        PARQUET_TYPE == parquet::Type::BYTE_ARRAY) {
      return data_converter_.NeedsConversion();
    }
  }
  DCHECK(!data_converter_.NeedsConversion());
  return false;
}

// TODO: consider performing filter selectivity checks in this function.
template <typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
Status ScalarColumnReader<InternalType, PARQUET_TYPE, MATERIALIZED>::InitDataDecoder(
    uint8_t* data, int size) {
  // Data can be empty if the column contains all NULLs
  DCHECK_GE(size, 0);
  DCHECK(slot_desc_ == nullptr || slot_desc_->type().type != TYPE_BOOLEAN)
      << "Bool has specialized impl";
  if (!IsDictionaryEncoding(page_encoding_)
      && page_encoding_ != parquet::Encoding::PLAIN) {
    return GetUnsupportedDecodingError();
  }

  // PLAIN_DICTIONARY is deprecated in Parquet V2. It means the same as RLE_DICTIONARY
  // so internally PLAIN_DICTIONARY can be used to represent both encodings.
  if (page_encoding_ == parquet::Encoding::RLE_DICTIONARY) {
    page_encoding_ = parquet::Encoding::PLAIN_DICTIONARY;
  }

  // If slot_desc_ is NULL, we don't need to decode any values so dict_decoder_ does
  // not need to be initialized.
  if (IsDictionaryEncoding(page_encoding_) && slot_desc_ != nullptr) {
    if (!dict_decoder_init_) {
      return Status("File corrupt. Missing dictionary page.");
    }
    RETURN_IF_ERROR(dict_decoder_.SetData(data, size));
  }
  // Allocate a temporary buffer to hold InternalType values if we need to convert
  // before writing to the final slot.
  if (NeedsConversionInline() && conversion_buffer_ == nullptr) {
    int64_t buffer_size = sizeof(InternalType) * parent_->state_->batch_size();
    conversion_buffer_ =
        parent_->perm_pool_->TryAllocateAligned(buffer_size, alignof(InternalType));
    if (conversion_buffer_ == nullptr) {
      return parent_->perm_pool_->mem_tracker()->MemLimitExceeded(parent_->state_,
          "Failed to allocate conversion buffer in Parquet scanner", buffer_size);
    }
  }
  return Status::OK();
}

template <>
Status ScalarColumnReader<bool, parquet::Type::BOOLEAN, true>::InitDataDecoder(
    uint8_t* data, int size) {
  // Data can be empty if the column contains all NULLs
  DCHECK_GE(size, 0);

  /// Boolean decoding is delegated to 'bool_decoder_'.
  if (bool_decoder_->SetData(page_encoding_, data, size)) return Status::OK();
  return GetUnsupportedDecodingError();
}

template <typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
bool ScalarColumnReader<InternalType, PARQUET_TYPE,
    MATERIALIZED>::SkipEncodedValuesInPage(int64_t num_values) {
  if (bool_decoder_) {
    return bool_decoder_->SkipValues(num_values);
  }
  if (IsDictionaryEncoding(page_encoding_)) {
    return dict_decoder_.SkipValues(num_values);
  } else {
    DCHECK_EQ(page_encoding_, Encoding::PLAIN);
    int64_t encoded_len = ParquetPlainEncoder::EncodedLen<PARQUET_TYPE>(
        data_, data_end_, fixed_len_size_, num_values);
    if (encoded_len < 0) return false;
    data_ += encoded_len;
  }
  return true;
}

template <typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
template <bool IN_COLLECTION>
bool ScalarColumnReader<InternalType, PARQUET_TYPE, MATERIALIZED>::ReadValue(
    Tuple* tuple) {
  // NextLevels() should have already been called and def and rep levels should be in
  // valid range.
  DCHECK_GE(rep_level_, 0);
  DCHECK_GE(def_level_, 0);
  DCHECK_GE(def_level_, def_level_of_immediate_repeated_ancestor()) <<
      "Caller should have called NextLevels() until we are ready to read a value";

  if (MATERIALIZED) {
    if (def_level_ >= max_def_level()) {
      bool continue_execution;
      if (IsDictionaryEncoding(page_encoding_)) {
        continue_execution = NeedsConversionInline() ?
            ReadSlot<Encoding::PLAIN_DICTIONARY, true>(tuple) :
            ReadSlot<Encoding::PLAIN_DICTIONARY, false>(tuple);
      } else {
        DCHECK_EQ(page_encoding_, Encoding::PLAIN);
        continue_execution = NeedsConversionInline() ?
            ReadSlot<Encoding::PLAIN, true>(tuple) :
            ReadSlot<Encoding::PLAIN, false>(tuple);
      }
      if (!continue_execution) return false;
    } else {
      SetNullSlot(tuple);
    }
  }
  return NextLevels<IN_COLLECTION>();
}

template <typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
template <bool IN_COLLECTION>
bool ScalarColumnReader<InternalType, PARQUET_TYPE, MATERIALIZED>::ReadValueBatch(
    int max_values, int tuple_size, uint8_t* RESTRICT tuple_mem,
    int* RESTRICT num_values) RESTRICT {
  // Repetition level is only present if this column is nested in a collection type.
  if (IN_COLLECTION) {
    DCHECK_GT(max_rep_level(), 0) << slot_desc()->DebugString();
  } else {
    DCHECK_EQ(max_rep_level(), 0) << slot_desc()->DebugString();
  }

  int val_count = 0;
  bool continue_execution = true;
  while (val_count < max_values && !RowGroupAtEnd() && continue_execution) {
    DCHECK_GE(num_buffered_values_, 0);
    // Read next page if necessary. It will skip values if necessary, so we can start
    // materializing the values right after.
    if (num_buffered_values_ == 0) {
      if (!NextPage()) {
        continue_execution = parent_->parse_status_.ok();
        continue;
      }
    }

    // Not materializing anything - skip decoding any levels and rely on the value
    // count from page metadata to return the correct number of rows.
    if (!MATERIALIZED && !IN_COLLECTION && file_pos_slot_desc() == nullptr) {
      // We cannot filter pages in this context.
      DCHECK(!DoesPageFiltering());
      int vals_to_add = min(num_buffered_values_, max_values - val_count);
      val_count += vals_to_add;
      num_buffered_values_ -= vals_to_add;
      DCHECK_GE(num_buffered_values_, 0);
      continue;
    }
    // Fill the rep level cache if needed. We are flattening out the fields of the
    // nested collection into the top-level tuple returned by the scan, so we don't
    // care about the nesting structure unless the position slot is being populated,
    // or we filter out rows.
    if (IN_COLLECTION && (AnyPosSlotToBeFilled() || DoesPageFiltering()) &&
        !rep_levels_.CacheHasNext()) {
      parent_->parse_status_.MergeStatus(
            rep_levels_.CacheNextBatch(num_buffered_values_));
      if (UNLIKELY(!parent_->parse_status_.ok())) return false;
    }

    const int remaining_val_capacity = max_values - val_count;
    uint8_t* next_tuple = tuple_mem + val_count * tuple_size;
    if (def_levels_.NextRepeatedRunLength() > 0) {
      // Fast path to materialize a run of values with the same definition level. This
      // avoids checking for NULL/not-NULL for every value.
      int ret_val_count = 0;
      continue_execution = MaterializeValueBatchRepeatedDefLevel(
          remaining_val_capacity, tuple_size, next_tuple, &ret_val_count);
      val_count += ret_val_count;
    } else {
      // We don't have a repeated run - cache def levels and process value-by-value.
      if (!def_levels_.CacheHasNext()) {
        parent_->parse_status_.MergeStatus(
            def_levels_.CacheNextBatch(num_buffered_values_));
        if (UNLIKELY(!parent_->parse_status_.ok())) return false;
      }

      // Read data page and cached levels to materialize values.
      int ret_val_count = 0;
      continue_execution = MaterializeValueBatch<IN_COLLECTION>(
          remaining_val_capacity, tuple_size, next_tuple, &ret_val_count);
      val_count += ret_val_count;
    }
    // Now that we have read some values, let's check whether we should skip some
    // due to page filtering.
    if (DoesPageFiltering() && ConsumedCurrentCandidateRange<IN_COLLECTION>()) {
      if (IsLastCandidateRange()) {
        *num_values = val_count;
        num_buffered_values_ = 0;
        return val_count > 0;
      }
      AdvanceCandidateRange();
      if (PageHasRemainingCandidateRows()) {
        if(!SkipRowsInPage()) return false;
      } else {
        if (!JumpToNextPage()) return false;
      }
    }
    if (SHOULD_TRIGGER_COL_READER_DEBUG_ACTION(val_count)) {
      continue_execution &= ColReaderDebugAction(&val_count);
    }
  }
  *num_values = val_count;
  return continue_execution;
}

template <typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
template <bool IN_COLLECTION, Encoding::type ENCODING, bool NEEDS_CONVERSION>
bool ScalarColumnReader<InternalType, PARQUET_TYPE, MATERIALIZED>::MaterializeValueBatch(
    int max_values, int tuple_size, uint8_t* RESTRICT tuple_mem,
    int* RESTRICT num_values) RESTRICT {
  DCHECK(MATERIALIZED || IN_COLLECTION || file_pos_slot_desc() != nullptr);
  DCHECK_GT(num_buffered_values_, 0);
  DCHECK(def_levels_.CacheHasNext());
  if (IN_COLLECTION && (AnyPosSlotToBeFilled() || DoesPageFiltering())) {
    DCHECK(rep_levels_.CacheHasNext());
  }
  int cache_start_idx = def_levels_.CacheCurrIdx();
  uint8_t* curr_tuple = tuple_mem;
  int val_count = 0;
  DCHECK_LE(def_levels_.CacheRemaining(), num_buffered_values_);
  max_values = min(max_values, num_buffered_values_);
  while (def_levels_.CacheHasNext() && val_count < max_values) {
    if (DoesPageFiltering()) {
      int peek_rep_level = IN_COLLECTION ? rep_levels_.PeekLevel() : 0;
      if (RowsRemainingInCandidateRange() == 0 && peek_rep_level == 0) break;
    }

    int rep_level = IN_COLLECTION ? rep_levels_.ReadLevel() : 0;
    if (rep_level == 0) ++current_row_;

    Tuple* tuple = reinterpret_cast<Tuple*>(curr_tuple);
    int def_level = def_levels_.CacheGetNext();

    if (IN_COLLECTION) {
      if (def_level < def_level_of_immediate_repeated_ancestor()) {
        // A containing repeated field is empty or NULL, skip the value.
        continue;
      }
      if (pos_slot_desc()) {
        ReadItemPositionBatched(rep_level,
            tuple->GetBigIntSlot(pos_slot_desc_->tuple_offset()));
      }
    } else if (file_pos_slot_desc()) {
      ReadFilePositionBatched(rep_level,
          tuple->GetBigIntSlot(file_pos_slot_desc_->tuple_offset()));
    }

    if (MATERIALIZED) {
      if (def_level >= max_def_level()) {
        bool continue_execution = ReadSlot<ENCODING, NEEDS_CONVERSION>(tuple);
        if (UNLIKELY(!continue_execution)) return false;
      } else {
        SetNullSlot(tuple);
      }
    }
    curr_tuple += tuple_size;
    ++val_count;
  }
  num_buffered_values_ -= (def_levels_.CacheCurrIdx() - cache_start_idx);
  DCHECK_GE(num_buffered_values_, 0);
  *num_values = val_count;
  return true;
}

// Note that the structure of this function is very similar to MaterializeValueBatch()
// above, except it is unrolled to operate on multiple values at a time.
template <typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
bool ScalarColumnReader<InternalType, PARQUET_TYPE,
    MATERIALIZED>::MaterializeValueBatchRepeatedDefLevel(int max_values, int tuple_size,
    uint8_t* RESTRICT tuple_mem, int* RESTRICT num_values) RESTRICT {
  DCHECK_GT(num_buffered_values_, 0);
  if (max_rep_level_ > 0 &&
      (AnyPosSlotToBeFilled() || DoesPageFiltering())) {
    DCHECK(rep_levels_.CacheHasNext());
  }
  int32_t def_level_repeats = def_levels_.NextRepeatedRunLength();
  DCHECK_GT(def_level_repeats, 0);
  // Peek at the def level. The number of def levels we'll consume depends on several
  // conditions below.
  uint8_t def_level = def_levels_.GetRepeatedValue(0);
  int32_t num_def_levels_to_consume = 0;

  // Find the upper limit of how many def levels we can consume.
  if (def_level < def_level_of_immediate_repeated_ancestor()) {
    DCHECK_GT(max_rep_level_, 0) << "Only possible if in a collection.";
    // A containing repeated field is empty or NULL. We don't need to return any values
    // but need to advance any rep levels.
    if (AnyPosSlotToBeFilled()) {
      num_def_levels_to_consume =
          min<uint32_t>(def_level_repeats, rep_levels_.CacheRemaining());
    } else {
      num_def_levels_to_consume = def_level_repeats;
    }
  } else {
    // Cannot consume more levels than allowed by buffered input values and output space.
    num_def_levels_to_consume = min(min(
        num_buffered_values_, max_values), def_level_repeats);
    if (max_rep_level_ > 0 && AnyPosSlotToBeFilled()) {
      num_def_levels_to_consume =
          min<uint32_t>(num_def_levels_to_consume, rep_levels_.CacheRemaining());
    }
  }
  // Page filtering can also put an upper limit on 'num_def_levels_to_consume'.
  if (DoesPageFiltering()) {
    int rows_remaining = RowsRemainingInCandidateRange();
    if (max_rep_level_ == 0) {
      num_def_levels_to_consume = min(num_def_levels_to_consume, rows_remaining);
      if (file_pos_slot_desc()) {
        ReadFilePositions(num_def_levels_to_consume, tuple_size, tuple_mem);
      } else {
        current_row_ += num_def_levels_to_consume;
      }
    } else {
      // We need to calculate how many 'primitive' values are there until the end
      // of the current candidate range. In the meantime we also fill the position
      // slots because we are consuming the repetition levels.
      num_def_levels_to_consume = FillPositionsInCandidateRange(rows_remaining,
          num_def_levels_to_consume, tuple_mem, tuple_size);
    }
  }
  // Now we have 'num_def_levels_to_consume' set, let's read the slots.
  if (def_level < def_level_of_immediate_repeated_ancestor()) {
    if (AnyPosSlotToBeFilled() && !DoesPageFiltering()) {
      rep_levels_.CacheSkipLevels(num_def_levels_to_consume);
    }
    *num_values = 0;
  } else {
    if (AnyPosSlotToBeFilled() && !DoesPageFiltering()) {
      ReadPositions(num_def_levels_to_consume, tuple_size, tuple_mem);
    }
    if (MATERIALIZED) {
      if (def_level >= max_def_level()) {
        if (!ReadSlots(num_def_levels_to_consume, tuple_size, tuple_mem)) {
          return false;
        }
      } else {
        Tuple::SetNullIndicators(
            null_indicator_offset_, num_def_levels_to_consume, tuple_size, tuple_mem);
      }
    }
    *num_values = num_def_levels_to_consume;
  }
  // We now know how many we actually consumed.
  def_levels_.GetRepeatedValue(num_def_levels_to_consume);
  num_buffered_values_ -= num_def_levels_to_consume;
  DCHECK_GE(num_buffered_values_, 0);
  return true;
}

template <typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
template <bool IN_COLLECTION>
bool ScalarColumnReader<InternalType, PARQUET_TYPE, MATERIALIZED>::MaterializeValueBatch(
    int max_values, int tuple_size, uint8_t* RESTRICT tuple_mem,
    int* RESTRICT num_values) RESTRICT {
  // Dispatch to the correct templated implementation of MaterializeValueBatch().
  if (IsDictionaryEncoding(page_encoding_)) {
    if (NeedsConversionInline()) {
      return MaterializeValueBatch<IN_COLLECTION, Encoding::PLAIN_DICTIONARY, true>(
          max_values, tuple_size, tuple_mem, num_values);
    } else {
      return MaterializeValueBatch<IN_COLLECTION, Encoding::PLAIN_DICTIONARY, false>(
          max_values, tuple_size, tuple_mem, num_values);
    }
  } else {
    DCHECK_EQ(page_encoding_, Encoding::PLAIN);
    if (NeedsConversionInline()) {
      return MaterializeValueBatch<IN_COLLECTION, Encoding::PLAIN, true>(
          max_values, tuple_size, tuple_mem, num_values);
    } else {
      return MaterializeValueBatch<IN_COLLECTION, Encoding::PLAIN, false>(
          max_values, tuple_size, tuple_mem, num_values);
    }
  }
}

template <typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
template <Encoding::type ENCODING, bool NEEDS_CONVERSION>
bool ScalarColumnReader<InternalType, PARQUET_TYPE, MATERIALIZED>::ReadSlot(
    Tuple* RESTRICT tuple) RESTRICT {
  void* slot = tuple->GetSlot(tuple_offset_);
  // Use an uninitialized stack allocation for temporary value to avoid running
  // constructors doing work unnecessarily, e.g. if T == StringValue.
  alignas(InternalType) uint8_t val_buf[sizeof(InternalType)];
  InternalType* val_ptr =
      reinterpret_cast<InternalType*>(NEEDS_CONVERSION ? val_buf : slot);

  if (UNLIKELY(!DecodeValue<ENCODING>(&data_, data_end_, val_ptr))) return false;
  if (UNLIKELY(NeedsValidationInline() && !ValidateValue(val_ptr)) ||
      (NEEDS_CONVERSION && UNLIKELY(!ConvertSlot(val_ptr, slot)))) {
    if (UNLIKELY(!parent_->parse_status_.ok())) return false;
    // The value is invalid but execution should continue - set the null indicator and
    // skip conversion.
    SetNullSlot(tuple);
    return true;
  }
  return true;
}

template <typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
bool ScalarColumnReader<InternalType, PARQUET_TYPE, MATERIALIZED>::ReadSlots(
    int64_t num_to_read, int tuple_size, uint8_t* RESTRICT tuple_mem) RESTRICT {
  if (NeedsConversionInline()) {
    return ReadAndConvertSlots(num_to_read, tuple_size, tuple_mem);
  } else {
    return ReadSlotsNoConversion(num_to_read, tuple_size, tuple_mem);
  }
}

template <typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
bool ScalarColumnReader<InternalType, PARQUET_TYPE, MATERIALIZED>::ReadAndConvertSlots(
    int64_t num_to_read, int tuple_size, uint8_t* RESTRICT tuple_mem) RESTRICT {
  DCHECK(NeedsConversionInline());
  DCHECK(conversion_buffer_ != nullptr);
  InternalType* first_val = reinterpret_cast<InternalType*>(conversion_buffer_);
  // Decode into the conversion buffer before doing the conversion into the output tuples.
  if (!DecodeValues(sizeof(InternalType), num_to_read, first_val)) return false;

  InternalType* curr_val = first_val;
  uint8_t* curr_tuple = tuple_mem;
  for (int64_t i = 0; i < num_to_read; ++i, ++curr_val, curr_tuple += tuple_size) {
    Tuple* tuple = reinterpret_cast<Tuple*>(curr_tuple);
    if ((NeedsValidationInline() && UNLIKELY(!ValidateValue(curr_val))) ||
        UNLIKELY(!ConvertSlot(curr_val, tuple->GetSlot(tuple_offset_)))) {
      if (UNLIKELY(!parent_->parse_status_.ok())) return false;
      // The value or the conversion is invalid but execution should continue - set the
      // null indicator.
      SetNullSlot(tuple);
      continue;
    }
  }
  return true;
}

template <typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
bool ScalarColumnReader<InternalType, PARQUET_TYPE, MATERIALIZED>::ReadSlotsNoConversion(
    int64_t num_to_read, int tuple_size, uint8_t* RESTRICT tuple_mem) RESTRICT {
  DCHECK(!NeedsConversionInline());
  // No conversion needed - decode directly into the output slots.
  InternalType* first_slot = reinterpret_cast<InternalType*>(tuple_mem + tuple_offset_);
  if (!DecodeValues(tuple_size, num_to_read, first_slot)) return false;
  if (NeedsValidationInline()) {
    // Validate the written slots.
    uint8_t* curr_tuple = tuple_mem;
    for (int64_t i = 0; i < num_to_read; ++i, curr_tuple += tuple_size) {
      Tuple* tuple = reinterpret_cast<Tuple*>(curr_tuple);
      InternalType* val = static_cast<InternalType*>(tuple->GetSlot(tuple_offset_));
      if (UNLIKELY(!ValidateValue(val))) {
        if (UNLIKELY(!parent_->parse_status_.ok())) return false;
        // The value is invalid but execution should continue - set the null indicator and
        // skip conversion.
        SetNullSlot(tuple);
      }
    }
  }
  return true;
}

template <typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
template <Encoding::type ENCODING>
bool ScalarColumnReader<InternalType, PARQUET_TYPE, MATERIALIZED>::DecodeValue(
    uint8_t** RESTRICT data, const uint8_t* RESTRICT data_end,
    InternalType* RESTRICT val) RESTRICT {
  DCHECK_EQ(page_encoding_, ENCODING);
  if (IsDictionaryEncoding(ENCODING)) {
    if (UNLIKELY(!dict_decoder_.GetNextValue(val))) {
      SetDictDecodeError();
      return false;
    }
  } else {
    DCHECK_EQ(ENCODING, Encoding::PLAIN);
    int encoded_len = ParquetPlainEncoder::Decode<InternalType, PARQUET_TYPE>(
        *data, data_end, fixed_len_size_, val);
    if (UNLIKELY(encoded_len < 0)) {
      SetPlainDecodeError();
      return false;
    }
    *data += encoded_len;
  }
  return true;
}

// Specialise for decoding INT64 timestamps from PLAIN decoding, which need to call
// out to the timestamp decoder.
template <>
template <>
bool ScalarColumnReader<TimestampValue, parquet::Type::INT64,
    true>::DecodeValue<Encoding::PLAIN>(uint8_t** RESTRICT data,
    const uint8_t* RESTRICT data_end, TimestampValue* RESTRICT val) RESTRICT {
  DCHECK_EQ(page_encoding_, Encoding::PLAIN);
  int encoded_len =
      GetTimestampDecoder().Decode<parquet::Type::INT64>(*data, data_end, val);
  if (UNLIKELY(encoded_len < 0)) {
    SetPlainDecodeError();
    return false;
  }
  *data += encoded_len;
  return true;
}

template <>
template <Encoding::type ENCODING>
bool ScalarColumnReader<bool, parquet::Type::BOOLEAN, true>::DecodeValue(
    uint8_t** RESTRICT data, const uint8_t* RESTRICT data_end,
    bool* RESTRICT value) RESTRICT {
  if (UNLIKELY(!bool_decoder_->DecodeValue<ENCODING>(value))) {
    SetBoolDecodeError();
    return false;
  }
  return true;
}

template <typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
bool ScalarColumnReader<InternalType, PARQUET_TYPE, MATERIALIZED>::DecodeValues(
    int64_t stride, int64_t count, InternalType* RESTRICT out_vals) RESTRICT {
  if (IsDictionaryEncoding(page_encoding_)) {
    return DecodeValues<Encoding::PLAIN_DICTIONARY>(stride, count, out_vals);
  } else {
    DCHECK_EQ(page_encoding_, Encoding::PLAIN);
    return DecodeValues<Encoding::PLAIN>(stride, count, out_vals);
  }
}

template <typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
template <Encoding::type ENCODING>
bool ScalarColumnReader<InternalType, PARQUET_TYPE, MATERIALIZED>::DecodeValues(
    int64_t stride, int64_t count, InternalType* RESTRICT out_vals) RESTRICT {
  if (IsDictionaryEncoding(page_encoding_)) {
    if (UNLIKELY(!dict_decoder_.GetNextValues(out_vals, stride, count))) {
      SetDictDecodeError();
      return false;
    }
  } else {
    DCHECK_EQ(page_encoding_, Encoding::PLAIN);
    int64_t encoded_len = ParquetPlainEncoder::DecodeBatch<InternalType, PARQUET_TYPE>(
        data_, data_end_, fixed_len_size_, count, stride, out_vals);
    if (UNLIKELY(encoded_len < 0)) {
      SetPlainDecodeError();
      return false;
    }
    data_ += encoded_len;
  }
  return true;
}

// Specialise for decoding INT64 timestamps from PLAIN decoding, which need to call
// out to the timestamp decoder.
template <>
template <>
bool ScalarColumnReader<TimestampValue, parquet::Type::INT64,
    true>::DecodeValues<Encoding::PLAIN>(int64_t stride, int64_t count,
    TimestampValue* RESTRICT out_vals) RESTRICT {
  DCHECK_EQ(page_encoding_, Encoding::PLAIN);
  int64_t encoded_len = GetTimestampDecoder().DecodeBatch<parquet::Type::INT64>(
      data_, data_end_, count, stride, out_vals);
  if (UNLIKELY(encoded_len < 0)) {
    SetPlainDecodeError();
    return false;
  }
  data_ += encoded_len;
  return true;
}

template <>
bool ScalarColumnReader<bool, parquet::Type::BOOLEAN, true>::DecodeValues(
    int64_t stride, int64_t count, bool* RESTRICT out_vals) RESTRICT {
  if (!bool_decoder_->DecodeValues(stride, count, out_vals)) {
    SetBoolDecodeError();
    return false;
  }
  return true;
}

template <typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
void ScalarColumnReader<InternalType, PARQUET_TYPE, MATERIALIZED>::
    ReadFilePositionBatched(int16_t rep_level, int64_t* file_pos) {
  *file_pos = FilePositionOfCurrentRow();
}

template <typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
void ScalarColumnReader<InternalType, PARQUET_TYPE, MATERIALIZED>::ReadPositions(
    int64_t num_to_read, int tuple_size, uint8_t* RESTRICT tuple_mem) RESTRICT {
  DCHECK(file_pos_slot_desc() != nullptr || pos_slot_desc() != nullptr);
  DCHECK(file_pos_slot_desc() == nullptr || pos_slot_desc() == nullptr);
  if (file_pos_slot_desc()) {
    ReadFilePositions(num_to_read, tuple_size, tuple_mem);
  } else {
    ReadItemPositions(num_to_read, tuple_size, tuple_mem);
  }
}

template <typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
void ScalarColumnReader<InternalType, PARQUET_TYPE, MATERIALIZED>::ReadFilePositions(
    int64_t num_to_read, int tuple_size, uint8_t* RESTRICT tuple_mem) RESTRICT {
  DCHECK(file_pos_slot_desc() != nullptr);
  DCHECK(pos_slot_desc() == nullptr);
  int64_t* file_pos_slot = reinterpret_cast<Tuple*>(tuple_mem)->GetBigIntSlot(
      file_pos_slot_desc()->tuple_offset());
  StrideWriter<int64_t> file_out{reinterpret_cast<int64_t*>(file_pos_slot), tuple_size};
  for (int64_t i = 0; i < num_to_read; ++i) {
    int64_t* file_pos = file_out.Advance();
    uint8_t rep_level = max_rep_level() > 0 ? rep_levels_.CacheGetNext() : 0;
    if (rep_level == 0) ++current_row_;
    ReadFilePositionBatched(rep_level, file_pos);
  }
}

template <typename InternalType, parquet::Type::type PARQUET_TYPE, bool MATERIALIZED>
void ScalarColumnReader<InternalType, PARQUET_TYPE, MATERIALIZED>::ReadItemPositions(
    int64_t num_to_read, int tuple_size, uint8_t* RESTRICT tuple_mem) RESTRICT {
  DCHECK(file_pos_slot_desc() == nullptr);
  DCHECK(pos_slot_desc() != nullptr);
  int64_t* pos_slot = reinterpret_cast<Tuple*>(tuple_mem)->GetBigIntSlot(
      pos_slot_desc()->tuple_offset());
  StrideWriter<int64_t> out{reinterpret_cast<int64_t*>(pos_slot), tuple_size};
  for (int64_t i = 0; i < num_to_read; ++i) {
    int64_t* pos = out.Advance();
    uint8_t rep_level = max_rep_level() > 0 ? rep_levels_.CacheGetNext() : 0;
    if (rep_level == 0) ++current_row_;
    ReadItemPositionBatched(rep_level, pos);
  }
}

template <>
inline bool ScalarColumnReader<TimestampValue, parquet::Type::INT96, true>
::NeedsValidationInline() const {
  return true;
}

template <>
inline bool ScalarColumnReader<TimestampValue, parquet::Type::INT64, true>
::NeedsValidationInline() const {
  return true;
}

template <>
bool ScalarColumnReader<TimestampValue, parquet::Type::INT96, true>::ValidateValue(
    TimestampValue* val) const {
  if (UNLIKELY(!TimestampValue::IsValidDate(val->date())
      || !TimestampValue::IsValidTime(val->time()))) {
    // If both are corrupt, invalid time takes precedence over invalid date, because
    // invalid date may come from a more or less functional encoder that does not respect
    // the 1400..9999 limit, while an invalid time is a good indicator of buggy encoder
    // or memory garbage.
    TErrorCode::type errorCode = TimestampValue::IsValidTime(val->time())
        ? TErrorCode::PARQUET_TIMESTAMP_OUT_OF_RANGE
        : TErrorCode::PARQUET_TIMESTAMP_INVALID_TIME_OF_DAY;
    ErrorMsg msg(errorCode, filename(), node_.element->name);
    Status status = parent_->state_->LogOrReturnError(msg);
    if (!status.ok()) parent_->parse_status_ = status;
    return false;
  }
  return true;
}

template <>
bool ScalarColumnReader<TimestampValue, parquet::Type::INT64, true>::ValidateValue(
    TimestampValue* val) const {
  // The range was already checked during the int64_t->TimestampValue conversion, which
  // sets the date to invalid if it was out of range.
  if (UNLIKELY(!val->HasDate())) {
    ErrorMsg msg(TErrorCode::PARQUET_TIMESTAMP_OUT_OF_RANGE,
        filename(), node_.element->name);
    Status status = parent_->state_->LogOrReturnError(msg);
    if (!status.ok()) parent_->parse_status_ = status;
    return false;
  }
  DCHECK(TimestampValue::IsValidDate(val->date()));
  DCHECK(TimestampValue::IsValidTime(val->time()));
  return true;
}

template <>
inline bool ScalarColumnReader<DateValue, parquet::Type::INT32, true>
::NeedsValidationInline() const {
  return true;
}

template <>
bool ScalarColumnReader<DateValue, parquet::Type::INT32, true>::ValidateValue(
    DateValue* val) const {
  // The range was already checked during the int32_t->DateValue conversion, which
  // sets the date to invalid if it was out of range.
  if (UNLIKELY(!val->IsValid())) {
    ErrorMsg msg(TErrorCode::PARQUET_DATE_OUT_OF_RANGE,
        filename(), node_.element->name);
    Status status = parent_->state_->LogOrReturnError(msg);
    if (!status.ok()) parent_->parse_status_ = status;
    return false;
  }
  return true;
}

void BaseScalarColumnReader::CreateSubRanges(vector<ScanRange::SubRange>* sub_ranges) {
  sub_ranges->clear();
  if (!DoesPageFiltering()) return;
  int64_t data_start = metadata_->data_page_offset;
  int64_t data_start_based_on_offset_index = offset_index_.page_locations[0].offset;
  if (metadata_->__isset.dictionary_page_offset) {
    int64_t dict_start = metadata_->dictionary_page_offset;
    // This assumes that the first data page is coming right after the dictionary page
    sub_ranges->push_back( { dict_start, data_start - dict_start });
  } else if (data_start < data_start_based_on_offset_index) {
    // 'dictionary_page_offset' is not set, but the offset index and
    // column chunk metadata disagree on the data start => column chunk's data start
    // is actually the location of the dictionary page. Parquet-MR (at least
    // version 1.10 and earlier versions) writes Parquet files like that.
    int64_t dict_start = data_start;
    sub_ranges->push_back({dict_start, data_start_based_on_offset_index - dict_start});
  }
  for (int candidate_page_idx : candidate_data_pages_) {
    auto page_loc = offset_index_.page_locations[candidate_page_idx];
    sub_ranges->push_back( { page_loc.offset, page_loc.compressed_page_size });
  }
}

Status BaseScalarColumnReader::Reset(const HdfsFileDesc& file_desc,
    const parquet::ColumnChunk& col_chunk, int row_group_idx,
    int64_t row_group_first_row) {
  // Ensure metadata is valid before using it to initialize the reader.
  RETURN_IF_ERROR(ParquetMetadataUtils::ValidateRowGroupColumn(parent_->file_metadata_,
      parent_->filename(), row_group_idx, col_idx(), schema_element(),
      parent_->state_));
  num_buffered_values_ = 0;

  data_ = nullptr;
  data_end_ = nullptr;
  metadata_ = &col_chunk.meta_data;
  num_values_read_ = 0;
  def_level_ = ParquetLevel::INVALID_LEVEL;
  // See ColumnReader constructor.
  rep_level_ = max_rep_level() == 0 ? 0 : ParquetLevel::INVALID_LEVEL;
  pos_current_value_ = ParquetLevel::INVALID_POS;
  row_group_first_row_ = row_group_first_row;
  current_row_ = -1;

  vector<ScanRange::SubRange> sub_ranges;
  CreateSubRanges(&sub_ranges);

  RETURN_IF_ERROR(col_chunk_reader_.InitColumnChunk(
      file_desc, col_chunk, row_group_idx, move(sub_ranges)));

  ClearDictionaryDecoder();
  return Status::OK();
}

void BaseScalarColumnReader::Close(RowBatch* row_batch) {
  col_chunk_reader_.Close(row_batch == nullptr ? nullptr : row_batch->tuple_data_pool());
  DictDecoderBase* dict_decoder = GetDictionaryDecoder();
  if (dict_decoder != nullptr) dict_decoder->Close();
}

Status BaseScalarColumnReader::InitDictionary() {
  // Dictionary encoding is not supported for booleans.
  const bool is_boolean = node_.element->type == parquet::Type::BOOLEAN;
  const bool skip_data = slot_desc_ == nullptr || is_boolean;

  // TODO: maybe avoid malloc on every page?
  ScopedBuffer uncompressed_buffer(parent_->dictionary_pool_->mem_tracker());
  bool eos;
  bool is_dictionary_page;
  int64_t data_size;
  int num_entries;
  RETURN_IF_ERROR(col_chunk_reader_.TryReadDictionaryPage(&is_dictionary_page, &eos,
        skip_data, &uncompressed_buffer, &data_, &data_size, &num_entries));
  if (eos) return HandleTooEarlyEos();
  if (is_dictionary_page && is_boolean) {
    return Status("Unexpected dictionary page. Dictionary page is not"
        " supported for booleans.");
  }
  if (!is_dictionary_page || skip_data) return Status::OK();

  // The size of dictionary can be 0, if every value is null. The dictionary still has to
  // be reset in this case.
  DictDecoderBase* dict_decoder;
  if (data_size == 0) {
    data_end_ = data_;
    return CreateDictionaryDecoder(nullptr, 0, &dict_decoder);
  }
  // We cannot add data_size to data_ until we know it is not a nullptr.
  if (data_ == nullptr) {
    return Status("The dictionary values could not be read properly.");
  }
  data_end_ = data_ + data_size;

  RETURN_IF_ERROR(CreateDictionaryDecoder(data_, data_size, &dict_decoder));
  if (num_entries != dict_decoder->num_entries()) {
    return Status(TErrorCode::PARQUET_CORRUPT_DICTIONARY, filename(),
        slot_desc_->type().DebugString(),
        Substitute("Expected $0 entries but data contained $1 entries",
          num_entries, dict_decoder->num_entries()));
  }
  return Status::OK();
}

Status BaseScalarColumnReader::InitDictionaries(
    const vector<BaseScalarColumnReader*> readers) {
  for (BaseScalarColumnReader* reader : readers) {
    RETURN_IF_ERROR(reader->InitDictionary());
  }
  return Status::OK();
}

Status BaseScalarColumnReader::ReadDataPage() {
  // We're about to move to the next data page. The previous data page is
  // now complete, free up any memory allocated for it. If the data page contained
  // strings we need to attach it to the returned batch.
  col_chunk_reader_.ReleaseResourcesOfLastPage(parent_->scratch_batch_->aux_mem_pool);

  DCHECK_EQ(num_buffered_values_, 0);
  if ((DoesPageFiltering() &&
        candidate_page_idx_ == candidate_data_pages_.size() - 1) ||
      num_values_read_ == metadata_->num_values) {
    // No more pages to read
    // TODO: should we check for stream_->eosr()?
    return Status::OK();
  } else if (num_values_read_ > metadata_->num_values) {
    RETURN_IF_ERROR(LogCorruptNumValuesInMetadataError());
    return Status::OK();
  }

  // Read the next header, return if not found.
  RETURN_IF_ERROR(col_chunk_reader_.ReadNextDataPageHeader(&num_buffered_values_));
  if (num_buffered_values_ == 0) return HandleTooEarlyEos();
  DCHECK_GT(num_buffered_values_, 0);

  // Read the data in the data page.
  ParquetColumnChunkReader::DataPageInfo page;
  RETURN_IF_ERROR(col_chunk_reader_.ReadDataPageData(&page));
  DCHECK(page.is_valid);

  num_values_read_ += num_buffered_values_;

  RETURN_IF_ERROR(InitDataPageDecoders(page));

  // Skip rows if needed.
  RETURN_IF_ERROR(StartPageFiltering());

  if (parent_->candidate_ranges_.empty()) COUNTER_ADD(parent_->num_pages_counter_, 1);
  return Status::OK();
}

Status BaseScalarColumnReader::ReadNextDataPageHeader() {
  // We're about to move to the next data page. The previous data page is
  // now complete, free up any memory allocated for it. If the data page contained
  // strings we need to attach it to the returned batch.
  col_chunk_reader_.ReleaseResourcesOfLastPage(parent_->scratch_batch_->aux_mem_pool);

  DCHECK_EQ(num_buffered_values_, 0);
  if ((DoesPageFiltering() && candidate_page_idx_ == candidate_data_pages_.size() - 1)
      || num_values_read_ == metadata_->num_values) {
    // No more pages to read
    // TODO: should we check for stream_->eosr()?
    return Status::OK();
  } else if (num_values_read_ > metadata_->num_values) {
    RETURN_IF_ERROR(LogCorruptNumValuesInMetadataError());
    return Status::OK();
  }

  RETURN_IF_ERROR(col_chunk_reader_.ReadNextDataPageHeader(&num_buffered_values_));
  if (num_buffered_values_ == 0) return HandleTooEarlyEos();
  DCHECK_GT(num_buffered_values_, 0);

  num_values_read_ += num_buffered_values_;
  if (parent_->candidate_ranges_.empty()) COUNTER_ADD(parent_->num_pages_counter_, 1);
  return Status::OK();
}

Status BaseScalarColumnReader::ReadCurrentDataPage() {
  ParquetColumnChunkReader::DataPageInfo page;
  RETURN_IF_ERROR(col_chunk_reader_.ReadDataPageData(&page));
  DCHECK(page.is_valid);

  RETURN_IF_ERROR(InitDataPageDecoders(page));

  // Skip rows if needed.
  RETURN_IF_ERROR(StartPageFiltering());
  return Status::OK();
}

Status BaseScalarColumnReader::InitDataPageDecoders(
    const ParquetColumnChunkReader::DataPageInfo& page) {
  // Initialize the repetition level data
  DCHECK(page.rep_level_encoding == Encoding::RLE || page.rep_level_size == 0 );
  RETURN_IF_ERROR(rep_levels_.Init(filename(),
      parent_->perm_pool_.get(), parent_->state_->batch_size(), max_rep_level(),
      page.rep_level_ptr, page.rep_level_size));

  // Initialize the definition level data
  DCHECK(page.def_level_encoding == Encoding::RLE || page.def_level_size == 0 );
  RETURN_IF_ERROR(def_levels_.Init(filename(),
      parent_->perm_pool_.get(), parent_->state_->batch_size(), max_def_level(),
      page.def_level_ptr, page.def_level_size));

  page_encoding_ = page.data_encoding;
  data_ = page.data_ptr;
  data_end_ = data_ + page.data_size;
  // Data can be empty if the column contains all NULLs
  RETURN_IF_ERROR(InitDataDecoder(page.data_ptr, page.data_size));
  return Status::OK();
}

template <bool ADVANCE_REP_LEVEL>
bool BaseScalarColumnReader::NextLevels() {
  if (!ADVANCE_REP_LEVEL) DCHECK_EQ(max_rep_level(), 0) << slot_desc()->DebugString();

  levels_readahead_ = true;
  if (UNLIKELY(num_buffered_values_ == 0)) {
    if (!NextPage()) return parent_->parse_status_.ok();
  }
  if (DoesPageFiltering() && RowsRemainingInCandidateRange() == 0) {
    if (!ADVANCE_REP_LEVEL || max_rep_level() == 0 || rep_levels_.PeekLevel() == 0) {
      if (!IsLastCandidateRange()) AdvanceCandidateRange();
      if (PageHasRemainingCandidateRows()) {
        auto current_range = parent_->candidate_ranges_[current_row_range_];
        int64_t skip_rows = current_range.first - current_row_ - 1;
        DCHECK_GE(skip_rows, 0);
        int64_t remaining = 0;
        if (!SkipTopLevelRows(skip_rows, &remaining)) return false;
        DCHECK_EQ(remaining, 0);
      } else {
        if (!JumpToNextPage()) return parent_->parse_status_.ok();
      }
    }
  }

  --num_buffered_values_;
  DCHECK_GE(num_buffered_values_, 0);

  // Definition level is not present if column and any containing structs are required.
  def_level_ = max_def_level() == 0 ? 0 : def_levels_.ReadLevel();
  // The compiler can optimize these two conditions into a single branch by treating
  // def_level_ as unsigned.
  if (UNLIKELY(def_level_ < 0 || def_level_ > max_def_level())) {
    SetLevelDecodeError("def", def_level_, max_def_level());
    return false;
  }

  if (ADVANCE_REP_LEVEL && max_rep_level() > 0) {
    // Repetition level is only present if this column is nested in any collection type.
    rep_level_ = rep_levels_.ReadLevel();
    if (UNLIKELY(rep_level_ < 0 || rep_level_ > max_rep_level())) {
      SetLevelDecodeError("rep", rep_level_, max_rep_level());
      return false;
    }
    // Reset position counter if we are at the start of a new parent collection.
    if (rep_level_ <= max_rep_level() - 1) pos_current_value_ = 0;
    if (rep_level_ == 0) ++current_row_;
  } else {
    ++current_row_;
  }

  return parent_->parse_status_.ok();
}

void BaseScalarColumnReader::ResetPageFiltering() {
  offset_index_.page_locations.clear();
  candidate_data_pages_.clear();
  candidate_page_idx_ = -1;
  current_row_ = -1;
  levels_readahead_ = false;
  current_row_range_ = 0;
}

Status BaseScalarColumnReader::StartPageFiltering() {
  if (!DoesPageFiltering()) return Status::OK();
  ++candidate_page_idx_;
  current_row_ = FirstRowIdxInCurrentPage() - 1;
  // Move to the next candidate range.
  auto& candidate_row_ranges = parent_->candidate_ranges_;
  while (current_row_ >= candidate_row_ranges[current_row_range_].last) {
    DCHECK_LT(current_row_range_, candidate_row_ranges.size() - 1);
    ++current_row_range_;
  }
  int64_t range_start = candidate_row_ranges[current_row_range_].first;
  if (range_start > current_row_ + 1) {
    int64_t skip_rows = range_start - current_row_ - 1;
    int64_t remaining = 0;
    if (!SkipTopLevelRows(skip_rows, &remaining)) {
      return Status(ErrorMsg(TErrorCode::PARQUET_ROWS_SKIPPING,
          schema_element().name, filename()));
    }
    DCHECK_EQ(remaining, 0);
    DCHECK_EQ(current_row_, range_start - 1);
  }
  return Status::OK();
}

template <bool MULTI_PAGE>
bool BaseScalarColumnReader::SkipTopLevelRows(int64_t num_rows, int64_t* remaining) {
  DCHECK_GT(num_rows, 0);
  DCHECK_GT(num_buffered_values_, 0);
  if (!MULTI_PAGE) {
    DCHECK_GE(num_buffered_values_, num_rows);
  }
  // Fastest path: field is required and not nested.
  // So row count equals value count, and every value is stored in the page data.
  if (max_def_level() == 0 && max_rep_level() == 0) {
    int rows_skipped;
    if (MULTI_PAGE) {
      rows_skipped = std::min((int64_t)num_buffered_values_, num_rows);
    } else {
      rows_skipped = num_rows;
    }
    current_row_ += rows_skipped;
    num_buffered_values_ -= rows_skipped;
    *remaining = num_rows - rows_skipped;
    return SkipEncodedValuesInPage(rows_skipped);
  }
  int64_t num_values_to_skip = 0;
  if (max_rep_level() == 0) {
    // No nesting, but field is not required.
    // Skip as many values in the page data as many non-NULL values encountered.
    int i = 0;
    while (i < num_rows && num_buffered_values_ > 0) {
      int repeated_run_length = def_levels_.NextRepeatedRunLength();
      if (repeated_run_length > 0) {
        int read_count = min<int64_t>(num_rows - i, repeated_run_length);
        // IMPALA-11134: there can be a mismatch between page_header.num_values and
        // encoded def levels in old Parquet files.
        read_count = min(num_buffered_values_, read_count);
        int16_t def_level = def_levels_.GetRepeatedValue(read_count);
        if (def_level >= max_def_level_) num_values_to_skip += read_count;
        i += read_count;
        num_buffered_values_ -= read_count;
      } else if (def_levels_.CacheHasNext()) {
        int read_count = min<int64_t>(num_rows - i, def_levels_.CacheRemaining());
        // IMPALA-11134: there can be a mismatch between page_header.num_values and
        // encoded def levels in old Parquet files.
        read_count = min(num_buffered_values_, read_count);
        for (int j = 0; j < read_count; ++j) {
          if (def_levels_.CacheGetNext() >= max_def_level_) ++num_values_to_skip;
        }
        i += read_count;
        num_buffered_values_ -= read_count;
      } else {
        if (!def_levels_.CacheNextBatch(num_buffered_values_).ok()) return false;
      }
    }
    DCHECK_LE(i, num_rows);
    current_row_ += i;
    *remaining = num_rows - i;
  } else {
    // 'rep_level_' being zero denotes the start of a new top-level row.
    // From the 'def_level_' we can determine the number of non-NULL values.
    while (num_buffered_values_ > 0) {
      if (!def_levels_.CacheNextBatchIfEmpty(num_buffered_values_).ok()) return false;
      if (!rep_levels_.CacheNextBatchIfEmpty(num_buffered_values_).ok()) return false;
      if (num_rows == 0 && rep_levels_.PeekLevel() == 0) {
        // No more rows to skip, and the next value belongs to a new top-level row.
        break;
      }
      def_level_ = def_levels_.CacheGetNext();
      rep_level_ = rep_levels_.CacheGetNext();
      --num_buffered_values_;
      DCHECK_GE(num_buffered_values_, 0);
      if (def_level_ >= max_def_level()) ++num_values_to_skip;
      if (rep_level_ == 0) {
        ++current_row_;
        --num_rows;
      }
    }
    *remaining = num_rows;
  }
  return SkipEncodedValuesInPage(num_values_to_skip);
}

bool BaseScalarColumnReader::SetRowGroupAtEnd() {
  if (RowGroupAtEnd()) {
    return true;
  }
  if (num_buffered_values_ == 0) {
    NextPage();
  }
  if (DoesPageFiltering() && RowsRemainingInCandidateRange() == 0) {
    if (max_rep_level() == 0 || rep_levels_.PeekLevel() == 0) {
      if (!IsLastCandidateRange()) AdvanceCandidateRange();
      if (!PageHasRemainingCandidateRows()) {
        JumpToNextPage();
      }
    }
  }
  bool status = RowGroupAtEnd();
  if (!status) {
    return false;
  }
  return parent_->parse_status_.ok();
}

int BaseScalarColumnReader::FillPositionsInCandidateRange(int rows_remaining,
    int max_values, uint8_t* RESTRICT tuple_mem, int tuple_size) {
  DCHECK_GT(max_rep_level_, 0);
  DCHECK_EQ(rows_remaining, RowsRemainingInCandidateRange());
  int row_count = 0;
  int val_count = 0;
  int64_t *pos_slot = nullptr;
  if (pos_slot_desc_ != nullptr) {
    const int pos_slot_offset = pos_slot_desc()->tuple_offset();
    pos_slot = reinterpret_cast<Tuple*>(tuple_mem)->GetBigIntSlot(pos_slot_offset);
  }
  StrideWriter<int64_t> pos_writer{pos_slot, tuple_size};

  int64_t *file_pos_slot = nullptr;
  if (file_pos_slot_desc_ != nullptr) {
    const int file_pos_slot_offset = file_pos_slot_desc()->tuple_offset();
    file_pos_slot = reinterpret_cast<Tuple*>(tuple_mem)->GetBigIntSlot(
        file_pos_slot_offset);
  }
  StrideWriter<int64_t> file_pos_writer{file_pos_slot, tuple_size};
  while (rep_levels_.CacheRemaining() && row_count <= rows_remaining &&
         val_count < max_values) {
    if (row_count == rows_remaining && rep_levels_.CachePeekNext() == 0) break;
    int rep_level = rep_levels_.CacheGetNext();
    if (rep_level == 0) {
      ++row_count;
      ++current_row_;
    }
    ++val_count;
    if (file_pos_writer.IsValid()) {
      *file_pos_writer.Advance() = FilePositionOfCurrentRow();
    } else if (pos_writer.IsValid()) {
      if (rep_level <= max_rep_level() - 1) pos_current_value_ = 0;
      *pos_writer.Advance() = pos_current_value_++;
    }
  }
  return val_count;
}

void BaseScalarColumnReader::AdvanceCandidateRange() {
  DCHECK(DoesPageFiltering());
  auto& candidate_ranges = parent_->candidate_ranges_;
  DCHECK_LT(current_row_range_, candidate_ranges.size());
  DCHECK_EQ(current_row_, candidate_ranges[current_row_range_].last);
  ++current_row_range_;
  DCHECK_LE(current_row_, candidate_ranges[current_row_range_].last);
}

bool BaseScalarColumnReader::PageHasRemainingCandidateRows() const {
  DCHECK(DoesPageFiltering());
  DCHECK_LT(current_row_range_, parent_->candidate_ranges_.size());
  auto current_range = parent_->candidate_ranges_[current_row_range_];
  if (candidate_page_idx_ != candidate_data_pages_.size() - 1) {
    auto& next_page_loc =
        offset_index_.page_locations[candidate_data_pages_[candidate_page_idx_+1]];
    // If the next page contains rows with index higher than the start of the
    // current candidate range, it means we still have interesting rows in the
    // current page.
    return next_page_loc.first_row_index > current_range.first;
  }
  if (candidate_page_idx_ == candidate_data_pages_.size() - 1) {
    // We are in the last page, we need to skip rows if the current top level row
    // precedes the next candidate range.
    return current_row_ < current_range.first;
  }
  return false;
}

bool BaseScalarColumnReader::SkipRowsInPage() {
  auto current_range = parent_->candidate_ranges_[current_row_range_];
  DCHECK_LT(current_row_, current_range.first);
  int64_t skip_rows = current_range.first - current_row_ - 1;
  DCHECK_GE(skip_rows, 0);
  int64_t remaining = 0;
  return SkipTopLevelRows(skip_rows, &remaining);
}

bool BaseScalarColumnReader::JumpToNextPage() {
  DCHECK(DoesPageFiltering());
  num_buffered_values_ = 0;
  return NextPage();
}

Status BaseScalarColumnReader::GetUnsupportedDecodingError() {
  return Status(Substitute(
      "File '$0' is corrupt: unexpected encoding: $1 for data page of column '$2'.",
      filename(), PrintValue(page_encoding_), schema_element().name));
}

Status BaseScalarColumnReader::LogCorruptNumValuesInMetadataError() {
  ErrorMsg msg(TErrorCode::PARQUET_COLUMN_METADATA_INVALID,
      metadata_->num_values, num_values_read_, node_.element->name, filename());
  return parent_->state_->LogOrReturnError(msg);
}

Status BaseScalarColumnReader::HandleTooEarlyEos() {
  // The data pages contain fewer values than stated in the column metadata.
  DCHECK(col_chunk_reader_.stream()->eosr());
  DCHECK_LT(num_values_read_, metadata_->num_values);
  return LogCorruptNumValuesInMetadataError();
}

bool BaseScalarColumnReader::NextPage() {
  parent_->assemble_rows_timer_.Stop();
  parent_->parse_status_ = ReadDataPage();
  if (UNLIKELY(!parent_->parse_status_.ok())) return false;
  if (num_buffered_values_ == 0) {
    rep_level_ = ParquetLevel::ROW_GROUP_END;
    def_level_ = ParquetLevel::ROW_GROUP_END;
    pos_current_value_ = ParquetLevel::INVALID_POS;
    return false;
  }
  parent_->assemble_rows_timer_.Start();
  return true;
}

bool BaseScalarColumnReader::AdvanceNextPageHeader() {
  num_buffered_values_ = 0;
  parent_->assemble_rows_timer_.Stop();
  parent_->parse_status_ = ReadNextDataPageHeader();
  if (UNLIKELY(!parent_->parse_status_.ok())) return false;
  if (num_buffered_values_ == 0) {
    rep_level_ = ParquetLevel::ROW_GROUP_END;
    def_level_ = ParquetLevel::ROW_GROUP_END;
    pos_current_value_ = ParquetLevel::INVALID_POS;
    return false;
  }
  parent_->assemble_rows_timer_.Start();
  return true;
}

void BaseScalarColumnReader::SetLevelDecodeError(
    const char* level_name, int decoded_level, int max_level) {
  if (decoded_level < 0) {
    DCHECK_EQ(decoded_level, ParquetLevel::INVALID_LEVEL);
    parent_->parse_status_.MergeStatus(
        Status(Substitute("Corrupt Parquet file '$0': "
                          "could not read all $1 levels for column '$2'",
            filename(), level_name, schema_element().name)));
  } else {
    parent_->parse_status_.MergeStatus(Status(Substitute("Corrupt Parquet file '$0': "
        "invalid $1 level $2 > max $1 level $3 for column '$4'", filename(),
        level_name, decoded_level, max_level, schema_element().name)));
  }
}

/// Wrapper around 'SkipTopLevelRows' to skip across multiple pages.
/// Function handles 3 scenarios:
/// 1. Page Filtering: When this is enabled this function can be used
///    to skip to a particular 'skip_row_id'.
/// 2. Collection: When this scalar reader is reading elements of a collection
/// 3. Rest of the cases.
/// For page filtering, we keep track of first and last page indexes and keep
/// traversing to next page until we find a page that contains 'skip_row_id'.
/// At that point, we can just skip to the required row id.
/// If the page of 'skip_row_id' is not a candidate page, we will stop at the
/// next candidate page and 'skip_row_id' is skipped by the way.
/// Difference between scenario 2 and 3 is that in scenario 2, we end up
/// decompressing all the pages being skipped, whereas in scenario 3 we only
/// decompress pages required and avoid decompression needed. This is possible
/// because in scenario 3 'data_page_header.num_values' corresponds to number
/// of rows stored in the page. This is not true in scenario 2 because multiple
/// consecutive values can belong to same row.
template <bool IN_COLLECTION>
bool BaseScalarColumnReader::SkipRowsInternal(int64_t num_rows, int64_t skip_row_id) {
  if (DoesPageFiltering() && skip_row_id > 0) {
    // Checks if its the beginning of row group and advances to next page.
    if (candidate_page_idx_ < 0) {
      if (UNLIKELY(!NextPage())) {
        return false;
      }
    }
    // Keep advancing until we hit reach required page containing 'skip_row_id' or
    // jump over it if that page is not a candidate page.
    while (skip_row_id > LastRowIdxInCurrentPage()) {
      COUNTER_ADD(parent_->num_pages_skipped_by_late_materialization_counter_, 1);
      if (UNLIKELY(!JumpToNextPage())) {
        return false;
      }
    }
    int64_t last_row = LastProcessedRow();
    int64_t remaining = 0;
    if (skip_row_id >= FirstRowIdxInCurrentPage()) {
      // Skip to the required row id within the page. Only needs this when row id locates
      // in current page.
      if (last_row < skip_row_id) {
        if (UNLIKELY(!SkipTopLevelRows(skip_row_id - last_row, &remaining))) {
          return false;
        }
      }
    }
    // also need to adjust 'candidate_row_ranges' as we skipped to new row id.
    auto& candidate_row_ranges = parent_->candidate_ranges_;
    while (current_row_ > candidate_row_ranges[current_row_range_].last) {
      DCHECK_LT(current_row_range_, candidate_row_ranges.size());
      ++current_row_range_;
    }
    return true;
  } else if (IN_COLLECTION) {
    DCHECK_GT(num_rows, 0);
    // if all the values of current page are consumed, move to next page.
    if (num_buffered_values_ == 0) {
      if (!NextPage()) {
        return false;
      }
    }
    DCHECK_GT(num_buffered_values_, 0);
    int64_t remaining = 0;
    // Try to skip 'num_rows' and see if something remains.
    if (!SkipTopLevelRows<true>(num_rows, &remaining)) {
      return false;
    }
    // Again invoke the same method on remaining rows.
    if (remaining > 0) {
      return SkipRowsInternal<IN_COLLECTION>(remaining, skip_row_id);
    }
    return true;
  } else {
    // If everything consumed in current page, skip data pages (multiple skips if needed)
    // to reach required page.
    if (num_buffered_values_ == 0) {
      if (!AdvanceNextPageHeader()) {
        return false;
      }
      DCHECK_GT(num_buffered_values_, 0);
      // Keep advancing to next page header if rows to be skipped are more than number
      // of values in the page. Note we will just be reading headers and skipping
      // pages without decompressing them as we advance.
      while (num_rows > num_buffered_values_) {
        COUNTER_ADD(parent_->num_pages_skipped_by_late_materialization_counter_, 1);
        num_rows -= num_buffered_values_;
        current_row_ += num_buffered_values_;
        if (!col_chunk_reader_.SkipPageData().ok() || !AdvanceNextPageHeader()) {
          return false;
        }
        DCHECK_GT(num_buffered_values_, 0);
      }
      // Read the data page (includes decompressing them if required).
      Status page_read = ReadCurrentDataPage();
      if (!page_read.ok()) {
        return false;
      }
    }

    // Skip the remaining rows in the page.
    DCHECK_GT(num_buffered_values_, 0);
    int64_t remaining = 0;
    if (!SkipTopLevelRows<true>(num_rows, &remaining)) {
      return false;
    }
    if (remaining > 0) {
      return SkipRowsInternal<IN_COLLECTION>(remaining, skip_row_id);
    }
    return true;
  }
};

/// Returns a column reader for decimal types based on its size and parquet type.
static ParquetColumnReader* CreateDecimalColumnReader(
    const SchemaNode& node, const SlotDescriptor* slot_desc, HdfsParquetScanner* parent) {
  switch (node.element->type) {
    case parquet::Type::FIXED_LEN_BYTE_ARRAY:
      switch (slot_desc->type().GetByteSize()) {
      case 4:
        return new ScalarColumnReader<Decimal4Value, parquet::Type::FIXED_LEN_BYTE_ARRAY,
            true>(parent, node, slot_desc);
      case 8:
        return new ScalarColumnReader<Decimal8Value, parquet::Type::FIXED_LEN_BYTE_ARRAY,
            true>(parent, node, slot_desc);
      case 16:
        return new ScalarColumnReader<Decimal16Value, parquet::Type::FIXED_LEN_BYTE_ARRAY,
            true>(parent, node, slot_desc);
      }
      break;
    case parquet::Type::BYTE_ARRAY:
      switch (slot_desc->type().GetByteSize()) {
      case 4:
        return new ScalarColumnReader<Decimal4Value, parquet::Type::BYTE_ARRAY, true>(
            parent, node, slot_desc);
      case 8:
        return new ScalarColumnReader<Decimal8Value, parquet::Type::BYTE_ARRAY, true>(
            parent, node, slot_desc);
      case 16:
        return new ScalarColumnReader<Decimal16Value, parquet::Type::BYTE_ARRAY, true>(
            parent, node, slot_desc);
      }
      break;
    case parquet::Type::INT32:
      return new ScalarColumnReader<Decimal4Value, parquet::Type::INT32, true>(
          parent, node, slot_desc);
    case parquet::Type::INT64:
      return new ScalarColumnReader<Decimal8Value, parquet::Type::INT64, true>(
          parent, node, slot_desc);
    default:
      DCHECK(false) << "Invalid decimal primitive type";
  }
  DCHECK(false) << "Invalid decimal type";
  return nullptr;
}

ParquetColumnReader* ParquetColumnReader::Create(const SchemaNode& node,
    bool is_collection_field, const SlotDescriptor* slot_desc,
    HdfsParquetScanner* parent) {
  if (is_collection_field) {
    // Create collection reader (note this handles both NULL and non-NULL 'slot_desc')
    return new CollectionColumnReader(parent, node, slot_desc);
  } else if (slot_desc != nullptr) {
    // Create the appropriate ScalarColumnReader type to read values into 'slot_desc'
    switch (slot_desc->type().type) {
      case TYPE_BOOLEAN:
        return new ScalarColumnReader<bool, parquet::Type::BOOLEAN, true>(
            parent, node, slot_desc);
      case TYPE_TINYINT:
        return new ScalarColumnReader<int8_t, parquet::Type::INT32, true>(
            parent, node, slot_desc);
      case TYPE_SMALLINT:
        return new ScalarColumnReader<int16_t, parquet::Type::INT32, true>(parent, node,
            slot_desc);
      case TYPE_INT:
        return new ScalarColumnReader<int32_t, parquet::Type::INT32, true>(parent, node,
            slot_desc);
      case TYPE_BIGINT:
        switch (node.element->type) {
          case parquet::Type::INT32:
            return new ScalarColumnReader<int64_t, parquet::Type::INT32, true>(parent,
                node, slot_desc);
          default:
            return new ScalarColumnReader<int64_t, parquet::Type::INT64, true>(parent,
                node, slot_desc);
        }
      case TYPE_FLOAT:
        return new ScalarColumnReader<float, parquet::Type::FLOAT, true>(parent, node,
            slot_desc);
      case TYPE_DOUBLE:
        switch (node.element->type) {
          case parquet::Type::INT32:
            return new ScalarColumnReader<double , parquet::Type::INT32, true>(parent,
                node, slot_desc);
          case parquet::Type::FLOAT:
            return new ScalarColumnReader<double, parquet::Type::FLOAT, true>(parent,
                node, slot_desc);
          default:
            return new ScalarColumnReader<double, parquet::Type::DOUBLE, true>(parent,
                node, slot_desc);
        }
      case TYPE_TIMESTAMP:
        return CreateTimestampColumnReader(node, slot_desc, parent);
      case TYPE_DATE:
        return new ScalarColumnReader<DateValue, parquet::Type::INT32, true>(parent, node,
            slot_desc);
      case TYPE_STRING:
      case TYPE_VARCHAR:
      case TYPE_CHAR:
        return new ScalarColumnReader<StringValue, parquet::Type::BYTE_ARRAY, true>(
            parent, node, slot_desc);
      case TYPE_DECIMAL:
        return CreateDecimalColumnReader(node, slot_desc, parent);
      case TYPE_STRUCT:
        return new StructColumnReader(parent, node, slot_desc);
      default:
        DCHECK(false) << slot_desc->type().DebugString();
        return nullptr;
    }
  } else {
    // Special case for counting scalar values (e.g. count(*), no materialized columns in
    // the file, only materializing a position slot). We won't actually read any values,
    // only the rep and def levels, so it doesn't matter what kind of reader we make.
    return new ScalarColumnReader<int8_t, parquet::Type::INT32, false>(parent, node,
        slot_desc);
  }
}

ParquetColumnReader* ParquetColumnReader::CreateTimestampColumnReader(
    const SchemaNode& node, const SlotDescriptor* slot_desc,
    HdfsParquetScanner* parent) {
  if (node.element->type == parquet::Type::INT96) {
    return new ScalarColumnReader<TimestampValue, parquet::Type::INT96, true>(
        parent, node, slot_desc);
  }
  else if (node.element->type == parquet::Type::INT64) {
    return new ScalarColumnReader<TimestampValue, parquet::Type::INT64, true>(
        parent, node, slot_desc);
  }
  DCHECK(false) << slot_desc->type().DebugString();
  return nullptr;
}

}
