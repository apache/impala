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

#ifndef IMPALA_UTIL_DICT_ENCODING_H
#define IMPALA_UTIL_DICT_ENCODING_H

#include <map>

#include <boost/unordered_map.hpp>

#include "common/compiler-util.h"
#include "exec/parquet/parquet-common.h"
#include "gutil/strings/substitute.h"
#include "runtime/mem-pool.h"
#include "runtime/string-value.h"
#include "util/bit-util.h"
#include "util/mem-util.h"
#include "util/rle-encoding.h"
#include "util/ubsan.h"

namespace impala {

/// See the dictionary encoding section of https://github.com/Parquet/parquet-format.
/// This class supports dictionary encoding of all Impala types.
/// The encoding supports streaming encoding. Values are encoded as they are added while
/// the dictionary is being constructed. At any time, the buffered values can be
/// written out with the current dictionary size. More values can then be added to
/// the encoder, including new dictionary entries.
/// TODO: if the dictionary was made to be ordered, the dictionary would compress better.
/// Add this to the spec as future improvement.

/// Base class for encoders. This is convenient so users can have a type that
/// abstracts over the actual dictionary type.
/// Note: it does not provide a virtual Put(). Users are expected to know the subclass
/// type when using Put().
/// TODO: once we can easily remove virtual calls with codegen, this interface can
/// rely less on templating and be easier to follow. The type should be passed in
/// as an argument rather than template argument.
class DictEncoderBase {
 public:
  virtual ~DictEncoderBase() {
    DCHECK(buffered_indices_.empty());
    ReleaseBytes();
    DCHECK_EQ(dict_bytes_cnt_, 0);
  }

  /// This function will clear the buffered_indices and
  /// decrement the bytes used by dictionary.
  void Close() {
    ClearIndices();
    ReleaseBytes();
  }

  /// Writes out the encoded dictionary to buffer. buffer must be preallocated to
  /// dict_encoded_size() bytes.
  virtual void WriteDict(uint8_t* buffer) = 0;

  /// The number of entries in the dictionary.
  virtual int num_entries() const = 0;

  /// Returns true if the dictionary is full.
  virtual bool IsFull() const = 0;

  /// Clears all the indices (but leaves the dictionary).
  void ClearIndices() { buffered_indices_.clear(); }

  /// Returns a conservative estimate of the number of bytes needed to encode the buffered
  /// indices. Used to size the buffer passed to WriteData().
  int EstimatedDataEncodedSize() {
    return 1 + RleEncoder::MaxBufferSize(bit_width(), buffered_indices_.size());
  }

  /// The minimum bit width required to encode the currently buffered indices.
  int bit_width() const {
    if (UNLIKELY(num_entries() == 0)) return 0;
    if (UNLIKELY(num_entries() == 1)) return 1;
    return BitUtil::Log2Ceiling64(num_entries());
  }

  /// Writes out any buffered indices to buffer preceded by the bit width of this data.
  /// Returns the number of bytes written.
  /// If the supplied buffer is not big enough, returns -1.
  /// buffer must be preallocated with buffer_len bytes. Use EstimatedDataEncodedSize()
  /// to size buffer.
  int WriteData(uint8_t* buffer, int buffer_len);

  int dict_encoded_size() { return dict_encoded_size_; }

  void UsedbyTest() { used_by_test_ = true;}

 protected:
  DictEncoderBase(MemPool* pool, MemTracker* mem_tracker) :
      dict_encoded_size_(0),
      pool_(pool),
      dict_bytes_cnt_(0),
      dict_mem_tracker_(mem_tracker) { }

  /// Indices that have not yet be written out by WriteData().
  std::vector<int> buffered_indices_;

  /// Memtracker Consume is called every ENC_MEM_TRACK_CNT times.
  /// Periodicity of calling Memtracker Consume.
  const int ENC_MEM_TRACK_CNT = 8192;

  /// Number of times ConsumeBytes() was called.
  int num_call_track_{0};

  /// The number of bytes needed to encode the dictionary.
  int dict_encoded_size_{0};

  /// Pool to store StringValue data. Not owned.
  MemPool* pool_{nullptr};

  /// This will account for bytes consumed by nodes_
  int dict_bytes_cnt_{0};

  /// This will account for bytes consumed, last_accounted by memtracker
  int dict_bytes_cnt_memtrack_{0};

  /// This will track the memory used by nodes_
  MemTracker* dict_mem_tracker_{nullptr};

  /// Function to decrement the byte counter and decrease the bytes usage
  /// of the memory tracker.
  void ReleaseBytes() {
    if (dict_mem_tracker_ != nullptr) {
      dict_mem_tracker_->Release(dict_bytes_cnt_memtrack_);
      dict_bytes_cnt_ = 0;
      dict_bytes_cnt_memtrack_ = 0;
      num_call_track_ = 0;
    }
  }

  /// Used by dict-test.cc to check the usage of bytes by dictionary
  /// is properly accounted.
  bool used_by_test_{false};

  /// Function to increment the byte counter and increase the bytes usage
  /// of the memory tracker.
  void ConsumeBytes(int num_bytes) {
    if (dict_mem_tracker_ != nullptr) {
      dict_bytes_cnt_ += num_bytes;
      // Calling Memtracker frequently may be expensive so update
      // the memtracker every ENC_MEM_TRACK_CNT times.
      if (num_call_track_ % ENC_MEM_TRACK_CNT == 0 || used_by_test_ == true) {
        // TODO: TryConsume() can be called to check if memory limit has been exceeded.
        dict_mem_tracker_->Consume(dict_bytes_cnt_ - dict_bytes_cnt_memtrack_);
        dict_bytes_cnt_memtrack_ = dict_bytes_cnt_;
      }
      num_call_track_++;
    }
  }
};

template<typename T>
class DictEncoder : public DictEncoderBase {
 public:
  DictEncoder(MemPool* pool, int encoded_value_size, MemTracker* mem_tracker) :
      DictEncoderBase(pool, mem_tracker), buckets_(HASH_TABLE_SIZE, Node::INVALID_INDEX),
      encoded_value_size_(encoded_value_size) { }

  /// Encode value. Returns the number of bytes added to the dictionary page length
  /// (will be 0 if this value is already in the dictionary) or -1 if the dictionary is
  /// full (in which case the caller should give up on dictionary encoding). Note that
  /// this does not actually write any data, just buffers the value's index to be
  /// written later.
  int Put(const T& value);

  /// This function returns the size in bytes of the dictionary vector.
  /// It is used by dict-test.cc for validation of bytes consumed against
  /// memory tracked.
  int DictByteSize() {
    return sizeof(Node) * nodes_.size();
  }

  void WriteDict(uint8_t* buffer) override;

  int num_entries() const override { return nodes_.size(); }

  /// Returns true if the dictionary is full.
  bool IsFull() const override {
    return nodes_.size() >= Node::INVALID_INDEX;
  }

  /// Execute 'func' for each key that is present in the dictionary. Stops execution the
  /// first time 'func' returns an error, propagating the error. Returns OK otherwise.
  ///
  /// Can be useful if we fall back to plain encoding from dict encoding but still want to
  /// use a Bloom filter. In this case the filter can be filled with all elements that
  /// have occured so far.
  Status ForEachDictKey(const std::function<Status(const T&)>& func) {
    for (auto pair : nodes_) {
      RETURN_IF_ERROR(func(pair.value));
    }

    return Status::OK();
  }

 private:
  /// Size of the table. Must be a power of 2.
  enum { HASH_TABLE_SIZE = 1 << 16 };

  /// Dictates an upper bound on the capacity of the hash table.
  typedef uint16_t NodeIndex;

  /// Hash table mapping value to dictionary index (i.e. the number used to encode this
  /// value in the data). Each table entry is a index into the nodes_ vector (giving the
  /// first node of a chain for this bucket) or Node::INVALID_INDEX for an empty bucket.
  std::vector<NodeIndex> buckets_;

  /// Node in the chained hash table.
  struct Node {
    Node(const T& v, const NodeIndex& n) : value(v), next(n) { }

    /// The dictionary value.
    T value;

    /// Index into nodes_ for the next Node in the chain. INVALID_INDEX indicates end.
    NodeIndex next;

    /// The maximum number of values in the dictionary.  Chosen to be around 60% of
    /// HASH_TABLE_SIZE to limit the expected length of the chains.
    /// Changing this value will require re-tuning test_parquet_page_index.py.
    enum { INVALID_INDEX = 40000 };
  };

  /// The nodes of the hash table. Ordered by dictionary index (and so also represents
  /// the reverse mapping from encoded index to value).
  std::vector<Node> nodes_;

  /// Size of each encoded dictionary value. -1 for variable-length types.
  int encoded_value_size_;

  /// Hash function for mapping a value to a bucket.
  inline uint32_t Hash(const T& value) const;

  /// Adds value to the hash table and updates dict_encoded_size_. Returns the
  /// number of bytes added to dict_encoded_size_.
  /// bucket gives a pointer to the location (i.e. chain) to add the value
  /// so that the hash for value doesn't need to be recomputed.
  int AddToTable(const T& value, NodeIndex* bucket);
};

/// Number of decoded values to buffer at a time. A multiple of 32 is chosen to allow
/// efficient reading in batches from data_decoder_. Increasing the batch size up to
/// 128 seems to improve performance, but increasing further did not make a noticeable
/// difference. Defined outside DictDecoderBase to get static linkage because there is
/// no dict-encoding.cc file.
static constexpr int32_t DICT_DECODER_BUFFER_SIZE = 128;

/// Decoder class for dictionary encoded data. This class does not allocate any
/// buffers. The input buffers (dictionary buffer and RLE buffer) must be maintained
/// by the caller and valid as long as this object is.
class DictDecoderBase {
 public:
   DictDecoderBase(MemTracker* tracker) :
     dict_bytes_cnt_(0), dict_mem_tracker_(tracker) { }

  /// The rle encoded indices into the dictionary. Returns an error status if the buffer
  /// is too short or the bit_width metadata in the buffer is invalid.
  Status SetData(uint8_t* buffer, int buffer_len) {
    DCHECK_GE(buffer_len, 0);
    if (UNLIKELY(buffer_len == 0)) return Status("Dictionary cannot be 0 bytes");
    uint8_t bit_width = *buffer;
    if (UNLIKELY(bit_width < 0 || bit_width > sizeof(IndexType) * 8
          || bit_width > BatchedBitReader::MAX_BITWIDTH)) {
      return Status(strings::Substitute("Dictionary has invalid or unsupported bit "
          "width: $0", bit_width));
    }
    ++buffer;
    --buffer_len;
    data_decoder_.Reset(buffer, buffer_len, bit_width);
    num_repeats_ = 0;
    num_literal_values_ = 0;
    next_literal_idx_ = 0;
    return Status::OK();
  }

  virtual ~DictDecoderBase() {
    ReleaseBytes();
    DCHECK_EQ(dict_bytes_cnt_, 0);
  }

  virtual int num_entries() const = 0;

  /// Reads the dictionary value at the specified index into the buffer provided.
  /// The buffer must be large enough to receive the datatype for this dictionary.
  virtual void GetValue(int index, void* buffer) = 0;

  /// This function will decrement the bytes used by dictionary, MemTracker
  void Close() {
    ReleaseBytes();
  }

 protected:
  using IndexType = uint32_t;
  RleBatchDecoder<IndexType> data_decoder_;

  /// Greater than zero if we've started decoding a repeated run.
  int64_t num_repeats_ = 0;

  /// Greater than zero if we have buffered some literal values.
  int num_literal_values_ = 0;

  /// The index of the next decoded value to return.
  int next_literal_idx_ = 0;

  /// This will account for bytes consumed by dict_
  int dict_bytes_cnt_{0};

  /// This will track the memory used by dict_
  MemTracker* dict_mem_tracker_{nullptr};

  /// Function to decrement the byte counter and decrease the bytes usage
  /// of the memory tracker.
  void ReleaseBytes() {
    if (dict_mem_tracker_ != nullptr) {
      dict_mem_tracker_->Release(dict_bytes_cnt_);
      dict_bytes_cnt_ = 0;
    }
  }

  /// Function to increment the byte counter and increase the bytes usage
  /// of the memory tracker.
  void ConsumeBytes(int num_bytes) {
    if (dict_mem_tracker_ != nullptr) {
      dict_bytes_cnt_ += num_bytes;
      dict_mem_tracker_->Consume(num_bytes);
    }
  }
};

template<typename T>
class DictDecoder : public DictDecoderBase {
 public:
  /// Construct empty dictionary.
  DictDecoder(MemTracker* tracker):DictDecoderBase(tracker) {}

  /// Initialize the decoder with an input buffer containing the dictionary.
  /// 'dict_len' is the byte length of dict_buffer.
  /// For string data, the decoder returns StringValues with data directly from
  /// dict_buffer (i.e. no copies).
  /// fixed_len_size is the size that must be passed to decode fixed-length
  /// dictionary values (values stored using FIXED_LEN_BYTE_ARRAY).
  /// Returns true if the dictionary values were all successfully decoded, or false
  /// if the dictionary was corrupt.
  template<parquet::Type::type PARQUET_TYPE>
  bool Reset(uint8_t* dict_buffer, int dict_len, int fixed_len_size) WARN_UNUSED_RESULT;

  /// Should be only called for Timestamp columns.
  void SetTimestampHelper(ParquetTimestampDecoder timestamp_decoder) {
    timestamp_decoder_ = timestamp_decoder;
  }

  virtual int num_entries() const { return dict_.size(); }

  virtual void GetValue(int index, void* buffer) {
    DCHECK_GE(index, 0);
    DCHECK_LT(index, dict_.size());
    // Avoid an unaligned store by using memcpy
    T val = dict_[index];
    memcpy(buffer, reinterpret_cast<const void*>(&val), sizeof(T));
  }

  /// Returns the next value.  Returns false if the data is invalid.
  /// For StringValues, this does not make a copy of the data.  Instead,
  /// the string data is from the dictionary buffer passed into the c'tor.
  bool GetNextValue(T* value) WARN_UNUSED_RESULT;

  /// Batched version of GetNextValue(). Reads the next 'count' values into
  /// 'first_values'. Returns false if the data was invalid and 'count' values could not
  /// be successfully read. 'stride' is the stride in bytes between each subsequent value.
  bool GetNextValues(T* first_value, int64_t stride, int count) WARN_UNUSED_RESULT;

  /// This function returns the size in bytes of the dictionary vector.
  /// It is used by dict-test.cc for validation of bytes consumed against
  /// memory tracked.
  int DictByteSize() {
    return sizeof(T) * dict_.size();
  }

  /// Skip 'num_values' values from the input.
  bool SkipValues(int64_t num_values) WARN_UNUSED_RESULT;

 private:
  /// List of decoded values stored in the dict_
  std::vector<T> dict_;

  /// Contains extra data needed for Timestamp decoding.
  ParquetTimestampDecoder timestamp_decoder_;

  /// Decoded values, buffered to allow caller to consume one-by-one. If in the middle of
  /// a repeated run, the first element is the current dict value. If in a literal run,
  /// this contains 'num_literal_values_' values, with the next value to be returned at
  /// 'next_literal_idx_'.
  T decoded_values_[DICT_DECODER_BUFFER_SIZE];

  /// Copy as many as possible literal values, up to 'max_to_copy' from 'decoded_values_'
  /// to '*out'. Return the number copied and advance '*out'.
  uint32_t CopyLiteralsToOutput(
      uint32_t max_to_copy, StrideWriter<T>* RESTRICT out) RESTRICT;

  /// Slow path for GetNextValue() where we need to decode new values. Should not be
  /// inlined everywhere.
  bool DecodeNextValue(T* value);

  /// Specialized for Timestamp columns, simple proxy to ParquetPlainEncoder::Decode
  /// for other types.
  template<parquet::Type::type PARQUET_TYPE>
  int Decode(const uint8_t* buffer, const uint8_t* buffer_end,
      int fixed_len_size, T* v) {
    return  ParquetPlainEncoder::Decode<T, PARQUET_TYPE>(buffer, buffer_end,
        fixed_len_size,  v);
  }
};

template<typename T>
inline int DictEncoder<T>::Put(const T& value) {
  NodeIndex* bucket = &buckets_[Hash(value) & (HASH_TABLE_SIZE - 1)];
  NodeIndex i = *bucket;
  // Look for the value in the dictionary.
  while (i != Node::INVALID_INDEX) {
    const Node* n = &nodes_[i];
    if (LIKELY(n->value == value)) {
      // Value already in dictionary.
      buffered_indices_.push_back(i);
      return 0;
    }
    i = n->next;
  }
  // Value not found. Add it to the dictionary if there's space.
  i = nodes_.size();
  if (UNLIKELY(i >= Node::INVALID_INDEX)) return -1;
  buffered_indices_.push_back(i);
  return AddToTable(value, bucket);
}

template<typename T>
inline uint32_t DictEncoder<T>::Hash(const T& value) const {
  return HashUtil::Hash(&value, sizeof(value), 0);
}

template<>
inline uint32_t DictEncoder<StringValue>::Hash(const StringValue& value) const {
  return HashUtil::Hash(value.Ptr(), value.Len(), 0);
}

template<>
inline uint32_t DictEncoder<TimestampValue>::Hash(const TimestampValue& value) const {
  // TimestampValue needs to use its own hash function, because it has padding
  // that must be ignored for consistency.
  return value.Hash();
}

template<typename T>
inline int DictEncoder<T>::AddToTable(const T& value, NodeIndex* bucket) {
  DCHECK_GT(encoded_value_size_, 0);
  Node node(value, *bucket);
  ConsumeBytes(sizeof(node));
  // Prepend the new node to this bucket's chain.
  nodes_.push_back(node);
  *bucket = nodes_.size() - 1;
  dict_encoded_size_ += encoded_value_size_;
  return encoded_value_size_;
}

template<>
inline int DictEncoder<StringValue>::AddToTable(const StringValue& value,
    NodeIndex* bucket) {
  StringValue::SimpleString value_s = value.ToSimpleString();
  char* ptr_copy = reinterpret_cast<char*>(pool_->Allocate(value_s.len));
  Ubsan::MemCpy(ptr_copy, value_s.ptr, value_s.len);
  StringValue sv(ptr_copy, value_s.len);
  Node node(sv, *bucket);
  ConsumeBytes(sizeof(node));
  // Prepend the new node to this bucket's chain.
  nodes_.push_back(node);
  *bucket = nodes_.size() - 1;
  int bytes_added = ParquetPlainEncoder::ByteSize(sv);
  dict_encoded_size_ += bytes_added;
  return bytes_added;
}

// Force inlining - GCC does not always inline this into hot loops in Parquet scanner.
template <typename T>
ALWAYS_INLINE inline bool DictDecoder<T>::GetNextValue(T* value) {
  // IMPALA-959: Use memcpy() instead of '=' to set *value: addresses are not always 16
  // byte aligned for Decimal16Values.
  if (num_repeats_ > 0) {
    --num_repeats_;
    memcpy(value, &decoded_values_[0], sizeof(T));
    return true;
  } else if (next_literal_idx_ < num_literal_values_) {
    int idx = next_literal_idx_++;
    memcpy(value, &decoded_values_[idx], sizeof(T));
    return true;
  }
  // No decoded values left - need to decode some more.
  return DecodeNextValue(value);
}

template <typename T>
ALWAYS_INLINE inline bool DictDecoder<T>::GetNextValues(
    T* first_value, int64_t stride, int count) {
  DCHECK_GE(count, 0);
  StrideWriter<T> out(first_value, stride);
  if (num_repeats_ > 0) {
    // Consume any already-decoded repeated value.
    int num_to_copy = std::min<uint32_t>(num_repeats_, count);
    T repeated_val = decoded_values_[0];
    out.SetNext(repeated_val, num_to_copy);
    count -= num_to_copy;
    num_repeats_ -= num_to_copy;
  } else if (next_literal_idx_ < num_literal_values_) {
    // Consume any already-decoded literal values.
    count -= CopyLiteralsToOutput(count, &out);
  }
  DCHECK_GE(count, 0);
  while (count > 0) {
    uint32_t num_repeats = data_decoder_.NextNumRepeats();
    if (num_repeats > 0) {
      // Decode repeats directly to the output.
      uint32_t num_repeats_to_consume = std::min<uint32_t>(num_repeats, count);
      const IndexType idx = data_decoder_.GetRepeatedValue(num_repeats_to_consume);
      if (UNLIKELY(idx >= dict_.size())) return false;
      T repeated_val = dict_[idx];
      out.SetNext(repeated_val, num_repeats_to_consume);
      count -= num_repeats_to_consume;
    } else {
      // Decode as many literals as possible directly to the output, buffer the rest.
      uint32_t num_literals = data_decoder_.NextNumLiterals();
      if (UNLIKELY(num_literals == 0)) return false;
      // Case 1: decode the whole literal run directly to the output.
      // Case 2: decode none or some of the run to the output, buffer some remaining.
      if (count >= num_literals) { // Case 1
        if (UNLIKELY(!data_decoder_.DecodeLiteralValues(
                num_literals, dict_.data(), dict_.size(), &out))) {
          return false;
        }
        count -= num_literals;
      } else { // Case 2
        uint32_t num_to_decode = BitUtil::RoundDown(count, 32);
        if (num_to_decode > 0 && UNLIKELY(!data_decoder_.DecodeLiteralValues(
                num_to_decode, dict_.data(), dict_.size(), &out))) {
          return false;
        }
        count -= num_to_decode;
        DCHECK_GE(count, 0);
        if (count > 0) {
          if (UNLIKELY(!DecodeNextValue(out.Advance()))) return false;
          --count;
          // Consume any already-decoded literal values.
          count -= CopyLiteralsToOutput(count, &out);
        }
        return true;
      }
    }
  }
  return true;
}

template <typename T>
ALWAYS_INLINE inline bool DictDecoder<T>::SkipValues(int64_t num_values) {
  int64_t num_remaining = num_values;
  if (num_repeats_ > 0) {
    int64_t num_to_skip = std::min(num_remaining, num_repeats_);
    num_repeats_ -= num_to_skip;
    num_remaining -= num_to_skip;
  } else if (next_literal_idx_ < num_literal_values_) {
    int64_t num_to_skip = std::min<int64_t>(num_literal_values_ -
        next_literal_idx_, num_remaining);
    next_literal_idx_ += num_to_skip;
    num_remaining -= num_to_skip;
  }
  if (num_remaining > 0) return data_decoder_.SkipValues(num_remaining) == num_remaining;
  return true;
}

template <typename T>
uint32_t DictDecoder<T>::CopyLiteralsToOutput(
    uint32_t max_to_copy, StrideWriter<T>* out) {
  uint32_t num_to_copy =
      std::min<uint32_t>(num_literal_values_ - next_literal_idx_, max_to_copy);
  for (uint32_t i = 0; i < num_to_copy; ++i) {
    out->SetNext(decoded_values_[next_literal_idx_++]);
  }
  return num_to_copy;
}

template <typename T>
bool DictDecoder<T>::DecodeNextValue(T* value) {
  // IMPALA-959: Use memcpy() instead of '=' to set *value: addresses are not always 16
  // byte aligned for Decimal16Values.
  int32_t num_repeats = data_decoder_.NextNumRepeats();
  DCHECK_GE(num_repeats, 0);
  if (num_repeats > 0) {
    const IndexType idx = data_decoder_.GetRepeatedValue(num_repeats);
    if (UNLIKELY(idx >= dict_.size())) return false;
    memcpy(&decoded_values_[0], &dict_[idx], sizeof(T));
    memcpy(value, &decoded_values_[0], sizeof(T));
    num_repeats_ = num_repeats - 1;
    return true;
  } else {
    int32_t num_literals = data_decoder_.NextNumLiterals();
    if (UNLIKELY(num_literals == 0)) return false;

    DCHECK_GT(num_literals, 0);
    int32_t num_to_decode = std::min(num_literals, DICT_DECODER_BUFFER_SIZE);
    StrideWriter<T> dst(&decoded_values_[0], sizeof(T));
    if (UNLIKELY(!data_decoder_.DecodeLiteralValues(num_to_decode, dict_.data(),
            dict_.size(), &dst))) {
      return false;
    }
    num_literal_values_ = num_to_decode;
    memcpy(value, &decoded_values_[0], sizeof(T));
    next_literal_idx_ = 1;
    return true;
  }
}

template<typename T>
inline void DictEncoder<T>::WriteDict(uint8_t* buffer) {
  for (const Node& node: nodes_) {
    buffer += ParquetPlainEncoder::Encode(node.value, encoded_value_size_, buffer);
  }
}

inline int DictEncoderBase::WriteData(uint8_t* buffer, int buffer_len) {
  // Write bit width in first byte
  *buffer = bit_width();
  ++buffer;
  --buffer_len;

  RleEncoder encoder(buffer, buffer_len, bit_width());
  for (int index: buffered_indices_) {
    if (!encoder.Put(index)) return -1;
  }
  encoder.Flush();
  return 1 + encoder.len();
}

template <>
template<parquet::Type::type PARQUET_TYPE>
inline int DictDecoder<TimestampValue>::Decode(const uint8_t* buffer,
    const uint8_t* buffer_end, int fixed_len_size, TimestampValue* v) {
  return timestamp_decoder_.Decode<PARQUET_TYPE>(buffer, buffer_end, v);
}

template<typename T>
template<parquet::Type::type PARQUET_TYPE>
inline bool DictDecoder<T>::Reset(uint8_t* dict_buffer, int dict_len,
    int fixed_len_size) {
  dict_.clear();
  ReleaseBytes();
  uint8_t* end = dict_buffer + dict_len;
  while (dict_buffer < end) {
    T value;
    int decoded_len = Decode<PARQUET_TYPE>(dict_buffer, end,
        fixed_len_size, &value);
    if (UNLIKELY(decoded_len < 0)) return false;
    dict_buffer += decoded_len;
    dict_.push_back(value);
  }
  ConsumeBytes(sizeof(T) * dict_.size());
  return true;
}

}
#endif
