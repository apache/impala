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

#ifndef IMPALA_RLE_ENCODING_H
#define IMPALA_RLE_ENCODING_H

#include <math.h>

#include "common/compiler-util.h"
#include "util/bit-stream-utils.inline.h"
#include "util/bit-util.h"
#include "util/mem-util.h"
#include "util/test-info.h"

namespace impala {

/// Utility classes to do run length encoding (RLE) for fixed bit width values.  If runs
/// are sufficiently long, RLE is used, otherwise, the values are just bit-packed
/// (literal encoding).
/// For both types of runs, there is a byte-aligned indicator which encodes the length
/// of the run and the type of the run.
/// This encoding has the benefit that when there aren't any long enough runs, values
/// are always decoded at fixed (can be precomputed) bit offsets OR both the value and
/// the run length are byte aligned. This allows for very efficient decoding
/// implementations.
/// The encoding is:
///    encoded-block := run*
///    run := literal-run | repeated-run
///    literal-run := literal-indicator < literal bytes >
///    repeated-run := repeated-indicator < repeated value. padded to byte boundary >
///    literal-indicator := varint_encode( number_of_groups << 1 | 1)
///    repeated-indicator := varint_encode( number_of_repetitions << 1 )
//
/// Each run is preceded by a varint. The varint's least significant bit is used to
/// indicate whether the run is a literal run (lsb=1) or a repeated run (lsb=0). The rest
/// of the varint is used to determine the length of the run (eg how many times the
/// value repeats).
//
/// In the case of literal runs, the run length is always a multiple of 8 (i.e. encode
/// in groups of 8), so that no matter the bit-width of the value, the sequence will end
/// on a byte boundary without padding.
/// Given that we know it is a multiple of 8, we store the number of 8-groups rather than
/// the actual number of encoded ints. (This means that the total number of encoded values
/// can not be determined from the encoded data, since the number of values in the last
/// group may not be a multiple of 8). For the last group of literal runs, we pad
/// the group to 8 with zeros. This allows for 8 at a time decoding on the read side
/// without the need for additional checks.
//
/// There is a break-even point when it is more storage efficient to do run length
/// encoding. This value is computed based on the bit-width (by
/// RleEncoder::MinRepeatedRunLength).
///
/// TODO: think about how to use this for strings.  The bit packing isn't quite the same.
//
/// Examples with bit-width 1 (eg encoding booleans):
/// ----------------------------------------
/// 100 1s followed by 100 0s:
/// <varint(100 << 1)> <1, padded to 1 byte>  <varint(100 << 1)> <0, padded to 1 byte>
///  - (total 4 bytes)
//
/// alternating 1s and 0s (200 total):
/// 200 ints = 25 groups of 8
/// <varint((25 << 1) | 1)> <25 bytes of values, bitpacked>
/// (total 26 bytes, 1 byte overhead)
///
/// See https://github.com/apache/parquet-format/blob/master/Encodings.md#RLE
//

/// RLE decoder with a batch-oriented interface that enables fast decoding.
/// Users of this class must first initialize the class to point to a buffer of
/// RLE-encoded data, passed into the constructor or Reset(). Then they can
/// decode data by checking NextNumRepeats()/NextNumLiterals() to see if the
/// next run is a repeated or literal run, then calling GetRepeatedValue()
/// or GetLiteralValues() respectively to read the values.
///
/// End-of-input is signalled by NextNumRepeats() == NextNumLiterals() == 0.
/// Other decoding errors are signalled by functions returning false. If an
/// error is encountered then it is not valid to read any more data until
/// Reset() is called.
template <typename T>
class RleBatchDecoder {
 public:
  RleBatchDecoder(uint8_t* buffer, int buffer_len, int bit_width) {
    Reset(buffer, buffer_len, bit_width);
  }
  RleBatchDecoder() {}

  /// Reset the decoder to read from a new buffer.
  void Reset(uint8_t* buffer, int buffer_len, int bit_width);

  /// Return the size of the current repeated run. Returns zero if the current run is
  /// a literal run or if no more runs can be read from the input.
  int32_t NextNumRepeats();

  /// Get the value of the current repeated run and consume the given number of repeats.
  /// Only valid to call when NextNumRepeats() > 0. The given number of repeats cannot
  /// be greater than the remaining number of repeats in the run. 'num_repeats_to_consume'
  /// can be set to 0 to peek at the value without consuming repeats.
  T GetRepeatedValue(int32_t num_repeats_to_consume);

  /// Return the size of the current literal run. Returns zero if the current run is
  /// a repeated run or if no more runs can be read from the input.
  int32_t NextNumLiterals();

  /// Consume 'num_literals_to_consume' literals from the current literal run,
  /// copying the values to 'values'. 'num_literals_to_consume' must be <=
  /// NextNumLiterals(). Returns true if the requested number of literals were
  /// successfully read or false if an error was encountered, e.g. the input was
  /// truncated.
  bool GetLiteralValues(int32_t num_literals_to_consume, T* values) WARN_UNUSED_RESULT;

  /// Consume 'num_literals_to_consume' literals from the current literal run,
  /// decoding them using 'dict' and outputting them to 'out'.
  /// 'num_literals_to_consume' must be <= NextNumLiterals(). Returns true if
  /// the requested number of literals were successfully read or false if an error
  /// was encountered, e.g. the input was truncated or the value was not present
  /// in the dictionary. Errors can only be recovered from by calling Reset()
  /// to read from a new buffer.
  template <typename OutType>
  bool DecodeLiteralValues(int32_t num_literals_to_consume, OutType* dict,
      int64_t dict_len, StrideWriter<OutType>* RESTRICT out) WARN_UNUSED_RESULT;

  /// Convenience method to get the next value. Not efficient. Returns true on success
  /// or false if no more values can be read from the input or an error was encountered
  /// decoding the values.
  bool GetSingleValue(T* val) WARN_UNUSED_RESULT;

  /// Consume 'num_values_to_consume' values and copy them to 'values'.
  /// Returns the number of consumed values or 0 if an error occurred.
  int32_t GetValues(int32_t num_values_to_consume, T* values);

  /// Skip 'num_values' values.
  /// Returns the number of skipped values or 0 if an error occurred.
  int32_t SkipValues(int32_t num_values);

 private:
  /// Skip 'num_literals_to_skip' literals.
  bool SkipLiteralValues(int32_t num_literals_to_skip) WARN_UNUSED_RESULT;

  /// Skip 'num_values' repeated values.
  void SkipRepeatedValues(int32_t num_values);

  BatchedBitReader bit_reader_;

  /// Number of bits needed to encode the value. Must be between 0 and 64 after
  /// the decoder is initialized with a buffer. -1 indicates the decoder was not
  /// initialized.
  int bit_width_ = -1;

  /// If a repeated run, the number of repeats remaining in the current run to be read.
  /// If the current run is a literal run, this is 0.
  int32_t repeat_count_ = 0;

  /// If a literal run, the number of literals remaining in the current run to be read.
  /// If the current run is a repeated run, this is 0.
  int32_t literal_count_ = 0;

  /// If a repeated run, the current repeated value.
  T repeated_value_;

  /// Size of buffer for literal values. Large enough to decode a full batch of 32
  /// literals. The buffer is needed to allow clients to read in batches that are not
  /// multiples of 32.
  static constexpr int LITERAL_BUFFER_LEN = 32;

  /// Buffer containing 'num_buffered_literals_' values. 'literal_buffer_pos_' is the
  /// position of the next literal to be read from the buffer.
  T literal_buffer_[LITERAL_BUFFER_LEN];
  int num_buffered_literals_ = 0;
  int literal_buffer_pos_ = 0;

  /// Called when both 'literal_count_' and 'repeat_count_' have been exhausted.
  /// Sets either 'literal_count_' or 'repeat_count_' to the size of the next literal
  /// or repeated run, or leaves both at 0 if no more values can be read (either because
  /// the end of the input was reached or an error was encountered decoding).
  void NextCounts();

  /// Fill the literal buffer. Invalid to call if there are already buffered literals.
  /// Return false if the input was truncated. This does not advance 'literal_count_'.
  bool FillLiteralBuffer() WARN_UNUSED_RESULT;

  bool HaveBufferedLiterals() const {
    return literal_buffer_pos_ < num_buffered_literals_;
  }

  /// Output buffered literals, advancing 'literal_buffer_pos_' and decrementing
  /// 'literal_count_'. Returns the number of literals outputted.
  int32_t OutputBufferedLiterals(int32_t max_to_output, T* values);

  /// Skip buffered literals
  int32_t SkipBufferedLiterals(int32_t max_to_skip);

  /// Output buffered literals, advancing 'literal_buffer_pos_' and decrementing
  /// 'literal_count_'. Returns the number of literals outputted or 0 if a
  /// decoding error is encountered.
  template <typename OutType>
  int32_t DecodeBufferedLiterals(int32_t max_to_output, OutType* dict, int64_t dict_len,
      StrideWriter<OutType>* RESTRICT out);
};

/// Class to incrementally build the rle data. This class does not allocate any memory.
/// The encoding has two modes: encoding repeated runs and literal runs.
/// If the run is sufficiently short, it is more efficient to encode as a literal run.
/// This class does this by buffering a number of values at a time in a circular buffer.
/// If the values are not all the same then they are added to the literal run.
/// If the values are all the same, they are added to the repeated run.
/// When we switch modes, the previous run is flushed out.
/// When the buffer is full, then some multiple of eight data values must be flushed to
/// the BitWriter. If buffering more than eight values, and if there is a current run of
/// repeated values, then only eight values are flushed, and the start of the circular
/// buffer moves forward by eight.
class RleEncoder {
 public:
  /// buffer/buffer_len: preallocated output buffer.
  /// bit_width: max number of bits for value.
  /// min_repeated_run_length: the smallest number of repeated values that will be used
  /// for run length encoding. If this is zero, which is the required choice for non-test
  /// code, then a default length will be chosen, based on the bit_width.
  /// min_repeated_run_length must be a multiple of 8 and less than or equal to
  /// MAX_RUN_LENGTH_BUFFER.
  /// TODO: allow 0 bit_width (and have dict encoder use it)
  RleEncoder(
      uint8_t* buffer, int buffer_len, int bit_width, int min_repeated_run_length = 0)
    : bit_width_(bit_width),
      bit_writer_(buffer, buffer_len),
      min_repeated_run_length_(min_repeated_run_length) {
    DCHECK_GE(bit_width_, 0);
    DCHECK_LE(bit_width_, 64);
    if (min_repeated_run_length == 0) {
      // Choose a good value for min_repeated_run_length_, based on the bit_width.
      min_repeated_run_length_ = DefaultRunLength(bit_width);
    } else {
      DCHECK(TestInfo::is_test()) << "Only tests should specify min_repeated_run_length";
    }
    DCHECK_EQ(min_repeated_run_length_ % 8, 0);
    DCHECK_LE(min_repeated_run_length_, MAX_RUN_LENGTH_BUFFER);
    DCHECK_GT(min_repeated_run_length_, 0);
    max_run_byte_size_ = MinBufferSize(bit_width);
    DCHECK_GE(buffer_len, max_run_byte_size_) << "Input buffer not big enough.";
    Clear();
  }

  /// Returns the minimum buffer size needed to use the encoder for 'bit_width'.
  /// This is the maximum length that RleEncoder can fill when it finally flushes a
  /// literal run (which is when it checks if the buffer is full).
  /// It is not valid to pass a buffer less than this length.
  static int MinBufferSize(int bit_width) {
    DCHECK_EQ(MAX_VALUES_PER_LITERAL_RUN % 8, 0);
    // 1 indicator byte +
    // the size in bytes to hold the longest possible literal run of 'bit_width' values +
    // the maximum size in bytes required to flush any buffered values after a partial
    // write.
    int max_literal_run_size = 1 + ((MAX_VALUES_PER_LITERAL_RUN * bit_width) / 8)
        + (MAX_RUN_LENGTH_BUFFER / 8) - 1;
    // Up to MAX_VLQ_BYTE_LEN bytes for the indicator, and a single 'bit_width' value.
    int max_repeated_run_size = static_cast<int>(
        BatchedBitReader::MAX_VLQ_BYTE_LEN + BitUtil::Ceil(bit_width, 8));
    return std::max(max_literal_run_size, max_repeated_run_size);
  }

  /// Returns the maximum byte size it could take to encode 'num_values' of 'bit_width'.
  static int MaxBufferSize(int bit_width, int num_values) {
    if (bit_width > 2) {
      // The largest encoded size is for all long literals with no repeated runs.
      int bytes_per_run = (bit_width * MAX_VALUES_PER_LITERAL_RUN) / 8;
      int num_runs =
          static_cast<int>(BitUtil::Ceil(num_values, MAX_VALUES_PER_LITERAL_RUN));
      int literal_max_size = num_runs * (1 + bytes_per_run);
      return std::max(MinBufferSize(bit_width), literal_max_size);
    }
    int encoded_size;
    int num_bytes = static_cast<int>(BitUtil::Ceil(num_values * bit_width, 8));
    if (bit_width == 1) {
      // Worst case is we use 4 bytes for every 3 of the input e.g. for L8 R16 L8 R16 L8.
      encoded_size = 1 + static_cast<int>(BitUtil::Ceil(num_bytes * 4, 3));
    } else { // bit_width == 2
      // Worst case is we use 3 bytes for every 2 of the input e.g. for L8 R8 L8 R8 L8.
      encoded_size = 1 + static_cast<int>(BitUtil::Ceil(num_bytes * 3, 2));
    }
    return std::max(MinBufferSize(bit_width), encoded_size);
  }

  /// Returns the best value for min_repeated_run_length for the given bit_width.
  static int DefaultRunLength(int bit_width) {
    if (bit_width == 1) {
      // Using min_repeated_run_length=16 is never worse than 8.
      // There are cases where min_repeated_run_length=24 is better than 16, but this
      // depends on the data that is being encoded.
      return 16;
    }
    // For bit_width=2 there are cases where min_repeated_run_length=16 is better than 8,
    // but this depends on the data that is being encoded.

    // For other bit widths, any run length of 8 is more efficient than a literal
    // encoding.
    return 8;
  }

  /// Encode value.  Returns true if the value fits in buffer, false otherwise.
  /// This value must be representable with bit_width_ bits.
  bool Put(uint64_t value) WARN_UNUSED_RESULT;

  /// Flushes any pending values to the underlying buffer.
  /// Returns the total number of bytes written
  int Flush();

  /// Resets all the state in the encoder.
  void Clear();

  /// Returns pointer to underlying buffer
  uint8_t* buffer() { return bit_writer_.buffer(); }
  int32_t len() { return bit_writer_.bytes_written(); }
  bool buffer_full() const { return buffer_full_; }

  /// The maximum number of values that can be buffered by RleEncoder.
  /// The actual number of values that is buffered is the value of
  /// min_repeated_run_length_.
  static const int MAX_RUN_LENGTH_BUFFER = 16;

 private:
  /// Flushes any buffered values.  If this is part of a repeated run, this is largely
  /// a no-op.
  /// If it is part of a literal run, this will call FlushLiteralRun, which writes
  /// out the buffered literal values.
  void FlushBufferedValues();

  /// Flushes literal values to the underlying buffer.
  /// update_indicator_byte: if set then the current literal run is complete and the
  /// indicator byte is updated.
  /// count_to_flush: number of value to flush from the start of the buffer.
  /// This can be 0.
  void FlushLiteralRun(bool update_indicator_byte, int count_to_flush);

  /// Flushes a repeated run to the underlying buffer.
  void FlushRepeatedRun();

  /// Checks and sets buffer_full_. This must be called after flushing a run to
  /// make sure there are enough bytes remaining to encode the next run.
  void CheckBufferFull();

  /// Flush out 8 values from the buffer.
  /// This may flush the whole buffer, or just part of it.
  void FlushEightValues();

  /// Get the offset into buffered_values_ by offsetting from index by start_.
  int BufferOffset(int index);

  /// The maximum number of groups encodable by a 1-byte indicator.
  static const int MAX_ENCODABLE_GROUPS = 1 << 6;

  /// The maximum number of values in a single literal run
  static const int MAX_VALUES_PER_LITERAL_RUN = MAX_ENCODABLE_GROUPS * 8;

  /// Number of bits needed to encode the value. Must be between 0 and 64.
  const int bit_width_;

  /// Underlying buffer.
  BitWriter bit_writer_;

  /// If true, the buffer is full and subsequent Put()'s will fail.
  bool buffer_full_;

  /// The maximum byte size a single run can take.
  int max_run_byte_size_;

  /// We need to buffer at most MAX_RUN_LENGTH_BUFFER values to detect runs that can be
  /// encoded.
  /// This is a circular buffer. The current front of the buffer is at index start_.
  /// The amount of the buffer that can be used is the value of min_repeated_run_length_.
  uint64_t buffered_values_[MAX_RUN_LENGTH_BUFFER];

  /// Number of values in buffered_values_
  int num_buffered_values_;

  /// The offset in buffered_values_ that is the current start of the buffer.
  int start_;

  /// The current (also last) value that was written.
  int64_t current_value_;

  /// The count of how many times in a row that current_value_ has been seen.
  /// This is maintained even if we are in a literal run.
  /// If the repeat_count_ gets high enough, then we switch to encoding repeated runs.
  int repeat_count_;

  /// Number of literals in the current run.  This does not include the literals
  /// that might be in buffered_values_.  Only after we've got a group big enough
  /// can we decide if they should part of the literal_count_ or repeat_count_
  int literal_count_;

  /// Pointer to a byte in the underlying buffer that stores the indicator byte.
  /// This is reserved as soon as we need a literal run but the value is written
  /// when the literal run is complete.
  uint8_t* literal_indicator_byte_;

  /// The length of buffered_values_ that is used to buffer values in an attempt
  /// to accumulate repeated values to encode as runs.
  int min_repeated_run_length_;
};

/// This function buffers input values until it has accumulated a number equal to
/// min_repeated_run_length_. After seeing all min_repeated_run_length_ values, it
/// decides whether they should be encoded as a literal or as a repeated run.
/// When buffering more than 8 values, RleEncoder uses a circular buffer to store data,
/// so that runs of length greater than 8 can be detected. In this case, when the buffer
/// is full, then 8 items of data are flushed, and the logical start of the buffer
/// (start_) moves ahead by 8 through the circular buffer.
inline bool RleEncoder::Put(uint64_t value) {
  DCHECK(bit_width_ == 64 || value < (1LL << bit_width_));
  if (UNLIKELY(buffer_full_)) return false;

  if (LIKELY(current_value_ == value
      && repeat_count_ <= std::numeric_limits<int32_t>::max())) {
    ++repeat_count_;
    if (repeat_count_ > min_repeated_run_length_) {
      // This is just a continuation of the current run, no need to buffer the
      // values as buffered_values_ is already full.
      // Note that this is the fast path for long repeated runs.
      return true;
    }
  } else {
    if (repeat_count_ >= min_repeated_run_length_) {
      // We had a run that was long enough but it ended, either because of a different
      // value or because it exceeded the maximum run length. Flush the current repeated
      // run.
      DCHECK_EQ(literal_count_, 0);
      FlushRepeatedRun();
    }
    repeat_count_ = 1;
    current_value_ = value;
  }

  buffered_values_[BufferOffset(num_buffered_values_)] = value;
  if (++num_buffered_values_ == min_repeated_run_length_) {
    DCHECK_EQ(literal_count_ % 8, 0);
    // The buffer is full, so we need to flush something.
    FlushBufferedValues();
  }
  return true;
}

inline void RleEncoder::FlushLiteralRun(bool update_indicator_byte, int count_to_flush) {
  if (literal_indicator_byte_ == NULL) {
    // The literal indicator byte has not been reserved yet, get one now.
    literal_indicator_byte_ = bit_writer_.GetNextBytePtr();
    DCHECK(literal_indicator_byte_ != NULL);
  }

  // Write all the buffered values as bit packed literals
  for (int i = 0; i < count_to_flush; ++i) {
    bool success = bit_writer_.PutValue(buffered_values_[BufferOffset(i)], bit_width_);
    DCHECK(success) << "There is a bug in using CheckBufferFull()";
  }
  num_buffered_values_ -= count_to_flush;

  if (update_indicator_byte) {
    // At this point we need to write the indicator byte for the literal run.
    // We only reserve one byte, to allow for streaming writes of literal values.
    // The logic makes sure we flush literal runs often enough to not overrun
    // the 1 byte.
    DCHECK_EQ(literal_count_ % 8, 0);
    int num_groups = literal_count_ / 8;
    int32_t indicator_value = (num_groups << 1) | 1;
    DCHECK_EQ(indicator_value & 0xFFFFFF00, 0);
    *literal_indicator_byte_ = static_cast<uint8_t>(indicator_value);
    literal_indicator_byte_ = NULL;
    literal_count_ = 0;
    CheckBufferFull();
  }
}

inline void RleEncoder::FlushRepeatedRun() {
  DCHECK_GT(repeat_count_, 0) << "repeat_count_=" << repeat_count_
                              << " min_repeated_run_length_=" << min_repeated_run_length_;
  bool result = true;
  // The lsb of 0 indicates this is a repeated run
  uint32_t indicator_value = static_cast<uint32_t>(repeat_count_) << 1;
  result &= bit_writer_.PutUleb128Int(indicator_value);
  result &= bit_writer_.PutAligned(
      current_value_, static_cast<int>(BitUtil::Ceil(bit_width_, 8)));
  DCHECK(result);
  num_buffered_values_ = 0;
  repeat_count_ = 0;
  CheckBufferFull();
}

inline void RleEncoder::FlushEightValues() {
  literal_count_ += 8;
  DCHECK_EQ(literal_count_ % 8, 0);
  int num_groups = literal_count_ / 8;
  DCHECK_LE(num_groups + 1, MAX_ENCODABLE_GROUPS) << "The Literal run is too large";
  if (num_groups + 1 == MAX_ENCODABLE_GROUPS) {
    // We need to start a new literal run because the indicator byte we've reserved
    // cannot store more values.
    DCHECK(literal_indicator_byte_ != NULL);
    FlushLiteralRun(true, 8); // updates num_buffered_values_
  } else {
    FlushLiteralRun(false, 8); // updates num_buffered_values_
  }

  // Move start_ (the current offset into buffered_values_) on by 8.
  start_ = BufferOffset(8);

  if (repeat_count_ > num_buffered_values_) {
    repeat_count_ = num_buffered_values_;
  }
}

/// Flush some values that have been buffered.  At this point we decide whether
/// we need to switch between the run types or continue the current one.
inline void RleEncoder::FlushBufferedValues() {
  if (repeat_count_ >= min_repeated_run_length_) {
    // The buffer is entirely full of repeated values.
    // Clear the buffered values.  They are part of the repeated run now and we
    // don't want to flush them out as literals.
    num_buffered_values_ = 0;
    if (literal_count_ != 0) {
      // There was a current literal run.  All the values in it have been flushed
      // but we still need to update the indicator byte.
      DCHECK_EQ(literal_count_ % 8, 0);
      DCHECK_EQ(repeat_count_, min_repeated_run_length_);
      FlushLiteralRun(true, 0 /* num_buffered_values_ */);
    }
    DCHECK_EQ(literal_count_, num_buffered_values_);
    return;
  }
  // The buffer is full, but there is a current run of repeated values, but we do not yet
  // have enough for an encoded run. Flush out eight values.
  FlushEightValues();
}

inline int RleEncoder::BufferOffset(int index) {
  return (index + start_) % min_repeated_run_length_;
}

inline int RleEncoder::Flush() {
  if (literal_count_ > 0 || repeat_count_ > 0 || num_buffered_values_ > 0) {
    bool all_repeat = literal_count_ == 0 &&
        (repeat_count_ == num_buffered_values_ || num_buffered_values_ == 0);
    // There is something pending, figure out if it's a repeated or literal run
    if (repeat_count_ > 0 && all_repeat) {
      FlushRepeatedRun();
    } else  {
      DCHECK_EQ(literal_count_ % 8, 0);
      // Pad the last group of literals by adding 0's up to the byte boundary.
      while (num_buffered_values_ % 8 != 0) {
        buffered_values_[BufferOffset(num_buffered_values_++)] = 0;
      }
      DCHECK_EQ(num_buffered_values_ % 8, 0);

      // Flush out all of the buffer.
      int num_flushes = num_buffered_values_ / 8;
      for (int flush = 0; flush < num_flushes; flush++) {
        FlushEightValues();
      }
      // Force the end of any exiting literal run.
      FlushLiteralRun(true, 0);
      repeat_count_ = 0;
    }
  }
  bit_writer_.Flush();
  DCHECK_EQ(num_buffered_values_, 0);
  DCHECK_EQ(literal_count_, 0);
  DCHECK_EQ(repeat_count_, 0);

  return bit_writer_.bytes_written();
}

inline void RleEncoder::CheckBufferFull() {
  int bytes_written = bit_writer_.bytes_written();
  DCHECK_EQ(num_buffered_values_ % 8, 0);
  // Is there enough space that we would be able to flush all buffered values
  // plus the longest possible run we cold write? If not then we must refuse more data.
  if (bytes_written + (num_buffered_values_ / 8) + max_run_byte_size_
      > bit_writer_.buffer_len()) {
    buffer_full_ = true;
  }
}

inline void RleEncoder::Clear() {
  buffer_full_ = false;
  current_value_ = 0;
  repeat_count_ = 0;
  num_buffered_values_ = 0;
  literal_count_ = 0;
  start_ = 0;
  literal_indicator_byte_ = NULL;
  bit_writer_.Clear();
}

template <typename T>
inline void RleBatchDecoder<T>::Reset(uint8_t* buffer, int buffer_len, int bit_width) {
  DCHECK(buffer != nullptr);
  DCHECK_GE(buffer_len, 0);
  DCHECK_GE(bit_width, 0);
  DCHECK_LE(bit_width, BatchedBitReader::MAX_BITWIDTH);
  bit_reader_.Reset(buffer, buffer_len);
  bit_width_ = bit_width;
  repeat_count_ = 0;
  literal_count_ = 0;
  num_buffered_literals_ = 0;
  literal_buffer_pos_ = 0;
}

template <typename T>
inline int32_t RleBatchDecoder<T>::NextNumRepeats() {
  if (repeat_count_ > 0) return repeat_count_;
  if (literal_count_ == 0) NextCounts();
  return repeat_count_;
}

template <typename T>
inline T RleBatchDecoder<T>::GetRepeatedValue(int32_t num_repeats_to_consume) {
  DCHECK_GE(num_repeats_to_consume, 0);
  DCHECK_GE(repeat_count_, num_repeats_to_consume);
  repeat_count_ -= num_repeats_to_consume;
  return repeated_value_;
}

template <typename T>
inline void RleBatchDecoder<T>::SkipRepeatedValues(int32_t num_values) {
  DCHECK_GT(num_values, 0);
  DCHECK_GE(repeat_count_, num_values);
  repeat_count_ -= num_values;
}

template <typename T>
inline int32_t RleBatchDecoder<T>::NextNumLiterals() {
  if (literal_count_ > 0) return literal_count_;
  if (repeat_count_ == 0) NextCounts();
  return literal_count_;
}

template <typename T>
inline bool RleBatchDecoder<T>::GetLiteralValues(
    int32_t num_literals_to_consume, T* values) {
  DCHECK_GE(num_literals_to_consume, 0);
  DCHECK_GE(literal_count_, num_literals_to_consume);
  int32_t num_consumed = 0;
  // Copy any buffered literals left over from previous calls.
  if (HaveBufferedLiterals()) {
    num_consumed = OutputBufferedLiterals(num_literals_to_consume, values);
  }

  int32_t num_remaining = num_literals_to_consume - num_consumed;
  // Copy literals directly to the output, bypassing 'literal_buffer_' when possible.
  // Need to round to a batch of 32 if the caller is consuming only part of the current
  // run avoid ending on a non-byte boundary.
  int32_t num_to_bypass = std::min<int32_t>(literal_count_,
      BitUtil::RoundDownToPowerOf2(num_remaining, 32));
  if (num_to_bypass > 0) {
    int num_read =
        bit_reader_.UnpackBatch(bit_width_, num_to_bypass, values + num_consumed);
    // If we couldn't read the expected number, that means the input was truncated.
    if (num_read < num_to_bypass) return false;
    literal_count_ -= num_to_bypass;
    num_consumed += num_to_bypass;
    num_remaining = num_literals_to_consume - num_consumed;
  }

  if (num_remaining > 0) {
    // We weren't able to copy all the literals requested directly from the input.
    // Buffer literals and copy over the requested number.
    if (UNLIKELY(!FillLiteralBuffer())) return false;
    int32_t num_copied = OutputBufferedLiterals(num_remaining, values + num_consumed);
    DCHECK_EQ(num_copied, num_remaining) << "Should have buffered enough literals";
  }
  return true;
}

template <typename T>
inline bool RleBatchDecoder<T>::SkipLiteralValues(int32_t num_literals_to_skip) {
  DCHECK_GT(num_literals_to_skip, 0);
  DCHECK_GE(literal_count_, num_literals_to_skip);
  DCHECK(!HaveBufferedLiterals());

  int32_t num_remaining = num_literals_to_skip;

  // Need to round to a batch of 32 if the caller is skipping only part of the current
  // run to avoid ending on a non-byte boundary.
  int32_t num_to_skip = std::min<int32_t>(literal_count_,
      BitUtil::RoundDownToPowerOf2(num_remaining, 32));
  if (num_to_skip > 0) {
    bit_reader_.SkipBatch(bit_width_, num_to_skip);
    literal_count_ -= num_to_skip;
    num_remaining -= num_to_skip;
  }

  if (num_remaining > 0) {
    // Earlier we called RoundDownToPowerOf2() to skip literals that fit on byte boundary.
    // But some literals still need to be skipped. Let's fill the literal buffer
    // and skip 'num_remaining' values.
    if (UNLIKELY(!FillLiteralBuffer())) return false;
    if (SkipBufferedLiterals(num_remaining) != num_remaining) return false;
  }
  return true;
}

template <typename T>
template <typename OutType>
inline bool RleBatchDecoder<T>::DecodeLiteralValues(int32_t num_literals_to_consume,
    OutType* dict, int64_t dict_len, StrideWriter<OutType>* RESTRICT out) {
  DCHECK_GT(num_literals_to_consume, 0);
  DCHECK_GE(literal_count_, num_literals_to_consume);

  if (num_literals_to_consume == 0) return false;

  int32_t num_remaining = num_literals_to_consume;
  // Decode any buffered literals left over from previous calls.
  if (HaveBufferedLiterals()) {
    int32_t num_consumed = DecodeBufferedLiterals(num_remaining, dict, dict_len, out);
    if (UNLIKELY(num_consumed == 0)) return false;
    DCHECK_LE(num_consumed, num_remaining);
    num_remaining -= num_consumed;
  }

  // Copy literals directly to the output, bypassing 'literal_buffer_' when possible.
  // Need to round to a batch of 32 if the caller is consuming only part of the current
  // run avoid ending on a non-byte boundery.
  int32_t num_to_bypass =
      std::min<int32_t>(literal_count_, BitUtil::RoundDownToPowerOf2(num_remaining, 32));
  if (num_to_bypass > 0) {
    int num_read = bit_reader_.UnpackAndDecodeBatch(
        bit_width_, dict, dict_len, num_to_bypass, out->current, out->stride);
    // If we couldn't read the expected number, that means the input was truncated.
    if (num_read < num_to_bypass) return false;
    DCHECK_EQ(num_read, num_to_bypass);
    literal_count_ -= num_to_bypass;
    out->SkipNext(num_to_bypass);
    num_remaining -= num_to_bypass;
  }

  if (num_remaining > 0) {
    // We weren't able to copy all the literals requested directly from the input.
    // Buffer literals and copy over the requested number.
    if (UNLIKELY(!FillLiteralBuffer())) return false;
    int32_t num_copied = DecodeBufferedLiterals(num_remaining, dict, dict_len, out);
    if (UNLIKELY(num_copied == 0)) return false;
    DCHECK_EQ(num_copied, num_remaining) << "Should have buffered enough literals";
  }
  return true;
}

template <typename T>
inline bool RleBatchDecoder<T>::GetSingleValue(T* val) {
  if (NextNumRepeats() > 0) {
    DCHECK_EQ(0, NextNumLiterals());
    *val = GetRepeatedValue(1);
    return true;
  }
  if (NextNumLiterals() > 0) {
    DCHECK_EQ(0, NextNumRepeats());
    return GetLiteralValues(1, val);
  }
  return false;
}

template <typename T>
inline void RleBatchDecoder<T>::NextCounts() {
  DCHECK_GE(bit_width_, 0) << "RleBatchDecoder must be initialised";
  DCHECK_EQ(0, literal_count_);
  DCHECK_EQ(0, repeat_count_);
  // Read the next run's indicator int, it could be a literal or repeated run.
  // The int is encoded as a ULEB128-encoded value.
  uint32_t indicator_value = 0;
  if (UNLIKELY(!bit_reader_.GetUleb128Int(&indicator_value))) return;

  // lsb indicates if it is a literal run or repeated run
  bool is_literal = indicator_value & 1;
  // Don't try to handle run lengths that don't fit in an int32_t - just fail gracefully.
  // The Parquet standard does not allow longer runs - see PARQUET-1290.
  uint32_t run_len = indicator_value >> 1;
  DCHECK_LE(run_len, std::numeric_limits<int32_t>::max())
      << "Right-shifted uint32_t should fit in int32_t";
  if (is_literal) {
    // Use int64_t to avoid overflowing multiplication.
    int64_t literal_count = static_cast<int64_t>(run_len) * 8;
    if (UNLIKELY(literal_count > std::numeric_limits<int32_t>::max())) return;
    literal_count_ = literal_count;
  } else {
    if (UNLIKELY(run_len == 0)) return;
    bool result = bit_reader_.GetBytes<T>(BitUtil::Ceil(bit_width_, 8), &repeated_value_);
    if (UNLIKELY(!result)) return;
    repeat_count_ = run_len;
    DCHECK_GE(repeat_count_, 0);
  }
}

template <typename T>
inline bool RleBatchDecoder<T>::FillLiteralBuffer() {
  DCHECK(!HaveBufferedLiterals());
  int32_t num_to_buffer = std::min<int32_t>(LITERAL_BUFFER_LEN, literal_count_);
  num_buffered_literals_ =
      bit_reader_.UnpackBatch(bit_width_, num_to_buffer, literal_buffer_);
  // If we couldn't read the expected number, that means the input was truncated.
  if (UNLIKELY(num_buffered_literals_ < num_to_buffer)) return false;
  literal_buffer_pos_ = 0;
  return true;
}

template <typename T>
inline int32_t RleBatchDecoder<T>::OutputBufferedLiterals(
    int32_t max_to_output, T* values) {
  int32_t num_to_output =
      std::min<int32_t>(max_to_output, num_buffered_literals_ - literal_buffer_pos_);
  memcpy(values, &literal_buffer_[literal_buffer_pos_], sizeof(T) * num_to_output);
  literal_buffer_pos_ += num_to_output;
  literal_count_ -= num_to_output;
  return num_to_output;
}

template <typename T>
inline int32_t RleBatchDecoder<T>::SkipBufferedLiterals(
    int32_t max_to_skip) {
  int32_t num_to_skip =
      std::min<int32_t>(max_to_skip, num_buffered_literals_ - literal_buffer_pos_);
  literal_buffer_pos_ += num_to_skip;
  literal_count_ -= num_to_skip;
  return num_to_skip;
}

template <typename T>
template <typename OutType>
inline int32_t RleBatchDecoder<T>::DecodeBufferedLiterals(int32_t max_to_output,
    OutType* dict, int64_t dict_len, StrideWriter<OutType>* RESTRICT out) {
  int32_t num_to_output =
      std::min<int32_t>(max_to_output, num_buffered_literals_ - literal_buffer_pos_);
  for (int32_t i = 0; i < num_to_output; ++i) {
    T idx = literal_buffer_[literal_buffer_pos_ + i];
    if (UNLIKELY(idx < 0 || idx >= dict_len)) return 0;
    out->SetNext(dict[idx]);
  }
  literal_buffer_pos_ += num_to_output;
  literal_count_ -= num_to_output;
  return num_to_output;
}

template <typename T>
inline int32_t RleBatchDecoder<T>::GetValues(int32_t num_values_to_consume, T* values) {
  DCHECK_GT(num_values_to_consume, 0);
  DCHECK(values != nullptr);

  int32_t num_consumed = 0;
  while (num_consumed < num_values_to_consume) {
    // Add RLE encoded values by repeating the current value this number of times.
    int32_t num_repeats = NextNumRepeats();
    if (num_repeats > 0) {
       int32_t num_repeats_to_set =
           std::min(num_repeats, num_values_to_consume - num_consumed);
       T repeated_value = GetRepeatedValue(num_repeats_to_set);
       for (int i = 0; i < num_repeats_to_set; ++i) {
         values[num_consumed + i] = repeated_value;
       }
       num_consumed += num_repeats_to_set;
       continue;
    }

    // Add remaining literal values, if any.
    int32_t num_literals = NextNumLiterals();
    if (num_literals == 0) break;
    int32_t num_literals_to_set =
        std::min(num_literals, num_values_to_consume - num_consumed);
    if (!GetLiteralValues(num_literals_to_set, values + num_consumed)) {
      return 0;
    }
    num_consumed += num_literals_to_set;
  }
  return num_consumed;
}

template <typename T>
inline int32_t RleBatchDecoder<T>::SkipValues(int32_t num_values) {
  DCHECK_GT(num_values, 0);

  int32_t num_skipped = 0;
  if (HaveBufferedLiterals()) {
    num_skipped = SkipBufferedLiterals(num_values);
  }
  while (num_skipped < num_values) {
    // Skip RLE encoded values
    int32_t num_repeats = NextNumRepeats();
    if (num_repeats > 0) {
       int32_t num_repeats_to_consume = std::min(num_repeats, num_values - num_skipped);
       SkipRepeatedValues(num_repeats_to_consume);
       num_skipped += num_repeats_to_consume;
       continue;
    }

    // Skip literals
    int32_t num_literals = NextNumLiterals();
    if (num_literals == 0) break;
    int32_t num_literals_to_skip = std::min(num_literals, num_values - num_skipped);
    if (!SkipLiteralValues(num_literals_to_skip)) return 0;
    num_skipped += num_literals_to_skip;
  }
  return num_skipped;
}

template <typename T>
constexpr int RleBatchDecoder<T>::LITERAL_BUFFER_LEN;
}

#endif
