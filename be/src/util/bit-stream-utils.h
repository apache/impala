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


#ifndef IMPALA_UTIL_BIT_STREAM_UTILS_H
#define IMPALA_UTIL_BIT_STREAM_UTILS_H

#include <boost/cstdint.hpp>
#include <string.h>
#include "common/compiler-util.h"
#include "common/logging.h"
#include "util/bit-util.h"

namespace impala {

/// Utility class to write bit/byte streams.  This class can write data to either be
/// bit packed or byte aligned (and a single stream that has a mix of both).
/// This class does not allocate memory.
class BitWriter {
 public:
  /// buffer: buffer to write bits to.  Buffer should be preallocated with
  /// 'buffer_len' bytes.
  BitWriter(uint8_t* buffer, int buffer_len) :
      buffer_(buffer),
      max_bytes_(buffer_len) {
    Clear();
  }

  void Clear() {
    buffered_values_ = 0;
    byte_offset_ = 0;
    bit_offset_ = 0;
  }

  /// The number of current bytes written, including the current byte (i.e. may include a
  /// fraction of a byte). Includes buffered values.
  int bytes_written() const { return byte_offset_ + BitUtil::Ceil(bit_offset_, 8); }
  uint8_t* buffer() const { return buffer_; }
  int buffer_len() const { return max_bytes_; }

  /// Writes a value to buffered_values_, flushing to buffer_ if necessary.  This is bit
  /// packed.  Returns false if there was not enough space. num_bits must be <= 32.
  bool PutValue(uint64_t v, int num_bits);

  /// Writes v to the next aligned byte using num_bytes. If T is larger than num_bytes, the
  /// extra high-order bytes will be ignored. Returns false if there was not enough space.
  template<typename T>
  bool PutAligned(T v, int num_bytes);

  /// Write an unsigned ULEB-128 encoded int to the buffer. Return false if there was not
  /// enough room. The value is written byte aligned. For more details on ULEB-128:
  /// https://en.wikipedia.org/wiki/LEB128
  bool PutUleb128Int(uint32_t v);

  /// Get a pointer to the next aligned byte and advance the underlying buffer
  /// by num_bytes.
  /// Returns NULL if there was not enough space.
  uint8_t* GetNextBytePtr(int num_bytes = 1);

  /// Flushes all buffered values to the buffer. Call this when done writing to the buffer.
  /// If 'align' is true, buffered_values_ is reset and any future writes will be written
  /// to the next byte boundary.
  void Flush(bool align=false);

  /// Maximum supported bitwidth for writer.
  static const int MAX_BITWIDTH = 32;

 private:
  uint8_t* buffer_;
  int max_bytes_;

  /// Bit-packed values are initially written to this variable before being memcpy'd to
  /// buffer_. This is faster than writing values byte by byte directly to buffer_.
  uint64_t buffered_values_;

  int byte_offset_;       // Offset in buffer_
  int bit_offset_;        // Offset in buffered_values_
};

/// Utility class to read bit/byte stream. This class can read bits or bytes that are
/// either byte aligned or not. It also has utilities to read multiple bytes in one
/// read (e.g. encoded int). Exposes a batch-oriented interface to allow efficient
/// processing of multiple values at a time.
class BatchedBitReader {
 public:
  /// 'buffer' is the buffer to read from.  The buffer's length is 'buffer_len'.
  /// Does not take ownership of the buffer.
  BatchedBitReader(const uint8_t* buffer, int64_t buffer_len) {
    Reset(buffer, buffer_len);
  }

  BatchedBitReader() {}

  // The implicit copy constructor is left defined. If a BatchedBitReader is copied, the
  // two copies do not share any state. Invoking functions on either copy continues
  // reading from the current read position without modifying the state of the other
  // copy.

  /// Resets the read to start reading from the start of 'buffer'. The buffer's
  /// length is 'buffer_len'. Does not take ownership of the buffer.
  void Reset(const uint8_t* buffer, int64_t buffer_len) {
    DCHECK(buffer != nullptr);
    DCHECK_GE(buffer_len, 0);
    buffer_pos_ = buffer;
    buffer_end_ = buffer + buffer_len;
  }

  /// Gets up to 'num_values' bit-packed values, starting from the current byte in the
  /// buffer and advance the read position. 'bit_width' must be <= 32.
  /// If 'bit_width' * 'num_values' is not a multiple of 8, the trailing bytes are
  /// skipped and the next UnpackBatch() call will start reading from the next byte.
  ///
  /// If the caller does not want to drop trailing bits, 'num_values' must be exactly the
  /// total number of values the caller wants to read from a run of bit-packed values, or
  /// 'bit_width' * 'num_values' must be a multiple of 8. This condition is always
  /// satisfied if 'num_values' is a multiple of 32.
  ///
  /// Returns the number of values read.
  template<typename T>
  int UnpackBatch(int bit_width, int num_values, T* v);

  /// Skip 'num_values_to_skip' bit-packed values.
  /// 'num_values_to_skip * bit_width' is either divisible by 8, or
  /// 'num_values_to_skip' equals to the count of the remaining bit-packed values.
  bool SkipBatch(int bit_width, int num_values_to_skip);

  /// Unpack bit-packed values in the same way as UnpackBatch() and decode them using the
  /// dictionary 'dict' with 'dict_len' entries. Return -1 if a decoding error is
  /// encountered, i.e. if the bit-packed values are not valid indices in 'dict'.
  /// Otherwise returns the number of values decoded. The values are written to 'v' with
  /// a stride of 'stride' bytes.
  template <typename T>
  int UnpackAndDecodeBatch(
      int bit_width, T* dict, int64_t dict_len, int num_values, T* v, int64_t stride);

  /// Reads an unpacked 'num_bytes'-sized value from the buffer and stores it in 'v'. T
  /// needs to be a little-endian native type and big enough to store 'num_bytes'.
  /// Returns false if there are not enough bytes left.
  template<typename T>
  bool GetBytes(int num_bytes, T* v);

  /// Read an unsigned ULEB-128 encoded int from the stream. The encoded int must start
  /// at the beginning of a byte. Return false if there were not enough bytes in the
  /// buffer or the int is invalid. For more details on ULEB-128:
  /// https://en.wikipedia.org/wiki/LEB128
  bool GetUleb128Int(uint32_t* v);

  /// Returns the number of bytes left in the stream.
  int bytes_left() { return buffer_end_ - buffer_pos_; }

  /// Maximum byte length of a vlq encoded int
  static const int MAX_VLQ_BYTE_LEN = 5;

  /// Maximum supported bitwidth for reader.
  static const int MAX_BITWIDTH = 32;

 private:
  /// Current read position in the buffer.
  const uint8_t* buffer_pos_ = nullptr;

  /// Pointer to the byte after the end of the buffer.
  const uint8_t* buffer_end_ = nullptr;
};
}

#endif
