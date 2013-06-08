// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef IMPALA_EXPERIMENTS_BIT_STREAM_UTILS_8BYTE_H
#define IMPALA_EXPERIMENTS_BIT_STREAM_UTILS_8BYTE_H

#include <boost/cstdint.hpp>
#include <string.h>
#include "common/compiler-util.h"
#include "common/logging.h"
#include "util/bit-util.h"

namespace impala {

// Utility class to write bit/byte streams.  This class can write data to either be
// bit packed or byte aligned (and a single stream that has a mix of both).
// This class does not allocate memory.
class BitWriter_8byte {
 public:
  // buffer: buffer to write bits to.  Buffer should be preallocated with
  // 'buffer_len' bytes. 'buffer_len' must be a multiple of 8.
  BitWriter_8byte(uint8_t* buffer, int buffer_len) :
      buffer_(reinterpret_cast<uint64_t*>(buffer)),
      max_bytes_(buffer_len),
      offset_(0),
      bit_offset_(0) {
    DCHECK_EQ(buffer_len % 8, 0);
  }

  void Clear() {
    offset_ = 0;
    bit_offset_ = 0;
    memset(buffer_, 0, max_bytes_);
  }

  uint8_t* buffer() const { return reinterpret_cast<uint8_t*>(buffer_); }
  int buffer_len() const { return max_bytes_; }

  inline int bytes_written() const {
    return offset_ * 8 + BitUtil::Ceil(bit_offset_, 8);
  }

  // Writes a value to the buffer.  This is bit packed.  Returns false if
  // there was not enough space.
  bool PutValue(uint64_t v, int num_bits);

  // Writes v to the next aligned byte.
  template<typename T>
  bool PutAligned(T v, int num_bits);

  // Write a Vlq encoded int to the buffer.  Returns false if there was not enough
  // room.  The value is written byte aligned.
  // For more details on vlq:
  // en.wikipedia.org/wiki/Variable-length_quantity
  bool PutVlqInt(int32_t v);

  // Get a pointer to the next aligned byte and advance the underlying buffer
  // by num_bytes.
  // Returns NULL if there was not enough space.
  uint8_t* GetNextBytePtr(int num_bytes = 1);

 private:
  uint64_t* buffer_;
  int max_bytes_;
  int offset_;            // Offset into buffer_
  int bit_offset_;        // Offset into current uint64_t
};

// Utility class to read bit/byte stream.  This class can read bits or bytes
// that are either byte aligned or not.  It also has utilities to read multiple
// bytes in one read (e.g. encoded int).
class BitReader_8byte {
 public:
  // buffer: buffer to read from.  The buffer's length is 'buffer_len' and must be a
  // multiple of 8.
  BitReader_8byte(uint8_t* buffer, int buffer_len) :
      buffer_(reinterpret_cast<uint64_t*>(buffer)),
      max_bytes_(buffer_len),
      offset_(0),
      bit_offset_(0) {
    DCHECK_EQ(buffer_len % 8, 0);
  }

  BitReader_8byte() : buffer_(NULL), max_bytes_(0) {}

  // Gets the next value from the buffer.
  // Returns true if 'v' could be read or false if there are not enough bytes left.
  template<typename T>
  bool GetValue(int num_bits, T* v);

  // Reads a T sized value from the buffer.  T needs to be a native type and little
  // endian.  The value is assumed to be byte aligned so the stream will be advance
  // to the start of the next byte before v is read.
  template<typename T>
  bool GetAligned(int num_bits, T* v);

  // Reads a vlq encoded int from the stream.  The encoded int must start at the
  // beginning of a byte. Return false if there were not enough bytes in the buffer.
  bool GetVlqInt(int32_t* v);

  // Returns the number of bytes left in the stream, not including the current byte (i.e.,
  // there may be an additional fraction of a byte).
  inline int bytes_left() {
    return max_bytes_ - (offset_ * 8 + BitUtil::Ceil(bit_offset_, 8));
  }

  // Maximum byte length of a vlq encoded int
  static const int MAX_VLQ_BYTE_LEN = 5;
 
 private:
  uint64_t* buffer_;
  int max_bytes_;
  int offset_;            // Offset into buffer_
  int bit_offset_;        // Offset into current uint64_t

  // Advances offset_ and/or bit_offset_ to next byte boundary in buffer_.
  inline void Align();
};

}

#endif
