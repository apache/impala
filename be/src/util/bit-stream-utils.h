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


#ifndef IMPALA_UTIL_BIT_STREAM_UTILS_H
#define IMPALA_UTIL_BIT_STREAM_UTILS_H

#include <boost/cstdint.hpp>
#include <string.h>
#include "common/compiler-util.h"
#include "common/logging.h"

namespace impala {

// Utility class to write bit/byte streams.  This class can write data to either be
// bit packed or byte aligned (and a single stream that has a mix of both).
// This class does not allocate memory.
class BitWriter {
 public:
  // buffer: buffer to write bits to.  Buffer should be preallocated with 'num_bytes'
  BitWriter(uint8_t* buffer, int num_bytes) :
      buffer_(buffer), 
      num_bytes_(num_bytes) {
    Clear();
  }

  void Clear() {
    byte_offset_ = 0;
    bit_offset_ = 0;
    memset(buffer_, 0, num_bytes_);
  }

  uint8_t* buffer() const { return buffer_; }
  int bytes_written() const { return byte_offset_ + (bit_offset_ != 0); }
  int buffer_len() const { return num_bytes_; }

  // Writes a bool to the buffer.  This is bit packed.  Returns false if
  // there was not enough space.
  bool PutBool(bool b);

#if 0
  // TODO: when we support nested schemas, we will need to write multi-bit values.
  bool PutValue(int v, int num_bits);
#endif

  // Writes v to the next aligned byte.  
  template<typename T>
  bool PutAligned(T v);
  
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
  uint8_t* buffer_;
  int num_bytes_;
  int byte_offset_;
  int bit_offset_;        // Offset in current byte
};

// Utility class to read bit/byte stream.  This class can read bits or bytes
// that are either byte aligned or not.  It also has utilities to read multiple
// bytes in one read (e.g. encoded int).
class BitReader {
 public:
  // buffer: buffer to read from.  the length is 'num_bytes'
  BitReader(uint8_t* buffer, int num_bytes) :
      buffer_(buffer), 
      num_bytes_(num_bytes),
      byte_offset_(0),
      bit_offset_(0) {
  }

  BitReader() : buffer_(NULL), num_bytes_(0) {}

  // Gets the next bool from the buffers.  
  // Returns true if 'v' could be read or false if there are not enough bytes left.
  bool GetBool(bool* b);

#if 0
  // TODO: when we support nested schemas, we will need to read multi-bit values.
  bool GetValue(int num_bits, int* v);
#endif

  // Reads a T sized value from the buffer.  T needs to be a native type and little
  // endian.  The value is assumed to be byte aligned so the stream will be advance
  // to the start of the next byte before v is read.
  template<typename T>
  bool GetAligned(T* v);

  // Reads a vlq encoded int from the stream.  The encoded int must start at the
  // beginning of a byte. Return false if there were not enough bytes in the buffer.
  bool GetVlqInt(int32_t* v);

  // Returns the number of bytes left in the stream, including the current byte.
  int bytes_left() { return num_bytes_ - byte_offset_; }

  // Maximum byte length of a vlq encoded int
  static const int MAX_VLQ_BYTE_LEN = 5;
 
 private:
  uint8_t* buffer_;
  int num_bytes_;
  int byte_offset_;
  int bit_offset_;        // Offset in current byte
};

}

#endif
