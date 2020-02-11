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

#pragma once

#include <cstdint>
#include <sstream>
#include "common/logging.h"
#include "common/status.h"
#include "util/bit-util.h"

namespace impala {

#define RETURN_IF_FALSE(x) if (UNLIKELY(!(x))) return false

/// Class for reading and writing various data types.
/// Note: be very careful using *signed* ints.  Casting from a signed int to
/// an unsigned is not a problem.  However, bit shifts will do sign extension
/// on unsigned ints, which is rarely the right thing to do for byte level
/// operations.
class ReadWriteUtil {
 public:
  /// Maximum length for Writeable VInt
  static const int MAX_VINT_LEN = 9;

  /// Maximum lengths for Zigzag encodings.
  const static int MAX_ZINT_LEN = 5;
  const static int MAX_ZLONG_LEN = 10;

  /// Put a zigzag encoded integer into a buffer and return its length.
  static int PutZInt(int32_t integer, uint8_t* buf);

  /// Put a zigzag encoded long integer into a buffer and return its length.
  static int PutZLong(int64_t longint, uint8_t* buf);

  /// Get a big endian integer from a buffer.  The buffer does not have to be word aligned.
  template<typename T>
  static T GetInt(const uint8_t* buffer);

  /// Get a variable-length Long or int value from a byte buffer of length size. Access
  /// beyond the buffer size will return -1.
  /// Returns the length of the long/int
  /// If the size byte is corrupted then return -1;
  static int GetVLong(uint8_t* buf, int64_t* vlong, int32_t size);
  static int GetVInt(uint8_t* buf, int32_t* vint, int32_t size);

  /// Writes a variable-length Long or int value to a byte buffer.
  /// Returns the number of bytes written.
  static int64_t PutVLong(int64_t val, uint8_t* buf);
  static int64_t PutVInt(int32_t val, uint8_t* buf);

  /// Returns size of the encoded long value, including the 1 byte for length.
  static int VLongRequiredBytes(int64_t val);

  /// Read a variable-length Long value from a byte buffer starting at the specified
  /// byte offset and the buffer passed is of length size, accessing beyond the
  /// buffer length will result in returning -1 value to the caller.
  static int GetVLong(uint8_t* buf, int64_t offset, int64_t* vlong, int32_t size);

  /// Put an Integer into a buffer in big endian order.  The buffer must be big
  /// enough.
  static void PutInt(uint8_t* buf, uint16_t integer);
  static void PutInt(uint8_t* buf, uint32_t integer);
  static void PutInt(uint8_t* buf, uint64_t integer);

  /// Dump the first length bytes of buf to a Hex string.
  static std::string HexDump(const uint8_t* buf, int64_t length);
  static std::string HexDump(const char* buf, int64_t length);

  /// Determines the sign of a VInt/VLong from the first byte.
  static bool IsNegativeVInt(int8_t byte);

  /// Determines the total length in bytes of a Writable VInt/VLong from the first byte.
  static int DecodeVIntSize(int8_t byte);

  /// Return values for ReadZLong() and ReadZInt(). We return these in a single struct,
  /// rather than using an output parameter, for performance (this way both values are
  /// returned as registers).
  template <typename T>
  struct ZResult {
    /// False if there was a problem reading the value.
    bool ok;
    /// The decoded value. Only valid if 'ok' is true.
    T val;

    ZResult(T v) : ok(true), val(v) { }
    static ZResult error() { return ZResult(); }

   private:
    ZResult() : ok(false) { }
  };

  typedef ZResult<int64_t> ZLongResult;
  typedef ZResult<int32_t> ZIntResult;

  /// Read a zig-zag encoded long. This is the integer encoding defined by google.com
  /// protocol-buffers: https://developers.google.com/protocol-buffers/docs/encoding. *buf
  /// is incremented past the encoded long. 'buf_end' should point to the end of 'buf'
  /// (i.e. the first invalid byte).
  ///
  /// Returns a non-OK result if the encoded int spans too much many bytes. Unspecified
  /// for values that have the correct number of bytes but overflow the destination type
  /// (for both long and int, there are extra bits in the highest-order byte).
  static inline ZLongResult ReadZLong(uint8_t** buf, uint8_t* buf_end) {
    return ReadZInteger<MAX_ZLONG_LEN, ZLongResult>(buf, buf_end);
  }


  /// Read a zig-zag encoded int.
  static inline ZIntResult ReadZInt(uint8_t** buf, uint8_t* buf_end) {
    return ReadZInteger<MAX_ZINT_LEN, ZIntResult>(buf, buf_end);
  }

  /// The following methods read data from a buffer without assuming the buffer is long
  /// enough. If the buffer isn't long enough or another error occurs, they return false
  /// and update the status with the error. Otherwise they return true. buffer is advanced
  /// past the data read and buf_len is decremented appropriately.

  /// Read a native type T (e.g. bool, float) directly into output (i.e. input is cast
  /// directly to T and incremented by sizeof(T)).
  template <class T>
  static bool Read(uint8_t** buf, int* buf_len, T* val, Status* status);

  /// Skip the next num_bytes bytes.
  static bool SkipBytes(uint8_t** buf, int* buf_len, int num_bytes, Status* status);

 private:
  /// Implementation for ReadZLong() and ReadZInt(). MAX_LEN is MAX_ZLONG_LEN or
  /// MAX_ZINT_LEN.
  template<int MAX_LEN, typename ZResult>
  static ZResult ReadZInteger(uint8_t** buf, uint8_t* buf_end);
};

template<>
inline uint16_t ReadWriteUtil::GetInt(const uint8_t* buf) {
  return (buf[0] << 8) | buf[1];
}

template<>
inline uint32_t ReadWriteUtil::GetInt(const uint8_t* buf) {
  return (buf[0] << 24) | (buf[1] << 16) | (buf[2] << 8) | buf[3];
}

template<>
inline uint64_t ReadWriteUtil::GetInt(const uint8_t* buf) {
  uint64_t upper_half = GetInt<uint32_t>(buf);
  uint64_t lower_half = GetInt<uint32_t>(buf + 4);
  return lower_half | upper_half << 32;
}

inline void ReadWriteUtil::PutInt(uint8_t* buf, uint16_t integer) {
  buf[0] = integer >> 8;
  buf[1] = integer;
}

inline void ReadWriteUtil::PutInt(uint8_t* buf, uint32_t integer) {
  uint32_t big_endian = BitUtil::ByteSwap(integer);
  memcpy(buf, &big_endian, sizeof(uint32_t));
}

inline void ReadWriteUtil::PutInt(uint8_t* buf, uint64_t integer) {
  uint64_t big_endian = BitUtil::ByteSwap(integer);
  memcpy(buf, &big_endian, sizeof(uint64_t));
}

inline int ReadWriteUtil::GetVInt(uint8_t* buf, int32_t* vint, int32_t size) {
  int64_t vlong = 0;
  int len = GetVLong(buf, &vlong, size);
  *vint = static_cast<int32_t>(vlong);
  return len;
}

inline int ReadWriteUtil::GetVLong(uint8_t* buf, int64_t* vlong, int32_t size) {
  return GetVLong(buf, 0, vlong, size);
}

inline int ReadWriteUtil::GetVLong(
    uint8_t* buf, int64_t offset, int64_t* vlong, int32_t size) {
  // Buffer access out of bounds.
  if (size == 0) return -1;

  // Buffer access out of bounds.
  if (offset > size) return -1;
  int8_t firstbyte = (int8_t) buf[0 + offset];

  int len = DecodeVIntSize(firstbyte);

  // Buffer access out of bounds.
  if (len > MAX_VINT_LEN || len > size) return -1;
  if (len == 1) {
    *vlong = static_cast<int64_t>(firstbyte);
    return len;
  }

  *vlong &= ~*vlong;

  for (int i = 1; i < len; i++) {
    *vlong = (*vlong << 8) | buf[i+offset];
  }

  if (IsNegativeVInt(firstbyte)) {
    *vlong = *vlong ^ ((int64_t) - 1);
  }

  return len;
}

// Returns size of the encoded long value, including the 1 byte for length for val < -112
// or val > 127.
inline int ReadWriteUtil::VLongRequiredBytes(int64_t val) {
  if (val >= -112 && val <= 127) return 1;
  // If 'val' is negtive, take the one's complement.
  if (val < 0) val = ~val;
  return 9 - __builtin_clzll(val)/8;
}

// Serializes 'val' to a binary stream with zero-compressed encoding. For -112<=val<=127,
// only one byte is used with the actual value. For other values of 'val', the first byte
// value indicates whether the long is positive or negative, and the number of bytes that
// follow. If the first byte value v is between -113 and -120, the following long is
// positive, with number of bytes that follow are -(v+112). If the first byte value v is
// between -121 and -128, the following long is negative, with number of bytes that follow
// are -(v+120). Bytes are stored in the high-non-zero-byte-first order. Returns the
// number of bytes written.
// For more information, see the documentation for 'WritableUtils.writeVLong()' method:
// https://hadoop.apache.org/docs/r2.7.2/api/org/apache/hadoop/io/WritableUtils.html
inline int64_t ReadWriteUtil::PutVLong(int64_t val, uint8_t* buf) {
  int64_t num_bytes = VLongRequiredBytes(val);

  if (num_bytes == 1) {
    DCHECK(val >= -112 && val <= 127);
    // store the value itself instead of the length
    buf[0] = static_cast<int8_t>(val);
    return 1;
  }

  // This is how we encode the length for a length less than or equal to 8
  DCHECK_GE(num_bytes, 2);
  DCHECK_LE(num_bytes, 9);
  if (val < 0) {
    DCHECK_LT(val, -112);
    // The first byte in 'buf' should contain a value between -121 and -128 that makes the
    // following condition true: -(buf[0] + 120) == num_bytes - 1.
    // Note that 'num_bytes' includes the 1 extra byte for length.
    buf[0] = -(num_bytes + 119);
    // If 'val' is negtive, take the one's complement.
    // See the source code for WritableUtils.writeVLong() method:
    // https://hadoop.apache.org/docs/r2.7.2/api/src-html/org/apache/hadoop/io/
    // WritableUtils.html#line.271
    val = ~val;
  } else {
    DCHECK_GT(val, 127);
    // The first byte in 'buf' should contain a value between -113 and -120 that makes the
    // following condition true: -(buf[0] + 112) == num_bytes - 1.
    // Note that 'num_bytes' includes the 1 extra byte for length.
    buf[0] = -(num_bytes + 111);
  }

  // write to the buffer in Big Endianness
  for (int i = 1; i < num_bytes; ++i) {
    buf[i] = (val >> (8 * (num_bytes - i - 1))) & 0xFF;
  }

  return num_bytes;
}

inline int64_t ReadWriteUtil::PutVInt(int32_t val, uint8_t* buf) {
  return PutVLong(val, buf);
}

template <class T>
inline bool ReadWriteUtil::Read(uint8_t** buf, int* buf_len, T* val, Status* status) {
  int val_len = sizeof(T);
  if (UNLIKELY(val_len > *buf_len)) {
    std::stringstream ss;
    ss << "Cannot read " << val_len << " bytes, buffer length is " << *buf_len;
    *status = Status(ss.str());
    return false;
  }
  *val = *reinterpret_cast<T*>(*buf);
  *buf += val_len;
  *buf_len -= val_len;
  return true;
}

inline bool ReadWriteUtil::SkipBytes(uint8_t** buf, int* buf_len, int num_bytes,
                                     Status* status) {
  DCHECK_GE(*buf_len, 0);
  if (UNLIKELY(num_bytes > *buf_len)) {
    std::stringstream ss;
    ss << "Cannot skip " << num_bytes << " bytes, buffer length is " << *buf_len;
    *status = Status(ss.str());
    return false;
  }
  *buf += num_bytes;
  *buf_len -= num_bytes;
  return true;
}

inline bool ReadWriteUtil::IsNegativeVInt(int8_t byte) {
  return byte < -120 || (byte >= -112 && byte < 0);
}

inline int ReadWriteUtil::DecodeVIntSize(int8_t byte) {
  if (byte >= -112) {
    return 1;
  } else if (byte < -120) {
    return -119 - byte;
  }
  return -111 - byte;
}

}
