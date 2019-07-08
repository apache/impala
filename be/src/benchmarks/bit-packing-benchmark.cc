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

// Test bit packing performance when unpacking data for all supported bit-widths.
// This compares:
// * BitReader - the original bit reader that unpacks a value at a time.
// * Unpack32Scalar - a batched implementation using scalar operations to unpack batches
//    of 32 values.
// * UnpackScalar - an implementation that can unpack a variable number of values, using
//   Unpack32Scalar internally.
//
//
// Machine Info: Intel(R) Core(TM) i7-7700 CPU @ 3.60GHz
// Unpack32Values bit_width 0:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader           1.05e+04 1.06e+04 1.07e+04         1X         1X         1X
//                      Unpack32Scalar           1.29e+05 1.32e+05 1.34e+05      12.3X      12.5X      12.5X
//                        UnpackScalar           3.18e+05 3.23e+05 3.26e+05      30.3X      30.4X      30.5X
//
// Unpack32Values bit_width 1:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader           1.28e+04 1.29e+04 1.31e+04         1X         1X         1X
//                      Unpack32Scalar           8.22e+04 8.42e+04  8.5e+04       6.4X      6.51X       6.5X
//                        UnpackScalar           7.96e+04 8.08e+04 8.16e+04       6.2X      6.24X      6.24X
//
// Unpack32Values bit_width 2:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader           1.18e+04 1.19e+04  1.2e+04         1X         1X         1X
//                      Unpack32Scalar           8.37e+04 8.41e+04 8.49e+04      7.06X      7.06X      7.06X
//                        UnpackScalar            7.6e+04 7.66e+04 7.73e+04      6.42X      6.44X      6.43X
//
// Unpack32Values bit_width 3:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader           1.26e+04 1.27e+04 1.28e+04         1X         1X         1X
//                      Unpack32Scalar           8.31e+04 8.37e+04 8.44e+04      6.61X       6.6X      6.61X
//                        UnpackScalar           9.42e+04 9.54e+04 9.61e+04      7.49X      7.52X      7.52X
//
// Unpack32Values bit_width 4:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader           1.26e+04 1.27e+04 1.28e+04         1X         1X         1X
//                      Unpack32Scalar            8.4e+04 8.46e+04 8.52e+04      6.68X      6.68X      6.67X
//                        UnpackScalar           7.46e+04 7.52e+04 7.59e+04      5.93X      5.93X      5.94X
//
// Unpack32Values bit_width 5:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader           1.23e+04 1.25e+04 1.25e+04         1X         1X         1X
//                      Unpack32Scalar           8.25e+04 8.32e+04 8.39e+04      6.68X      6.67X      6.69X
//                        UnpackScalar           9.37e+04 9.43e+04 9.52e+04       7.6X      7.56X      7.58X
//
// Unpack32Values bit_width 6:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader           1.23e+04 1.24e+04 1.25e+04         1X         1X         1X
//                      Unpack32Scalar           8.23e+04 8.32e+04 8.39e+04      6.69X       6.7X      6.71X
//                        UnpackScalar           9.28e+04 9.42e+04 9.52e+04      7.54X      7.59X      7.61X
//
// Unpack32Values bit_width 7:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader            1.2e+04 1.23e+04 1.24e+04         1X         1X         1X
//                      Unpack32Scalar           8.19e+04 8.26e+04 8.32e+04       6.8X      6.73X      6.72X
//                        UnpackScalar           9.26e+04 9.36e+04 9.46e+04      7.69X      7.62X      7.64X
//
// Unpack32Values bit_width 8:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader           1.15e+04  1.2e+04 1.23e+04         1X         1X         1X
//                      Unpack32Scalar           6.33e+04 8.31e+04 8.51e+04       5.5X       6.9X      6.94X
//                        UnpackScalar           5.64e+04    7e+04 7.29e+04      4.91X      5.82X      5.95X
//
// Unpack32Values bit_width 9:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader           1.07e+04 1.14e+04 1.16e+04         1X         1X         1X
//                      Unpack32Scalar           6.33e+04 8.16e+04 8.27e+04       5.9X      7.12X      7.14X
//                        UnpackScalar           6.95e+04 9.22e+04 9.32e+04      6.47X      8.05X      8.05X
//
// Unpack32Values bit_width 10:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader           1.18e+04 1.19e+04  1.2e+04         1X         1X         1X
//                      Unpack32Scalar           8.13e+04 8.18e+04 8.23e+04      6.89X      6.85X      6.84X
//                        UnpackScalar           9.17e+04 9.26e+04 9.34e+04      7.76X      7.76X      7.75X
//
// Unpack32Values bit_width 11:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader           1.17e+04 1.18e+04 1.19e+04         1X         1X         1X
//                      Unpack32Scalar           8.07e+04 8.14e+04 8.22e+04      6.89X       6.9X      6.92X
//                        UnpackScalar           9.13e+04 9.22e+04 9.29e+04      7.79X      7.82X      7.83X
//
// Unpack32Values bit_width 12:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader           1.17e+04 1.17e+04 1.19e+04         1X         1X         1X
//                      Unpack32Scalar           8.07e+04 8.14e+04  8.2e+04      6.92X      6.93X      6.92X
//                        UnpackScalar           8.94e+04 9.14e+04 9.23e+04      7.67X      7.78X      7.79X
//
// Unpack32Values bit_width 13:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader           1.13e+04 1.15e+04 1.16e+04         1X         1X         1X
//                      Unpack32Scalar           7.85e+04 8.05e+04 8.14e+04      6.93X      6.98X      6.99X
//                        UnpackScalar           8.77e+04 9.09e+04 9.17e+04      7.74X      7.88X      7.87X
//
// Unpack32Values bit_width 14:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader           1.14e+04 1.15e+04 1.16e+04         1X         1X         1X
//                      Unpack32Scalar           7.81e+04    8e+04 8.06e+04      6.84X      6.95X      6.94X
//                        UnpackScalar           8.87e+04 9.01e+04  9.1e+04      7.77X      7.83X      7.84X
//
// Unpack32Values bit_width 15:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader           1.12e+04 1.13e+04 1.14e+04         1X         1X         1X
//                      Unpack32Scalar           7.66e+04 7.96e+04 8.06e+04      6.85X      7.01X      7.04X
//                        UnpackScalar           8.81e+04 8.94e+04 9.04e+04      7.87X      7.88X       7.9X
//
// Unpack32Values bit_width 16:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader           1.13e+04 1.13e+04 1.14e+04         1X         1X         1X
//                      Unpack32Scalar           8.11e+04 8.19e+04 8.27e+04       7.2X      7.22X      7.23X
//                        UnpackScalar           8.82e+04 8.91e+04 8.97e+04      7.84X      7.85X      7.85X
//
// Unpack32Values bit_width 17:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader           1.11e+04 1.12e+04 1.13e+04         1X         1X         1X
//                      Unpack32Scalar           7.75e+04 7.86e+04 7.91e+04         7X      7.04X      7.03X
//                        UnpackScalar           8.75e+04 8.82e+04 8.89e+04      7.89X      7.91X       7.9X
//
// Unpack32Values bit_width 18:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader           1.09e+04 1.11e+04 1.12e+04         1X         1X         1X
//                      Unpack32Scalar           7.71e+04 7.81e+04 7.88e+04      7.06X      7.02X      7.03X
//                        UnpackScalar           8.68e+04 8.81e+04 8.89e+04      7.95X      7.92X      7.93X
//
// Unpack32Values bit_width 19:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader           1.09e+04  1.1e+04 1.11e+04         1X         1X         1X
//                      Unpack32Scalar           7.69e+04 7.75e+04 7.83e+04      7.06X      7.07X      7.07X
//                        UnpackScalar           8.62e+04 8.72e+04 8.78e+04      7.91X      7.96X      7.93X
//
// Unpack32Values bit_width 20:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader           1.07e+04  1.1e+04 1.11e+04         1X         1X         1X
//                      Unpack32Scalar           7.58e+04 7.74e+04 7.82e+04       7.1X      7.06X      7.07X
//                        UnpackScalar           8.43e+04 8.64e+04 8.74e+04      7.89X      7.89X       7.9X
//
// Unpack32Values bit_width 21:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader           1.07e+04 1.08e+04 1.09e+04         1X         1X         1X
//                      Unpack32Scalar           7.58e+04 7.66e+04 7.76e+04      7.07X      7.09X      7.14X
//                        UnpackScalar           8.51e+04 8.61e+04 8.69e+04      7.95X      7.97X         8X
//
// Unpack32Values bit_width 22:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader           1.05e+04 1.08e+04 1.09e+04         1X         1X         1X
//                      Unpack32Scalar           7.49e+04 7.62e+04 7.71e+04       7.1X      7.08X       7.1X
//                        UnpackScalar           8.39e+04 8.59e+04 8.65e+04      7.96X      7.98X      7.97X
//
// Unpack32Values bit_width 23:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader           1.06e+04 1.07e+04 1.08e+04         1X         1X         1X
//                      Unpack32Scalar           7.44e+04 7.54e+04 7.62e+04      7.03X      7.07X      7.08X
//                        UnpackScalar           8.44e+04 8.54e+04 8.61e+04      7.97X         8X         8X
//
// Unpack32Values bit_width 24:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader           1.05e+04 1.06e+04 1.07e+04         1X         1X         1X
//                      Unpack32Scalar           7.54e+04 7.62e+04 7.69e+04      7.17X      7.18X       7.2X
//                        UnpackScalar           8.42e+04 8.51e+04 8.62e+04      8.01X      8.01X      8.07X
//
// Unpack32Values bit_width 25:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader           1.04e+04 1.05e+04 1.05e+04         1X         1X         1X
//                      Unpack32Scalar           7.42e+04 7.49e+04 7.57e+04      7.15X      7.17X      7.18X
//                        UnpackScalar           8.32e+04 8.43e+04 8.51e+04      8.01X      8.07X      8.07X
//
// Unpack32Values bit_width 26:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader           1.02e+04 1.04e+04 1.05e+04         1X         1X         1X
//                      Unpack32Scalar           7.24e+04 7.46e+04 7.55e+04      7.12X      7.16X      7.19X
//                        UnpackScalar           8.15e+04 8.43e+04 8.51e+04      8.01X      8.09X       8.1X
//
// Unpack32Values bit_width 27:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader           1.01e+04 1.03e+04 1.04e+04         1X         1X         1X
//                      Unpack32Scalar           7.26e+04  7.4e+04 7.47e+04      7.18X      7.18X       7.2X
//                        UnpackScalar           8.13e+04 8.35e+04 8.43e+04      8.03X      8.11X      8.12X
//
// Unpack32Values bit_width 28:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader              1e+04 1.03e+04 1.03e+04         1X         1X         1X
//                      Unpack32Scalar           7.07e+04 7.41e+04 7.48e+04      7.05X      7.22X      7.23X
//                        UnpackScalar           7.97e+04 8.34e+04 8.42e+04      7.96X      8.13X      8.14X
//
// Unpack32Values bit_width 29:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader           1.01e+04 1.01e+04 1.02e+04         1X         1X         1X
//                      Unpack32Scalar           7.06e+04 7.27e+04 7.35e+04      7.02X      7.18X      7.19X
//                        UnpackScalar              8e+04 8.23e+04 8.32e+04      7.96X      8.12X      8.14X
//
// Unpack32Values bit_width 30:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader           9.95e+03 1.01e+04 1.02e+04         1X         1X         1X
//                      Unpack32Scalar              7e+04 7.23e+04 7.32e+04      7.04X      7.16X      7.18X
//                        UnpackScalar            7.9e+04 8.18e+04 8.24e+04      7.94X      8.09X      8.08X
//
// Unpack32Values bit_width 31:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader           9.06e+03 9.33e+03 9.43e+03         1X         1X         1X
//                      Unpack32Scalar           6.52e+04 7.17e+04 7.26e+04      7.19X      7.69X       7.7X
//                        UnpackScalar           7.67e+04  8.1e+04 8.19e+04      8.46X      8.69X      8.69X
//
// Unpack32Values bit_width 32:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
//                           BitReader           9.73e+03 9.96e+03    1e+04         1X         1X         1X
//                      Unpack32Scalar           1.06e+05  1.1e+05 1.11e+05      10.9X        11X      11.1X
//                        UnpackScalar           1.41e+05 1.49e+05  1.5e+05      14.5X      14.9X        15X
#include <cmath>
#include <cstdio>
#include <cstdlib>

#include <algorithm>
#include <iostream>
#include <numeric>
#include <vector>

#include "gutil/strings/substitute.h"
#include "util/benchmark.h"
#include "util/bit-packing.h"
#include "util/bit-stream-utils.inline.h"
#include "util/cpu-info.h"

#include "common/names.h"

using namespace impala;

constexpr int NUM_OUT_VALUES = 1024 * 1024;
static_assert(NUM_OUT_VALUES % 32 == 0, "NUM_OUT_VALUES must be divisible by 32");

uint32_t out_buffer[NUM_OUT_VALUES];

struct BenchmarkParams {
  int bit_width;
  const uint8_t* data;
  int64_t data_len;
};

/// Legacy value-at-a-time implementation of bit unpacking. Retained here for
/// purposes of comparison in the benchmark.
class BitReader {
 public:
  /// 'buffer' is the buffer to read from.  The buffer's length is 'buffer_len'.
  /// Does not take ownership of the buffer.
  BitReader(const uint8_t* buffer, int buffer_len) { Reset(buffer, buffer_len); }

  BitReader() : buffer_(NULL), max_bytes_(0) {}

  // The implicit copy constructor is left defined. If a BitReader is copied, the
  // two copies do not share any state. Invoking functions on either copy continues
  // reading from the current read position without modifying the state of the other
  // copy.

  /// Resets the read to start reading from the start of 'buffer'. The buffer's
  /// length is 'buffer_len'. Does not take ownership of the buffer.
  void Reset(const uint8_t* buffer, int buffer_len) {
    buffer_ = buffer;
    max_bytes_ = buffer_len;
    byte_offset_ = 0;
    bit_offset_ = 0;
    int num_bytes = std::min(8, max_bytes_);
    memcpy(&buffered_values_, buffer_, num_bytes);
  }

  /// Gets the next value from the buffer.  Returns true if 'v' could be read or false if
  /// there are not enough bytes left. num_bits must be <= 32.
  template<typename T>
  bool GetValue(int num_bits, T* v);

  /// Reads a 'num_bytes'-sized value from the buffer and stores it in 'v'. T needs to be a
  /// little-endian native type and big enough to store 'num_bytes'. The value is assumed
  /// to be byte-aligned so the stream will be advanced to the start of the next byte
  /// before 'v' is read. Returns false if there are not enough bytes left.
  template<typename T>
  bool GetBytes(int num_bytes, T* v);

  /// Returns the number of bytes left in the stream, not including the current byte (i.e.,
  /// there may be an additional fraction of a byte).
  int bytes_left() { return max_bytes_ - (byte_offset_ + BitUtil::Ceil(bit_offset_, 8)); }

  /// Maximum supported bitwidth for reader.
  static const int MAX_BITWIDTH = 32;

 private:
  const uint8_t* buffer_;
  int max_bytes_;

  /// Bytes are memcpy'd from buffer_ and values are read from this variable. This is
  /// faster than reading values byte by byte directly from buffer_.
  uint64_t buffered_values_;

  int byte_offset_;       // Offset in buffer_
  int bit_offset_;        // Offset in buffered_values_
};

template <typename T>
bool BitReader::GetValue(int num_bits, T* v) {
  DCHECK(num_bits == 0 || buffer_ != NULL);
  // TODO: revisit this limit if necessary
  DCHECK_LE(num_bits, MAX_BITWIDTH);
  DCHECK_LE(num_bits, sizeof(T) * 8);

  // First do a cheap check to see if we may read past the end of the stream, using
  // constant upper bounds for 'bit_offset_' and 'num_bits'.
  if (UNLIKELY(byte_offset_ + sizeof(buffered_values_) + MAX_BITWIDTH / 8 > max_bytes_)) {
    // Now do the precise check.
    if (UNLIKELY(byte_offset_ * 8 + bit_offset_ + num_bits > max_bytes_ * 8)) {
      return false;
    }
  }

  DCHECK_GE(bit_offset_, 0);
  DCHECK_LE(bit_offset_, 64);
  *v = BitUtil::TrailingBits(buffered_values_, bit_offset_ + num_bits) >> bit_offset_;

  bit_offset_ += num_bits;
  if (bit_offset_ >= 64) {
    byte_offset_ += 8;
    bit_offset_ -= 64;

    int bytes_remaining = max_bytes_ - byte_offset_;
    if (LIKELY(bytes_remaining >= 8)) {
      memcpy(&buffered_values_, buffer_ + byte_offset_, 8);
    } else {
      memcpy(&buffered_values_, buffer_ + byte_offset_, bytes_remaining);
    }

    // Read bits of v that crossed into new buffered_values_
    *v |= BitUtil::TrailingBits(buffered_values_, bit_offset_)
          << (num_bits - bit_offset_);
  }
  DCHECK_LE(bit_offset_, 64);
  return true;
}

template<typename T>
bool BitReader::GetBytes(int num_bytes, T* v) {
  DCHECK_LE(num_bytes, sizeof(T));
  int bytes_read = BitUtil::Ceil(bit_offset_, 8);
  if (UNLIKELY(byte_offset_ + bytes_read + num_bytes > max_bytes_)) return false;

  // Advance byte_offset to next unread byte and read num_bytes
  byte_offset_ += bytes_read;
  *v = 0; // Ensure unset bytes are initialized to zero.
  memcpy(v, buffer_ + byte_offset_, num_bytes);
  byte_offset_ += num_bytes;

  // Reset buffered_values_
  bit_offset_ = 0;
  int bytes_remaining = max_bytes_ - byte_offset_;
  if (LIKELY(bytes_remaining >= 8)) {
    memcpy(&buffered_values_, buffer_ + byte_offset_, 8);
  } else {
    memcpy(&buffered_values_, buffer_ + byte_offset_, bytes_remaining);
  }
  return true;
}

/// Benchmark calling BitReader::GetValue() in a loop to unpack 32 * 'batch_size' values.
void BitReaderBenchmark(int batch_size, void* data) {
  const BenchmarkParams* p = reinterpret_cast<BenchmarkParams*>(data);
  BitReader reader(p->data, p->data_len);
  for (int i = 0; i < batch_size; ++i) {
    for (int j = 0; j < 32; ++j) {
      const int64_t offset = (i * 32 + j) % NUM_OUT_VALUES;
      if (UNLIKELY(!reader.GetValue<uint32_t>(p->bit_width, &out_buffer[offset]))) {
        reader.Reset(p->data, p->data_len);
        const bool success = reader.GetValue<uint32_t>(p->bit_width, &out_buffer[offset]);
        DCHECK(success);
      }
    }
  }
}

/// Benchmark calling Unpack32Values() in a loop to unpack 32 * 'batch_size' values.
void Unpack32Benchmark(int batch_size, void* data) {
  const BenchmarkParams* p = reinterpret_cast<BenchmarkParams*>(data);
  const uint8_t* pos = reinterpret_cast<const uint8_t*>(p->data);
  const uint8_t* const data_end = pos + p->data_len;
  for (int i = 0; i < batch_size; ++i) {
    if (UNLIKELY(pos >= data_end)) pos = reinterpret_cast<const uint8_t*>(p->data);
    const int64_t offset = (i * 32) % NUM_OUT_VALUES;
    pos = BitPacking::Unpack32Values(
        p->bit_width, pos, data_end - pos, out_buffer + offset);
  }
}

/// Benchmark calling UnpackValues() to unpack 32 * 'batch_size' values.
void UnpackBenchmark(int batch_size, void* data) {
  const BenchmarkParams* p = reinterpret_cast<BenchmarkParams*>(data);
  const int64_t total_values_to_unpack = 32L * batch_size;
  for (int64_t unpacked = 0; unpacked < total_values_to_unpack;
       unpacked += NUM_OUT_VALUES) {
    const int64_t unpack_batch =
        min<int64_t>(NUM_OUT_VALUES, total_values_to_unpack - unpacked);
    BitPacking::UnpackValues(
        p->bit_width, p->data, p->data_len, unpack_batch, out_buffer);
  }
}

int main(int argc, char **argv) {
  CpuInfo::Init();
  cout << endl << Benchmark::GetMachineInfo() << endl;

  for (int bit_width = 0; bit_width <= 32; ++bit_width) {
    Benchmark suite(Substitute("Unpack32Values bit_width $0", bit_width));
    const int64_t data_len = NUM_OUT_VALUES * bit_width / 8;
    vector<uint8_t> data(data_len);
    std::iota(data.begin(), data.end(), 0);
    BenchmarkParams params{bit_width, data.data(), data_len};
    suite.AddBenchmark(Substitute("BitReader", bit_width), BitReaderBenchmark, &params);
    suite.AddBenchmark(
        Substitute("Unpack32Scalar", bit_width), Unpack32Benchmark, &params);
    suite.AddBenchmark(Substitute("UnpackScalar", bit_width), UnpackBenchmark, &params);
    cout << suite.Measure() << endl;
  }
  return 0;
}

