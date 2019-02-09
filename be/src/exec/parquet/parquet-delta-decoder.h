///this script to the Apache Software Foundation (ASF) under one
/// or more contributor license agreements.  See the NOTICE file
/// distributed with this work for additional information
/// regarding copyright ownership.  The ASF licenses this file
/// to you under the Apache License, Version 2.0 (the
/// "License"); you may not use this file except in compliance
/// with the License.  You may obtain a copy of the License at
///
///   http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing,
/// software distributed under the License is distributed on an
/// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
/// KIND, either express or implied.  See the License for the
/// specific language governing permissions and limitations
/// under the License.

#pragma once

#include <array>
#include <cmath>
#include <cstdint>
#include <functional>
#include <limits>
#include <type_traits>
#include <vector>

#include "util/bit-stream-utils.h"
#include "util/debug-util.h"

namespace impala {

/// This class can be used to decode delta encoded values from a buffer. To decode values
/// from a page, first call 'NewPage()' with a pointer pointing to the delta encoding
/// header, then use the 'NextValue*()' functions to extract values.
template <typename INT_T>
class ParquetDeltaDecoder {
  static_assert(std::is_same_v<int32_t, INT_T> || std::is_same_v<int64_t, INT_T>,
      "Only INT32 and INT64 are supported.");

  public:
    ParquetDeltaDecoder() {}

    /// Set a new delta encoded page as the input. The data pointed to by 'input_buffer'
    /// is the start of the delta encoding header. The function reads the global header
    /// and the first block header (if it exists).
    /// Returns an error if the header is invalid or if a reading error occurs.
    Status NewPage(const uint8_t* input_buffer, int input_buffer_len) WARN_UNUSED_RESULT;

    /// Returns the total number of values contained in this page.
    /// Only valid to call when 'NewPage()' has already been called.
    std::size_t GetTotalValueCount() const {
      DCHECK(initialized_);
      return total_value_count_;
    }

    /// Tries to extract one value and write it to '*value'. Returns 1 if a value was
    /// extracted, 0 if there were no values left and -1 if an error occured.
    /// Only valid to call when 'NewPage()' has already been called.
    int NextValue(INT_T* value) WARN_UNUSED_RESULT;

    /// Tries to extract 'num_values' values and write them to the buffer pointed to by
    /// 'values' with the given stride. 'num_values' must be non-negative.
    ///
    /// The stride denotes the distance (in bytes) between the initial bytes of
    /// consecutive values in the output. It must be at least the size of the type of the
    /// values being written. If the stride is larger than the size of the type, the bytes
    /// between the values are not modified.
    ///
    /// The buffer has to be large enough to hold the values (including stride).
    ///
    /// Returns the number of extracted values, 0 if there were no values left or -1 if an
    /// error occured.
    ///
    /// Only valid to call when 'NewPage()' has already been called.
    int NextValues(int num_values, uint8_t* values, std::size_t stride)
        WARN_UNUSED_RESULT;

    /// Like 'NextValues()', but converts the extracted values to `OutType`. 'stride' must
    /// be at least sizeof(OutType).
    template <typename OutType>
    WARN_UNUSED_RESULT
    int NextValuesConverted(int num_values, uint8_t* values, std::size_t stride);

    /// Tries to skip 'num_values' values. 'num_values' must be non-negative.
    /// Returns the number of skipped values, 0 if there were no values left or -1 if an
    /// error occured.
    /// Only valid to call when 'NewPage()' has already been called.
    int SkipValues(int num_values) WARN_UNUSED_RESULT;

  private:
    using UINT_T = typename std::make_unsigned<INT_T>::type;

    /// Tries to write or skip 'num_values' values. Returns the number of extracted or
    /// skipped values, 0 if there were no values left or -1 if an error occured.
    ///
    /// If the template parameter 'SKIP' is true, the extracted values are discarded,
    /// otherwise they are written to the buffer pointed to by 'values' using 'stride'. We
    /// have to decode all the values even if we skip them as they are needed to calculate
    /// the following values.
    template<bool SKIP, typename OutType>
    int GetOrSkipNextValues(int num_values, uint8_t* values, std::size_t stride)
        WARN_UNUSED_RESULT;

    template<bool SKIP, typename OutType>
    uint8_t* GetOrSkipFirstValue(uint8_t* values, int stride) WARN_UNUSED_RESULT;

    /// Writes or skips the contents of the buffer, but at most 'num_values' values.
    /// Returns a pair containing the number of values written/skipped and a
    /// pointer pointing to the address where the next value should be written.
    template<bool SKIP, typename OutType>
    std::pair<int, uint8_t*> GetOrSkipBufferedValues(int num_values,
        uint8_t* values, std::size_t stride) WARN_UNUSED_RESULT;

    template<bool SKIP, typename OutType>
    int GetOrSkipFromInput(int num_values, uint8_t* values, int stride)
        WARN_UNUSED_RESULT;

    /// Processes a batch ('BATCH_SIZE' values) from the input. If 'SKIP' is false, the
    /// values are decoded and written to 'out' with the given stride. If 'SKIP' is true,
    /// the values are not written out. Returns the number of actual elements processed,
    /// not counting padding values.
    template <bool SKIP, typename OutType>
    int ReadAndDecodeBatchFromInput(OutType* __restrict__ out, int stride);

    /// Read the bit-packed deltas into the buffer and decode them.
    bool FillBuffer() WARN_UNUSED_RESULT;

    bool ReadBlockHeader() WARN_UNUSED_RESULT;

    /// Compile-time dispatcher function that chooses between skipping values and writing
    /// them to 'dest'.
    template<bool SKIP, typename OutType>
    uint8_t* CopyOrSkipBufferedValues(int num_values, uint8_t* dest, int stride);

    /// Copies (and converts if needed) 'num_values' values from the buffer to 'dest' with
    /// the given stride.
    template <typename OutType>
    uint8_t* CopyBufferedValues(int num_values, uint8_t* dest, int stride);

    /// Skip values from the buffer.
    void SkipBufferedValues(int num_values);

    /// Stores whether the decoder is initialised with a page, i.e. 'NewPage()' has been
    /// called.
    bool initialized_ = false;

    /// The number of values in a block.
    std::size_t block_size_in_values_;

    /// The number of miniblocks in a block.
    std::size_t miniblocks_in_block_;

    /// The number of values in a miniblock.
    std::size_t miniblock_size_in_values_;

    /// The total number of values in the input.
    std::size_t total_value_count_;

    /// The number of remaining values in the input buffer.
    std::size_t input_values_remaining_;

    /// The last processed value. We use it to store the first value in the input (in the
    /// header) and the last value in each buffered chunk so that we can decode the next
    /// chunk.
    INT_T last_read_value_;

    /// The minimal delta in the current block.
    INT_T min_delta_in_block_;

    /// The number of values already read from the current miniblock.
    std::size_t values_read_from_miniblock_;

    /// The number of miniblocks fully read from the current block.
    std::size_t miniblocks_read_from_block_;

    /// The bitwidths of the miniblocks in the current block are stored here.
    std::vector<uint8_t> bitwidths_;

    /// We use a buffer so that we can always read byte-aligned from the bit-packed input.
    /// Whole batches are normally written directly to the output bypassing the buffer,
    /// but if the client asks for fewer values we decode and write a whole batch into the
    /// buffer, then return values from it until it is empty. This way each read call is
    /// byte-aligned.
    ///
    /// 'num_buffered_values_' is the number of valid values in the buffer, and
    /// 'next_buffered_value_index_' is the index of the next value to be given to the
    /// user (or skipped).
    static constexpr int BATCH_SIZE = 32;
    static_assert(BATCH_SIZE > 0, "BATCH_SIZE should be positive.");
    static_assert(BATCH_SIZE % 8 == 0,
        "BATCH_SIZE should guarantee byte-aligned reading.");
    std::array<INT_T, BATCH_SIZE> buffer_;
    int num_buffered_values_;
    int next_buffered_value_index_;

    /// We use this reader to read from the input and handle the bit-packed values.
    BatchedBitReader reader_;
};

} /// namespace impala
