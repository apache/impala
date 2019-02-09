/// Licensed to the Apache Software Foundation (ASF) under one
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

#include <cstdint>
#include <type_traits>
#include <vector>

#include "common/status.h"
#include "util/bit-stream-utils.h"

namespace impala {

/// This class can be used to delta encode values according to the Parquet delta encoding
/// standard (see https://github.com/apache/parquet-format/blob/master/Encodings.md).
/// After constructing a new instance, initialising it ('Init()') and providing an output
/// buffer ('NewPage()'), the user can enter values with the 'Put()' method. It returns
/// true if the value was accepted and false otherwise, for example if there is not enough
/// space in the buffer. In this case, the user should call 'FinalizePage()' and
/// 'NewPage()' with a new buffer and call 'Put()' with the value again.
///
/// For each page we reserve space for the header at the beginning of the buffer when
/// entering the first value. We do not know the exact header size at this time, because
/// the header contains the first value and the total number of values in the page as
/// variable length integers. We reserve space for the largest possible header based on
/// the first value by assuming that 'max_page_value_count_' values will be inserted. This
/// number can be stored in two bytes. Later if it turns out that the number of values can
/// be stored in one byte, we need to move the contents to the left as we cannot leave
/// empty space between the header and the data. This is handled in 'FinalizePage()'.
template <typename INT_T>
class ParquetDeltaEncoder {
  static_assert(std::is_same_v<int32_t, INT_T> || std::is_same_v<int64_t, INT_T>,
      "Only INT32 and INT64 are supported.");

  using UINT_T = std::make_unsigned_t<INT_T>;

  public:
    /// The default value of 'max_page_value_count_'.
    static constexpr int DEFAULT_MAX_TOTAL_VALUE_COUNT = 16000;

    // Creates an uninitialised encoder. 'Init()' and 'NewBuffer()' must be called before
    // values can be inserted.
    ParquetDeltaEncoder();

    /// Initialises the encoder with the given parameters.
    ///
    /// 'block_size_in_values' is the number of values in a block, which must be non-zero
    /// and a multiple of 128. 'miniblocks_in_block' is the number of miniblocks in a
    /// block. This must be a divisor of 'block_size_in_values' and their quotient, the
    /// number of values in a miniblock, must be a multiple of 32.
    ///
    /// 'max_page_value_count' is the number of values allowed in a page.
    ///
    /// If the parameters are incorrect, an error is returned.
    ///
    /// If 'Init()' returned an error it can be called again but after successful
    /// initialisation the encoder cannot be re-initialised and 'Init()' will always
    /// return an error.
    Status Init(size_t block_size_in_values, size_t miniblocks_in_block,
        unsigned int max_page_value_count = DEFAULT_MAX_TOTAL_VALUE_COUNT)
        WARN_UNUSED_RESULT;

    /// Start writing a new page. The page will be written to the buffer pointed to by
    /// 'output_buffer'.
    void NewPage(uint8_t* output_buffer, int output_buffer_len);

    /// Write the header and the whole page into the output buffer. Before calling this
    /// function, the contents of the output buffer are not valid because the header size
    /// and contents are not known before writing the last value. After this call, it is
    /// invalid to call 'Put()' without calling 'NewPage()' first.
    /// Returns the total number of bytes written to the output buffer.
    int FinalizePage();

    /// Put a new value. If the value is the first value in a block, it is checked whether
    /// there is enough free space in the output buffer for the whole new block in the
    /// worst case (block sizes are not known in advance). If not, the function returns
    /// false and the value is not stored. In this case, the caller should call
    /// 'FinalizePage()' and 'NewPage()' with a new buffer and put the value again. If the
    /// value is accepted, the function returns true.
    bool Put(INT_T value) WARN_UNUSED_RESULT;

    /// Returns an output buffer size that is guaranteed to be enough for this encoder to
    /// write 'value_count' values to a new page even in the worst case. The current state
    /// of the encoder (i.e. values already written) is not taken into account, it is
    /// assumed that a new page will be started.
    ///
    /// Note that the returned size may be larger than the minimal buffer size required to
    /// hold 'value_count' values in the worst case if 'value_count' is such that the last
    /// block will not be full. This is because when attempting to 'Put()' the first value
    /// in the last block, the writer only accepts the value if there is enough space in
    /// the buffer for a full block in the worst case.
    int WorstCaseOutputSize(const int value_count) const;

  private:
    bool IsInitialized() const;
    bool WriteHeader() WARN_UNUSED_RESULT;
    bool FlushBlock() WARN_UNUSED_RESULT;
    bool WriteMinDelta() WARN_UNUSED_RESULT;

    // Returns a vector with the bitwidths of the miniblocks in the current block. Does
    // not include empty miniblocks.
    std::vector<uint8_t> CalculateMiniblockBitwidths();

    bool WriteMiniblockWidths(const std::vector<uint8_t>& miniblock_bitwidths)
        WARN_UNUSED_RESULT;

    bool WriteMiniblocks(const std::vector<uint8_t>& miniblock_bitwidths)
        WARN_UNUSED_RESULT;

    void ResetBlock();
    void SetOutputBuffer(uint8_t* output_buffer, int output_buffer_len);
    int HeaderSize(INT_T first_value, unsigned int total_value_count) const;
    int WorstCaseBlockSize() const;
    void AdvanceBufferPos(int bytes);

    /// The max ZigZag byte length is the same as that of the largest unsigned
    /// value encoded as a VLQ int.
    static constexpr int MAX_ZIGZAG_BYTE_LEN = BitWriter::VlqRequiredSize(
        std::numeric_limits<UINT_T>::max());

    /// The number of values in a block.
    std::size_t block_size_in_values_;

    /// The number of miniblocks in a block.
    std::size_t miniblocks_in_block_;

    /// The number of values in a miniblock.
    std::size_t miniblock_size_in_values_;

    /// The maximal number of values we allow to write to a page.
    unsigned int max_page_value_count_;

    /// We reserve space for the header at the beginning of the buffer. This variable
    /// stores the space reserved.
    int reserved_space_for_header_;

    /// The total number of values in the whole page.
    unsigned int total_value_count_;

    /// The first value in the page. It will be written to the page header as a zigzag VLQ
    /// int.
    INT_T first_value_;

    /// The previous value is cached here so we can calculate deltas.
    INT_T previous_value_;

    /// The delta values of the current block are cached here. They are written to the
    /// output buffer when the block is filled up.
    std::vector<INT_T> delta_buffer_;

    /// The minimal delta in the current block.
    INT_T min_delta_in_block_;

    /// The buffer to which we write the encoded values.
    uint8_t* output_buffer_;

    /// Pointer in 'output_buffer_' to the address where the next write should happen.
    uint8_t* output_buffer_pos_;

    /// The length of the remaining buffer.
    int output_buffer_len_;
};

} /// namespace impala
