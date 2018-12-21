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

#include <cstdio>
#include <cstdlib>
#include <random>

#include "testutil/gtest-util.h"
#include "testutil/mem-util.h"
#include "util/bit-packing.h"
#include "util/bit-stream-utils.inline.h"

#include "common/names.h"

using std::uniform_int_distribution;
using std::mt19937;

namespace impala {

namespace {
uint32_t ComputeMask(int bit_width) {
  return (bit_width < 32) ? ((1U << bit_width) - 1) : ~0U;
}
}

/// Test unpacking a subarray of values to/from smaller buffers that are sized to exactly
/// fit the the input and output. 'in' is the original unpacked input, 'packed' is the
/// bit-packed data. The test copies 'num_in_values' packed values to a smaller temporary
/// buffer, then unpacks them to another temporary buffer. Both buffers are sized to the
/// minimum number of bytes required to fit the packed/unpacked data.
///
/// This is to test that we do not overrun either the input or output buffer for smaller
/// batch sizes.
void UnpackSubset(const uint32_t* in, const uint8_t* packed, int num_in_values,
    int bit_width, bool aligned);

/// Test a packing/unpacking round-trip of the 'num_in_values' values in 'in',
/// packed with 'bit_width'. If 'aligned' is true, buffers for packed and unpacked data
/// are allocated at a 64-byte aligned address. Otherwise the buffers are misaligned
/// by 1 byte from a 64-byte aligned address.
void PackUnpack(const uint32_t* in, int num_in_values, int bit_width, bool aligned) {
  LOG(INFO) << "num_in_values = " << num_in_values << " bit_width = " << bit_width
            << " aligned = " << aligned;

  // Mask out higher bits so that the values to pack are in range.
  const uint32_t mask = ComputeMask(bit_width);
  const int misalignment = aligned ? 0 : 1;

  const int bytes_required = BitUtil::RoundUpNumBytes(bit_width * num_in_values);
  AlignedAllocation storage(bytes_required + misalignment);
  uint8_t* packed = storage.data() + misalignment;

  BitWriter writer(packed, bytes_required);
  if (bit_width > 0) {
    for (int i = 0; i < num_in_values; ++i) {
      ASSERT_TRUE(writer.PutValue(in[i] & mask, bit_width));
    }
  }
  writer.Flush();
  LOG(INFO) << "Wrote " << writer.bytes_written() << " bytes.";

  // Test unpacking all the values. Trying to unpack extra values should have the same
  // result because the input buffer size 'num_in_values' limits the number of values to
  // return.
  for (const int num_to_unpack : {num_in_values, num_in_values + 1, num_in_values + 77}) {
    LOG(INFO) << "Unpacking " << num_to_unpack;
    // Size buffer exactly so that ASAN can detect reads/writes that overrun the buffer.
    AlignedAllocation out_storage(num_to_unpack * sizeof(uint32_t) + misalignment);
    uint32_t* out = reinterpret_cast<uint32_t*>(out_storage.data() + misalignment);
    const auto result = BitPacking::UnpackValues(
        bit_width, packed, writer.bytes_written(), num_to_unpack, out);
    ASSERT_EQ(packed + writer.bytes_written(), result.first)
        << "Unpacked different # of bytes from the # written";
    if (bit_width == 0) {
      // If no bits, we can get back as many as we ask for.
      ASSERT_EQ(num_to_unpack, result.second) << "Unpacked wrong # of values";
    } else if (bit_width < CHAR_BIT) {
      // We may get back some garbage values that we didn't actually pack if we
      // didn't use all of the trailing byte.
      const int max_packed_values = writer.bytes_written() * CHAR_BIT / bit_width;
      ASSERT_EQ(min(num_to_unpack, max_packed_values), result.second)
          << "Unpacked wrong # of values";
    } else {
      ASSERT_EQ(num_in_values, result.second) << "Unpacked wrong # of values";
    }

    for (int i = 0; i < num_in_values; ++i) {
      EXPECT_EQ(in[i] & mask, out[i]) << "Didn't get back input value " << i;
    }
  }
  UnpackSubset(in, packed, num_in_values, bit_width, aligned);
}

void UnpackSubset(const uint32_t* in, const uint8_t* packed, int num_in_values,
    int bit_width, bool aligned) {
  const int misalignment = aligned ? 0 : 1;
  for (int num_to_unpack : {1, 10, 77, num_in_values - 7}) {
    if (num_to_unpack < 0 || num_to_unpack > num_in_values) continue;

    // Size buffers exactly so that ASAN can detect buffer overruns.
    const int64_t bytes_to_read = BitUtil::RoundUpNumBytes(num_to_unpack * bit_width);
    AlignedAllocation packed_copy_storage(bytes_to_read + misalignment);
    uint8_t* packed_copy = packed_copy_storage.data() + misalignment;
    memcpy(packed_copy, packed, bytes_to_read);
    AlignedAllocation out_storage(num_to_unpack * sizeof(uint32_t) + misalignment);
    uint32_t* out = reinterpret_cast<uint32_t*>(out_storage.data() + misalignment);
    const auto result = BitPacking::UnpackValues(
        bit_width, packed_copy, bytes_to_read, num_to_unpack, out);
    ASSERT_EQ(packed_copy + bytes_to_read, result.first) << "Read wrong # of bytes";
    ASSERT_EQ(num_to_unpack, result.second) << "Unpacked wrong # of values";

    for (int i = 0; i < num_to_unpack; ++i) {
      ASSERT_EQ(in[i] & ComputeMask(bit_width), out[i]) << "Didn't get back input value "
                                                         << i;
    }
  }
}

TEST(BitPackingTest, RandomUnpack) {
  constexpr int NUM_IN_VALUES = 64 * 1024;
  uint32_t in[NUM_IN_VALUES];
  mt19937 rng;
  uniform_int_distribution<uint32_t> dist;
  std::generate(std::begin(in), std::end(in), [&rng, &dist] { return dist(rng); });

  // Test various odd input lengths to exercise boundary cases for full and partial
  // batches of 32.
  vector<int> lengths{NUM_IN_VALUES, NUM_IN_VALUES - 1, NUM_IN_VALUES - 16,
      NUM_IN_VALUES - 19, NUM_IN_VALUES - 31};
  for (int i = 0; i < 32; ++i) {
    lengths.push_back(i);
  }

  for (int bit_width = 0; bit_width <= 32; ++bit_width) {
    for (const int length : lengths) {
      // Test that unpacking to/from aligned and unaligned memory works.
      for (const bool aligned : {true, false}) {
        PackUnpack(in, length, bit_width, aligned);
      }
    }
  }
}
}

