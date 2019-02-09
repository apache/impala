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

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <random>
#include <unordered_map>

#include "testutil/gtest-util.h"
#include "testutil/rand-util.h"
#include "testutil/mem-util.h"
#include "util/arithmetic-util.h"
#include "util/bit-packing.h"
#include "util/bit-stream-utils.inline.h"

#include "common/names.h"

using std::uniform_int_distribution;
using std::mt19937;

namespace impala {

namespace {

constexpr int NUM_IN_VALUES = 64 * 1024;

template <typename UINT_T>
constexpr UINT_T ComputeMask(int bit_width) {
  return (bit_width < sizeof(UINT_T) * CHAR_BIT) ?
      ((1UL << bit_width) - 1) : static_cast<UINT_T>(~0UL);
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
template <typename UINT_T>
void UnpackSubset(const UINT_T* in, const uint8_t* packed, int num_in_values,
    int bit_width, bool aligned);

/// Test a packing/unpacking round-trip of the 'num_in_values' values in 'in',
/// packed with 'bit_width'. If 'aligned' is true, buffers for packed and unpacked data
/// are allocated at a 64-byte aligned address. Otherwise the buffers are misaligned
/// by 1 byte from a 64-byte aligned address.
template <typename UINT_T>
void PackUnpack(const UINT_T* in, int num_in_values, int bit_width, bool aligned) {
  LOG(INFO) << "num_in_values = " << num_in_values << " bit_width = " << bit_width
            << " aligned = " << aligned;

  // Mask out higher bits so that the values to pack are in range.
  const UINT_T mask = ComputeMask<UINT_T>(bit_width);
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
    AlignedAllocation out_storage(num_to_unpack * sizeof(UINT_T) + misalignment);
    UINT_T* out = reinterpret_cast<UINT_T*>(out_storage.data() + misalignment);
    const auto result = BitPacking::UnpackValues<UINT_T>(
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
      EXPECT_EQ(in[i] & mask, out[i]) << "Didn't get back input value " << i << "."
          << " Bit width: " << bit_width << ".";
    }
  }
  UnpackSubset<UINT_T>(in, packed, num_in_values, bit_width, aligned);
}

template <typename UINT_T>
void UnpackSubset(const UINT_T* in, const uint8_t* packed, int num_in_values,
    int bit_width, bool aligned) {
  const int misalignment = aligned ? 0 : 1;
  for (int num_to_unpack : {1, 10, 77, num_in_values - 7}) {
    if (num_to_unpack < 0 || num_to_unpack > num_in_values) continue;

    // Size buffers exactly so that ASAN can detect buffer overruns.
    const int64_t bytes_to_read = BitUtil::RoundUpNumBytes(num_to_unpack * bit_width);
    AlignedAllocation packed_copy_storage(bytes_to_read + misalignment);
    uint8_t* packed_copy = packed_copy_storage.data() + misalignment;
    memcpy(packed_copy, packed, bytes_to_read);
    AlignedAllocation out_storage(num_to_unpack * sizeof(UINT_T) + misalignment);
    UINT_T* out = reinterpret_cast<UINT_T*>(out_storage.data() + misalignment);
    const auto result = BitPacking::UnpackValues<UINT_T>(
        bit_width, packed_copy, bytes_to_read, num_to_unpack, out);
    ASSERT_EQ(packed_copy + bytes_to_read, result.first) << "Read wrong # of bytes";
    ASSERT_EQ(num_to_unpack, result.second) << "Unpacked wrong # of values";

    for (int i = 0; i < num_to_unpack; ++i) {
      ASSERT_EQ(in[i] & ComputeMask<UINT_T>(bit_width), out[i])
          << "Didn't get back input value " << i;
    }
  }
}

template <typename INT_T>
std::vector<INT_T> GenerateRandomInput(int length, INT_T lower, INT_T higher) {
  std::vector<INT_T> in(length);
  mt19937 rng;
  RandTestUtil::SeedRng("BIT_PACKING_TEST_RANDOM_SEED", &rng);
  uniform_int_distribution<INT_T> dist(lower, higher);
  std::generate(in.begin(), in.end(), [&rng, &dist] { return dist(rng); });

  return in;
}

// Test various odd input lengths to exercise boundary cases for full and partial
// batches of 32.
std::vector<int> GetLengths() {
  vector<int> lengths{NUM_IN_VALUES, NUM_IN_VALUES - 1, NUM_IN_VALUES - 16,
      NUM_IN_VALUES - 19, NUM_IN_VALUES - 31};
  for (int i = 0; i < 32; ++i) {
    lengths.push_back(i);
  }

  return lengths;
}

template <typename UINT_T>
void RandomUnpackTest() {
  const std::vector<UINT_T> in = GenerateRandomInput<UINT_T>(NUM_IN_VALUES,
      std::numeric_limits<UINT_T>::min(), std::numeric_limits<UINT_T>::max());

  const std::vector<int> lengths = GetLengths();

  constexpr int MAX_BITWIDTH = BitPacking::MAX_BITWIDTH;
  const int max_bit_width = std::min<int>(MAX_BITWIDTH, sizeof(UINT_T) * CHAR_BIT);
  for (int bit_width = 0; bit_width <= max_bit_width; ++bit_width) {
    for (const int length : lengths) {
      // Test that unpacking to/from aligned and unaligned memory works.
      for (const bool aligned : {true, false}) {
        PackUnpack<UINT_T>(in.data(), length, bit_width, aligned);
      }
    }
  }
}

TEST(BitPackingTest, RandomUnpack8) {
  RandomUnpackTest<uint8_t>();
}

TEST(BitPackingTest, RandomUnpack16) {
  RandomUnpackTest<uint16_t>();
}

TEST(BitPackingTest, RandomUnpack32) {
  RandomUnpackTest<uint32_t>();
}

TEST(BitPackingTest, RandomUnpack64) {
  RandomUnpackTest<uint64_t>();
}

// This is not the full dictionary encoding, only a big bit-packed literal run, no RLE is
// used.
template <typename T>
std::pair<std::vector<T>, std::vector<uint8_t>>
DictEncode(const std::vector<T>& input, int bit_width) {
  // Create dictionary and indices.
  std::unordered_map<T, std::size_t> index_map;

  std::vector<T> dict;
  std::vector<uint64_t> indices;

  for (const T value : input) {
    auto it = index_map.find(value);
    if (it != index_map.end()) {
      indices.push_back(it->second);
    } else {
      const std::size_t next_index = dict.size();
      index_map[value] = next_index;
      dict.push_back(value);
      indices.push_back(next_index);
    }
  }

  // Bit pack the indices.
  const int bytes_required = BitUtil::RoundUpNumBytes(bit_width * input.size());
  std::vector<uint8_t> out_data(bytes_required);

  // We do not write data if we do not have any. Doing it could also lead to undefined
  // behaviour as out_data.data() may be nullptr and BitWriter uses memcpy internally, and
  // passing a null pointer to memcpy is undefined behaviour.
  if (bytes_required > 0) {
    BitWriter writer(out_data.data(), bytes_required);
    if (bit_width > 0) {
      for (const uint64_t index : indices) {
        EXPECT_TRUE(writer.PutValue(index, bit_width));
      }
    }
    writer.Flush();
  }

  return std::make_pair(dict, out_data);
}

template <typename T>
void ExpectEqualsWithStride(const T* expected, int num_values,
    const uint8_t* actual, int actual_length, int stride) {
  for (std::size_t i = 0; i < num_values; i++) {
    const T expected_value = expected[i];

    DCHECK_LE(i * stride + sizeof(T), actual_length);
    const T actual_value = *reinterpret_cast<const T*>(&actual[i * stride]);

    EXPECT_EQ(expected_value, actual_value);
  }
}

// This does not test the full dictionary decoding, only the unpacking of a single
// bit-packed literal run (no RLE).
template <typename UINT_T>
void RandomUnpackAndDecodeTest() {
  constexpr int MAX_BITWIDTH = BitPacking::MAX_BITWIDTH;
  const int max_bit_width = std::min<int>({MAX_BITWIDTH, sizeof(UINT_T) * 8,
      sizeof(uint32_t) * 8 /* dictionary indices are 32 bit values */});

  const std::vector<int> lengths = GetLengths();

  for (int bit_width = 0; bit_width <= max_bit_width; ++bit_width) {
    const UINT_T max_dict_size = bit_width == sizeof(UINT_T) * 8
        ? std::numeric_limits<UINT_T>::max() : (1UL << bit_width);

    const std::vector<UINT_T> input = GenerateRandomInput<UINT_T>(NUM_IN_VALUES, 0,
        max_dict_size - 1 /* inclusive range */);

    for (int length : lengths) {
      const std::vector<UINT_T> in_data(input.begin(), input.begin() + length);

      std::pair<std::vector<UINT_T>, std::vector<uint8_t>> dict_and_data
          = DictEncode(in_data, bit_width);
      std::vector<UINT_T>& dict = dict_and_data.first;
      const std::vector<uint8_t>& data = dict_and_data.second;

      const std::vector<int> strides = {sizeof(UINT_T), sizeof(UINT_T) + 5,
        2 * sizeof(UINT_T) + 5};

      for (int stride : strides) {
        bool decode_error = false;
        std::vector<uint8_t> out(in_data.size() * stride);

        std::pair<const uint8_t*, int64_t> res
            = BitPacking::UnpackAndDecodeValues<UINT_T>(bit_width, data.data(),
                data.size(), dict.data(), dict.size(), in_data.size(),
            reinterpret_cast<UINT_T*>(out.data()), stride, &decode_error);

        EXPECT_FALSE(decode_error);
        EXPECT_EQ(in_data.size(), res.second);
        ExpectEqualsWithStride<UINT_T>(in_data.data(), in_data.size(), out.data(),
            out.size(), stride);
      }
    }
  }
}

TEST(BitPackingTest, RandomUnpackAndDecode8) {
  RandomUnpackAndDecodeTest<uint8_t>();
}

TEST(BitPackingTest, RandomUnpackAndDecode16) {
  RandomUnpackAndDecodeTest<uint16_t>();
}

TEST(BitPackingTest, RandomUnpackAndDecode32) {
  RandomUnpackAndDecodeTest<uint32_t>();
}

TEST(BitPackingTest, RandomUnpackAndDecode64) {
  RandomUnpackAndDecodeTest<uint64_t>();
}

template <typename INT_T>
struct DeltaData {
  DeltaData(INT_T base_value, INT_T delta_offset, std::size_t num_values,
      std::vector<uint8_t> packed_deltas)
      : base_value(base_value), delta_offset(delta_offset), num_values(num_values),
      packed_deltas(packed_deltas)
  {}

  INT_T base_value;
  INT_T delta_offset;
  std::size_t num_values;
  std::vector<uint8_t> packed_deltas;
};

template <typename ParquetType>
DeltaData<ParquetType> DeltaEncode(const std::vector<ParquetType>& input, int bit_width) {
  using UnsignedParquetType = typename std::make_unsigned<ParquetType>::type;
  const ParquetType base_value = input.empty() ? 0 : input[0];
  std::vector<ParquetType> deltas;

  /// Calculate the deltas.
  for (int i = 1; i < input.size(); i++) {
    UnsignedParquetType current = input[i];
    UnsignedParquetType previous = input[i - 1];
    ParquetType delta = current - previous;
    deltas.push_back(delta);
  }

  /// Offset the deltas.
  const auto it = std::min_element(deltas.begin(), deltas.end());
  const ParquetType min_delta = (it == deltas.end()) ? 0 : *it;

  std::for_each(deltas.begin(), deltas.end(), [min_delta](ParquetType& element) {
      element = ArithmeticUtil::AsUnsigned<std::minus>(element, min_delta);
      });

  // Bit pack the deltas.
  const int bytes_required = BitUtil::RoundUpNumBytes(bit_width * deltas.size());
  std::vector<uint8_t> out_data(bytes_required);

  if (bytes_required > 0) {
    BitWriter writer(out_data.data(), bytes_required);
    if (bit_width > 0) {
      for (const ParquetType delta : deltas) {
        const UnsignedParquetType delta_unsigned = delta;
        EXPECT_TRUE(writer.PutValue(delta_unsigned, bit_width));
      }
    }
    writer.Flush();
  }

  return DeltaData<ParquetType>(base_value, min_delta, deltas.size(), out_data);
}

template <typename INT_T, typename ParquetType>
void RandomUnpackAndDeltaDecodeTest() {
  constexpr int MAX_BITWIDTH = std::min<int>(BitPacking::MAX_BITWIDTH,
      sizeof(INT_T) * 8);
  for (int bit_width = 0; bit_width <= MAX_BITWIDTH; ++bit_width) {
    const INT_T max_delta = bit_width == 0 ? 0 : (1UL << (bit_width - 1)) - 1;
    const INT_T min_delta = bit_width == 0 ? 0 : -max_delta - 1;

    const std::vector<ParquetType> delta_data = GenerateRandomInput<ParquetType>(
        NUM_IN_VALUES, min_delta, max_delta);

    const std::vector<int> lengths = GetLengths();

    for (int length : lengths) {
      std::vector<ParquetType> in_data;

      std::partial_sum(delta_data.begin(), delta_data.begin() + length,
          std::back_inserter(in_data),
          /// We add the elements as unsigned to have defined overflow.
          ArithmeticUtil::AsUnsigned<std::plus, ParquetType>);

      /// Convert the input values to INT_T to compare them with the output.
      const std::vector<INT_T> in_data_as_int_t(in_data.begin(), in_data.end());

      DeltaData<ParquetType> encoded = DeltaEncode<ParquetType>(in_data, bit_width);

      const std::vector<int> strides = {sizeof(INT_T), sizeof(INT_T) + 5,
        2 * sizeof(INT_T) + 5};

      for (int stride : strides) {
        bool decode_error = false;
        std::vector<uint8_t> out(in_data.size() * stride);

        uint8_t* out_ptr = nullptr;
        if (in_data.empty()) {
          /// We do not copy anything and do not increment the pointer if we do not have
          /// any elements.
          out_ptr = out.data();
        } else {
          /// If we have elements, copy the base value to the beginning so the vector
          /// after unpacking can be compared directly to the vector of input numbers.
          memcpy(out.data(), &encoded.base_value, sizeof(INT_T));
          out_ptr = out.data() + stride;
        }

        /// Converting to ParquetType.
        ParquetType base_value = encoded.base_value;
        ParquetType delta_offset = encoded.delta_offset;

        std::pair<const uint8_t*, int64_t> res
            = BitPacking::UnpackAndDeltaDecodeValues<INT_T, ParquetType>(
                bit_width, encoded.packed_deltas.data(), encoded.packed_deltas.size(),
                &base_value, delta_offset, encoded.num_values,
                reinterpret_cast<INT_T*>(out_ptr), stride, &decode_error);

        EXPECT_FALSE(decode_error);
        EXPECT_EQ(in_data_as_int_t.size(), res.second + (length == 0 ? 0 : 1));
        ExpectEqualsWithStride<INT_T>(in_data_as_int_t.data(), in_data_as_int_t.size(),
            out.data(), out.size(), stride);
      }
    }
  }
}

TEST(BitPackingTest, RandomUnpackAndDeltaDecode8_32) {
  RandomUnpackAndDeltaDecodeTest<int8_t, int32_t>();
}

TEST(BitPackingTest, RandomUnpackAndDeltaDecode16_32) {
  RandomUnpackAndDeltaDecodeTest<int16_t, int32_t>();
}

TEST(BitPackingTest, RandomUnpackAndDeltaDecode32_32) {
  RandomUnpackAndDeltaDecodeTest<int32_t, int32_t>();
}

TEST(BitPackingTest, RandomUnpackAndDeltaDecode8_64) {
  RandomUnpackAndDeltaDecodeTest<int8_t, int64_t>();
}

TEST(BitPackingTest, RandomUnpackAndDeltaDecode16_64) {
  RandomUnpackAndDeltaDecodeTest<int16_t, int64_t>();
}

TEST(BitPackingTest, RandomUnpackAndDeltaDecode32_64) {
  RandomUnpackAndDeltaDecodeTest<int32_t, int64_t>();
}

TEST(BitPackingTest, RandomUnpackAndDeltaDecode64_64) {
  RandomUnpackAndDeltaDecodeTest<int64_t, int64_t>();
}

}
