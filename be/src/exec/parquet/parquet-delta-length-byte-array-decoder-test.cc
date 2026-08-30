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
#include <string>
#include <vector>

#include "exec/parquet/parquet-delta-encoder.h"
#include "exec/parquet/parquet-delta-length-byte-array-decoder.h"
#include "runtime/string-value.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

namespace impala {

/// Build a DELTA_LENGTH_BYTE_ARRAY encoded page buffer from a list of strings.
/// Uses ParquetDeltaEncoder<int32_t> for the lengths section.
static void BuildPage(const vector<string>& values, vector<uint8_t>* page_out) {
  // Collect lengths and concatenated data.
  vector<int32_t> lengths;
  string concat;
  for (const auto& v : values) {
    lengths.push_back(static_cast<int32_t>(v.size()));
    concat += v;
  }

  const int nvals = static_cast<int>(values.size());
  ParquetDeltaEncoder<int32_t> enc;
  ASSERT_OK(enc.Init(128, 4, std::max(nvals, 1)));
  int worst = enc.WorstCaseOutputSize(nvals);
  vector<uint8_t> len_buf(std::max(worst, 1));
  enc.NewPage(len_buf.data(), worst);
  for (int32_t len : lengths) ASSERT_TRUE(enc.Put(len));
  int len_bytes = enc.FinalizePage();

  page_out->assign(len_buf.begin(), len_buf.begin() + len_bytes);
  page_out->insert(page_out->end(), concat.begin(), concat.end());
}

/// Read all values from the decoder and return them as strings.
static vector<string> ReadAll(ParquetDeltaLengthByteArrayDecoder* dec) {
  vector<string> result;
  StringValue sv;
  int rc;
  while ((rc = dec->NextValue(&sv)) == 1) {
    result.emplace_back(sv.Ptr(), sv.Len());
  }
  EXPECT_EQ(0, rc) << "Expected page exhausted (0), got error (-1)";
  return result;
}

// ---- tests ------------------------------------------------------------------

TEST(ParquetDeltaLengthByteArrayDecoderTest, BasicRoundTrip) {
  const vector<string> input = {"Hello", "World", "Foobar", "ABCDEF"};
  vector<uint8_t> page;
  BuildPage(input, &page);

  ParquetDeltaLengthByteArrayDecoder dec;
  ASSERT_OK(dec.NewPage(page.data(), static_cast<int>(page.size()),
      static_cast<int>(page.size())));
  EXPECT_EQ(4, dec.GetTotalValueCount());

  auto got = ReadAll(&dec);
  ASSERT_EQ(input, got);
}

TEST(ParquetDeltaLengthByteArrayDecoderTest, EmptyPage) {
  vector<uint8_t> page;
  BuildPage({}, &page);

  ParquetDeltaLengthByteArrayDecoder dec;
  // Zero-length page is valid (all-NULL column).
  ASSERT_OK(dec.NewPage(nullptr, 0, 0));
  EXPECT_EQ(0, dec.GetTotalValueCount());
  StringValue sv;
  EXPECT_EQ(0, dec.NextValue(&sv));
}

TEST(ParquetDeltaLengthByteArrayDecoderTest, SingleValue) {
  const vector<string> input = {"impala"};
  vector<uint8_t> page;
  BuildPage(input, &page);

  ParquetDeltaLengthByteArrayDecoder dec;
  ASSERT_OK(dec.NewPage(page.data(), static_cast<int>(page.size()),
      static_cast<int>(page.size())));
  EXPECT_EQ(1, dec.GetTotalValueCount());

  auto got = ReadAll(&dec);
  ASSERT_EQ(input, got);
}

TEST(ParquetDeltaLengthByteArrayDecoderTest, EmptyStrings) {
  const vector<string> input = {"", "", "non-empty", ""};
  vector<uint8_t> page;
  BuildPage(input, &page);

  ParquetDeltaLengthByteArrayDecoder dec;
  ASSERT_OK(dec.NewPage(page.data(), static_cast<int>(page.size()),
      static_cast<int>(page.size())));
  EXPECT_EQ(4, dec.GetTotalValueCount());

  auto got = ReadAll(&dec);
  ASSERT_EQ(input, got);
}

TEST(ParquetDeltaLengthByteArrayDecoderTest, AllEmptyStrings) {
  const vector<string> input = {"", "", ""};
  vector<uint8_t> page;
  BuildPage(input, &page);

  ParquetDeltaLengthByteArrayDecoder dec;
  ASSERT_OK(dec.NewPage(page.data(), static_cast<int>(page.size()),
      static_cast<int>(page.size())));
  EXPECT_EQ(3, dec.GetTotalValueCount());

  auto got = ReadAll(&dec);
  ASSERT_EQ(input, got);
}

TEST(ParquetDeltaLengthByteArrayDecoderTest, PageNumValuesBoundsCheck) {
  // The encoded page has 4 values. Passing page_num_values < 4 must return an error.
  const vector<string> input = {"a", "bb", "ccc", "dddd"};
  vector<uint8_t> page;
  BuildPage(input, &page);

  ParquetDeltaLengthByteArrayDecoder dec;
  // Passing 3 (< 4) must fail.
  EXPECT_FALSE(
      dec.NewPage(page.data(), static_cast<int>(page.size()), 3).ok());
  // Passing exactly 4 must succeed.
  ASSERT_OK(dec.NewPage(page.data(), static_cast<int>(page.size()), 4));
  EXPECT_EQ(4, dec.GetTotalValueCount());
}

TEST(ParquetDeltaLengthByteArrayDecoderTest, SkipValues) {
  const vector<string> input = {"a", "bb", "ccc", "dddd", "eeeee"};
  vector<uint8_t> page;
  BuildPage(input, &page);

  ParquetDeltaLengthByteArrayDecoder dec;
  ASSERT_OK(dec.NewPage(page.data(), static_cast<int>(page.size()),
      static_cast<int>(page.size())));
  EXPECT_EQ(2, dec.SkipValues(2));

  // Read remaining 3.
  auto got = ReadAll(&dec);
  const vector<string> expected = {"ccc", "dddd", "eeeee"};
  ASSERT_EQ(expected, got);
}

TEST(ParquetDeltaLengthByteArrayDecoderTest, SkipAll) {
  const vector<string> input = {"x", "y", "z"};
  vector<uint8_t> page;
  BuildPage(input, &page);

  ParquetDeltaLengthByteArrayDecoder dec;
  ASSERT_OK(dec.NewPage(page.data(), static_cast<int>(page.size()),
      static_cast<int>(page.size())));

  EXPECT_EQ(3, dec.SkipValues(10)); // skip more than available
  EXPECT_EQ(0, dec.SkipValues(1));  // already exhausted
}

TEST(ParquetDeltaLengthByteArrayDecoderTest, BatchRead) {
  const vector<string> input = {"alpha", "beta", "gamma", "delta", "epsilon"};
  vector<uint8_t> page;
  BuildPage(input, &page);

  ParquetDeltaLengthByteArrayDecoder dec;
  ASSERT_OK(dec.NewPage(page.data(), static_cast<int>(page.size()),
      static_cast<int>(page.size())));

  // Read in one batch.
  const int n = static_cast<int>(input.size());
  vector<StringValue> out(n);
  int decoded = dec.NextValues(n, out.data(), sizeof(StringValue));
  ASSERT_EQ(n, decoded);

  for (int i = 0; i < n; ++i) {
    EXPECT_EQ(input[i], string(out[i].Ptr(), out[i].Len()))
        << "Mismatch at index " << i;
  }
}

TEST(ParquetDeltaLengthByteArrayDecoderTest, BatchReadWithStride) {
  // Read into a buffer with extra padding between values.
  const vector<string> input = {"one", "two", "three"};
  vector<uint8_t> page;
  BuildPage(input, &page);

  ParquetDeltaLengthByteArrayDecoder dec;
  ASSERT_OK(dec.NewPage(page.data(), static_cast<int>(page.size()),
      static_cast<int>(page.size())));

  // Use stride of 2 * sizeof(StringValue) to test stride > sizeof(StringValue).
  const int64_t stride = 2 * sizeof(StringValue);
  const int n = static_cast<int>(input.size());
  vector<uint8_t> buf(stride * n, 0);
  int decoded = dec.NextValues(n,
      reinterpret_cast<StringValue*>(buf.data()), stride);
  ASSERT_EQ(n, decoded);

  for (int i = 0; i < n; ++i) {
    StringValue* sv = reinterpret_cast<StringValue*>(buf.data() + i * stride);
    EXPECT_EQ(input[i], string(sv->Ptr(), sv->Len()))
        << "Mismatch at index " << i;
  }
}

TEST(ParquetDeltaLengthByteArrayDecoderTest, LargePageManyValues) {
  const int kCount = 500;
  vector<string> input;
  input.reserve(kCount);
  for (int i = 0; i < kCount; ++i) {
    input.push_back("value-" + std::to_string(i));
  }
  vector<uint8_t> page;
  BuildPage(input, &page);

  ParquetDeltaLengthByteArrayDecoder dec;
  ASSERT_OK(dec.NewPage(page.data(), static_cast<int>(page.size()),
      static_cast<int>(page.size())));
  EXPECT_EQ(kCount, dec.GetTotalValueCount());

  auto got = ReadAll(&dec);
  ASSERT_EQ(input, got);
}

TEST(ParquetDeltaLengthByteArrayDecoderTest, PointerIntoPageBuffer) {
  // Short strings (length <= SmallableString::SMALL_LIMIT) are inlined into the
  // StringValue struct by Smallify(); they must NOT be expected to point into the
  // page buffer.  Long strings still point into the page buffer.
  const string long_str = "this_is_longer_than_small_limit";
  const vector<string> input = {long_str};
  vector<uint8_t> page;
  BuildPage(input, &page);

  ParquetDeltaLengthByteArrayDecoder dec;
  ASSERT_OK(dec.NewPage(page.data(), static_cast<int>(page.size()),
      static_cast<int>(page.size())));

  StringValue sv;
  ASSERT_EQ(1, dec.NextValue(&sv));
  EXPECT_FALSE(sv.IsSmall());
  // The pointer must lie within the page buffer.
  EXPECT_GE(reinterpret_cast<const uint8_t*>(sv.Ptr()), page.data());
  EXPECT_LT(reinterpret_cast<const uint8_t*>(sv.Ptr()),
      page.data() + page.size());
}

TEST(ParquetDeltaLengthByteArrayDecoderTest, ShortStringsInlined) {
  // Strings short enough to fit the small-string representation must be inlined
  // (smallified) so that callers need not keep the page buffer alive for them.
  const vector<string> input = {"hello", "world", ""};
  vector<uint8_t> page;
  BuildPage(input, &page);

  ParquetDeltaLengthByteArrayDecoder dec;
  ASSERT_OK(dec.NewPage(page.data(), static_cast<int>(page.size()),
      static_cast<int>(page.size())));

  for (const string& s : input) {
    StringValue sv;
    ASSERT_EQ(1, dec.NextValue(&sv));
    EXPECT_TRUE(sv.IsSmall()) << "expected smallified for \"" << s << "\"";
  }
}

TEST(ParquetDeltaLengthByteArrayDecoderTest, NewPageResets) {
  const vector<string> first = {"aaa", "bbb"};
  const vector<string> second = {"x", "yy", "zzz"};

  vector<uint8_t> page1, page2;
  BuildPage(first, &page1);
  BuildPage(second, &page2);

  ParquetDeltaLengthByteArrayDecoder dec;

  ASSERT_OK(dec.NewPage(page1.data(), static_cast<int>(page1.size()),
      static_cast<int>(page1.size())));
  auto got1 = ReadAll(&dec);
  ASSERT_EQ(first, got1);

  // Re-use decoder for a second page.
  ASSERT_OK(dec.NewPage(page2.data(), static_cast<int>(page2.size()),
      static_cast<int>(page2.size())));
  auto got2 = ReadAll(&dec);
  ASSERT_EQ(second, got2);
}

TEST(ParquetDeltaLengthByteArrayDecoderTest, ExhaustedReturnsZero) {
  const vector<string> input = {"only"};
  vector<uint8_t> page;
  BuildPage(input, &page);

  ParquetDeltaLengthByteArrayDecoder dec;
  ASSERT_OK(dec.NewPage(page.data(), static_cast<int>(page.size()),
      static_cast<int>(page.size())));

  StringValue sv;
  EXPECT_EQ(1, dec.NextValue(&sv));   // reads the one value
  EXPECT_EQ(0, dec.NextValue(&sv));   // exhausted -> 0
  EXPECT_EQ(0, dec.NextValue(&sv));   // still 0
}

TEST(ParquetDeltaLengthByteArrayDecoderTest, TruncatedDataSection) {
  // Pass data_len smaller than the full page so bytes_remaining_ is set smaller
  // than the total claimed by the lengths; FillLengthsBuffer must return -1.
  const vector<string> input = {"hello", "world", "impala"};
  vector<uint8_t> page;
  BuildPage(input, &page);

  ParquetDeltaLengthByteArrayDecoder dec;
  ASSERT_OK(dec.NewPage(page.data(), static_cast<int>(page.size()) - 3,
      static_cast<int>(page.size())));
  StringValue sv;
  EXPECT_EQ(-1, dec.NextValue(&sv));
}

TEST(ParquetDeltaLengthByteArrayDecoderTest, PageNumValuesUpperBound) {
  // page_num_values includes NULL slots and may exceed the actual encoded count;
  // NewPage must succeed and yield only the encoded values.
  const vector<string> input = {"a", "bb", "ccc"};
  vector<uint8_t> page;
  BuildPage(input, &page);

  ParquetDeltaLengthByteArrayDecoder dec;
  ASSERT_OK(dec.NewPage(page.data(), static_cast<int>(page.size()), 100));
  EXPECT_EQ(3, dec.GetTotalValueCount());
  ASSERT_EQ(input, ReadAll(&dec));
}

TEST(ParquetDeltaLengthByteArrayDecoderTest, HasOnlySmallStringsTrue) {
  const vector<string> input = {"hi", "ok", ""};
  vector<uint8_t> page;
  BuildPage(input, &page);

  ParquetDeltaLengthByteArrayDecoder dec;
  ASSERT_OK(dec.NewPage(page.data(), static_cast<int>(page.size()),
      static_cast<int>(page.size())));
  ReadAll(&dec);
  EXPECT_TRUE(dec.HasOnlySmallStrings());
}

TEST(ParquetDeltaLengthByteArrayDecoderTest, HasOnlySmallStringsFalse) {
  const string long_str(SmallableString::SMALL_LIMIT + 1, 'x');
  const vector<string> input = {"short", long_str};
  vector<uint8_t> page;
  BuildPage(input, &page);

  ParquetDeltaLengthByteArrayDecoder dec;
  ASSERT_OK(dec.NewPage(page.data(), static_cast<int>(page.size()),
      static_cast<int>(page.size())));
  ReadAll(&dec);
  EXPECT_FALSE(dec.HasOnlySmallStrings());
}

TEST(ParquetDeltaLengthByteArrayDecoderTest, HasOnlySmallStringsResetsOnNewPage) {
  // After a page with a long string (flag=false), a new page with only short
  // strings must reset the flag back to true.
  const string long_str(SmallableString::SMALL_LIMIT + 1, 'x');
  vector<uint8_t> page1, page2;
  BuildPage({long_str}, &page1);
  BuildPage({"hi", "ok"}, &page2);

  ParquetDeltaLengthByteArrayDecoder dec;
  ASSERT_OK(dec.NewPage(page1.data(), static_cast<int>(page1.size()),
      static_cast<int>(page1.size())));
  ReadAll(&dec);
  ASSERT_FALSE(dec.HasOnlySmallStrings());

  ASSERT_OK(dec.NewPage(page2.data(), static_cast<int>(page2.size()),
      static_cast<int>(page2.size())));
  ReadAll(&dec);
  EXPECT_TRUE(dec.HasOnlySmallStrings());
}

TEST(ParquetDeltaLengthByteArrayDecoderTest, MoreThanBatchSizeValues) {
  // 1025 values forces FillLengthsBuffer() to be called more than once
  // (LENGTHS_BATCH_SIZE = 1024).
  const int kCount = 1025;
  vector<string> input;
  input.reserve(kCount);
  for (int i = 0; i < kCount; ++i) input.push_back(std::to_string(i));
  vector<uint8_t> page;
  BuildPage(input, &page);

  ParquetDeltaLengthByteArrayDecoder dec;
  ASSERT_OK(dec.NewPage(page.data(), static_cast<int>(page.size()),
      static_cast<int>(page.size())));
  EXPECT_EQ(kCount, dec.GetTotalValueCount());
  ASSERT_EQ(input, ReadAll(&dec));
}

TEST(ParquetDeltaLengthByteArrayDecoderTest, ExactlyBatchSizeValues) {
  // Exactly LENGTHS_BATCH_SIZE values: FillLengthsBuffer fills once and is then empty.
  const int kCount = 1024;
  vector<string> input;
  input.reserve(kCount);
  for (int i = 0; i < kCount; ++i) input.push_back(std::to_string(i));
  vector<uint8_t> page;
  BuildPage(input, &page);

  ParquetDeltaLengthByteArrayDecoder dec;
  ASSERT_OK(dec.NewPage(page.data(), static_cast<int>(page.size()),
      static_cast<int>(page.size())));
  EXPECT_EQ(kCount, dec.GetTotalValueCount());
  ASSERT_EQ(input, ReadAll(&dec));
}

TEST(ParquetDeltaLengthByteArrayDecoderTest, SkipValuesAcrossBatchBoundary) {
  // 1025 values: skipping all forces FillLengthsBuffer() to refill during SkipValues.
  const int kCount = 1025;
  vector<string> input;
  input.reserve(kCount);
  for (int i = 0; i < kCount; ++i) input.push_back(std::to_string(i));
  vector<uint8_t> page;
  BuildPage(input, &page);

  ParquetDeltaLengthByteArrayDecoder dec;
  ASSERT_OK(dec.NewPage(page.data(), static_cast<int>(page.size()),
      static_cast<int>(page.size())));
  EXPECT_EQ(kCount, dec.SkipValues(kCount));
  StringValue sv;
  EXPECT_EQ(0, dec.NextValue(&sv));
}

TEST(ParquetDeltaLengthByteArrayDecoderTest, TruncatedDataSectionSkip) {
  // SkipValues on a truncated page must return -1 (FillLengthsBuffer hits the check).
  const vector<string> input = {"hello", "world", "impala"};
  vector<uint8_t> page;
  BuildPage(input, &page);

  ParquetDeltaLengthByteArrayDecoder dec;
  ASSERT_OK(dec.NewPage(page.data(), static_cast<int>(page.size()) - 3,
      static_cast<int>(page.size())));
  EXPECT_EQ(-1, dec.SkipValues(3));
}

} // namespace impala
