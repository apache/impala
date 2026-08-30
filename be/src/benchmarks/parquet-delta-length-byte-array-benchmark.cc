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

// Benchmarks for ParquetDeltaLengthByteArrayDecoder, covering:
//   1. Short vs long vs mixed strings   (smallify path vs zero-copy path)
//   2. NextValue vs NextValues vs Skip  (access patterns)
//   3. Page size                        (FillLengthsBuffer() refill overhead)
//   4. Read/skip ratio                  (interspersed decode+skip)
//   5. Delta vs Plain encoding          (same data, PLAIN as a baseline)
//
// Run from IMPALA_HOME after building:
//   be/build/release/benchmarks/parquet-delta-length-byte-array-benchmark
//
// ---- Sample results (debug build, 13th Gen Intel Core i9-13900) ----
//
// Short (len=5) vs Long (len=20) vs Mixed | NextValues
//    all-long   146 iters/ms   1X      (zero-copy: only pointer stored, no Smallify)
//    mixed M-L  137 iters/ms   0.94X   (short-first phase: S,L,S,L)
//    mixed L-M  117 iters/ms   0.80X   (long-first phase:  L,S,L,S; ~15% below M-L)
//    all-short   98 iters/ms   0.67X   (Smallify copies len bytes per value)
//
// NextValue vs NextValues vs SkipValues | mixed strings
//    NextValue (1-by-1)  50 iters/ms   1X
//    NextValues (batch)  72 iters/ms   1.43X
//    SkipValues          89 iters/ms   1.76X
//
// Page size: 1024 vs 1025 vs 2048 vs 10000 values | NextValues
//    1024 values (no refill)  248 iters/ms   1X
//    1025 values (1 refill)   247 iters/ms   0.997X  <- refill overhead ~0.3%
//    2048 values (2 refills)  125 iters/ms   0.503X  (linear scaling, expected)
//    10000 values (9 refills)  25 iters/ms   0.102X  (linear scaling, expected)
//
// Delta vs Plain (baseline) | mixed strings
//    NextValues (batch)  delta 1X   plain 1.8X   (delta pays delta-decode of lengths)
//    NextValue (1-by-1)  delta 1X   plain 3.7X
//    SkipValues          delta 1X   plain 9.4X   (plain only reads length prefixes;
//                                                 delta must decode every delta)

#include <algorithm>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#include "exec/parquet/parquet-common.h"
#include "exec/parquet/parquet-delta-encoder.h"
#include "exec/parquet/parquet-delta-length-byte-array-decoder.h"
#include "runtime/string-value.h"
#include "util/benchmark.h"
#include "util/cpu-info.h"

using namespace impala;
using std::cerr;
using std::cout;
using std::endl;
using std::min;
using std::string;
using std::vector;

// Default number of values per page — straddles four LENGTHS_BATCH_SIZE (1024) fills.
static constexpr int DEFAULT_NUM_VALUES = 5000;
// SmallableString::SMALL_LIMIT — strings <= this length are smallified (inlined).
static constexpr int SMALL_LIMIT = 11;

// ---- Page builder ---------------------------------------------------------------

// Build a DELTA_LENGTH_BYTE_ARRAY encoded page from a list of strings.
static vector<uint8_t> BuildPage(const vector<string>& values) {
  vector<int32_t> lengths;
  string concat;
  for (const auto& v : values) {
    lengths.push_back(static_cast<int32_t>(v.size()));
    concat += v;
  }
  int nvals = static_cast<int>(values.size());
  ParquetDeltaEncoder<int32_t> enc;
  Status s = enc.Init(128, 4, std::max(nvals, 1));
  DCHECK(s.ok()) << s.GetDetail();
  int worst = enc.WorstCaseOutputSize(nvals);
  vector<uint8_t> buf(std::max(worst, 1));
  enc.NewPage(buf.data(), worst);
  for (int32_t len : lengths) DCHECK(enc.Put(len));
  int len_bytes = enc.FinalizePage();
  vector<uint8_t> result(buf.begin(), buf.begin() + len_bytes);
  result.insert(result.end(), concat.begin(), concat.end());
  return result;
}

// Build a PLAIN encoded BYTE_ARRAY page: each value is a 4-byte little-endian
// length prefix followed by the raw bytes. Used as a baseline for comparison.
static vector<uint8_t> BuildPlainPage(const vector<string>& values) {
  vector<uint8_t> result;
  for (const auto& v : values) {
    int32_t len = static_cast<int32_t>(v.size());
    const uint8_t* lp = reinterpret_cast<const uint8_t*>(&len);
    result.insert(result.end(), lp, lp + sizeof(int32_t));
    result.insert(result.end(), v.begin(), v.end());
  }
  return result;
}

// ---- String generators ----------------------------------------------------------

// n strings of exactly 'len' bytes, cycling through a-z to vary content.
static vector<string> GenUniform(int n, int len) {
  vector<string> v;
  v.reserve(n);
  for (int i = 0; i < n; ++i) v.push_back(string(len, 'a' + i % 26));
  return v;
}

// n strings alternating between two lengths: even indices get 'len_a', odd get
// 'len_b'. Swapping len_a/len_b flips the alternation phase (short-first vs long-first).
static vector<string> GenMixed(int n, int len_a, int len_b) {
  vector<string> v;
  v.reserve(n);
  for (int i = 0; i < n; ++i)
    v.push_back(string(i % 2 == 0 ? len_a : len_b, 'a' + i % 26));
  return v;
}

// ---- Data holder ----------------------------------------------------------------

struct PageData {
  vector<uint8_t> page;        // DELTA_LENGTH_BYTE_ARRAY encoded
  vector<uint8_t> plain_page;  // PLAIN encoded, same values (baseline)
  int num_values;
  vector<StringValue> out_buf;

  explicit PageData(const vector<string>& strings)
    : page(BuildPage(strings)),
      plain_page(BuildPlainPage(strings)),
      num_values(static_cast<int>(strings.size())),
      out_buf(strings.size()) {}
};

// ---- Benchmark functions --------------------------------------------------------

void Bench_NextValue(int iters, void* d) {
  PageData* data = static_cast<PageData*>(d);
  ParquetDeltaLengthByteArrayDecoder dec;
  for (int iter = 0; iter < iters; ++iter) {
    Status s = dec.NewPage(data->page.data(), static_cast<int>(data->page.size()),
        data->num_values);
    DCHECK(s.ok());
    StringValue* sv = data->out_buf.data();
    while (dec.NextValue(sv++) == 1) {}
  }
}

void Bench_NextValues(int iters, void* d) {
  PageData* data = static_cast<PageData*>(d);
  ParquetDeltaLengthByteArrayDecoder dec;
  for (int iter = 0; iter < iters; ++iter) {
    Status s = dec.NewPage(data->page.data(), static_cast<int>(data->page.size()),
        data->num_values);
    DCHECK(s.ok());
    int n = dec.NextValues(data->num_values, data->out_buf.data(),
        static_cast<int64_t>(sizeof(StringValue)));
    DCHECK_EQ(n, data->num_values);
  }
}

void Bench_SkipValues(int iters, void* d) {
  PageData* data = static_cast<PageData*>(d);
  ParquetDeltaLengthByteArrayDecoder dec;
  for (int iter = 0; iter < iters; ++iter) {
    Status s = dec.NewPage(data->page.data(), static_cast<int>(data->page.size()),
        data->num_values);
    DCHECK(s.ok());
    int n = dec.SkipValues(data->num_values);
    DCHECK_EQ(n, data->num_values);
  }
}

// ---- PLAIN baseline (same data, PLAIN encoding) ---------------------------------

// Batch decode via the production PLAIN path (ParquetPlainEncoder::DecodeBatch).
void Bench_Plain_NextValues(int iters, void* d) {
  PageData* data = static_cast<PageData*>(d);
  const uint8_t* begin = data->plain_page.data();
  const uint8_t* end = begin + data->plain_page.size();
  for (int iter = 0; iter < iters; ++iter) {
    int64_t n = ParquetPlainEncoder::DecodeBatch<StringValue, parquet::Type::BYTE_ARRAY>(
        begin, end, 0, data->num_values,
        static_cast<int64_t>(sizeof(StringValue)), data->out_buf.data());
    DCHECK_GE(n, 0);
  }
}

// One-by-one decode via ParquetPlainEncoder::Decode.
void Bench_Plain_NextValue(int iters, void* d) {
  PageData* data = static_cast<PageData*>(d);
  const uint8_t* begin = data->plain_page.data();
  const uint8_t* end = begin + data->plain_page.size();
  for (int iter = 0; iter < iters; ++iter) {
    const uint8_t* p = begin;
    StringValue* sv = data->out_buf.data();
    for (int i = 0; i < data->num_values; ++i) {
      int read = ParquetPlainEncoder::Decode<StringValue, parquet::Type::BYTE_ARRAY>(
          p, end, 0, sv++);
      DCHECK_GT(read, 0);
      p += read;
    }
  }
}

// Skip by walking the 4-byte length prefixes; PLAIN needs no length decoding.
void Bench_Plain_SkipValues(int iters, void* d) {
  PageData* data = static_cast<PageData*>(d);
  const uint8_t* begin = data->plain_page.data();
  for (int iter = 0; iter < iters; ++iter) {
    const uint8_t* p = begin;
    for (int i = 0; i < data->num_values; ++i) {
      int32_t len;
      memcpy(&len, p, sizeof(int32_t));
      p += sizeof(int32_t) + len;
    }
  }
}

// Interleave batches of READ decodes with batches of SKIP skips.
template <int READ, int SKIP>
void Bench_ReadSkip(int iters, void* d) {
  PageData* data = static_cast<PageData*>(d);
  ParquetDeltaLengthByteArrayDecoder dec;
  for (int iter = 0; iter < iters; ++iter) {
    Status s = dec.NewPage(data->page.data(), static_cast<int>(data->page.size()),
        data->num_values);
    DCHECK(s.ok());
    StringValue* sv = data->out_buf.data();
    int remaining = data->num_values;
    while (remaining > 0) {
      int n = dec.NextValues(min(READ, remaining), sv,
          static_cast<int64_t>(sizeof(StringValue)));
      if (n <= 0) break;
      sv += n;
      remaining -= n;
      if (remaining > 0) {
        int s = dec.SkipValues(min(SKIP, remaining));
        if (s <= 0) break;
        remaining -= s;
      }
    }
  }
}

// ---- Correctness check ----------------------------------------------------------

// Sanity-check: decode the page independently and compare against 'expected'.
static void VerifyPage(PageData* data, const vector<string>& expected) {
  ParquetDeltaLengthByteArrayDecoder dec;
  Status s = dec.NewPage(data->page.data(), static_cast<int>(data->page.size()),
      data->num_values);
  if (!s.ok()) { cerr << "NewPage failed: " << s.GetDetail() << endl; return; }
  int n = dec.NextValues(data->num_values, data->out_buf.data(),
      static_cast<int64_t>(sizeof(StringValue)));
  if (n != data->num_values) {
    cerr << "NextValues decoded " << n << " of " << data->num_values << endl;
    return;
  }
  for (int i = 0; i < data->num_values; ++i) {
    const StringValue& sv = data->out_buf[i];
    if (sv.Len() != static_cast<int>(expected[i].size()) ||
        memcmp(sv.Ptr(), expected[i].data(), sv.Len()) != 0) {
      cerr << "Mismatch at index " << i << ": expected len " << expected[i].size()
           << ", got len " << sv.Len() << endl;
      return;
    }
  }
}

// ---- Scenarios ------------------------------------------------------------------

// Scenario 1: does string length (and thus the smallify path) affect throughput?
//   all-short  len=5   <= SMALL_LIMIT: all strings inlined into StringValue
//   all-long   len=20  >  SMALL_LIMIT: all strings are zero-copy refs into the page
//   mixed M-L / L-M: strict 50/50 alternation of len=5 (M) and len=20 (L), differing
//     only in which length is first. Same multiset and bytes, yet the two phases
//     measure ~15% apart at full clock (see sample-results header).
void ShortVsLongVsMixed() {
  auto short_strs = GenUniform(DEFAULT_NUM_VALUES, 5);
  auto long_strs  = GenUniform(DEFAULT_NUM_VALUES, 20);
  auto mixed_ml   = GenMixed  (DEFAULT_NUM_VALUES, 5, 20);  // M,L,M,L (short first)
  auto mixed_lm   = GenMixed  (DEFAULT_NUM_VALUES, 20, 5);  // L,M,L,M (long first)

  PageData short_data(short_strs);
  PageData long_data (long_strs);
  PageData mixed_ml_data(mixed_ml);
  PageData mixed_lm_data(mixed_lm);

  Benchmark suite("Short (len=5) vs Long (len=20) vs Mixed | NextValues");
  suite.AddBenchmark("all-short",  Bench_NextValues, &short_data);
  suite.AddBenchmark("all-long",   Bench_NextValues, &long_data);
  suite.AddBenchmark("mixed M-L",  Bench_NextValues, &mixed_ml_data);
  suite.AddBenchmark("mixed L-M",  Bench_NextValues, &mixed_lm_data);
  cout << suite.Measure();

  VerifyPage(&short_data, short_strs);
  VerifyPage(&long_data,  long_strs);
  VerifyPage(&mixed_ml_data, mixed_ml);
  VerifyPage(&mixed_lm_data, mixed_lm);
}

// Scenario 2: how much faster is batch decode vs one-by-one, and how fast is skip?
void AccessPatternComparison() {
  auto strs = GenMixed(DEFAULT_NUM_VALUES, 5, 20);
  PageData data(strs);

  Benchmark suite("NextValue vs NextValues vs SkipValues | mixed strings");
  suite.AddBenchmark("NextValue (1-by-1)", Bench_NextValue,  &data);
  suite.AddBenchmark("NextValues (batch)", Bench_NextValues, &data);
  suite.AddBenchmark("SkipValues",         Bench_SkipValues, &data);
  cout << suite.Measure();

  VerifyPage(&data, strs);
}

// Scenario 3: FillLengthsBuffer() refill overhead as page size crosses 1024 boundary.
//   1024 values: buffer filled exactly once, no refill
//   1025 values: first refill triggered
//   2048 values: exactly two fills
//   10000 values: nine fills — amortises per-fill overhead
void PageSizeComparison() {
  auto strs_1024  = GenUniform(1024,  5);
  auto strs_1025  = GenUniform(1025,  5);
  auto strs_2048  = GenUniform(2048,  5);
  auto strs_10000 = GenUniform(10000, 5);

  PageData data_1024 (strs_1024);
  PageData data_1025 (strs_1025);
  PageData data_2048 (strs_2048);
  PageData data_10000(strs_10000);

  Benchmark suite("Page size: 1024 vs 1025 vs 2048 vs 10000 values | NextValues");
  suite.AddBenchmark("1024 values (no refill)",  Bench_NextValues, &data_1024);
  suite.AddBenchmark("1025 values (1 refill)",   Bench_NextValues, &data_1025);
  suite.AddBenchmark("2048 values (2 refills)",  Bench_NextValues, &data_2048);
  suite.AddBenchmark("10000 values (9 refills)", Bench_NextValues, &data_10000);
  cout << suite.Measure();

  VerifyPage(&data_1024,  strs_1024);
  VerifyPage(&data_1025,  strs_1025);
  VerifyPage(&data_2048,  strs_2048);
  VerifyPage(&data_10000, strs_10000);
}

// Scenario 4: read/skip ratio — models late materialisation patterns.
//   read=80 skip=20: mostly reading (selective filter, most rows pass)
//   read=50 skip=50: half/half
//   read=20 skip=80: mostly skipping (highly selective filter)
void ReadSkipRatio() {
  auto strs = GenMixed(DEFAULT_NUM_VALUES, 5, 20);
  PageData data_80_20(strs);
  PageData data_50_50(strs);
  PageData data_20_80(strs);

  Benchmark suite("Read/skip: 80/20 vs 50/50 vs 20/80 | mixed strings");
  suite.AddBenchmark("read=80 skip=20", Bench_ReadSkip<80, 20>, &data_80_20);
  suite.AddBenchmark("read=50 skip=50", Bench_ReadSkip<50, 50>, &data_50_50);
  suite.AddBenchmark("read=20 skip=80", Bench_ReadSkip<20, 80>, &data_20_80);
  cout << suite.Measure();
}

// Scenario 5: delta-length decoder vs PLAIN baseline on identical data.
//   Reads: delta pays an extra delta-decode for the lengths, so it should be
//   somewhat slower than PLAIN.
//   Skips: PLAIN only reads a 4-byte length prefix per value, while delta must
//   decode every delta, so the gap is expected to be largest here.
void DeltaVsPlain() {
  auto strs = GenMixed(DEFAULT_NUM_VALUES, 5, 20);
  PageData data(strs);

  Benchmark read("Delta vs Plain | NextValues (batch) | mixed strings");
  read.AddBenchmark("delta NextValues", Bench_NextValues,       &data);
  read.AddBenchmark("plain NextValues", Bench_Plain_NextValues, &data);
  cout << read.Measure();

  Benchmark one("Delta vs Plain | NextValue (1-by-1) | mixed strings");
  one.AddBenchmark("delta NextValue", Bench_NextValue,       &data);
  one.AddBenchmark("plain NextValue", Bench_Plain_NextValue, &data);
  cout << one.Measure();

  Benchmark skip("Delta vs Plain | SkipValues | mixed strings");
  skip.AddBenchmark("delta SkipValues", Bench_SkipValues,       &data);
  skip.AddBenchmark("plain SkipValues", Bench_Plain_SkipValues, &data);
  cout << skip.Measure();

  VerifyPage(&data, strs);
}

// ---- Main -----------------------------------------------------------------------

int main(int argc, char** argv) {
  CpuInfo::Init();
  cout << Benchmark::GetMachineInfo() << endl;
  cout << "  DEFAULT_NUM_VALUES  = " << DEFAULT_NUM_VALUES << endl;
  cout << "  SMALL_LIMIT         = " << SMALL_LIMIT << " bytes" << endl;
  cout << "  LENGTHS_BATCH_SIZE  = 1024 values per FillLengthsBuffer() call" << endl;
  cout << "\n\n";

  cout << "━━━━━━━━━━━ DELTA_LENGTH_BYTE_ARRAY decoder performance ━━━━━━━━━━━\n\n";

  ShortVsLongVsMixed();
  cout << "\n\n";

  AccessPatternComparison();
  cout << "\n\n";

  PageSizeComparison();
  cout << "\n\n";

  ReadSkipRatio();
  cout << "\n\n";

  DeltaVsPlain();
  cout << "\n\n";

  return 0;
}
