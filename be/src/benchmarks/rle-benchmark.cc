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

#include <iostream>
#include <sstream>

#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "experiments/bit-stream-utils.8byte.inline.h"
#include "util/benchmark.h"
#include "util/bit-stream-utils.inline.h"
#include "util/cpu-info.h"

// Benchmark to measure how quickly we can do bit encoding and decoding.

// encode:               Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//     "BitWriter (8 byte) 1-Bit"                66.9                  1X
//              "BitWriter 1-Bit"               107.3              1.604X
//     "BitWriter (8 byte) 2-Bit"               74.42                  1X
//              "BitWriter 2-Bit"               105.6              1.419X
//     "BitWriter (8 byte) 3-Bit"               76.91                  1X
//              "BitWriter 3-Bit"                 104              1.353X
//     "BitWriter (8 byte) 4-Bit"               80.37                  1X
//              "BitWriter 4-Bit"               102.7              1.278X
//     "BitWriter (8 byte) 5-Bit"               79.29                  1X
//              "BitWriter 5-Bit"                 101              1.274X
//     "BitWriter (8 byte) 6-Bit"               80.37                  1X
//              "BitWriter 6-Bit"               99.28              1.235X
//     "BitWriter (8 byte) 7-Bit"               80.19                  1X
//              "BitWriter 7-Bit"               98.09              1.223X
//     "BitWriter (8 byte) 8-Bit"               84.93                  1X
//              "BitWriter 8-Bit"                  97              1.142X
//     "BitWriter (8 byte) 9-Bit"               79.85                  1X
//              "BitWriter 9-Bit"               95.09              1.191X
//    "BitWriter (8 byte) 10-Bit"               80.51                  1X
//             "BitWriter 10-Bit"               94.17               1.17X
//    "BitWriter (8 byte) 11-Bit"               79.36                  1X
//             "BitWriter 11-Bit"                93.2              1.174X
//    "BitWriter (8 byte) 12-Bit"               80.79                  1X
//             "BitWriter 12-Bit"               92.09               1.14X
//    "BitWriter (8 byte) 13-Bit"               78.28                  1X
//             "BitWriter 13-Bit"               90.83               1.16X
//    "BitWriter (8 byte) 14-Bit"               78.57                  1X
//             "BitWriter 14-Bit"               89.71              1.142X
//    "BitWriter (8 byte) 15-Bit"               77.28                  1X
//             "BitWriter 15-Bit"                  88              1.139X
//    "BitWriter (8 byte) 16-Bit"               86.98                  1X
//             "BitWriter 16-Bit"               88.08              1.013X

// decode:               Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//     "BitWriter (8 byte) 1-Bit"               132.9                  1X
//              "BitWriter 1-Bit"               126.9             0.9546X
//     "BitWriter (8 byte) 2-Bit"               132.9                  1X
//              "BitWriter 2-Bit"               125.6             0.9448X
//     "BitWriter (8 byte) 3-Bit"               132.8                  1X
//              "BitWriter 3-Bit"               122.7             0.9237X
//     "BitWriter (8 byte) 4-Bit"               133.1                  1X
//              "BitWriter 4-Bit"               123.6             0.9284X
//     "BitWriter (8 byte) 5-Bit"               132.2                  1X
//              "BitWriter 5-Bit"               118.2             0.8942X
//     "BitWriter (8 byte) 6-Bit"               132.9                  1X
//              "BitWriter 6-Bit"               117.6              0.885X
//     "BitWriter (8 byte) 7-Bit"               132.3                  1X
//              "BitWriter 7-Bit"               112.8             0.8525X
//     "BitWriter (8 byte) 8-Bit"               132.9                  1X
//              "BitWriter 8-Bit"               119.2             0.8971X
//     "BitWriter (8 byte) 9-Bit"               131.8                  1X
//              "BitWriter 9-Bit"               111.3             0.8447X
//    "BitWriter (8 byte) 10-Bit"               131.4                  1X
//             "BitWriter 10-Bit"               108.5             0.8255X
//    "BitWriter (8 byte) 11-Bit"               131.7                  1X
//             "BitWriter 11-Bit"               106.9             0.8118X
//    "BitWriter (8 byte) 12-Bit"               132.9                  1X
//             "BitWriter 12-Bit"               108.8             0.8189X
//    "BitWriter (8 byte) 13-Bit"                 131                  1X
//             "BitWriter 13-Bit"               103.1             0.7873X
//    "BitWriter (8 byte) 14-Bit"               131.6                  1X
//             "BitWriter 14-Bit"               101.6             0.7724X
//    "BitWriter (8 byte) 15-Bit"               131.1                  1X
//             "BitWriter 15-Bit"               99.91             0.7622X
//    "BitWriter (8 byte) 16-Bit"                 133                  1X
//             "BitWriter 16-Bit"               105.2             0.7907X

using namespace std;
using namespace impala;

const int BUFFER_LEN = 64 * 4096;

struct TestData {
  uint8_t* array;
  uint8_t* buffer;
  int num_values;
  int num_bits;
  int max_value;
  MemPool* pool;
  bool result;
};

void TestBitWriterEncode(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  int buffer_size = BitUtil::Ceil(data->num_bits * data->num_values, 8);
  for (int i = 0; i < batch_size; ++i) {
    BitWriter writer(data->buffer, buffer_size);
    // Unroll this to focus more on Put performance.
    for (int j = 0; j < data->num_values; j += 8) {
      writer.PutValue(j + 0, data->num_bits);
      writer.PutValue(j + 1, data->num_bits);
      writer.PutValue(j + 2, data->num_bits);
      writer.PutValue(j + 3, data->num_bits);
      writer.PutValue(j + 4, data->num_bits);
      writer.PutValue(j + 5, data->num_bits);
      writer.PutValue(j + 6, data->num_bits);
      writer.PutValue(j + 7, data->num_bits);
    }
    writer.Flush();
  }
}

void TestBitWriter8ByteEncode(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  int buffer_size = BitUtil::Ceil(data->num_bits * data->num_values, 8);
  for (int i = 0; i < batch_size; ++i) {
    BitWriter_8byte writer(data->buffer, buffer_size);
    // Unroll this to focus more on Put performance.
    for (int j = 0; j < data->num_values; j += 8) {
      writer.PutValue(j + 0, data->num_bits);
      writer.PutValue(j + 1, data->num_bits);
      writer.PutValue(j + 2, data->num_bits);
      writer.PutValue(j + 3, data->num_bits);
      writer.PutValue(j + 4, data->num_bits);
      writer.PutValue(j + 5, data->num_bits);
      writer.PutValue(j + 6, data->num_bits);
      writer.PutValue(j + 7, data->num_bits);
    }
  }
}

void TestBitWriterDecode(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  int64_t v;
  for (int i = 0; i < batch_size; ++i) {
    BitReader reader(data->buffer, BUFFER_LEN);
    // Unroll this to focus more on Put performance.
    for (int j = 0; j < data->num_values; j += 8) {
      reader.GetValue(data->num_bits, &v);
      reader.GetValue(data->num_bits, &v);
      reader.GetValue(data->num_bits, &v);
      reader.GetValue(data->num_bits, &v);
      reader.GetValue(data->num_bits, &v);
      reader.GetValue(data->num_bits, &v);
      reader.GetValue(data->num_bits, &v);
      reader.GetValue(data->num_bits, &v);
    }
  }
}

void TestBitWriter8ByteDecode(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  data->result = true;
  int64_t v;
  for (int i = 0; i < batch_size; ++i) {
    BitReader_8byte reader(data->buffer, BUFFER_LEN);
    // Unroll this to focus more on Put performance.
    for (int j = 0; j < data->num_values; j += 8) {
      data->result &= reader.GetValue(data->num_bits, &v);
      data->result &= reader.GetValue(data->num_bits, &v);
      data->result &= reader.GetValue(data->num_bits, &v);
      data->result &= reader.GetValue(data->num_bits, &v);
      data->result &= reader.GetValue(data->num_bits, &v);
      data->result &= reader.GetValue(data->num_bits, &v);
      data->result &= reader.GetValue(data->num_bits, &v);
      data->result &= reader.GetValue(data->num_bits, &v);
    }
  }
  CHECK(data->result);
}

int main(int argc, char** argv) {
  CpuInfo::Init();

  MemTracker tracker;
  MemPool pool(&tracker);

  int num_values = 4096;
  int max_bits = 16;

  Benchmark encode_suite("encode");
  TestData data[max_bits];
  for (int i = 0; i < max_bits; ++i) {
    data[i].buffer = new uint8_t[BUFFER_LEN];
    data[i].num_values = num_values;
    data[i].num_bits = i + 1;
    data[i].max_value = 1 << i;
    data[i].pool = &pool;

    stringstream suffix;
    suffix << " " << (i+1) << "-Bit";

    stringstream name;
    name << "\"BitWriter (8 byte)" << suffix.str() << "\"";
    int baseline =
        encode_suite.AddBenchmark(name.str(), TestBitWriter8ByteEncode, &data[i], -1);

    name.str("");
    name << "\"BitWriter" << suffix.str() << "\"";
    encode_suite.AddBenchmark(name.str(), TestBitWriterEncode, &data[i], baseline);
  }
  cout << encode_suite.Measure() << endl;

  Benchmark decode_suite("decode");
  for (int i = 0; i < max_bits; ++i) {
    stringstream suffix;
    suffix << " " << (i+1) << "-Bit";

    stringstream name;
    name << "\"BitWriter (8 byte)" << suffix.str() << "\"";
    int baseline =
        decode_suite.AddBenchmark(name.str(), TestBitWriter8ByteDecode, &data[i], -1);

    name.str("");
    name << "\"BitWriter" << suffix.str() << "\"";
    decode_suite.AddBenchmark(name.str(), TestBitWriterDecode, &data[i], baseline);
  }
  cout << decode_suite.Measure() << endl;

  return 0;
}
