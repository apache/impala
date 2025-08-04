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

#include <cstdint>
#include <iostream>

#include "exec/parquet/parquet-byte-stream-split-decoder.h"
#include "exec/parquet/parquet-byte-stream-split-encoder.h"
#include "util/benchmark.h"
#include "util/cpu-info.h"

using namespace impala;

constexpr int DATA_BATCH_SIZE = 1000;

// -------------------------------- Benchmark Results --------------------------------- //

//                    Machine Info: 13th Gen Intel(R) Core(TM) i9-13900
//                                 Data Batch Size = 1000
//                          Data Pool Size for Pooled Data = 124
//                           Skip Sizes (Read | Skip): 82 | 18
//                      Stride Sizes (S | M | L): 15 | 2985 | 213525

// ━━━━━━━━━━━━━━━━━━━━━ Byte Stream Split functionality comparison ━━━━━━━━━━━━━━━━━━━━━━

// ────────────────────── Compile VS Runtime | Sequential | Batched ──────────────────────
//            Function iters/ms  10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                        (relative) (relative) (relative)
// ┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄
//         Compile Int         2.46e+03 2.49e+03 2.52e+03         1X         1X         1X
//         Runtime Int              467      470      475      0.19X     0.189X     0.188X
//        Compile Long         1.17e+03 1.19e+03 1.21e+03     0.476X     0.479X      0.48X
//        Runtime Long              200      202      203    0.0811X    0.0811X    0.0806X


// ───────────────────── Type Comparison | Runtime | Random | Batched ────────────────────
//            Function iters/ms  10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                        (relative) (relative) (relative)
// ┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄
//                 Int              452      470      474         1X         1X         1X
//               Float              453      469      474         1X     0.998X         1X
//             6 bytes              269      283      284     0.596X     0.602X       0.6X
//                Long              194      202      203     0.429X     0.429X     0.429X
//              Double              194      202      203     0.429X     0.429X     0.429X
//            11 bytes              137      141      142     0.304X       0.3X       0.3X


// ────────────── Repeating VS Sequential VS Random | Compile Time | Batched ─────────────
//            Function iters/ms  10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                        (relative) (relative) (relative)
// ┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄
//       Repeating Int         2.36e+03 2.47e+03 2.51e+03         1X         1X         1X
//      Sequential Int         2.41e+03 2.48e+03 2.52e+03      1.02X         1X         1X
//          Random Int          2.4e+03 2.49e+03 2.52e+03      1.02X      1.01X         1X
//      Repeating Long         1.16e+03 1.18e+03 1.22e+03     0.491X     0.479X     0.484X
//     Sequential Long         1.15e+03 1.19e+03 1.21e+03     0.486X     0.479X      0.48X
//         Random Long         1.14e+03 1.18e+03 1.21e+03     0.484X     0.477X     0.481X


// ──────────────── Singles VS Batch VS Stride | Compile Time | Sequential ───────────────
//            Function iters/ms  10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                        (relative) (relative) (relative)
// ┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄
//         Singles Int         1.24e+03 1.27e+03 1.28e+03         1X         1X         1X
//           Batch Int         2.42e+03 2.48e+03 2.51e+03      1.95X      1.95X      1.96X
//          Stride Int         2.41e+03 2.49e+03 2.51e+03      1.94X      1.95X      1.96X
//        Singles Long              812      827      837     0.653X      0.65X     0.652X
//          Batch Long         1.16e+03 1.19e+03 1.21e+03     0.934X     0.931X     0.941X
//         Stride Long         1.18e+03 1.21e+03 1.23e+03     0.949X     0.954X     0.962X


// ──────── Small VS Medium VS Large Stride | Compile Time | Sequential | Batched ────────
//            Function iters/ms  10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                        (relative) (relative) (relative)
// ┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄
//        S Stride Int         2.41e+03 2.49e+03 2.52e+03         1X         1X         1X
//        M Stride Int         1.92e+03    2e+03 2.03e+03     0.795X     0.804X     0.806X
//        L Stride Int         1.87e+03 1.92e+03 1.95e+03     0.774X     0.772X     0.774X
//       S Stride Long         1.16e+03 1.22e+03 1.23e+03     0.481X     0.488X      0.49X
//       M Stride Long         1.01e+03 1.08e+03 1.09e+03     0.419X     0.434X     0.433X
//       L Stride Long              987 1.03e+03 1.04e+03     0.409X     0.413X     0.414X

// --------------------------------- Data Structures ---------------------------------- //

template <int B_SIZE>
struct BSSTestData {
  const std::vector<uint8_t>& input_bdata;
  const int stride;

  std::vector<uint8_t> encoded_bdata;
  std::vector<uint8_t> output;

  BSSTestData(const std::vector<uint8_t>& b, int s = B_SIZE) : input_bdata(b), stride(s) {
    output.resize(stride * (input_bdata.size() / B_SIZE));
    GenerateBSSEncoded();
  }

 private:
  void GenerateBSSEncoded() {
    ParquetByteStreamSplitEncoder<0> encoder(B_SIZE);
    std::vector<uint8_t> temp(input_bdata.size());
    encoded_bdata.resize(input_bdata.size());
    encoder.NewPage(temp.data(), temp.size());
    for (int i = 0; i < input_bdata.size() / B_SIZE; i++) {
      if (!encoder.PutBytes(input_bdata.data() + i * B_SIZE)) {
        std::cerr << "Error: Value could not be put at ind " << i << std::endl;
        return;
      }
    }
    if (encoder.FinalizePage(encoded_bdata.data(), encoded_bdata.size())
        != input_bdata.size() / B_SIZE) {
      std::cerr << "Error: Could not write all values upon FinalizePage" << std::endl;
      return;
    }
  }
};

// --------------------------------- Helper Functions --------------------------------- //

// ............ Data Generator Functions ............ //

// Fill the vector with the same repeating data. (42,42,42,42,...)
void DataSameRepGen(std::vector<uint8_t>* bdata, int b_size) {
  for (int i = 0; i < DATA_BATCH_SIZE * b_size; i++) {
    bdata->push_back(0x75);
  }
}

// Fill the vector with sequential data (41,42,43,44,...).
void DataSequentialGen(std::vector<uint8_t>* bdata, int b_size) {
  bdata->resize(DATA_BATCH_SIZE * b_size);
  long offset = rand() * rand() - DATA_BATCH_SIZE;
  for (int i = 0; i < DATA_BATCH_SIZE; i++) {
    long j = i + offset;
    memcpy(bdata->data() + i * b_size, &j, std::min((int)sizeof(j), b_size));
  }
}

// Fill the vector with completely random data.
void DataRandGen(std::vector<uint8_t>* bdata, int b_size) {
  srand(154698135);
  for (int i = 0; i < DATA_BATCH_SIZE * b_size; i++) {
    bdata->push_back(rand() % numeric_limits<uint8_t>::max());
  }
}

// .......... Benchmark Data Transformer Functions .......... //

template <int BSIZE>
std::vector<uint8_t> GenerateStrided(const std::vector<uint8_t>& input, int stride) {
  std::vector<uint8_t> strided_bd(input.size() / BSIZE * stride);
  for (int i = 0; i < input.size() / BSIZE; i++) {
    memcpy(strided_bd.data() + i * stride, input.data() + i * BSIZE, BSIZE);
  }
  return strided_bd;
}

// ........... Output Checking Functions ............ //

// We could use operator== instead of this, but using this function gives better
// readability, and makes debugging easier.
void testOutputCorrectness(
    const std::vector<uint8_t>& output, const std::vector<uint8_t>& expected) {
  if (output.size() != expected.size()) {
    std::cerr << "Vector sizes do not match" << std::endl;
    std::cerr << "Output size (bytes): " << output.size() <<
        ", Expected size (bytes): " << expected.size() << std::endl;
    return;
  }
  for (int i = 0; i < expected.size(); i++) {
    if (output[i] != expected[i]) {
      std::cerr << "Vectors do not match at index " << i << std::endl;
      return;
    }
  }
}

// ------------------------------ Benchmarked Functions ------------------------------- //

// ................... BSS Tests .................... //

template <int B_SIZE, class ParquetByteStreamSplitDecoder>
void BSS_DecodeBatch(int batch_size, void* d, ParquetByteStreamSplitDecoder& decoder) {
  BSSTestData<B_SIZE>* data = reinterpret_cast<BSSTestData<B_SIZE>*>(d);

  for (int batch = 0; batch < batch_size; batch++) {
    uint8_t* output_ptr = data->output.data();
    decoder.NewPage(data->encoded_bdata.data(), data->encoded_bdata.size());

    if (decoder.NextValues(data->encoded_bdata.size() / B_SIZE, output_ptr, B_SIZE)
        != data->encoded_bdata.size() / B_SIZE) {
      std::cerr << "Error: Could not decode all values" << std::endl;
      return;
    }
  }
}

template <int B_SIZE>
void BSSRun_DecodeBatch(int batch_size, void* d) {
  ParquetByteStreamSplitDecoder<0> decoder(B_SIZE);
  BSS_DecodeBatch<B_SIZE>(batch_size, d, decoder);
}

template <int B_SIZE>
void BSSComp_DecodeBatch(int batch_size, void* d) {
  ParquetByteStreamSplitDecoder<B_SIZE> decoder;
  BSS_DecodeBatch<B_SIZE>(batch_size, d, decoder);
}

template <typename T>
void BSSComp_DecodeSingles(int batch_size, void* d) {
  BSSTestData<sizeof(T)>* data = reinterpret_cast<BSSTestData<sizeof(T)>*>(d);
  ParquetByteStreamSplitDecoder<sizeof(T)> decoder;

  for (int batch = 0; batch < batch_size; batch++) {
    uint8_t* output_ptr = data->output.data();
    decoder.NewPage(data->encoded_bdata.data(), data->encoded_bdata.size());
    for (int j = 0; j < data->encoded_bdata.size() / sizeof(T); j++) {
      if (decoder.NextValue(reinterpret_cast<T*>(output_ptr)) != 1) {
        std::cerr << "Error: Could not decode all values" << std::endl;
        return;
      }
      output_ptr += sizeof(T);
    }
  }
}

template <int B_SIZE>
void BSSComp_DecodeStride(int batch_size, void* d) {
  BSSTestData<B_SIZE>* data = reinterpret_cast<BSSTestData<B_SIZE>*>(d);
  ParquetByteStreamSplitDecoder<B_SIZE> decoder;

  for (int batch = 0; batch < batch_size; batch++) {
    uint8_t* output_ptr = data->output.data();
    decoder.NewPage(data->encoded_bdata.data(), data->encoded_bdata.size());
    if (decoder.NextValues(data->encoded_bdata.size() / B_SIZE, output_ptr, data->stride)
        != data->encoded_bdata.size() / B_SIZE) {
      std::cerr << "Error: Could not decode all values" << std::endl;
      return;
    }
  }
}

template <int B_SIZE, int READ, int SKIP>
void BSSComp_DecodeSkip(int batch_size, void* d) {
  BSSTestData<B_SIZE>* data = reinterpret_cast<BSSTestData<B_SIZE>*>(d);
  ParquetByteStreamSplitDecoder<B_SIZE> decoder;

  for (int batch = 0; batch < batch_size; batch++) {
    uint8_t* output_ptr = data->output.data();
    decoder.NewPage(data->encoded_bdata.data(), data->encoded_bdata.size());
    for (int i = 0; i < decoder.GetTotalValueCount(); i += READ + SKIP) {
      if (decoder.NextValues(READ, output_ptr, B_SIZE) < 0) {
        std::cerr << "Error reading values at index " << i << std::endl;
        return;
      }
      if (decoder.SkipValues(SKIP) < 0) {
        std::cerr << "Error skipping values at index " << i << std::endl;
        return;
      }
      output_ptr += READ * B_SIZE;
    }
  }
}

// ------------------------------- Benchmark Functions -------------------------------- //

// ................. BSS Benchmarks ................. //

void CompileVSRuntime() {
  std::vector<uint8_t> byte_data4b;
  std::vector<uint8_t> byte_data8b;
  DataSequentialGen(&byte_data4b, 4);
  DataSequentialGen(&byte_data8b, 8);

  BSSTestData<4> dataIntTempl(byte_data4b);
  BSSTestData<8> dataLongTempl(byte_data8b);
  BSSTestData<4> dataIntConstr(byte_data4b);
  BSSTestData<8> dataLongConstr(byte_data8b);

  // Compile - template, Runtime - constructor
  Benchmark suite("Compile VS Runtime | Sequential | Batched");
  suite.AddBenchmark("Compile Int", BSSComp_DecodeBatch<sizeof(int)>, &dataIntConstr);
  suite.AddBenchmark("Runtime Int", BSSRun_DecodeBatch<sizeof(int)>, &dataIntTempl);
  suite.AddBenchmark("Compile Long", BSSComp_DecodeBatch<sizeof(long)>, &dataLongConstr);
  suite.AddBenchmark("Runtime Long", BSSRun_DecodeBatch<sizeof(long)>, &dataLongTempl);
  std::cout << suite.Measure();

  // Test the output data to make sure that the functions are not optimised out

  testOutputCorrectness(dataIntTempl.output, dataIntTempl.input_bdata);
  testOutputCorrectness(dataLongTempl.output, dataLongTempl.input_bdata);
  testOutputCorrectness(dataIntConstr.output, dataIntConstr.input_bdata);
  testOutputCorrectness(dataLongConstr.output, dataLongConstr.input_bdata);
}

void TypeComparison() {
  std::vector<uint8_t> byte_data4b;
  std::vector<uint8_t> byte_data8b;
  std::vector<uint8_t> byte_data6b;
  std::vector<uint8_t> byte_data11b;

  DataRandGen(&byte_data4b, 4);
  DataRandGen(&byte_data6b, 6);
  DataRandGen(&byte_data8b, 8);
  DataRandGen(&byte_data11b, 11);

  BSSTestData<4> dataInt(byte_data4b);
  BSSTestData<8> dataLong(byte_data8b);
  BSSTestData<6> data6b(byte_data6b);
  BSSTestData<4> dataFloat(byte_data4b);
  BSSTestData<8> dataDouble(byte_data8b);
  BSSTestData<11> data11b(byte_data11b);

  // Since we are comparing types that are not a size of 4 or 8, we must use the runtime
  // version.
  Benchmark suite("Type Comparison | Runtime | Random | Batched");
  suite.AddBenchmark("Int", BSSRun_DecodeBatch<sizeof(int)>, &dataInt);
  suite.AddBenchmark("Float", BSSRun_DecodeBatch<sizeof(float)>, &dataFloat);
  suite.AddBenchmark("6 bytes", BSSRun_DecodeBatch<6>, &data6b);
  suite.AddBenchmark("Long", BSSRun_DecodeBatch<sizeof(long)>, &dataLong);
  suite.AddBenchmark("Double", BSSRun_DecodeBatch<sizeof(double)>, &dataDouble);
  suite.AddBenchmark("11 bytes", BSSRun_DecodeBatch<11>, &data11b);
  std::cout << suite.Measure();

  // Test the output data to make sure that the functions are not optimised out

  testOutputCorrectness(dataInt.output, dataInt.input_bdata);
  testOutputCorrectness(dataLong.output, dataLong.input_bdata);
  testOutputCorrectness(data6b.output, data6b.input_bdata);
  testOutputCorrectness(dataFloat.output, dataFloat.input_bdata);
  testOutputCorrectness(dataDouble.output, dataDouble.input_bdata);
  testOutputCorrectness(data11b.output, data11b.input_bdata);
}

void RepeatingVSSequentialVSRandom() {
  std::vector<uint8_t> repeating_data4b;
  std::vector<uint8_t> repeating_data8b;
  std::vector<uint8_t> sequential_data4b;
  std::vector<uint8_t> sequential_data8b;
  std::vector<uint8_t> random_data4b;
  std::vector<uint8_t> random_data8b;

  DataSameRepGen(&repeating_data4b, 4);
  DataSameRepGen(&repeating_data8b, 8);
  DataSequentialGen(&sequential_data4b, 4);
  DataSequentialGen(&sequential_data8b, 8);
  DataRandGen(&random_data4b, 4);
  DataRandGen(&random_data8b, 8);

  BSSTestData<4> dataIntRep(repeating_data4b);
  BSSTestData<8> dataLongRep(repeating_data8b);

  BSSTestData<4> dataIntSeq(sequential_data4b);
  BSSTestData<8> dataLongSeq(sequential_data8b);

  BSSTestData<4> dataIntRand(random_data4b);
  BSSTestData<8> dataLongRand(random_data8b);

  Benchmark suite("Repeating VS Sequential VS Random | Compile Time | Batched");
  suite.AddBenchmark("Repeating Int", BSSComp_DecodeBatch<sizeof(int)>, &dataIntRep);
  suite.AddBenchmark("Sequential Int", BSSComp_DecodeBatch<sizeof(int)>, &dataIntSeq);
  suite.AddBenchmark("Random Int", BSSComp_DecodeBatch<sizeof(int)>, &dataIntRand);
  suite.AddBenchmark("Repeating Long", BSSComp_DecodeBatch<sizeof(long)>, &dataLongRep);
  suite.AddBenchmark("Sequential Long", BSSComp_DecodeBatch<sizeof(long)>, &dataLongSeq);
  suite.AddBenchmark("Random Long", BSSComp_DecodeBatch<sizeof(long)>, &dataLongRand);
  std::cout << suite.Measure();

  // Test the output data to make sure that the functions are not optimised out

  testOutputCorrectness(dataIntRep.output, dataIntRep.input_bdata);
  testOutputCorrectness(dataLongRep.output, dataLongRep.input_bdata);
  testOutputCorrectness(dataIntSeq.output, dataIntSeq.input_bdata);
  testOutputCorrectness(dataLongSeq.output, dataLongSeq.input_bdata);
  testOutputCorrectness(dataIntRand.output, dataIntRand.input_bdata);
  testOutputCorrectness(dataLongRand.output, dataLongRand.input_bdata);
}

void SinglesVSBatchVSStride() {
  std::vector<uint8_t> byte_data4b;
  std::vector<uint8_t> byte_data8b;
  DataSequentialGen(&byte_data4b, 4);
  DataSequentialGen(&byte_data8b, 8);

  BSSTestData<4> dataIntSingles(byte_data4b);
  BSSTestData<8> dataLongSingles(byte_data8b);

  BSSTestData<4> dataIntBatch(byte_data4b);
  BSSTestData<8> dataLongBatch(byte_data8b);

  constexpr int stride = sizeof(int) + sizeof(long) + 7;

  BSSTestData<4> dataIntStride(byte_data4b, stride);
  BSSTestData<8> dataLongStride(byte_data8b, stride);

  Benchmark suite("Singles VS Batch VS Stride | Compile Time | Sequential");
  suite.AddBenchmark("Singles Int", BSSComp_DecodeSingles<int>, &dataIntSingles);
  suite.AddBenchmark("Batch Int", BSSComp_DecodeBatch<sizeof(int)>, &dataIntBatch);
  suite.AddBenchmark("Stride Int", BSSComp_DecodeStride<sizeof(int)>, &dataIntStride);
  suite.AddBenchmark("Singles Long", BSSComp_DecodeSingles<long>, &dataLongSingles);
  suite.AddBenchmark("Batch Long", BSSComp_DecodeBatch<sizeof(long)>, &dataLongBatch);
  suite.AddBenchmark("Stride Long", BSSComp_DecodeStride<sizeof(long)>, &dataLongStride);
  std::cout << suite.Measure();

  // Test the output data to make sure that the functions are not optimised out

  testOutputCorrectness(dataIntSingles.output, dataIntSingles.input_bdata);
  testOutputCorrectness(dataLongSingles.output, dataLongSingles.input_bdata);

  testOutputCorrectness(dataIntBatch.output, dataIntBatch.input_bdata);
  testOutputCorrectness(dataLongBatch.output, dataLongBatch.input_bdata);

  testOutputCorrectness(dataIntStride.output,
      GenerateStrided<sizeof(int)>(dataIntStride.input_bdata, dataIntStride.stride));
  testOutputCorrectness(dataLongStride.output,
      GenerateStrided<sizeof(long)>(dataLongStride.input_bdata, dataLongStride.stride));
}

void StrideSizeComparison(int strideS, int strideM, int strideL) {
  std::vector<uint8_t> byte_data4b;
  std::vector<uint8_t> byte_data8b;

  DataSequentialGen(&byte_data4b, 4);
  DataSequentialGen(&byte_data8b, 8);

  BSSTestData<4> dataIntSStride(byte_data4b, strideS);
  BSSTestData<4> dataIntMStride(byte_data4b, strideM);
  BSSTestData<4> dataIntLStride(byte_data4b, strideL);
  BSSTestData<8> dataLongSStride(byte_data8b, strideS);
  BSSTestData<8> dataLongMStride(byte_data8b, strideM);
  BSSTestData<8> dataLongLStride(byte_data8b, strideL);

  Benchmark suite("Small VS Medium VS Large Stride | Compile Time | Sequential | Batched");
  suite.AddBenchmark("S Stride Int", BSSComp_DecodeStride<sizeof(int)>, &dataIntSStride);
  suite.AddBenchmark("M Stride Int", BSSComp_DecodeStride<sizeof(int)>, &dataIntMStride);
  suite.AddBenchmark("L Stride Int", BSSComp_DecodeStride<sizeof(int)>, &dataIntLStride);
  suite.AddBenchmark("S Stride Long", BSSComp_DecodeStride<sizeof(long)>,
      &dataLongSStride);
  suite.AddBenchmark("M Stride Long", BSSComp_DecodeStride<sizeof(long)>,
      &dataLongMStride);
  suite.AddBenchmark("L Stride Long", BSSComp_DecodeStride<sizeof(long)>,
      &dataLongLStride);
  std::cout << suite.Measure();

  // Test the output data to make sure that the functions are not optimised out

  testOutputCorrectness(dataIntSStride.output,
      GenerateStrided<sizeof(int)>(dataIntSStride.input_bdata, dataIntSStride.stride));
  testOutputCorrectness(dataIntMStride.output,
      GenerateStrided<sizeof(int)>(dataIntMStride.input_bdata, dataIntMStride.stride));
  testOutputCorrectness(dataIntLStride.output,
      GenerateStrided<sizeof(int)>(dataIntLStride.input_bdata, dataIntLStride.stride));

  testOutputCorrectness(dataLongSStride.output,
      GenerateStrided<sizeof(long)>(dataLongSStride.input_bdata, dataLongSStride.stride));
  testOutputCorrectness(dataLongMStride.output,
      GenerateStrided<sizeof(long)>(dataLongMStride.input_bdata, dataLongMStride.stride));
  testOutputCorrectness(dataLongLStride.output,
      GenerateStrided<sizeof(long)>(dataLongLStride.input_bdata, dataLongLStride.stride));
}

// ---------------------------------- Main Function ----------------------------------- //

int main(int argc, char** argv) {
  constexpr int pool = 124;
  constexpr int strideS = sizeof(int) + sizeof(long) + 3;
  constexpr int strideM = 199 * strideS;
  constexpr int strideL = 14235 * strideS;
  constexpr int read = 82;
  constexpr int skip = 18;

  CpuInfo::Init();
  std::cout << "           " << Benchmark::GetMachineInfo() << std::endl;
  std::cout << "                        Data Batch Size = " << DATA_BATCH_SIZE
      << std::endl;
  std::cout << "                 Data Pool Size for Pooled Data = " << pool << std::endl;
  std::cout << "                  Skip Sizes (Read | Skip): " <<
      read << " | " << skip << std::endl;
  std::cout << "             Stride Sizes (S | M | L): " <<
  strideS << " | " << strideM << " | " << strideL << std::endl;
  std::cout << "\n\n";

  std::cout << "\n\n";
  std::cout << "━━━━━━━━━━━━━━━━━━━━━ Byte Stream Split functionality comparison "
  << "━━━━━━━━━━━━━━━━━━━━━━\n";
  std::cout << "\n";

  CompileVSRuntime();
  std::cout << "\n\n";
  TypeComparison();
  std::cout << "\n\n";
  RepeatingVSSequentialVSRandom();
  std::cout << "\n\n";
  SinglesVSBatchVSStride();
  std::cout << "\n\n";
  StrideSizeComparison(strideS, strideM, strideL);
  std::cout << "\n\n";

  return 0;
}
