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

#include <algorithm>
#include <iostream>
#include <fstream>
#include <limits>
#include <map>
#include <random>
#include <sstream>
#include <type_traits>
#include <vector>

#include "exec/parquet/parquet-common.h"
#include "exec/parquet/parquet-delta-decoder.h"
#include "exec/parquet/parquet-delta-encoder.h"
#include "gutil/strings/substitute.h"
#include "runtime/mem-tracker.h"
#include "util/arithmetic-util.h"
#include "util/bit-packing.inline.h"
#include "util/dict-encoding.h"
#include "util/benchmark.h"
#include "util/cpu-info.h"

/// The logic in this benchmark file can be conceptually divided into three stages:
///   1. Generating the configurations (benchmark parameters) as 'Config' objects.
///   2. Generating 'BenchmarkUnit' objects from the configurations. This includes
///      generating the input of the benchmarks and allocating output buffers for them.
///   3. Organizing the 'BenchmarkUnit' objects into groups, assigning them to 'Benchmark'
///      objects and runnning the benchmarks.

// Machine Info: Intel(R) Core(TM) i7-7700 CPU @ 3.60GHz
// ParquetType: INT32; OutType: int8_t
//   Mean delta: 1.
//   Plain: 40964.
//   Delta: 3806.
//   Dict: 9259.
//
//   Mean delta: 50.
//   Plain: 40964.
//   Delta: 10726.
//   Dict: 11274.
//
// ParquetType: INT32; OutType: int16_t
//   Mean delta: 1.
//   Plain: 40964.
//   Delta: 3043.
//   Dict: 37100.
//
//   Mean delta: 50.
//   Plain: 40964.
//   Delta: 11143.
//   Dict: 18577.
//
// ParquetType: INT32; OutType: int32_t
//   Mean delta: 1.
//   Plain: 40964.
//   Delta: 3026.
//   Dict: 37139.
//
//   Mean delta: 50.
//   Plain: 40964.
//   Delta: 10663.
//   Dict: 58544.
//
// ParquetType: INT64; OutType: int64_t
//   Mean delta: 1.
//   Plain: 81928.
//   Delta: 3007.
//   Dict: 57345.
//
//   Mean delta: 50.
//   Plain: 81928.
//   Delta: 10653.
//   Dict: 99020.
//
// Running benchmark suites.
// Benchmark:                 Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
// !ParquetType=INT32;OutType=int8_t;Encoding=plain;Access=batch     ;MeanDelta=1;Stride=4                287      293      295         1X         1X         1X
// !ParquetType=INT32;OutType=int8_t;Encoding=delta;Access=batch     ;MeanDelta=1;Stride=4               77.3     80.9     82.4      0.27X     0.277X     0.279X
// !ParquetType=INT32;OutType=int8_t;Encoding=dict ;Access=batch     ;MeanDelta=1;Stride=4                112      115      117     0.391X     0.393X     0.395X
// Checking the correctness of the results... Ok.
//
// Benchmark:                 Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
// !ParquetType=INT32;OutType=int8_t;Encoding=plain;Access=batch     ;MeanDelta=50;Stride=4                290      300      303         1X         1X         1X
// !ParquetType=INT32;OutType=int8_t;Encoding=delta;Access=batch     ;MeanDelta=50;Stride=4               83.8     87.3     88.8     0.289X     0.292X     0.293X
// !ParquetType=INT32;OutType=int8_t;Encoding=dict ;Access=batch     ;MeanDelta=50;Stride=4                119      122      124     0.411X     0.408X     0.409X
// Checking the correctness of the results... Ok.
//
// Benchmark:                 Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
// !ParquetType=INT32;OutType=int16_t;Encoding=plain;Access=batch     ;MeanDelta=1;Stride=4                292      299      302         1X         1X         1X
// !ParquetType=INT32;OutType=int16_t;Encoding=delta;Access=batch     ;MeanDelta=1;Stride=4               80.8     84.5     86.5     0.276X     0.283X     0.287X
// !ParquetType=INT32;OutType=int16_t;Encoding=dict ;Access=batch     ;MeanDelta=1;Stride=4               47.6     48.9     49.8     0.163X     0.164X     0.165X
// Checking the correctness of the results... Ok.
//
// Benchmark:                 Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
// !ParquetType=INT32;OutType=int16_t;Encoding=plain;Access=batch     ;MeanDelta=50;Stride=4                293      300      303         1X         1X         1X
// !ParquetType=INT32;OutType=int16_t;Encoding=delta;Access=batch     ;MeanDelta=50;Stride=4               78.6     81.5     83.2     0.268X     0.272X     0.275X
// !ParquetType=INT32;OutType=int16_t;Encoding=dict ;Access=batch     ;MeanDelta=50;Stride=4               86.8     89.2     90.6     0.296X     0.298X     0.299X
// Checking the correctness of the results... Ok.
//
// Benchmark:                 Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
// !ParquetType=INT32;OutType=int32_t;Encoding=plain;Access=batch     ;MeanDelta=1;Stride=4                285      290      293         1X         1X         1X
// !ParquetType=INT32;OutType=int32_t;Encoding=delta;Access=batch     ;MeanDelta=1;Stride=4               81.8     85.3     86.7     0.287X     0.294X     0.296X
// !ParquetType=INT32;OutType=int32_t;Encoding=dict ;Access=batch     ;MeanDelta=1;Stride=4               48.2     49.8     50.3      0.17X     0.172X     0.172X
// Checking the correctness of the results... Ok.
//
// Benchmark:                 Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
// !ParquetType=INT32;OutType=int32_t;Encoding=plain;Access=batch     ;MeanDelta=50;Stride=4                295      299      304         1X         1X         1X
// !ParquetType=INT32;OutType=int32_t;Encoding=delta;Access=batch     ;MeanDelta=50;Stride=4               83.8     87.1     88.4     0.284X     0.291X     0.291X
// !ParquetType=INT32;OutType=int32_t;Encoding=dict ;Access=batch     ;MeanDelta=50;Stride=4               32.8     33.4     33.8     0.111X     0.111X     0.111X
// Checking the correctness of the results... Ok.
//
// Benchmark:                 Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
// !ParquetType=INT64;OutType=int64_t;Encoding=plain;Access=batch     ;MeanDelta=1;Stride=8                255      258      262         1X         1X         1X
// !ParquetType=INT64;OutType=int64_t;Encoding=delta;Access=batch     ;MeanDelta=1;Stride=8                 83     86.4     87.9     0.325X     0.334X     0.336X
// !ParquetType=INT64;OutType=int64_t;Encoding=dict ;Access=batch     ;MeanDelta=1;Stride=8               48.7     49.5     50.2     0.191X     0.192X     0.192X
// Checking the correctness of the results... Ok.
//
// Benchmark:                 Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
//                                                                          (relative) (relative) (relative)
// ---------------------------------------------------------------------------------------------------------
// !ParquetType=INT64;OutType=int64_t;Encoding=plain;Access=batch     ;MeanDelta=50;Stride=8                255      261      265         1X         1X         1X
// !ParquetType=INT64;OutType=int64_t;Encoding=delta;Access=batch     ;MeanDelta=50;Stride=8               82.9     86.2     87.7     0.325X      0.33X     0.331X
// !ParquetType=INT64;OutType=int64_t;Encoding=dict ;Access=batch     ;MeanDelta=50;Stride=8               31.8     32.5     33.2     0.124X     0.125X     0.125X
// Checking the correctness of the results... Ok.

using namespace impala;

template <typename INT_T>
std::vector<uint8_t> DeltaEncodeAll(const std::vector<INT_T>& plain,
    const std::size_t block_size, const std::size_t miniblock_size) {
  const std::size_t miniblocks_in_block = block_size / miniblock_size;
  DCHECK(block_size % miniblock_size == 0);

  ParquetDeltaEncoder<INT_T> encoder;
  Status init_success = encoder.Init(block_size, miniblocks_in_block);
  DCHECK(init_success.ok());

  std::vector<uint8_t> buffer(encoder.WorstCaseOutputSize(plain.size()), 0);
  encoder.NewPage(buffer.data(), buffer.size());

  for (INT_T value : plain) {
    bool success = encoder.Put(value);
    DCHECK(success);
  }

  int written_bytes = encoder.FinalizePage();

  buffer.resize(written_bytes);

  return buffer;
}

template <typename INT_T>
std::pair<std::vector<uint8_t>, int> DictEncodeAll(const std::vector<INT_T>& plain) {
  MemTracker track_encoder;
  MemTracker tracker;
  MemPool pool(&tracker);

  int dict_len;
  std::vector<uint8_t> output;

  {
    DictEncoder<INT_T> encoder(&pool, sizeof(INT_T), &track_encoder);

    for (const INT_T value : plain) {
      int success = encoder.Put(value);
      DCHECK_GE(success, 0);
    }

    dict_len = encoder.dict_encoded_size();
    const int data_len = encoder.EstimatedDataEncodedSize();

    output = std::vector<uint8_t>(dict_len + data_len, 0);

    encoder.WriteDict(output.data());
    const int data_written = encoder.WriteData(output.data() + dict_len, data_len);
    DCHECK_GE(data_written, 0);

    output.resize(dict_len + data_written);

    encoder.Close();
  }

  pool.FreeAll();
  tracker.Close();
  track_encoder.Close();

  return std::make_pair(output, dict_len);
}

/// This struct holds the information that is given to the benchmark functions.
struct DeltaBenchmarkData {
  std::shared_ptr<const std::vector<uint8_t>> input;
  std::shared_ptr<std::vector<uint8_t>> output;
  int num_values;
  int stride;

  // In case of dictionary encoding, the dictionary and the indices are stored
  // contiguously in the same vector. This variable stores the length of the
  // dictionary in bytes.
  int dict_len;

  DeltaBenchmarkData(std::shared_ptr<const std::vector<uint8_t>> input,
      std::shared_ptr<std::vector<uint8_t>> output,
      int num_values, int stride, int dict_len = 0):
    input(input),
    output(output),
    num_values(num_values),
    stride(stride),
    dict_len(dict_len)
  {
  }

  const uint8_t* input_begin() const {
    return input->data();
  }

  const uint8_t* input_end() const {
    return input->data() + input->size();
  }

  uint8_t* output_begin() {
    return output->data();
  }

  uint8_t* output_end() {
    return output->data() + output->size();
  }
};

/// A struct representing benchmark configurations.
struct Config {
  enum ParquetType {
    INT32 = 32,
    INT64 = 64
  };

  enum OutType {
    int8 = 8,
    int16 = 16,
    int32 = 32,
    int64 = 64
  };

  enum Encoding {
    PLAIN = 0,
    DELTA,
    DICT
  };

  enum Access {
    ONY_BY_ONE,
    BATCH
  };

  ParquetType parquet_type;
  OutType out_type;
  Encoding encoding;
  Access access;
  int mean_delta;
  int stride;

  Config(ParquetType parquet_type, OutType out_type, Encoding encoding, Access access,
      int mean_delta, int stride)
      : parquet_type(parquet_type), out_type(out_type), encoding(encoding),
      access(access), mean_delta(mean_delta), stride(stride)
  {
  }

  /// This is used in naming the benchmarks.
  ///
  /// We cannot get information programmatically from 'Benchmark' objects, they only
  /// output the results as a string. To analyze the results, we have to use a
  /// post-processing script. To facilitate this, the names of the benchmark cases follow
  /// the pattern "param_name=value;". A '!' is prepended to the name so that it is easier
  /// for a post-processing tool to select the lines from the benchmark output that
  /// contain benchmark results.
  std::string to_string() const {
    const std::string sep = ";";
    std::stringstream name;
    name << "!"
        << "ParquetType=" << (parquet_type == INT32 ? "INT32" : "INT64") << sep
        << "OutType=int" << out_type << "_t" << sep
        << "Encoding=" << EncodingToStr() << sep

        /// Adding extra whitespace to "batch" to keep it aligned with "one_by_one".
        << "Access=" << (access == BATCH ? "batch     " : "one_by_one") << sep
        << "MeanDelta=" << mean_delta << sep
        << "Stride=" << stride;

    return name.str();
  }

 private:
  std::string EncodingToStr() const {
    switch (encoding) {
      case PLAIN: return "plain";
      case DELTA: return "delta";
      case DICT: return "dict "; // Extra whitespace for alignment.
      default: return "unknown";
    }
  }
};

/// Struct template to get the Config runtime representation of parquet types from the
/// equivalent static C++ types.
template <typename INT_T>
struct IntToConfigParquetType;

template <>
struct IntToConfigParquetType<int32_t> {
  static constexpr Config::ParquetType parquet_type = Config::INT32;
};

template <>
struct IntToConfigParquetType<int64_t> {
  static constexpr Config::ParquetType parquet_type = Config::INT64;
};

/// Struct template to get the parquet runtime representation of parquet types from the
/// equivalent static C++ types.
template <typename INT_T>
struct IntToParquetType;

template <>
struct IntToParquetType<int32_t> {
  static constexpr parquet::Type::type type = parquet::Type::INT32;
};

template <>
struct IntToParquetType<int64_t> {
  static constexpr parquet::Type::type type = parquet::Type::INT64;
};

/// Struct template to get the Config runtime representation of output types (used in
/// tuples) from the equivalent static C++ types.
template <typename INT_T>
struct IntToConfigOutType;

template <>
struct IntToConfigOutType<int8_t> {
  static constexpr Config::OutType out_type = Config::int8;
};

template <>
struct IntToConfigOutType<int16_t> {
  static constexpr Config::OutType out_type = Config::int16;
};

template <>
struct IntToConfigOutType<int32_t> {
  static constexpr Config::OutType out_type = Config::int32;
};

template <>
struct IntToConfigOutType<int64_t> {
  static constexpr Config::OutType out_type = Config::int64;
};

/// A struct holding all the information that is necessary to materialize a benchmark case
/// and test its results.
struct BenchmarkUnit {
  Config config;
  Benchmark::BenchmarkFunction function;
  std::shared_ptr<DeltaBenchmarkData> benchmark_data;

  /// The plain data. Used for checking if the result is correct after
  /// benchmarking. In case of plain encoded benchmark units, this should be
  /// the same as the input.
  std::shared_ptr<const std::vector<uint8_t>> plain;

  BenchmarkUnit(const Config config, const Benchmark::BenchmarkFunction function,
      DeltaBenchmarkData benchmark_data,
      std::shared_ptr<const std::vector<uint8_t>> plain):
    config(config),
    function(function),
    benchmark_data(std::make_shared<DeltaBenchmarkData>(benchmark_data)),
    plain(plain)
  {
  }
};

/// Benchmark functions come in versions according to the encoding type and the reading
/// pattern (one by one or in batches).
template <typename INT_T, typename OutType>
void PlainBenchmark_OneByOne(int batch_size, void* data) {
  DeltaBenchmarkData* benchmark_data = reinterpret_cast<DeltaBenchmarkData*>(data);
  const uint8_t* const input = benchmark_data->input_begin();
  const uint8_t* const input_end = benchmark_data->input_end();
  const int num_values = benchmark_data->num_values;
  const int stride = benchmark_data->stride;

  DCHECK_GE(stride, sizeof(INT_T));

  for (int batch = 0; batch < batch_size; batch++) {
    const uint8_t* input_begin = input;

    uint8_t* output = benchmark_data->output_begin();
    for (int i = 0; i < num_values; i++) {
      constexpr parquet::Type::type PARQUET_TYPE = IntToParquetType<INT_T>::type;

      const int read = ParquetPlainEncoder::Decode<OutType, PARQUET_TYPE>(input_begin,
          input_end, 0, reinterpret_cast<OutType*>(output));

      if (UNLIKELY(read != sizeof(INT_T))) {
        LOG(ERROR) << "Error: value not read in iteration " << i << ".";
        exit(1);
      }

      input_begin += read;
      output += stride;
    }
  }
}

template <typename INT_T, typename OutType>
void DeltaBenchmark_OneByOne(int batch_size, void* data) {
  DeltaBenchmarkData* benchmark_data = reinterpret_cast<DeltaBenchmarkData*>(data);
  const uint8_t* input = benchmark_data->input_begin();
  const int input_size = benchmark_data->input_end() - input;
  const int num_values = benchmark_data->num_values;
  const int stride = benchmark_data->stride;

  DCHECK_GE(stride, sizeof(INT_T));

  ParquetDeltaDecoder<INT_T> decoder;
  for (int batch = 0; batch < batch_size; batch++) {
    Status page_init_status = decoder.NewPage(input, input_size);
    if (!page_init_status.ok()) {
      LOG(ERROR) << page_init_status.msg().msg();
      exit(1);
    }

    uint8_t* output = benchmark_data->output_begin();
    for (int i = 0; i < num_values; i++) {
      const int read = decoder.template NextValuesConverted<OutType>(1, output,
          sizeof(OutType));

      if (UNLIKELY(read != 1)) {
        LOG(ERROR) << "Error: Value not read.";
        exit(1);
      }

      output += stride;
    }
  }
}

template <typename INT_T, typename OutType>
void DictBenchmark_OneByOne(int batch_size, void* data) {
  DeltaBenchmarkData* benchmark_data = reinterpret_cast<DeltaBenchmarkData*>(data);
  const uint8_t* input = benchmark_data->input_begin();
  uint8_t* non_const_input = const_cast<uint8_t*>(input);
  const int input_size = benchmark_data->input_end() - input;
  const int num_values = benchmark_data->num_values;
  const int stride = benchmark_data->stride;
  const int dict_len = benchmark_data->dict_len;

  DCHECK_GE(stride, sizeof(INT_T));

  MemTracker decode_tracker;
  {
    DictDecoder<OutType> decoder(&decode_tracker);
    for (int batch = 0; batch < batch_size; batch++) {
      constexpr parquet::Type::type PARQUET_TYPE = IntToParquetType<INT_T>::type;
      const bool dict_set_success = decoder.template Reset<PARQUET_TYPE>(non_const_input,
          dict_len, sizeof(INT_T));
      if (UNLIKELY(!dict_set_success)) {
        LOG(ERROR) << "Error initializing the dictionary.";
        exit(1);
      }

      Status data_set_success = decoder.SetData(non_const_input + dict_len,
          input_size - dict_len);
      if (UNLIKELY(!data_set_success.ok())) {
        LOG(ERROR) << "Error setting the data for the dictionary decoder.";
        exit(1);
      }

      uint8_t* output = benchmark_data->output_begin();
      for (int i = 0; i < num_values; i++) {
        const bool success = decoder.GetNextValue(reinterpret_cast<OutType*>(output));

        if (UNLIKELY(!success)) {
          LOG(ERROR) << "Error: value not read.";
          exit(1);
        }

        output += stride;
      }
    }
  }

  decode_tracker.Close();
}

// Size of batches in batched decoding.
constexpr int READ_BATCH_SIZE = 1024;

template <typename INT_T, typename OutType>
void PlainBenchmark_Batch(int batch_size, void* data) {
  DeltaBenchmarkData* benchmark_data = reinterpret_cast<DeltaBenchmarkData*>(data);
  const uint8_t* const input = benchmark_data->input_begin();
  const uint8_t* const input_end = benchmark_data-> input_end();
  const int num_values = benchmark_data->num_values;
  const int stride = benchmark_data->stride;

  DCHECK_GE(stride, sizeof(INT_T));

  for (int batch = 0; batch < batch_size; batch++) {
    const uint8_t* input_begin = input;

    uint8_t* output = benchmark_data->output_begin();
    constexpr parquet::Type::type PARQUET_TYPE = IntToParquetType<INT_T>::type;

    const int iterations = num_values / READ_BATCH_SIZE;
    for (int i = 0; i < iterations; i++) {
      const int read = ParquetPlainEncoder::DecodeBatch<OutType, PARQUET_TYPE>(input_begin,
          input_end, 0, READ_BATCH_SIZE, stride, reinterpret_cast<OutType*>(output));

      if (UNLIKELY(read != READ_BATCH_SIZE * sizeof(INT_T))) {
        LOG(ERROR) << "Error: values not written.";
      }

      input_begin += read;
      output += READ_BATCH_SIZE * stride;
    }

    const int remainder = num_values % READ_BATCH_SIZE;
    const int read = ParquetPlainEncoder::DecodeBatch<OutType, PARQUET_TYPE>(input_begin,
        input_end, 0, remainder, stride, reinterpret_cast<OutType*>(output));

    if (UNLIKELY(read != remainder * sizeof(INT_T))) {
      LOG(ERROR) << "Error: values not written.";
    }
  }
}

template <typename INT_T, typename OutType>
void DeltaBenchmark_Batch(int batch_size, void* data) {
  DeltaBenchmarkData* benchmark_data = reinterpret_cast<DeltaBenchmarkData*>(data);
  const uint8_t* input = benchmark_data->input_begin();
  const int input_size = benchmark_data->input_end() - input;
  const int num_values = benchmark_data->num_values;
  const int stride = benchmark_data->stride;

  DCHECK_GE(stride, sizeof(INT_T));

  ParquetDeltaDecoder<INT_T> decoder;
  for (int batch = 0; batch < batch_size; batch++) {
    Status page_init_status = decoder.NewPage(input, input_size);
    if (!page_init_status.ok()) {
      LOG(ERROR) << page_init_status.msg().msg();
      exit(1);
    }

    uint8_t* output = benchmark_data->output_begin();
    int values_read = 0;

    for (int i = 0; i < (num_values + READ_BATCH_SIZE - 1) / READ_BATCH_SIZE; i++) {
      int read = decoder.template NextValuesConverted<OutType>(
          READ_BATCH_SIZE, output, stride);

      if (UNLIKELY(read < 0)) {
        LOG(ERROR) << "Error: values not written.";
      }

      values_read += read;
      output += read * stride;
    }

    if (values_read != num_values) {
      LOG(ERROR) << "Error: the number of values read is incorrect.";
      exit(1);
    }
  }
}

template <typename INT_T, typename OutType>
void DictBenchmark_Batch(int batch_size, void* data) {
  DeltaBenchmarkData* benchmark_data = reinterpret_cast<DeltaBenchmarkData*>(data);
  const uint8_t* input = benchmark_data->input_begin();
  uint8_t* non_const_input = const_cast<uint8_t*>(input);
  const int input_size = benchmark_data->input_end() - input;
  const int num_values = benchmark_data->num_values;
  const int stride = benchmark_data->stride;
  const int dict_len = benchmark_data->dict_len;

  DCHECK_GE(stride, sizeof(INT_T));

  MemTracker decode_tracker;
  {
    DictDecoder<OutType> decoder(&decode_tracker);
    for (int batch = 0; batch < batch_size; batch++) {

      constexpr parquet::Type::type PARQUET_TYPE = IntToParquetType<INT_T>::type;
      const bool dict_set_success = decoder.template Reset<PARQUET_TYPE>(non_const_input,
          dict_len, sizeof(INT_T));
      if (UNLIKELY(!dict_set_success)) {
        LOG(ERROR) << "Error initializing the dictionary.";
        exit(1);
      }

      Status data_set_success = decoder.SetData(non_const_input + dict_len,
          input_size - dict_len);
      if (UNLIKELY(!data_set_success.ok())) {
        LOG(ERROR) << "Error setting the data for the dictionary decoder.";
        exit(1);
      }

      uint8_t* output = benchmark_data->output_begin();
      const int iterations = num_values / READ_BATCH_SIZE;
      for (int i = 0; i < iterations; i++) {
        const bool success = decoder.GetNextValues(reinterpret_cast<OutType*>(output),
            stride, READ_BATCH_SIZE);

        if (UNLIKELY(!success)) {
          LOG(ERROR) << "Error: values not read.";
        }

        output += READ_BATCH_SIZE * stride;
      }

      const int remainder = num_values % READ_BATCH_SIZE;
      const bool success =  decoder.GetNextValues(reinterpret_cast<OutType*>(output),
          stride, remainder);

      if (UNLIKELY(!success)) {
        LOG(ERROR) << "Error: values not read.";
      }
    }
  }

  decode_tracker.Close();
}

/// Return `base + delta` if it does not overflow, otherwise return `base - delta`.
template <typename OutType>
OutType AddOrSubtractNoOverflow(const OutType base, const OutType delta) {
  const bool overflow = delta > 0
      && base > std::numeric_limits<OutType>::max() - delta;
  const bool underflow = delta < 0
      && base < std::numeric_limits<OutType>::min() - delta;

  if (overflow || underflow) return base - delta;

  return base + delta;
}

template <typename INT_T, typename OutType>
std::vector<INT_T> GenerateInputNumbers(std::mt19937& gen, int mean_delta, int size) {
  std::uniform_int_distribution<OutType> first_value_dist(
      std::numeric_limits<OutType>::min(), std::numeric_limits<OutType>::max());
  INT_T first_value = static_cast<INT_T>(first_value_dist(gen));

  std::normal_distribution<double> delta_dist(mean_delta, mean_delta * 0.75);

  std::vector<INT_T> res(size);

  res[0] = first_value;

  for (std::size_t i = 1; i < size; i++) {
    const OutType delta = static_cast<OutType>(delta_dist(gen));
    const OutType previous = static_cast<OutType>(res[i - 1]);
    // This causes the values to stagnate close to the max value if it is reached.
    const OutType current = AddOrSubtractNoOverflow<OutType>(previous, delta);

    res[i] = static_cast<INT_T>(current);
  }

  return res;
}

template <typename INT_T>
std::vector<uint8_t> NumbersToBytes(const std::vector<INT_T>& numbers) {
  const int byte_size = numbers.size() * sizeof(INT_T);
  std::vector<uint8_t> res(byte_size);
  memcpy(&res[0], &numbers[0], byte_size);

  return res;
}

template <typename INT_T>
std::pair<std::vector<uint8_t>, int> EncodeNumbers(const std::vector<INT_T>& numbers,
    const Config::Encoding encoding) {
  std::pair<std::vector<uint8_t>, int> res = std::make_pair(std::vector<uint8_t>{}, -1);
  switch (encoding) {
    case Config::PLAIN: {
      res.first = NumbersToBytes<INT_T>(numbers);
      break;
    }
    case Config::DELTA: {
      res.first = DeltaEncodeAll<INT_T>(numbers, 128, 32);
      break;
    }
    case Config::DICT: {
      res = DictEncodeAll<INT_T>(numbers);
      break;
    }
    default: {
      LOG(ERROR) << "Error: Unknown encoding.";
      std::exit(1);
    }
  }
  return res;
}

template <typename INT_T, typename OutType>
Benchmark::BenchmarkFunction GetBenchmarkFunction(const Config::Encoding encoding,
    const Config::Access access) {
  if (access == Config::BATCH) {
    switch (encoding) {
      case Config::PLAIN: return PlainBenchmark_Batch<INT_T, OutType>;
      case Config::DELTA: return DeltaBenchmark_Batch<INT_T, OutType>;
      case Config::DICT: return DictBenchmark_Batch<INT_T, OutType>;
    }
  } else if (access == Config::ONY_BY_ONE) {
    switch (encoding) {
      case Config::PLAIN: return PlainBenchmark_OneByOne<INT_T, OutType>;
      case Config::DELTA: return DeltaBenchmark_OneByOne<INT_T, OutType>;
      case Config::DICT: return DictBenchmark_OneByOne<INT_T, OutType>;
    }
  }

  LOG(ERROR) << "Error: unknown encoding or access mode.";
  std::exit(1);
}

std::shared_ptr<std::vector<uint8_t>> GetOutputBuffer(int size) {
  return std::make_shared<std::vector<uint8_t>>(size, 0);
}

/// A class for lazily generating input numbers and encoded inputs. If an input with a
/// given set of parameters is requested, it checks whether one has already been
/// generated and returns that in this case, otherwise generates a new one.
template <typename INT_T, typename OutType>
class LazyInputGenerator {
 public:
  /// A pair whose first element is the input buffer, the second is the length
  /// of the dictionary in case of dictionary encoding; for other encodings,
  /// the value is not used.
  using InputInfo = std::pair<std::shared_ptr<std::vector<uint8_t>>, int>;

  LazyInputGenerator(std::mt19937& gen)
      : number_vectors_(),
        input_infos_(),
        gen_(gen)
  {}

  InputInfo get_input(const int mean_delta, const Config::Encoding encoding) {
    /// Ensure that we have numbers generated for the configuration.
    auto numbers_it = number_vectors_.find(mean_delta);
    if (numbers_it == number_vectors_.end()) {
      number_vectors_.emplace(mean_delta,
          GenerateInputNumbers<INT_T, OutType>(gen_, mean_delta, NUM_VALUES));
      numbers_it = number_vectors_.find(mean_delta);
    }

    /// Ensure that there is a map for the mean delta in input_infos.
    std::map<Config::Encoding, InputInfo>& mean_delta_map = input_infos_[mean_delta];

    /// Ensure that we have the encoded input data.
    auto input_info_it = mean_delta_map.find(encoding);
    if (input_info_it == mean_delta_map.end()) {
      std::pair<std::vector<uint8_t>, int> vec_and_dict_len
          = EncodeNumbers<INT_T>(numbers_it->second, encoding);
      std::shared_ptr<std::vector<uint8_t>> shared_vec
          = std::make_shared<std::vector<uint8_t>>(vec_and_dict_len.first);
      auto input_info = std::make_pair(shared_vec, vec_and_dict_len.second);
      mean_delta_map.emplace(encoding, input_info);

      input_info_it = mean_delta_map.find(encoding);
    }

    return input_info_it->second;
  }

  /// Returns a report of the sizes of the different inputs that have been generated.
  std::string report() const {
    if (input_infos_.empty()) return "";

    std::stringstream s;

    s << "ParquetType: INT" << sizeof(INT_T) * 8 << "; OutType: int"
      << sizeof(OutType) * 8 << "_t" << std::endl;

    for (auto& pair : input_infos_) {
      const int mean_delta = pair.first;

      const string indent = "  ";
      s << indent << "Mean delta: " << mean_delta << "." << std::endl;

      /// Use a deterministic order.
      const std::map<Config::Encoding, InputInfo>& mean_delta_map = pair.second;

      for (const Config::Encoding encoding
          : {Config::PLAIN, Config::DELTA, Config::DICT}) {
        auto info_it = mean_delta_map.find(encoding);
        if (info_it != mean_delta_map.end()) {
          const std::shared_ptr<std::vector<uint8_t>>& buffer = info_it->second.first;
          std::string encoding_name;

          switch (encoding) {
            case Config::PLAIN: encoding_name = "Plain"; break;
            case Config::DELTA: encoding_name = "Delta"; break;
            case Config::DICT: encoding_name = "Dict"; break;
            default: encoding_name = "Unknown encoding.";
          }

          s << indent << encoding_name << ": " << buffer->size() << "." << std::endl;
        }
      }

      s << std::endl;
    }

    return s.str();
  }

 private:
  int NUM_VALUES = 10 * 1024 + 1;
  /// Random generated number vectors stored by the mean (expected value) of
  /// the deltas of the numbers.
  std::map<int, std::vector<INT_T>> number_vectors_;

  /// InputInfo values stored by mean delta and encoding.
  std::map<int, std::map<Config::Encoding, InputInfo>> input_infos_;

  std::mt19937& gen_;
};

template <typename INT_T, typename OutType>
std::vector<BenchmarkUnit> GenerateBenchmarkUnits(
    LazyInputGenerator<INT_T, OutType>& input_gen,
    const std::vector<Config>& configs) {
  std::map<int, std::vector<INT_T>> number_vectors;

  using InputInfo = typename LazyInputGenerator<INT_T, OutType>::InputInfo;

  std::vector<BenchmarkUnit> res;
  for (const Config& config : configs) {
    InputInfo input_info = input_gen.get_input(config.mean_delta, config.encoding);
    InputInfo plain_input_info = input_gen.get_input(config.mean_delta, Config::PLAIN);
    const int element_count = plain_input_info.first->size() / sizeof(INT_T);

    Benchmark::BenchmarkFunction function
        = GetBenchmarkFunction<INT_T, OutType>(config.encoding, config.access);

    DeltaBenchmarkData data(input_info.first,
        GetOutputBuffer(element_count * config.stride), element_count, config.stride,
        input_info.second);

    BenchmarkUnit unit(config, function, data, plain_input_info.first);
    res.push_back(unit);
  }

  return res;
}

/// Function template to test equality of values of different types.
template <typename OrigType, typename DecodedType>
bool EqualIntegerValuesWithType(const uint8_t* orig, const uint8_t* decoded) {
  OrigType orig_value;
  memcpy(&orig_value, orig, sizeof(OrigType));

  DecodedType decoded_value;
  memcpy(&decoded_value, decoded, sizeof(DecodedType));

  return orig_value == decoded_value;
}

bool EqualIntegerValues(const uint8_t* orig, const int orig_size,
    const uint8_t* decoded, const int decoded_size) {
  if (orig_size == 4) {
    switch (decoded_size) {
      case 1: return EqualIntegerValuesWithType<int32_t, int8_t>(orig, decoded);
      case 2: return EqualIntegerValuesWithType<int32_t, int16_t>(orig, decoded);
      case 4: return EqualIntegerValuesWithType<int32_t, int32_t>(orig, decoded);
      case 8: return EqualIntegerValuesWithType<int32_t, int64_t>(orig, decoded);
      default: return false;
    }
  } else if (orig_size == 8) {
    switch (decoded_size) {
      case 1: return EqualIntegerValuesWithType<int64_t, int8_t>(orig, decoded);
      case 2: return EqualIntegerValuesWithType<int64_t, int16_t>(orig, decoded);
      case 4: return EqualIntegerValuesWithType<int64_t, int32_t>(orig, decoded);
      case 8: return EqualIntegerValuesWithType<int64_t, int64_t>(orig, decoded);
      default: return false;
    }
  }

  return false;
}

/// Checking whether the benchmarks have produced the functionally correct results.
bool CheckCorrectResults(const BenchmarkUnit& unit) {
  const int original_width = unit.config.parquet_type / 8;
  const int output_width = unit.config.out_type / 8;
  const int stride = unit.benchmark_data->stride;
  const int num_values = unit.benchmark_data->num_values;

  const uint8_t* original_buffer = unit.plain->data();
  const uint8_t* decoded_buffer = unit.benchmark_data->output_begin();

  for (int i = 0; i < num_values; i++) {
    const uint8_t* original_value = original_buffer + i * original_width;
    const uint8_t* decoded_value = decoded_buffer + i * stride;

    if (!EqualIntegerValues(original_value, original_width,
          decoded_value, output_width)) {
      return false;
    }
  }

  return true;
}

/// Given a vector of vectors of 'BenchmarkUnit' objects, this function creates a
/// 'Benchmark' object for every outer vector and assigns the 'BenchmarkUnits' in the same
/// inner vector to the same 'Benchmark' object, runs the benchmarks, prints the results
/// to stdout and 'report_file' and checks the correctness of the results.
void RunBenchmarks(const std::vector<std::vector<BenchmarkUnit>>& bm_units_by_suite,
    const std::string& report_file) {
  std::ofstream out_file(report_file);

  for (const std::vector<BenchmarkUnit>& units : bm_units_by_suite) {
    Benchmark benchmark("Benchmark");
    std::vector<BenchmarkUnit> measured_units;

    for (const BenchmarkUnit& unit : units) {
        benchmark.AddBenchmark(unit.config.to_string(), unit.function,
            unit.benchmark_data.get());
        measured_units.push_back(unit);
    }

    if (!measured_units.empty()) {
      const std::string benchmark_results = benchmark.Measure();
      out_file << benchmark_results << std::endl;
      std::cout << benchmark_results;

      std::cout << "Checking the correctness of the results... ";
      bool all_correct = true;
      for (const BenchmarkUnit& unit : measured_units) {
        if (!CheckCorrectResults(unit)) {
          all_correct = false;
          std::cout << "Incorrect results in benchmark " << unit.config.to_string()
              << "." << std::endl;
        }
      }

      if (all_correct) std::cout << "Ok." << std::endl << std::endl;
    }
  }
}

/// Generates configurations. The configuration parameters are the Cartesian product of
/// 'mean_deltas', 'encodings', 'accesses' and 'strides', filtered by the function
/// 'filter'. The filtering function should return true for configurations that should be
/// used in the benchmarks.
template <typename INT_T, typename OutType>
std::vector<Config> GenerateConfigs(const std::vector<int>& mean_deltas,
    const std::vector<Config::Encoding>& encodings,
    const std::vector<Config::Access>& accesses, const std::vector<int>& strides,
    std::function<bool(const Config&)> filter) {
  constexpr Config::ParquetType parquet_type
      = IntToConfigParquetType<INT_T>::parquet_type;
  constexpr Config::OutType out_type = IntToConfigOutType<OutType>::out_type;


  std::vector<Config> res;
  for (const OutType mean_delta : mean_deltas) {
    for (const int stride : strides) {
      for (const Config::Access access : accesses) {
        for (const Config::Encoding encoding : encodings) {
          const Config config(parquet_type, out_type, encoding,
              access, mean_delta, stride);

          /// Measurements are not relevant if the mean delta overflows.
          const bool mean_delta_ok
              = config.mean_delta < (1UL << (config.out_type - 1)) - 1;
          const bool stride_ok = config.stride * 8 >= config.out_type;

          if (mean_delta_ok && stride_ok && filter(config)) {
            // We only display this warning if the user didn't filter this config out.
            if constexpr (parquet_type == Config::INT64 && out_type != Config::int64) {
              std::cout << "Warning: Parquet type 'INT64' cannot be used with output "
                  << "type " << out_type << ". Ignoring this configuration." << std::endl;
              return {};
            }
            res.push_back(config);
          }
        }
      }
    }
  }

  return res;
}

template <typename T>
void AddToVec(std::vector<T>* accumulator,
    const std::vector<T>& to_add) {
  DCHECK(accumulator != nullptr);
  accumulator->insert(accumulator->end(), to_add.begin(), to_add.end());
}

template <typename T>
bool VecContains(const std::vector<T>& vec, const T& elem) {
  return std::find(vec.begin(), vec.end(), elem) != vec.end();
}

/// Function template that generates configurations and 'BenchmarkUnit' objects.
template <typename INT_T, typename OutType>
void GenerateInputAndBenchmarkUnits(std::mt19937& gen,
    std::vector<BenchmarkUnit>& units,
    const std::vector<int>& mean_deltas,
    const std::vector<Config::Encoding>& encodings,
    const std::vector<Config::Access>& accesses, const std::vector<int>& strides,
    std::function<bool(const Config& config)> filter) {
  const std::vector<Config> configs = GenerateConfigs<INT_T, OutType>(mean_deltas,
      encodings, accesses, strides, filter);

  if constexpr (std::is_same_v<INT_T, int64_t> && !std::is_same_v<OutType, int64_t>) {
    DCHECK(configs.empty()) << "Parquet type INT64 can only be used with BIGINT.";
  } else {
    LazyInputGenerator<INT_T, OutType> input_gen(gen);
    AddToVec(&units, GenerateBenchmarkUnits<INT_T, OutType>(input_gen, configs));

    const std::string report = input_gen.report();

    if (!report.empty()) std::cout << report;
  }
}

/// Non-template function for generating 'BenchmarkUnit' objects. Dispatches to templated
/// versions.
std::vector<BenchmarkUnit> GenerateBenchmarkUnitsRuntime(std::mt19937& gen,
    const std::vector<Config::ParquetType>& parquet_types,
    const std::vector<Config::OutType>& out_types, const std::vector<int>& mean_deltas,
    const std::vector<Config::Encoding>& encodings,
    const std::vector<Config::Access>& accesses, const std::vector<int>& strides,
    std::function<bool(const Config& config)> filter) {
  std::vector<BenchmarkUnit> units;
  if (VecContains(parquet_types, Config::INT32)) {
    using INT_T = int32_t;

    if (VecContains(out_types, Config::int8)) {
      using OutType = int8_t;

      GenerateInputAndBenchmarkUnits<INT_T, OutType>(gen, units, mean_deltas, encodings,
          accesses, strides, filter);
    }

    if (VecContains(out_types, Config::int16)) {
      using OutType = int16_t;

      GenerateInputAndBenchmarkUnits<INT_T, OutType>(gen, units, mean_deltas, encodings,
          accesses, strides, filter);
    }

    if (VecContains(out_types, Config::int32)) {
      using OutType = int32_t;

      GenerateInputAndBenchmarkUnits<INT_T, OutType>(gen, units, mean_deltas, encodings,
          accesses, strides, filter);
    }
  }

  if (VecContains(parquet_types, Config::INT64)) {
    using INT_T = int64_t;

    // Only OutType == int64_t will be valid, warnings will be issued for the other cases
    // if such configurations remain after filtering.
    if (VecContains(out_types, Config::int8)) {
      using OutType = int8_t;

      GenerateInputAndBenchmarkUnits<INT_T, OutType>(gen, units, mean_deltas, encodings,
          accesses, strides, filter);
    }

    if (VecContains(out_types, Config::int16)) {
      using OutType = int16_t;

      GenerateInputAndBenchmarkUnits<INT_T, OutType>(gen, units, mean_deltas, encodings,
          accesses, strides, filter);
    }

    if (VecContains(out_types, Config::int32)) {
      using OutType = int32_t;

      GenerateInputAndBenchmarkUnits<INT_T, OutType>(gen, units, mean_deltas, encodings,
          accesses, strides, filter);
    }

    if (VecContains(out_types, Config::int64)) {
      using OutType = int64_t;

      GenerateInputAndBenchmarkUnits<INT_T, OutType>(gen, units, mean_deltas, encodings,
          accesses, strides, filter);
    }
  }

  return units;
}

/// Group the 'BenchmarkUnit's together according to 'partition_function' and add them to
/// 'Benchmark' suites. The function 'partition_function' should define an equivalence
/// relation - it returns true if the arguments should belogs to the same benchmark suite.
std::vector<std::vector<BenchmarkUnit>> GroupBenchmarkUnits(
    std::vector<BenchmarkUnit>& units,
    std::function<bool(const Config&, const Config&)> partition_function) {
  auto begin = units.begin();
  auto end = units.end();

  std::vector<std::vector<BenchmarkUnit>> res;

  while (begin != end) {
    const Config& first_config = begin->config;
    auto predicate = [&] (const BenchmarkUnit& unit) -> bool {
      return partition_function(first_config, unit.config);
    };

    auto boundary_it = std::stable_partition(begin, end, predicate);

    std::vector<BenchmarkUnit> group;
    for (auto it = begin; it != boundary_it; it++) {
      group.push_back(*it);
    }

    res.push_back(group);
    begin = boundary_it;
  }

  return res;
}

int main(int argc, char **argv) {
  CpuInfo::Init();
  std::cout << std::endl << Benchmark::GetMachineInfo() << std::endl;

  // Define the set of possible values in each dimension. By default the set of generated
  // 'Config's is the Cartesian product of these, but it can be narrowed down using the
  // filter function below.
  const std::vector<Config::ParquetType> parquet_types = {Config::INT32, Config::INT64};
  const std::vector<Config::OutType> out_types = {Config::int8, Config::int16,
    Config::int32, Config::int64};
  const std::vector<int> mean_deltas = {1, 50, 100, 500, 1000, 100000};
  const std::vector<Config::Encoding> encodings
      = {Config::PLAIN, Config::DELTA, Config::DICT};
  const std::vector<Config::Access> accesses = {Config::ONY_BY_ONE, Config::BATCH};
  const std::vector<int> strides = {4, 8, 12, 16, 20, 30, 40, 50, 80, 100, 120, 150, 180,
      200, 400};

  /// This is used to tune which configurations should be measured.
  auto filter = [] (const Config& config) -> bool {
    // We only want to measure out_types int8, int16 and int32 with the INT32 Parquet
    // type.
    if (config.parquet_type == Config::INT32 && config.out_type == Config::int64) {
      return false;
    }

    // We only support BIGINT (i.e. int64) with Parquet type INT64.
    if (config.parquet_type == Config::INT64 && config.out_type != Config::int64) {
      return false;
    }

    return config.access == Config::BATCH
        && config.stride == ((int) config.parquet_type / 8)
        && config.mean_delta <= 50;
  };

  std::random_device rd;
  std::mt19937 gen(rd());
  std::vector<BenchmarkUnit> units = GenerateBenchmarkUnitsRuntime(gen, parquet_types,
      out_types, mean_deltas, encodings, accesses, strides, filter);

  auto partition_function = [] (const Config& config1, const Config& config2) {
    return true
        && config1.parquet_type == config2.parquet_type
        && config1.out_type == config2.out_type
        && config1.mean_delta == config2.mean_delta
        ;
  };

  std::vector<std::vector<BenchmarkUnit>> suites
      = GroupBenchmarkUnits(units, partition_function);

  std::cout << "Running benchmark suites." << std::endl;

  RunBenchmarks(suites, "delta-benchmark-report.txt");

  return 0;
}
