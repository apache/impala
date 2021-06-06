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


#ifndef IMPALA_UTIL_BENCHMARK_H
#define IMPALA_UTIL_BENCHMARK_H

#include <string>
#include <vector>

namespace impala {

/// Utility class for microbenchmarks.
/// This can be utilized to create a benchmark suite.  For example:
///  Benchmark suite("benchmark");
///  suite.AddBenchmark("Implementation #1", Implementation1Fn, data);
///  suite.AddBenchmark("Implementation #2", Implementation2Fn, data);
///  ...
///  string result = suite.Measure();
class Benchmark {
 public:
  /// Name of the microbenchmark.  This is outputted in the result.
  /// micro_heuristics is a bool argument which indicates whether micro benchmark
  /// style should be used; that is, we look for a set of runs which doesn't context
  /// switch so that we can measure pure userland code performance, as opposed to a
  /// more complex benchmark that might issue blocking system calls.
  Benchmark(const std::string& name, bool micro_heuristics = true);

  /// Function to benchmark.  The function should run iters time (to minimize function
  /// call overhead).  The second argument is opaque and is whatever data the test
  /// function needs to execute.
  typedef void (*BenchmarkFunction)(int iters, void*);
  /// SetupFunction that might be required to be executed before every iteration of
  /// 'BenchmarkFunction'. This function is not measured.
  typedef void (*BenchmarkSetupFunction)(void*);

  /// Add a benchmark with 'name' to the suite.  The first benchmark is assumed to
  /// be the baseline.  Reporting will be done relative to that.
  /// Returns a unique index for this benchmark.
  /// baseline_idx is the base function to compare this one against.
  /// Specify -1 to not have a baseline.
  int AddBenchmark(const std::string& name, BenchmarkFunction fn, void* args,
      int baseline_idx = 0);

  /// Runs all the benchmarks and returns the result in a formatted string.
  /// max_time is the total time to benchmark the function, in ms.
  /// initial_batch_size is the initial batch size to the run the function.  The
  /// harness function will automatically ramp up the batch_size.  The benchmark
  /// will take *at least* initial_batch_size * function invocation time.
  /// 'fn' is the setup function to be run before every iteration if provided and
  /// it will not be measured.
  std::string Measure(
      int max_time = 50, int initial_batch_size = 10, BenchmarkSetupFunction fn = NULL);

  /// Output machine/build configuration as a string
  static std::string GetMachineInfo();

 private:
  friend class BenchmarkTest;

  /// Benchmarks the 'function' returning the result as invocations per ms.
  /// args is an opaque argument passed as the second argument to the function.
  static double Measure(BenchmarkFunction function, void* args, int max_time,
      int initial_batch_size, bool micro);

  struct BenchmarkResult {
    std::string name;
    BenchmarkFunction fn;
    void* args;
    std::vector<double> rates;
    int baseline_idx;
  };

  std::string name_;
  std::vector<BenchmarkResult> benchmarks_;
  bool micro_heuristics_;
};

}

#endif
