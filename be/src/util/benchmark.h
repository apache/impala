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


#ifndef IMPALA_UTIL_BENCHMARK_H
#define IMPALA_UTIL_BENCHMARK_H

#include <string>
#include <vector>

namespace impala {

// Utility class for microbenchmarks.
// This can be utilized to create a benchmark suite.  For example:
//  Benchmark suite("benchmark");
//  suite.AddBenchmark("Implementation #1", Implementation1Fn, data);
//  suite.AddBenchmark("Implementation #2", Implementation2Fn, data);
//  ...
//  string result = suite.Measure();
class Benchmark {
 public:
  // Name of the microbenchmark.  This is outputted in the result.  
  Benchmark(const std::string& name);

  // Function to benchmark.  The function should run iters time (to minimize function
  // call overhead).  The second argument is opaque and is whatever data the test 
  // function needs to execute.
  typedef void (*BenchmarkFunction)(int iters, void*);

  // Add a benchmark with 'name' to the suite.  The first benchmark is assumed to
  // be the baseline.  Reporting will be done relative to that.
  // Returns a unique index for this benchmark.
  // baseline_idx is the base function to compare this one against.
  // Specify -1 to not have a baseline.
  int AddBenchmark(const std::string& name, BenchmarkFunction fn, void* args,
      int baseline_idx = 0);

  // Runs all the benchmarks and returns the result in a formatted string.
  std::string Measure();

  // Output machine/build configuration as a string
  static std::string GetMachineInfo();

 private:
  friend class BenchmarkTest;

  // Benchmarks the 'function' returning the result as invocations per ms.
  // args is an opaque argument passed as the second argument to the function.
  // max_time is the total time to benchmark the function, in ms.
  // initial_batch_size is the initial batch size to the run the function.  The
  // harness function will automatically ramp up the batch_size.  The benchmark
  // will take *at least* initial_batch_size * function invocation time.
  static double Measure(BenchmarkFunction function, void* args,
      int max_time = 1000, int initial_batch_size = 1000);

  struct BenchmarkResult {
    std::string name;
    BenchmarkFunction fn; 
    void* args;
    double rate;
    int baseline_idx;
  };

  std::string name_;
  std::vector<BenchmarkResult> benchmarks_;
};
  
}

#endif
