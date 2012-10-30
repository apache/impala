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

namespace impala {

// Static utility class for microbenchmarks.
class Benchmark {
 public:
  // Function to benchmark.  The function should run iters time (to minimize function
  // call overhead).  The second argument is opaque and is whatever data the test 
  // function needs to execute.
  typedef void (*BenchmarkFunction)(int iters, void*);

  // Benchmarks the 'function' returning the result as invocations per ms.
  // args is an opaque argument passed as the second argument to the function.
  // max_time is the total time to benchmark the function, in ms.
  // initial_batch_size is the initial batch size to the run the function.  The
  // harness function will automatically ramp up the batch_size.  The benchmark
  // will take *at least* initial_batch_size * function invocation time.
  static double Measure(BenchmarkFunction function, void* args,
      int max_time = 1000, int initial_batch_size = 1000);
};
  
}

#endif
