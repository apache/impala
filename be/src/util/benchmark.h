// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

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
