// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include <iostream>

#include "util/benchmark.h"
#include "util/cpu-info.h"
#include "util/stopwatch.h"

using namespace std;

namespace impala {
  
double Benchmark::Measure(BenchmarkFunction function, void* args,
    int max_time, int batch_size) {
  int64_t target_cycles = CpuInfo::cycles_per_ms() * max_time;
  int64_t iters = 0;
  
  // Run it with the default batch size to roughly estimate how many iterations
  // it will take
  StopWatch sw;
  sw.Start();
  function(batch_size, args);
  sw.Stop();
  iters += batch_size;

  if (sw.ElapsedTime() < target_cycles) {
    int64_t iters_guess = (target_cycles / sw.ElapsedTime()) * batch_size;
    // Shoot for 110% of the guess. Going a little over is not a big deal.
    iters_guess *= 1.1;
    // Modify the batch size based on the guess.  We ran the function a small number
    // of times to estimate how fast the function is.  Run the remaining iterations at
    // in 20% increments.
    // TODO: we can make this more sophisticated if need to be dynamically ramp up and
    // ramp down the sizes.
    batch_size = (iters_guess - iters) / 5;
  }

  while (sw.ElapsedTime() < target_cycles) {
    sw.Start();
    function(batch_size, args);
    sw.Stop();
    iters += batch_size;
  }

  double ms_elapsed = sw.ElapsedTime() / CpuInfo::cycles_per_ms();
  return iters / ms_elapsed;
}

}
