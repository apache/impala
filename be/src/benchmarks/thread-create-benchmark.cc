// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/stopwatch.h"

#include <boost/thread/thread.hpp>

using namespace boost;
using namespace impala;
using namespace std;

// Benchmark for thread creation time using the ParallelExecutor.  The total time
// also includes thread shutdown and object creation overhead.
//   Time to start up 1 threads: 0
//   Time to start up 5 threads: 0
//   Time to start up 50 threads: 0
//   Time to start up 500 threads: 5ms
//   Total time: 11ms

void EmptyThread() {
}

void TimeParallelExecutorStartup(int num_threads) {
  StopWatch sw;
  sw.Start();
  thread_group threads;
  for (int i = 0; i < num_threads; ++i) {
    threads.add_thread(new thread(EmptyThread));
  }
  threads.join_all();
  sw.Stop();
  cout << "Time to start up " << num_threads << " threads: " 
        << PrettyPrinter::Print(sw.Ticks(), TCounterType::CPU_TICKS) << endl;
}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  CpuInfo::Init();

  // Measure how long it takes to start up a bunch of threads
  StopWatch total_time;
  total_time.Start();

  TimeParallelExecutorStartup(1);
  TimeParallelExecutorStartup(5);
  TimeParallelExecutorStartup(50);
  TimeParallelExecutorStartup(500);
  total_time.Stop();

  cout << "Total time: "
        << PrettyPrinter::Print(total_time.Ticks(), TCounterType::CPU_TICKS) << endl;

  return 0;
}
