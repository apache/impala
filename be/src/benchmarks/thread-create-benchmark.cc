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
        << PrettyPrinter::Print(sw.ElapsedTime(), TCounterType::CPU_TICKS) << endl;
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
        << PrettyPrinter::Print(total_time.ElapsedTime(), TCounterType::CPU_TICKS) 
        << endl;

  return 0;
}
