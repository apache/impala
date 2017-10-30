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

#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/pretty-printer.h"
#include "util/thread.h"
#include "util/stopwatch.h"

#include <boost/thread/thread.hpp>
#include <boost/bind.hpp>

#include "common/names.h"

using namespace impala;

// Benchmark for thread creation time using native threads and
// Impala's Thread class.

// -----------------Benchmark 1: Single-threaded thread creation
// (Native):Time to start up 1 * 1 = 1 threads: 1136K clock cycles
// (Native):Time to start up 1 * 5 = 5 threads: 918K clock cycles
// (Native):Time to start up 1 * 50 = 50 threads: 4ms
// (Native):Time to start up 1 * 500 = 500 threads: 37ms
// (Native):Time to start up 1 * 5000 = 5000 threads: 237ms
// Total time (Native): 280ms

// (Impala):Time to start up 1 * 1 = 1 threads: 861K clock cycles
// (Impala):Time to start up 1 * 5 = 5 threads: 936K clock cycles
// (Impala):Time to start up 1 * 50 = 50 threads: 7ms
// (Impala):Time to start up 1 * 500 = 500 threads: 31ms
// (Impala):Time to start up 1 * 5000 = 5000 threads: 461ms
// Total time (IMPALA): 502ms

//  Impala thread overhead: 221ms, which is 78.9033%

//  -----------------Benchmark 2: Multi-threaded thread creation
// (Native):Time to start up 20 * 1 = 20 threads: 2ms
// (Native):Time to start up 20 * 5 = 100 threads: 28ms
// (Native):Time to start up 20 * 50 = 1000 threads: 89ms
// (Native):Time to start up 20 * 500 = 10000 threads: 977ms
// Total time (Native): 1s098ms

// (Impala):Time to start up 20 * 1 = 20 threads: 3ms
// (Impala):Time to start up 20 * 5 = 100 threads: 7ms
// (Impala):Time to start up 20 * 50 = 1000 threads: 97ms
// (Impala):Time to start up 20 * 500 = 10000 threads: 1s088ms
// Total time (IMPALA): 1s196ms

//  Impala thread overhead: 98ms, which is 8.94135%

// The difference between Impala and native thread creation throughput is explained almost
// entirely by Impala thread creation blocking until the thread ID is available returning
// (hence the difference is less marked in the multi-threaded creation case where another
// creation thread is usually available to do work). See Thread.StartThread() for more
// details. Without blocking, thread creation benchmark times are always within ~5% of
// each other.

void EmptyThread() {
}

// Runs N native threads, each executing 'f'
void NativeThreadStarter(int num_threads, const function<void ()>& f) {
  thread_group threads;
  for (int i = 0; i < num_threads; ++i) {
    threads.add_thread(new thread(f));
  }
  threads.join_all();
}

// Runs N Impala Threads, each executing 'f'
void ImpalaThreadStarter(int num_threads, const function<void ()>& f) {
  vector<unique_ptr<Thread>> threads;
  threads.reserve(num_threads);
  for (int i=0; i < num_threads; ++i) {
    unique_ptr<Thread> thread;
    Status s = Thread::Create("mythreadgroup", "thread", f, &thread);
    DCHECK(s.ok());
    threads.push_back(move(thread));
  }
  for (unique_ptr<Thread>& thread: threads) {
    thread->Join();
  }
}

// Times how long it takes to run num_threads 'executors', each of
// which spawns num_threads_per_executor empty threads, and to wait
// for all of them to finish.
void TimeParallelExecutors(int num_threads, int num_threads_per_executor,
    bool use_native_threads = true) {
  StopWatch sw;
  sw.Start();
  if (use_native_threads) {
    function<void ()> f =
        bind(NativeThreadStarter, num_threads_per_executor, EmptyThread);
    NativeThreadStarter(num_threads, f);
  } else {
    function<void ()> f =
        bind(ImpalaThreadStarter, num_threads_per_executor, EmptyThread);
    ImpalaThreadStarter(num_threads, f);
  }
  sw.Stop();
  cout << (use_native_threads ? "(Native):" : "(Impala):")
       << "Time to start up " << num_threads << " * " << num_threads_per_executor << " = "
       << num_threads * num_threads_per_executor << " threads: "
       << PrettyPrinter::Print(sw.ElapsedTime(), TUnit::CPU_TICKS) << endl;
}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  CpuInfo::Init();
  impala::InitThreading();

  cout << "-----------------Benchmark 1: Single-threaded thread creation" << endl;

  // Measure how long it takes to start up a bunch of threads
  StopWatch total_time;
  total_time.Start();

  TimeParallelExecutors(1, 1);
  TimeParallelExecutors(1, 5);
  TimeParallelExecutors(1, 50);
  TimeParallelExecutors(1, 500);
  TimeParallelExecutors(1, 5000);

  total_time.Stop();

  cout << "Total time (Native): "
       << PrettyPrinter::Print(total_time.ElapsedTime(), TUnit::CPU_TICKS)
       << endl << endl;

  // Measure how long it takes to start up a bunch of threads
  StopWatch total_time_imp;
  total_time_imp.Start();

  TimeParallelExecutors(1, 1, false);
  TimeParallelExecutors(1, 5, false);
  TimeParallelExecutors(1, 50, false);
  TimeParallelExecutors(1, 500, false);
  TimeParallelExecutors(1, 5000, false);

  total_time_imp.Stop();

  cout << "Total time (IMPALA): "
       << PrettyPrinter::Print(total_time_imp.ElapsedTime(), TUnit::CPU_TICKS)
       << endl << endl;

  int64_t difference = total_time_imp.ElapsedTime() - total_time.ElapsedTime();
  cout << "Impala thread overhead: "
       << PrettyPrinter::Print(difference, TUnit::CPU_TICKS)
       << ", which is " << (difference * 100.0 / total_time.ElapsedTime())
       << "%" << endl << endl;


  cout << "-----------------Benchmark 2: Multi-threaded thread creation" << endl;

  // Measure how long it takes to start up a bunch of threads
  StopWatch total_time_parallel_native;
  total_time_parallel_native.Start();

  TimeParallelExecutors(20, 1);
  TimeParallelExecutors(20, 5);
  TimeParallelExecutors(20, 50);
  TimeParallelExecutors(20, 500);

  total_time_parallel_native.Stop();

  cout << "Total time (Native): "
       << PrettyPrinter::Print(total_time_parallel_native.ElapsedTime(),
                               TUnit::CPU_TICKS)
       << endl << endl;

  // Measure how long it takes to start up a bunch of threads
  StopWatch total_time_parallel_impala;
  total_time_parallel_impala.Start();

  TimeParallelExecutors(20, 1, false);
  TimeParallelExecutors(20, 5, false);
  TimeParallelExecutors(20, 50, false);
  TimeParallelExecutors(20, 500, false);

  total_time_parallel_impala.Stop();

  cout << "Total time (IMPALA): "
       << PrettyPrinter::Print(total_time_parallel_impala.ElapsedTime(),
                               TUnit::CPU_TICKS)
       << endl;

  difference = total_time_parallel_impala.ElapsedTime()
      - total_time_parallel_native.ElapsedTime() ;
  cout << "Impala thread overhead: "
       << PrettyPrinter::Print(difference, TUnit::CPU_TICKS)
       << ", which is " << (difference * 100.0 / total_time_parallel_native.ElapsedTime())
       << "%" << endl;

  return 0;
}
