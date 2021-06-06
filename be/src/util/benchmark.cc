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

#include <cmath>
#include <iomanip>
#include <iostream>
#include <sstream>

#include <sys/time.h>
#include <sys/resource.h>
#include <unistd.h>

#include "util/benchmark.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/stopwatch.h"

#include "common/names.h"

namespace impala {

// Private measurement function.  This function is a bit unusual in that it
// throws exceptions; the intention is to abort the measurement when an unreliable
// result is detected, but to also provide some useful context as to which benchmark
// was being run.  The method used to determine whether the measurement is unreliable
// is detecting context switches using getrusage().  Pass micro_heuristics=false to
// the class constructor if you do not want this behavior.
double Benchmark::Measure(BenchmarkFunction function, void* args,
    int max_time, int batch_size, bool micro_heuristics) {
  int64_t target_cycles = CpuInfo::cycles_per_ms() * max_time;
  int64_t iters = 0;
  struct rusage ru_start;
  struct rusage ru_stop;
  StopWatch sw;
  int64_t lost_time = 0;

  // Run it with the default batch size to roughly estimate how many iterations
  // it will take.
  for (;;) {
    int64 begin_time = sw.ElapsedTime();
    if (micro_heuristics) getrusage(RUSAGE_THREAD, &ru_start);
    sw.Start();
    function(batch_size, args);
    sw.Stop();
    if (micro_heuristics) getrusage(RUSAGE_THREAD, &ru_stop);
    if (!micro_heuristics || ru_stop.ru_nivcsw == ru_start.ru_nivcsw) {
      iters = batch_size;
      break;
    }

    // Taking too long and we keep getting switched out; either the machine is busy,
    // or the benchmark takes too long to run and should not be used with this
    // microbenchmark suite.  Bail.
    if (sw.ElapsedTime() > target_cycles) {
      throw std::runtime_error("Benchmark failed to complete due to context switching.");
    }

    // Divide the batch size by the number of context switches until we find a size
    // small enough that we don't switch
    batch_size = max<int>(1, batch_size / (1+(ru_stop.ru_nivcsw - ru_start.ru_nivcsw)));
    lost_time += sw.ElapsedTime() - begin_time;
  }

  double iters_guess = (target_cycles / (sw.ElapsedTime() - lost_time)) * batch_size;
  // Shoot for 110% of the guess. Going a little over is not a big deal.
  iters_guess *= 1.1;
  // Modify the batch size based on the guess.  We ran the function a small number
  // of times to estimate how fast the function is.  Run the remaining iterations at
  // in 20% increments.
  batch_size = max<int>(1, (iters_guess - iters) / 5);

  while (sw.ElapsedTime() < target_cycles) {
    int64 begin_time = sw.ElapsedTime();
    if (micro_heuristics) getrusage(RUSAGE_THREAD, &ru_start);
    sw.Start();
    function(batch_size, args);
    sw.Stop();
    if (micro_heuristics) getrusage(RUSAGE_THREAD, &ru_stop);
    if (!micro_heuristics || ru_stop.ru_nivcsw == ru_start.ru_nivcsw) {
      iters += batch_size;
    } else {
      // We could have a vastly different estimate for batch size now and might have
      // started context switching again.  Divide down by 1 + the number of context
      // switches as a guess of the number of iterations to perform with each batch.
      lost_time += sw.ElapsedTime() - begin_time;
      batch_size = max<int>(1, batch_size / (1+(ru_stop.ru_nivcsw - ru_start.ru_nivcsw)));
    }
  }

  // Arbitrary fudge factor for throwing in the towel - we give up if > 90% of
  // measurements were dropped.
  if (lost_time > 10 * sw.ElapsedTime()) {
    throw std::runtime_error("Benchmark failed to complete due to noisy measurements.");
  }
  if (lost_time > sw.ElapsedTime()) {
    LOG(WARNING) << "More than 50% of benchmark time lost due to context switching.";
  }

  double ms_elapsed = (sw.ElapsedTime() - lost_time) / CpuInfo::cycles_per_ms();
  return iters / ms_elapsed;
}

Benchmark::Benchmark(const string& name, bool micro_heuristics) : name_(name),
  micro_heuristics_(micro_heuristics) {
#ifndef NDEBUG
  LOG(ERROR) << "WARNING: Running benchmark in DEBUG mode.";
#endif
  CpuInfo::VerifyPerformanceGovernor();
  CpuInfo::VerifyTurboDisabled();
}

int Benchmark::AddBenchmark(const string& name, BenchmarkFunction fn, void* args,
    int baseline_idx) {
  if (baseline_idx == -1) baseline_idx = benchmarks_.size();
  CHECK_LE(baseline_idx, benchmarks_.size());
  BenchmarkResult benchmark;
  benchmark.name = name;
  benchmark.fn = fn;
  benchmark.args = args;
  benchmark.baseline_idx = baseline_idx;
  benchmarks_.push_back(benchmark);
  return benchmarks_.size() - 1;
}

string Benchmark::Measure(
    int max_time, int initial_batch_size, BenchmarkSetupFunction fn) {
  if (benchmarks_.empty()) return "";

  if (fn != NULL) fn(benchmarks_[0].args);

  // Run a warmup to iterate through the data
  benchmarks_[0].fn(10, benchmarks_[0].args);

  // The number of times a benchmark is repeated
  const int NUM_REPS = 60;
  // Which percentiles of the benchmark to report. Reports the LO_PERCENT, MID_PERCENT,
  // and HI_PERCENT percentile result.
  const int LO_PERCENT = 10;
  const int MID_PERCENT = 50;
  const int HI_PERCENT = 100 - LO_PERCENT;
  const size_t LO_IDX =
      floor(((LO_PERCENT / 100.0) * static_cast<double>(NUM_REPS)) - 0.5);
  const size_t MID_IDX =
      floor(((MID_PERCENT / 100.0) * static_cast<double>(NUM_REPS)) - 0.5);
  const size_t HI_IDX =
      floor(((HI_PERCENT / 100.0) * static_cast<double>(NUM_REPS)) - 0.5);

  const int function_out_width = 35;
  const int rate_out_width = 10;
  const int percentile_out_width = 9;
  const int comparison_out_width = 11;
  const int padding = 0;
  const int total_width = function_out_width + rate_out_width + 3 * comparison_out_width +
      3 * percentile_out_width + padding;

  stringstream ss;
  try {
    for (int j = 0; j < NUM_REPS; ++j) {
      for (int i = 0; i < benchmarks_.size(); ++i) {
        if (fn != NULL) fn(benchmarks_[i].args);
        benchmarks_[i].rates.push_back(
            Measure(benchmarks_[i].fn, benchmarks_[i].args, max_time, initial_batch_size,
              micro_heuristics_));
      }
    }
  } catch (std::exception& e) {
    LOG(ERROR) << e.what();
    LOG(WARNING) << "Exiting silently from " << name_ <<
      " to avoid spurious test failure.";
    _exit(0);
  }

  ss << name_ << ":"
     << setw(function_out_width - name_.size() - 1) << "Function"
     << setw(rate_out_width) << "iters/ms"
     << setw(percentile_out_width -4) << LO_PERCENT << "%ile"
     << setw(percentile_out_width - 4) << MID_PERCENT << "%ile"
     << setw(percentile_out_width - 4) << HI_PERCENT << "%ile"
     << setw(comparison_out_width - 4) << LO_PERCENT << "%ile"
     << setw(comparison_out_width - 4) << MID_PERCENT << "%ile"
     << setw(comparison_out_width - 4) << HI_PERCENT << "%ile" << endl;
  ss << setw(function_out_width + rate_out_width + 3 * percentile_out_width +
            comparison_out_width) << "(relative)"
     << setw(comparison_out_width) << "(relative)"
     << setw(comparison_out_width) << "(relative)" << endl;
  for (int i = 0; i < total_width; ++i) {
    ss << '-';
  }
  ss << endl;

  int previous_baseline_idx = -1;
  for (int i = 0; i < benchmarks_.size(); ++i) {
    sort(benchmarks_[i].rates.begin(), benchmarks_[i].rates.end());
    const double base_line_lo = benchmarks_[benchmarks_[i].baseline_idx].rates[LO_IDX];
    const double base_line_mid = benchmarks_[benchmarks_[i].baseline_idx].rates[MID_IDX];
    const double base_line_hi = benchmarks_[benchmarks_[i].baseline_idx].rates[HI_IDX];
    if (previous_baseline_idx != benchmarks_[i].baseline_idx && i > 0) ss << endl;
    ss << setw(function_out_width) << benchmarks_[i].name
       << setw(rate_out_width + percentile_out_width) << setprecision(3)
           << benchmarks_[i].rates[LO_IDX]
       << setw(percentile_out_width) << setprecision(3)
           << benchmarks_[i].rates[MID_IDX]
       << setw(percentile_out_width) << setprecision(3)
           << benchmarks_[i].rates[HI_IDX]
       << setw(comparison_out_width - 1) << setprecision(3)
       << (benchmarks_[i].rates[LO_IDX] / base_line_lo) << "X"
       << setw(comparison_out_width - 1) << setprecision(3)
       << (benchmarks_[i].rates[MID_IDX] / base_line_mid) << "X"
       << setw(comparison_out_width - 1) << setprecision(3)
       << (benchmarks_[i].rates[HI_IDX] / base_line_hi) << "X" << endl;
    previous_baseline_idx = benchmarks_[i].baseline_idx;
  }

  return ss.str();
}

// TODO: maybe add other things like amount of RAM, etc
string Benchmark::GetMachineInfo() {
  stringstream ss;
  ss << "Machine Info: " << CpuInfo::model_name();
  return ss.str();
}

}
