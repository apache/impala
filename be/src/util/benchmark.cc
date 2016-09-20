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

#include <iomanip>
#include <iostream>
#include <sstream>

#include "util/benchmark.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/stopwatch.h"

#include "common/names.h"

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
    batch_size = max<int>(1, (iters_guess - iters) / 5);
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

Benchmark::Benchmark(const string& name) : name_(name) {
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

string Benchmark::Measure(int max_time, int initial_batch_size) {
  if (benchmarks_.empty()) return "";

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
  for (int j = 0; j < NUM_REPS; ++j) {
    for (int i = 0; i < benchmarks_.size(); ++i) {
      benchmarks_[i].rates.push_back(
          Measure(benchmarks_[i].fn, benchmarks_[i].args, max_time, initial_batch_size));
    }
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
