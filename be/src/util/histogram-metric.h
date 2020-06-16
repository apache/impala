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

#pragma once

#include "util/hdr-histogram.h"
#include "util/metrics.h"
#include "util/spinlock.h"
#include "util/stopwatch.h"

namespace impala {

class HdrHistogram;

/// Metric which constructs (using HdrHistogram) a histogram of a set of values.
/// Thread-safe: histogram access protected by a spin lock.
class HistogramMetric : public Metric {
 public:
  /// Constructs a new histogram metric. `highest_trackable_value` is the maximum value
  /// that may be entered into the histogram. `num_significant_digits` is the precision
  /// that values must be stored with.
  HistogramMetric(const TMetricDef& def, uint64_t highest_trackable_value,
      int num_significant_digits);

  virtual void ToJson(rapidjson::Document* document, rapidjson::Value* value) override;

  virtual TMetricKind::type ToPrometheus(std::string name, std::stringstream* value,
      std::stringstream* metric_kind) override;

  void Update(int64_t val) {
    std::lock_guard<SpinLock> l(lock_);
    histogram_->Increment(val);
  }

  uint64_t MinValue() const { return histogram_->MinValue(); }
  uint64_t MaxValue() const { return histogram_->MaxValue(); }
  uint64_t TotalCount() const { return histogram_->TotalCount(); }
  uint64_t TotalSum() const { return histogram_->TotalSum(); }

  /// Reset the histogram by removing all previous entries.
  void Reset();

  virtual void ToLegacyJson(rapidjson::Document*) override {}

  const TUnit::type& unit() const { return unit_; }

  virtual std::string ToHumanReadable() override;

  /// Render a HdrHistogram into a human readable string representation. The histogram
  /// type is a template parameter so that it accepts both Impala's and Kudu's
  /// HdrHistogram classes.
  template <class T>
  static std::string HistogramToHumanReadable(T* histogram, TUnit::type unit);

 private:
  /// Protects histogram_ pointer itself.
  SpinLock lock_;
  boost::scoped_ptr<HdrHistogram> histogram_;
  const TUnit::type unit_;

  DISALLOW_COPY_AND_ASSIGN(HistogramMetric);
};

// Utility class to update histogram with elapsed time in code block.
class ScopedHistogramTimer {
 public:
  ScopedHistogramTimer(HistogramMetric* metric) : metric_(metric) { sw_.Start(); }

  ~ScopedHistogramTimer() { metric_->Update(sw_.ElapsedTime()); }

 private:
  HistogramMetric* const metric_;
  MonotonicStopWatch sw_;
};
} // namespace impala
