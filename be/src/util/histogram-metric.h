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

#ifndef IMPALA_UTIL_HISTOGRAM_METRIC
#define IMPALA_UTIL_HISTOGRAM_METRIC

#include "util/hdr-histogram.h"
#include "util/metrics.h"
#include "util/spinlock.h"

namespace impala {

/// Metric which constructs (using HdrHistogram) a histogram of a set of values.
/// Thread-safe: histogram access protected by a spin lock.
class HistogramMetric : public Metric {
 public:
  /// Constructs a new histogram metric. `highest_trackable_value` is the maximum value
  /// that may be entered into the histogram. `num_significant_digits` is the precision
  /// that values must be stored with.
  HistogramMetric(
      const TMetricDef& def, uint64_t highest_trackable_value, int num_significant_digits)
    : Metric(def),
      histogram_(new HdrHistogram(highest_trackable_value, num_significant_digits)),
      unit_(def.units) {
    DCHECK_EQ(TMetricKind::HISTOGRAM, def.kind);
  }

  virtual void ToJson(rapidjson::Document* document, rapidjson::Value* value) {
    rapidjson::Value container(rapidjson::kObjectType);
    AddStandardFields(document, &container);

    {
      boost::lock_guard<SpinLock> l(lock_);

      container.AddMember(
          "25th %-ile", histogram_->ValueAtPercentile(25), document->GetAllocator());
      container.AddMember(
          "50th %-ile", histogram_->ValueAtPercentile(50), document->GetAllocator());
      container.AddMember(
          "75th %-ile", histogram_->ValueAtPercentile(75), document->GetAllocator());
      container.AddMember(
          "90th %-ile", histogram_->ValueAtPercentile(90), document->GetAllocator());
      container.AddMember(
          "95th %-ile", histogram_->ValueAtPercentile(95), document->GetAllocator());
      container.AddMember(
          "99.9th %-ile", histogram_->ValueAtPercentile(99.9), document->GetAllocator());
      container.AddMember("max", histogram_->MaxValue(), document->GetAllocator());
      container.AddMember("min", histogram_->MinValue(), document->GetAllocator());
      container.AddMember("count", histogram_->TotalCount(), document->GetAllocator());
    }
    rapidjson::Value type_value(PrintTMetricKind(TMetricKind::HISTOGRAM).c_str(),
        document->GetAllocator());
    container.AddMember("kind", type_value, document->GetAllocator());
    rapidjson::Value units(PrintTUnit(unit()).c_str(), document->GetAllocator());
    container.AddMember("units", units, document->GetAllocator());

    *value = container;
  }

  void Update(int64_t val) {
    boost::lock_guard<SpinLock> l(lock_);
    histogram_->Increment(val);
  }

  /// Reset the histogram by removing all previous entries.
  void Reset() {
    boost::lock_guard<SpinLock> l(lock_);
    uint64_t highest = histogram_->highest_trackable_value();
    int digits = histogram_->num_significant_digits();
    histogram_.reset(new HdrHistogram(highest, digits));
  }

  virtual void ToLegacyJson(rapidjson::Document*) { }

  const TUnit::type& unit() const { return unit_; }

  virtual std::string ToHumanReadable() {
    boost::lock_guard<SpinLock> l(lock_);
    std::stringstream out;
    out << "Count: " << histogram_->TotalCount() << ", "
        << "min / max: " << PrettyPrinter::Print(histogram_->MinValue(), unit_)
        << " / " << PrettyPrinter::Print(histogram_->MaxValue(), unit_) << ", "
        << "25th %-ile: "
        << PrettyPrinter::Print(histogram_->ValueAtPercentile(25), unit_) << ", "
        << "50th %-ile: "
        << PrettyPrinter::Print(histogram_->ValueAtPercentile(50), unit_) << ", "
        << "75th %-ile: "
        << PrettyPrinter::Print(histogram_->ValueAtPercentile(75), unit_) << ", "
        << "90th %-ile: "
        << PrettyPrinter::Print(histogram_->ValueAtPercentile(90), unit_) << ", "
        << "95th %-ile: "
        << PrettyPrinter::Print(histogram_->ValueAtPercentile(95), unit_) << ", "
        << "99.9th %-ile: "
        << PrettyPrinter::Print(histogram_->ValueAtPercentile(99.9), unit_);
    return out.str();
  }

 private:
  // Protects histogram_ pointer itself.
  SpinLock lock_;
  boost::scoped_ptr<HdrHistogram> histogram_;
  const TUnit::type unit_;
};

}

#endif
