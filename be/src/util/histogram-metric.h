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

  virtual void ToJson(rapidjson::Document* document, rapidjson::Value* value) override {
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

  virtual void ToLegacyJson(rapidjson::Document*) override {}

  const TUnit::type& unit() const { return unit_; }

  virtual std::string ToHumanReadable() override {
    boost::lock_guard<SpinLock> l(lock_);
    return HistogramToHumanReadable(histogram_.get(), unit_);
  }

  /// Render a HdrHistogram into a human readable string representation. The histogram
  /// type is a template parameter so that it accepts both Impala's and Kudu's
  /// HdrHistogram classes.
  template <class T>
  static std::string HistogramToHumanReadable(T* histogram, TUnit::type unit) {
    DCHECK(histogram != nullptr);
    std::stringstream out;
    out << "Count: " << histogram->TotalCount() << ", "
        << "min / max: " << PrettyPrinter::Print(histogram->MinValue(), unit)
        << " / " << PrettyPrinter::Print(histogram->MaxValue(), unit) << ", "
        << "25th %-ile: "
        << PrettyPrinter::Print(histogram->ValueAtPercentile(25), unit) << ", "
        << "50th %-ile: "
        << PrettyPrinter::Print(histogram->ValueAtPercentile(50), unit) << ", "
        << "75th %-ile: "
        << PrettyPrinter::Print(histogram->ValueAtPercentile(75), unit) << ", "
        << "90th %-ile: "
        << PrettyPrinter::Print(histogram->ValueAtPercentile(90), unit) << ", "
        << "95th %-ile: "
        << PrettyPrinter::Print(histogram->ValueAtPercentile(95), unit) << ", "
        << "99.9th %-ile: "
        << PrettyPrinter::Print(histogram->ValueAtPercentile(99.9), unit);
    return out.str();
  }

 private:
  /// Protects histogram_ pointer itself.
  SpinLock lock_;
  boost::scoped_ptr<HdrHistogram> histogram_;
  const TUnit::type unit_;

  DISALLOW_COPY_AND_ASSIGN(HistogramMetric);
};

}

#endif
