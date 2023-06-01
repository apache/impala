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

#include "util/histogram-metric.h"

#include "kudu/util/hdr_histogram.h"
#include "util/json-util.h"
#include "util/pretty-printer.h"

#include "common/names.h"

namespace impala {

HistogramMetric::HistogramMetric(
    const TMetricDef& def, uint64_t highest_trackable_value, int num_significant_digits)
  : Metric(def),
    histogram_(new HdrHistogram(highest_trackable_value, num_significant_digits)),
    unit_(def.units) {
  DCHECK_EQ(TMetricKind::HISTOGRAM, def.kind);
}

void HistogramMetric::Reset() {
  lock_guard<SpinLock> l(lock_);
  uint64_t highest = histogram_->highest_trackable_value();
  int digits = histogram_->num_significant_digits();
  histogram_.reset(new HdrHistogram(highest, digits));
}

void HistogramMetric::ToJson(rapidjson::Document* document, rapidjson::Value* value) {
  rapidjson::Value container(rapidjson::kObjectType);
  AddStandardFields(document, &container);

  {
    lock_guard<SpinLock> l(lock_);

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
  rapidjson::Value type_value(
      PrintValue(TMetricKind::HISTOGRAM).c_str(), document->GetAllocator());
  container.AddMember("kind", type_value, document->GetAllocator());
  rapidjson::Value units(PrintValue(unit()).c_str(), document->GetAllocator());
  container.AddMember("units", units, document->GetAllocator());

  *value = container;
}

TMetricKind::type HistogramMetric::ToPrometheus(
    string name, stringstream* value, stringstream* metric_kind) {
  {
    lock_guard<SpinLock> l(lock_);

    // check if unit its 'TIME_MS','TIME_US' or 'TIME_NS' and convert it to seconds,
    // this is because prometheus only supports time format in seconds
    if (IsUnitTimeBased(unit_)) {
      *value << name << "{quantile=\"0.2\"} "
             << ConvertToPrometheusSecs(histogram_->ValueAtPercentile(25), unit_) << "\n";
    } else {
      *value << name << "{quantile=\"0.2\"} " << histogram_->ValueAtPercentile(25)
             << "\n";
    }

    if (IsUnitTimeBased(unit_)) {
      *value << name << "{quantile=\"0.5\"} "
             << ConvertToPrometheusSecs(histogram_->ValueAtPercentile(50), unit_) << "\n";
    } else {
      *value << name << "{quantile=\"0.5\"} " << histogram_->ValueAtPercentile(50)
             << "\n";
    }

    if (IsUnitTimeBased(unit_)) {
      *value << name << "{quantile=\"0.7\"} "
             << ConvertToPrometheusSecs(histogram_->ValueAtPercentile(75), unit_) << "\n";
    } else {
      *value << name << "{quantile=\"0.7\"} " << histogram_->ValueAtPercentile(75)
             << "\n";
    }

    if (IsUnitTimeBased(unit_)) {
      *value << name << "{quantile=\"0.9\"} "
             << ConvertToPrometheusSecs(histogram_->ValueAtPercentile(90), unit_) << "\n";
    } else {
      *value << name << "{quantile=\"0.9\"} " << histogram_->ValueAtPercentile(90)
             << "\n";
    }

    if (IsUnitTimeBased(unit_)) {
      *value << name << "{quantile=\"0.95\"} "
             << ConvertToPrometheusSecs(histogram_->ValueAtPercentile(95), unit_) << "\n";
    } else {
      *value << name << "{quantile=\"0.95\"} " << histogram_->ValueAtPercentile(95)
             << "\n";
    }

    if (IsUnitTimeBased(unit_)) {
      *value << name << "{quantile=\"0.999\"} "
             << ConvertToPrometheusSecs(histogram_->ValueAtPercentile(99.9), unit_)
             << "\n";
    } else {
      *value << name << "{quantile=\"0.999\"} " << histogram_->ValueAtPercentile(99.9)
             << "\n";
    }

    *value << name << "_count " << histogram_->TotalCount() << "\n";

    if (IsUnitTimeBased(unit_)) {
      *value << name << "_sum " << ConvertToPrometheusSecs(histogram_->TotalSum(), unit_);
    } else {
      *value << name << "_sum " << histogram_->TotalSum();
    }
  }

  *metric_kind << "# TYPE " << name << " summary";
  return TMetricKind::HISTOGRAM;
}

string HistogramMetric::ToHumanReadable() {
  lock_guard<SpinLock> l(lock_);
  return HistogramToHumanReadable(histogram_.get(), unit_);
}

template <class T>
string HistogramMetric::HistogramToHumanReadable(T* histogram, TUnit::type unit) {
  DCHECK(histogram != nullptr);
  stringstream out;
  out << "Count: " << histogram->TotalCount() << ", "
      << "sum: " << PrettyPrinter::Print(histogram->TotalSum(), unit) << ", "
      << "min / max: " << PrettyPrinter::Print(histogram->MinValue(), unit) << " / "
      << PrettyPrinter::Print(histogram->MaxValue(), unit) << ", "
      << "25th %-ile: " << PrettyPrinter::Print(histogram->ValueAtPercentile(25), unit)
      << ", "
      << "50th %-ile: " << PrettyPrinter::Print(histogram->ValueAtPercentile(50), unit)
      << ", "
      << "75th %-ile: " << PrettyPrinter::Print(histogram->ValueAtPercentile(75), unit)
      << ", "
      << "90th %-ile: " << PrettyPrinter::Print(histogram->ValueAtPercentile(90), unit)
      << ", "
      << "95th %-ile: " << PrettyPrinter::Print(histogram->ValueAtPercentile(95), unit)
      << ", "
      << "99.9th %-ile: "
      << PrettyPrinter::Print(histogram->ValueAtPercentile(99.9), unit);
  return out.str();
}

// Instantiate the template for both Impala and Kudu histograms.
template string HistogramMetric::HistogramToHumanReadable<>(HdrHistogram*, TUnit::type);
template string HistogramMetric::HistogramToHumanReadable<>(
    kudu::HdrHistogram*, TUnit::type);
} // namespace impala
