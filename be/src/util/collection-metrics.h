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

#include <cstdint>
#include <iosfwd>
#include <set>
#include <string>
#include <vector>

#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/framework/accumulator_set.hpp>
#include <boost/accumulators/framework/features.hpp>
#include <boost/accumulators/statistics/count.hpp>
#include <boost/accumulators/statistics/max.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/min.hpp>
#include <boost/accumulators/statistics/variance.hpp>
#include <boost/thread/lock_guard.hpp>

#include "common/logging.h"
#include "util/metrics-fwd.h"
#include "util/metrics.h"

namespace impala {

/// Collection metrics are those whose values have more structure than simple
/// scalar types. Therefore they need specialised ToJson() methods, and
/// typically a specialised API for updating the values they contain.

/// Metric whose value is a set of items
template <typename T>
class SetMetric : public Metric {
 public:
  static SetMetric* CreateAndRegister(MetricGroup* metrics, const std::string& key,
      const std::set<T>& value) {
    return metrics->RegisterMetric(new SetMetric(MetricDefs::Get(key), value));
  }

  SetMetric(const TMetricDef& def, const std::set<T>& value)
    : Metric(def), value_(value) {
    DCHECK_EQ(def.kind, TMetricKind::SET);
  }

  /// Put an item in this set.
  void Add(const T& item) {
    std::lock_guard<std::mutex> l(lock_);
    value_.insert(item);
  }

  /// Remove an item from this set by value.
  void Remove(const T& item) {
    std::lock_guard<std::mutex> l(lock_);
    value_.erase(item);
  }

  /// Copy out value.
  std::set<T> value() {
    std::lock_guard<std::mutex> l(lock_);
    return value_;
  }

  void Reset() { value_.clear(); }

  virtual TMetricKind::type ToPrometheus(
      std::string name, std::stringstream* val, std::stringstream* metric_kind) override {
    // this is not supported type in prometheus, so ignore
    return TMetricKind::SET;
  }

  virtual void ToJson(rapidjson::Document* document, rapidjson::Value* value) override;

  virtual void ToLegacyJson(rapidjson::Document* document) override;

  virtual std::string ToHumanReadable() override;

 private:
  /// Lock protecting the set
  std::mutex lock_;

  /// The set of items
  std::set<T> value_;
};

/// Metric which accumulates min, max and mean of all values, plus a count of samples
/// seen. The output can be controlled by passing a bitmask as a template parameter to
/// indicate which values should be printed or returned as JSON.
///
/// Printed output looks like: name: count:
/// 4, last: 0.0141, min: 4.546e-06, max: 0.0243, mean: 0.0336, stddev: 0.0336
///
/// After construction, all statistics are ill-defined, but count will be 0. The first call
/// to Update() will initialise all stats.
template <typename T, int StatsSelection>
class StatsMetric : public Metric {
 public:
  static StatsMetric* CreateAndRegister(MetricGroup* metrics, const std::string& key,
      const std::string& arg = "") {
    return metrics->RegisterMetric(new StatsMetric(MetricDefs::Get(key, arg)));
  }

  StatsMetric(const TMetricDef& def) : Metric(def), unit_(def.units) {
    DCHECK_EQ(def.kind, TMetricKind::STATS);
  }

  void Update(const T& value) {
    std::lock_guard<std::mutex> l(lock_);
    value_ = value;
    acc_(value);
  }

  void Reset() {
    std::lock_guard<std::mutex> l(lock_);
    acc_ = Accumulator();
  }

  virtual TMetricKind::type ToPrometheus(
      std::string name, std::stringstream* val, std::stringstream* metric_kind) override;

  virtual void ToJson(rapidjson::Document* document, rapidjson::Value* val) override;

  virtual void ToLegacyJson(rapidjson::Document* document) override;

  virtual std::string ToHumanReadable() override;

 private:
  /// The units of the values captured in this metric, used when pretty-printing.
  TUnit::type unit_;

  /// Lock protecting the value and the accumulator_set
  std::mutex lock_;

  /// The last value
  T value_;

  /// The set of accumulators that update the statistics on each Update()
  typedef boost::accumulators::accumulator_set<T,
      boost::accumulators::features<boost::accumulators::tag::mean,
                                    boost::accumulators::tag::count,
                                    boost::accumulators::tag::min,
                                    boost::accumulators::tag::max,
                                    boost::accumulators::tag::variance>> Accumulator;
  Accumulator acc_;

};

// These template classes are instantiated in the .cc file.
extern template class SetMetric<int32_t>;
extern template class SetMetric<string>;
extern template class StatsMetric<uint64_t, StatsType::ALL>;
extern template class StatsMetric<double, StatsType::ALL>;
extern template class StatsMetric<uint64_t, StatsType::MEAN>;
extern template class StatsMetric<uint64_t, StatsType::MAX | StatsType::MEAN>;
}
