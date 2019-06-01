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

// Forward definitions of metric-related classes. A lot of metric-related
// code is templatised and thus needs to be in headers, but in most headers
// that depend on metrics, only forward declarations of those classes are
// needed. This header should be included in those cases to reduce
// compilation time and dependencies.

#pragma once

#include "gen-cpp/MetricDefs_types.h"
#include "gen-cpp/MetricDefs_constants.h"

namespace impala {

class AtomicHighWaterMarkGauge;
class HistogramMetric;
class Metric;
class MetricGroup;
class NegatedGauge;
class SumGauge;

/// Enum to define which statistic types are available in the StatsMetric
struct StatsType {
  enum type {
    MIN = 1,
    MAX = 2,
    MEAN = 4,
    STDDEV = 8,
    COUNT = 16,
    ALL = 31
  };
};

template<typename T, TMetricKind::type metric_kind_t>
class ScalarMetric;
template<typename T, TMetricKind::type metric_kind_t>
class LockedMetric;
template<TMetricKind::type metric_kind_t>
class AtomicMetric;
template <typename T>
class SetMetric;
template <typename T, int StatsSelection=StatsType::ALL>
class StatsMetric;

typedef class LockedMetric<bool, TMetricKind::PROPERTY> BooleanProperty;
typedef class LockedMetric<std::string,TMetricKind::PROPERTY> StringProperty;
typedef class LockedMetric<double, TMetricKind::GAUGE> DoubleGauge;

/// We write 'Int' as a placeholder for all integer types.
typedef class AtomicMetric<TMetricKind::GAUGE> IntGauge;
typedef class AtomicMetric<TMetricKind::COUNTER> IntCounter;

}
