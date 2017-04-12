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
#include <boost/scoped_ptr.hpp>
#include <limits>
#include <map>

#include "testutil/gtest-util.h"
#include "util/collection-metrics.h"
#include "util/memory-metrics.h"
#include "util/metrics.h"
#include "util/histogram-metric.h"
#include "util/thread.h"

#include "common/names.h"

using namespace rapidjson;

namespace impala {

template <typename M, typename T>
void AssertValue(M* metric, const T& value,
    const string& human_readable) {
  EXPECT_EQ(metric->value(), value);
  if (!human_readable.empty()) {
    EXPECT_EQ(metric->ToHumanReadable(), human_readable);
  }
}

class MetricsTest : public testing::Test {
 public:
  void ResetMetricDefs() {
    MetricDefs::GetInstance()->metric_defs_ = g_MetricDefs_constants;
  }

  virtual void SetUp() {
    ResetMetricDefs();
  }

  virtual void TearDown() {
    ResetMetricDefs();
  }

  void AddMetricDef(const string& key, const TMetricKind::type kind,
      const TUnit::type units, const string& desc = "") {
    map<string, TMetricDef>& defs = MetricDefs::GetInstance()->metric_defs_.TMetricDefs;
    EXPECT_EQ(defs.end(), defs.find(key));

    TMetricDef def;
    def.__set_key(key);
    def.__set_kind(kind);
    def.__set_units(units);
    def.__set_description(desc);
    defs.insert(pair<string, TMetricDef>(key, def));
  }
};

TEST_F(MetricsTest, CounterMetrics) {
  MetricGroup metrics("CounterMetrics");
  AddMetricDef("counter", TMetricKind::COUNTER, TUnit::UNIT);
  IntCounter* int_counter = metrics.AddCounter<int64_t>("counter", 0);
  AssertValue(int_counter, 0, "0");
  int_counter->Increment(1);
  AssertValue(int_counter, 1, "1");
  int_counter->Increment(10);
  AssertValue(int_counter, 11, "11");
  int_counter->set_value(3456);
  AssertValue(int_counter, 3456, "3.46K");

  AddMetricDef("counter_with_units", TMetricKind::COUNTER, TUnit::BYTES);
  IntCounter* int_counter_with_units =
      metrics.AddCounter<int64_t>("counter_with_units", 10);
  AssertValue(int_counter_with_units, 10, "10.00 B");
}

TEST_F(MetricsTest, GaugeMetrics) {
  MetricGroup metrics("GaugeMetrics");
  AddMetricDef("gauge", TMetricKind::GAUGE, TUnit::NONE);
  IntGauge* int_gauge = metrics.AddGauge<int64_t>("gauge", 0);
  AssertValue(int_gauge, 0, "0");
  int_gauge->Increment(-1);
  AssertValue(int_gauge, -1, "-1");
  int_gauge->Increment(10);
  AssertValue(int_gauge, 9, "9");
  int_gauge->set_value(3456);
  AssertValue(int_gauge, 3456, "3456");

  AddMetricDef("gauge_with_units", TMetricKind::GAUGE, TUnit::TIME_S);
  IntGauge* int_gauge_with_units =
      metrics.AddGauge<int64_t>("gauge_with_units", 10);
  AssertValue(int_gauge_with_units, 10, "10s000ms");
}

TEST_F(MetricsTest, SumGauge) {
  MetricGroup metrics("SumGauge");
  AddMetricDef("gauge1", TMetricKind::GAUGE, TUnit::NONE);
  AddMetricDef("gauge2", TMetricKind::GAUGE, TUnit::NONE);
  AddMetricDef("sum", TMetricKind::GAUGE, TUnit::NONE);
  IntGauge* gauge1 = metrics.AddGauge<int64_t>("gauge1", 0);
  IntGauge* gauge2 = metrics.AddGauge<int64_t>("gauge2", 0);

  vector<IntGauge*> gauges({gauge1, gauge2});
  IntGauge* sum_gauge =
      metrics.RegisterMetric(new SumGauge<int64_t>(MetricDefs::Get("sum"), gauges));

  AssertValue(sum_gauge, 0, "0");
  gauge1->Increment(1);
  AssertValue(sum_gauge, 1, "1");
  gauge2->Increment(-1);
  AssertValue(sum_gauge, 0, "0");
  gauge2->Increment(100);
  AssertValue(sum_gauge, 100, "100");
}

TEST_F(MetricsTest, PropertyMetrics) {
  MetricGroup metrics("PropertyMetrics");
  AddMetricDef("bool_property", TMetricKind::PROPERTY, TUnit::NONE);
  BooleanProperty* bool_property = metrics.AddProperty("bool_property", false);
  AssertValue(bool_property, false, "false");
  bool_property->set_value(true);
  AssertValue(bool_property, true, "true");

  AddMetricDef("string_property", TMetricKind::PROPERTY, TUnit::NONE);
  StringProperty* string_property = metrics.AddProperty("string_property",
      string("string1"));
  AssertValue(string_property, "string1", "string1");
  string_property->set_value("string2");
  AssertValue(string_property, "string2", "string2");
}

TEST_F(MetricsTest, NonFiniteValues) {
  MetricGroup metrics("NanValues");
  AddMetricDef("inf_value", TMetricKind::GAUGE, TUnit::NONE);
  double inf = numeric_limits<double>::infinity();
  DoubleGauge* gauge = metrics.AddGauge("inf_value", inf);
  AssertValue(gauge, inf, "inf");
  double nan = numeric_limits<double>::quiet_NaN();
  gauge->set_value(nan);
  EXPECT_TRUE(std::isnan(gauge->value()));
  EXPECT_TRUE(gauge->ToHumanReadable() == "nan");
}

TEST_F(MetricsTest, SetMetrics) {
  MetricGroup metrics("SetMetrics");
  set<int> item_set;
  item_set.insert(4); item_set.insert(5); item_set.insert(6);
  AddMetricDef("set", TMetricKind::SET, TUnit::NONE);
  SetMetric<int>* set_metric = SetMetric<int>::CreateAndRegister(&metrics, "set",
      item_set);
  EXPECT_EQ(set_metric->ToHumanReadable(), "[4, 5, 6]");

  set_metric->Add(7);
  set_metric->Add(7);
  set_metric->Remove(4);
  set_metric->Remove(4);

  EXPECT_EQ(set_metric->ToHumanReadable(), "[5, 6, 7]");
}

TEST_F(MetricsTest, StatsMetrics) {
  // Uninitialised stats metrics don't print anything other than the count
  MetricGroup metrics("StatsMetrics");
  AddMetricDef("stats", TMetricKind::STATS, TUnit::NONE);
  StatsMetric<double>* stats_metric = StatsMetric<double>::CreateAndRegister(&metrics,
      "stats");
  EXPECT_EQ(stats_metric->ToHumanReadable(), "count: 0");

  stats_metric->Update(0.0);
  stats_metric->Update(1.0);
  stats_metric->Update(2.0);

  EXPECT_EQ(stats_metric->ToHumanReadable(), "count: 3, last: 2.000000, min: 0.000000, "
      "max: 2.000000, mean: 1.000000, stddev: 0.816497");

  AddMetricDef("stats_units", TMetricKind::STATS, TUnit::BYTES);
  StatsMetric<double>* stats_metric_with_units =
      StatsMetric<double>::CreateAndRegister(&metrics, "stats_units");
  EXPECT_EQ(stats_metric_with_units->ToHumanReadable(), "count: 0");

  stats_metric_with_units->Update(0.0);
  stats_metric_with_units->Update(1.0);
  stats_metric_with_units->Update(2.0);

  EXPECT_EQ(stats_metric_with_units->ToHumanReadable(), "count: 3, last: 2.00 B, min: 0, "
      "max: 2.00 B, mean: 1.00 B, stddev: 0.82 B");
}

TEST_F(MetricsTest, StatsMetricsSingle) {
  MetricGroup metrics("StatsMetrics");
  StatsMetric<int, StatsType::MAX | StatsType::MEAN>*
      stats_metric =
      StatsMetric<int, StatsType::MAX | StatsType::MEAN>::CreateAndRegister(&metrics,
          "impala-server.io.mgr.cached-file-handles-hit-ratio");
  EXPECT_EQ(stats_metric->ToHumanReadable(), "");
  stats_metric->Update(3);
  stats_metric->Update(10);
  stats_metric->Update(2);

  EXPECT_EQ(stats_metric->ToHumanReadable(), "last: 2, max: 10, mean: 5.000000");
}

TEST_F(MetricsTest, MemMetric) {
#ifndef ADDRESS_SANITIZER
  MetricGroup metrics("MemMetrics");
  RegisterMemoryMetrics(&metrics, false, nullptr, nullptr);
  // Smoke test to confirm that tcmalloc metrics are returning reasonable values.
  UIntGauge* bytes_in_use =
      metrics.FindMetricForTesting<UIntGauge>("tcmalloc.bytes-in-use");
  ASSERT_TRUE(bytes_in_use != NULL);

  uint64_t cur_in_use = bytes_in_use->value();
  EXPECT_GT(cur_in_use, 0);

  // Allocate 100MB to increase the number of bytes used. TCMalloc may also give up some
  // bytes during this allocation, so this allocation is deliberately large to ensure that
  // the bytes used metric goes up net.
  scoped_ptr<vector<uint64_t>> chunk(new vector<uint64_t>(100 * 1024 * 1024));
  EXPECT_GT(bytes_in_use->value(), cur_in_use);

  UIntGauge* total_bytes_reserved =
      metrics.FindMetricForTesting<UIntGauge>("tcmalloc.total-bytes-reserved");
  ASSERT_TRUE(total_bytes_reserved != NULL);
  ASSERT_GT(total_bytes_reserved->value(), 0);

  UIntGauge* pageheap_free_bytes =
      metrics.FindMetricForTesting<UIntGauge>("tcmalloc.pageheap-free-bytes");
  ASSERT_TRUE(pageheap_free_bytes != NULL);

  UIntGauge* pageheap_unmapped_bytes =
      metrics.FindMetricForTesting<UIntGauge>("tcmalloc.pageheap-unmapped-bytes");
  EXPECT_TRUE(pageheap_unmapped_bytes != NULL);
#endif
}

TEST_F(MetricsTest, JvmMetrics) {
  MetricGroup metrics("JvmMetrics");
  RegisterMemoryMetrics(&metrics, true, nullptr, nullptr);
  UIntGauge* jvm_total_used =
      metrics.GetOrCreateChildGroup("jvm")->FindMetricForTesting<UIntGauge>(
          "jvm.total.current-usage-bytes");
  ASSERT_TRUE(jvm_total_used != NULL);
  EXPECT_GT(jvm_total_used->value(), 0);
  UIntGauge* jvm_peak_total_used =
      metrics.GetOrCreateChildGroup("jvm")->FindMetricForTesting<UIntGauge>(
          "jvm.total.peak-current-usage-bytes");
  ASSERT_TRUE(jvm_peak_total_used != NULL);
  EXPECT_GT(jvm_peak_total_used->value(), 0);
}

void AssertJson(const Value& val, const string& name, const string& value,
    const string& description, const string& kind="", const string& units="") {
  EXPECT_EQ(val["name"].GetString(), name);
  EXPECT_EQ(val["human_readable"].GetString(), value);
  EXPECT_EQ(val["description"].GetString(), description);
  if (!kind.empty()) EXPECT_EQ(val["kind"].GetString(), kind);
  if (!units.empty()) EXPECT_EQ(val["units"].GetString(), units);
}

TEST_F(MetricsTest, CountersJson) {
  MetricGroup metrics("CounterMetrics");
  AddMetricDef("counter", TMetricKind::COUNTER, TUnit::UNIT, "description");
  metrics.AddCounter<int64_t>("counter", 0);
  Document document;
  Value val;
  metrics.ToJson(true, &document, &val);
  const Value& counter_val = val["metrics"][0u];
  AssertJson(counter_val, "counter", "0", "description", "COUNTER", "UNIT");
  EXPECT_EQ(counter_val["value"].GetInt(), 0);
}

TEST_F(MetricsTest, GaugesJson) {
  MetricGroup metrics("GaugeMetrics");
  AddMetricDef("gauge", TMetricKind::GAUGE, TUnit::NONE);
  metrics.AddGauge<int64_t>("gauge", 10);
  Document document;
  Value val;
  metrics.ToJson(true, &document, &val);
  AssertJson(val["metrics"][0u], "gauge", "10", "", "GAUGE", "NONE");
  EXPECT_EQ(val["metrics"][0u]["value"].GetInt(), 10);
}

TEST_F(MetricsTest, PropertiesJson) {
  MetricGroup metrics("Properties");
  AddMetricDef("property", TMetricKind::PROPERTY, TUnit::NONE);
  metrics.AddProperty("property", string("my value"));
  Document document;
  Value val;
  metrics.ToJson(true, &document, &val);

  AssertJson(val["metrics"][0u], "property", "my value", "", "PROPERTY", "NONE");
  EXPECT_EQ(string(val["metrics"][0u]["value"].GetString()), "my value");
}

TEST_F(MetricsTest, SetMetricsJson) {
  MetricGroup metrics("SetMetrics");
  set<int> item_set;
  item_set.insert(4); item_set.insert(5); item_set.insert(6);
  AddMetricDef("set", TMetricKind::SET, TUnit::NONE);
  SetMetric<int>::CreateAndRegister(&metrics, "set", item_set);

  Document document;
  Value val;
  metrics.ToJson(true, &document, &val);
  const Value& set_val = val["metrics"][0u];
  ASSERT_TRUE(set_val["items"].IsArray());
  EXPECT_EQ(set_val["items"].Size(), 3);
  EXPECT_EQ(set_val["items"][0u].GetInt(), 4);
  EXPECT_EQ(set_val["items"][1].GetInt(), 5);
  EXPECT_EQ(set_val["items"][2].GetInt(), 6);

  AssertJson(set_val, "set", "[4, 5, 6]", "");
}

TEST_F(MetricsTest, StatsMetricsJson) {
  MetricGroup metrics("StatsMetrics");
  AddMetricDef("stats_metric", TMetricKind::STATS, TUnit::UNIT);
  StatsMetric<double>* metric = StatsMetric<double>::CreateAndRegister(&metrics,
      "stats_metric");
  metric->Update(10.0);
  metric->Update(20.0);
  Document document;
  Value val;
  metrics.ToJson(true, &document, &val);
  const Value& stats_val = val["metrics"][0u];
  AssertJson(stats_val, "stats_metric", "count: 2, last: 20.000000, min: 10.000000, "
      "max: 20.000000, mean: 15.000000, stddev: 5.000000", "", "", "UNIT");

  EXPECT_EQ(stats_val["count"].GetInt(), 2);
  EXPECT_EQ(stats_val["last"].GetDouble(), 20.0);
  EXPECT_EQ(stats_val["min"].GetDouble(), 10.0);
  EXPECT_EQ(stats_val["max"].GetDouble(), 20.0);
  EXPECT_EQ(stats_val["mean"].GetDouble(), 15.0);
  EXPECT_EQ(stats_val["stddev"].GetDouble(), 5.0);
}

TEST_F(MetricsTest, HistogramMetrics) {
  MetricGroup metrics("HistoMetrics");
  TMetricDef metric_def =
      MakeTMetricDef("histogram-metric", TMetricKind::HISTOGRAM, TUnit::TIME_MS);
  constexpr int MAX_VALUE = 10000;
  HistogramMetric* metric = metrics.RegisterMetric(new HistogramMetric(
          metric_def, MAX_VALUE, 3));

  // Add value beyond limit to make sure it's recorded accurately.
  for (int i = 0; i <= MAX_VALUE + 1; ++i) metric->Update(i);

  Document document;
  Value val;
  metrics.ToJson(true, &document, &val);
  const Value& histo_val = val["metrics"][0u];
  EXPECT_EQ(histo_val["min"].GetInt(), 0);
  EXPECT_EQ(histo_val["max"].GetInt(), MAX_VALUE + 1);
  EXPECT_EQ(histo_val["25th %-ile"].GetInt(), 2500);
  EXPECT_EQ(histo_val["50th %-ile"].GetInt(), 5000);
  EXPECT_EQ(histo_val["75th %-ile"].GetInt(), 7500);
  EXPECT_EQ(histo_val["90th %-ile"].GetInt(), 9000);
  EXPECT_EQ(histo_val["95th %-ile"].GetInt(), 9496);
  EXPECT_EQ(histo_val["99.9th %-ile"].GetInt(), 9984);
  EXPECT_EQ(histo_val["min"].GetInt(), 0);
  EXPECT_EQ(histo_val["max"].GetInt(), MAX_VALUE + 1);

  EXPECT_EQ(metric->ToHumanReadable(), "Count: 10002, min / max: 0 / 10s001ms, "
      "25th %-ile: 2s500ms, 50th %-ile: 5s000ms, 75th %-ile: 7s500ms, "
      "90th %-ile: 9s000ms, 95th %-ile: 9s496ms, 99.9th %-ile: 9s984ms");
}

TEST_F(MetricsTest, UnitsAndDescriptionJson) {
  MetricGroup metrics("Units");
  AddMetricDef("counter", TMetricKind::COUNTER, TUnit::BYTES, "description");
  metrics.AddCounter("counter", 2048);
  Document document;
  Value val;
  metrics.ToJson(true, &document, &val);
  const Value& counter_val = val["metrics"][0u];
  AssertJson(counter_val, "counter", "2.00 KB", "description", "COUNTER", "BYTES");
  EXPECT_EQ(counter_val["value"].GetInt(), 2048);
}

TEST_F(MetricsTest, MetricGroupJson) {
  MetricGroup metrics("JsonTest");
  AddMetricDef("counter1", TMetricKind::COUNTER, TUnit::BYTES, "description");
  AddMetricDef("counter2", TMetricKind::COUNTER, TUnit::BYTES, "description");
  metrics.AddCounter("counter1", 2048);
  metrics.AddCounter("counter2", 2048);

  MetricGroup* find_result = metrics.FindChildGroup("child1");
  EXPECT_EQ(find_result, reinterpret_cast<MetricGroup*>(NULL));

  metrics.GetOrCreateChildGroup("child1");
  AddMetricDef("child_counter", TMetricKind::COUNTER, TUnit::BYTES, "description");
  metrics.GetOrCreateChildGroup("child2")->AddCounter("child_counter", 0);

  IntCounter* counter = metrics.FindMetricForTesting<IntCounter>(string("child_counter"));
  ASSERT_NE(counter, reinterpret_cast<IntCounter*>(NULL));

  Document document;
  Value val;
  metrics.ToJson(true, &document, &val);
  EXPECT_EQ(val["metrics"].Size(), 2);
  EXPECT_EQ(val["name"].GetString(), string("JsonTest"));

  EXPECT_EQ(val["child_groups"].Size(), 2);
  EXPECT_EQ(val["child_groups"][0u]["name"].GetString(), string("child1"));
  EXPECT_EQ(val["child_groups"][0u]["metrics"].Size(), 0);
  EXPECT_EQ(val["child_groups"][1]["name"].GetString(), string("child2"));
  EXPECT_EQ(val["child_groups"][1]["metrics"].Size(), 1);

  find_result = metrics.FindChildGroup("child1");
  ASSERT_NE(find_result, reinterpret_cast<MetricGroup*>(NULL));
  Value val2;
  find_result->ToJson(true, &document, &val2);

  EXPECT_EQ(val2["metrics"].Size(), 0);
  EXPECT_EQ(val2["name"].GetString(), string("child1"));
}

}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  return RUN_ALL_TESTS();
}
