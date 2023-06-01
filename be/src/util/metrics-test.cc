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
  EXPECT_EQ(metric->GetValue(), value);
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
  IntCounter* int_counter = metrics.AddCounter("counter", 0);
  AssertValue(int_counter, 0, "0");
  int_counter->Increment(1);
  AssertValue(int_counter, 1, "1");
  int_counter->Increment(10);
  AssertValue(int_counter, 11, "11");
  int_counter->SetValue(3456);
  AssertValue(int_counter, 3456, "3.46K");

  AddMetricDef("counter_with_units", TMetricKind::COUNTER, TUnit::BYTES);
  IntCounter* int_counter_with_units =
      metrics.AddCounter("counter_with_units", 10);
  AssertValue(int_counter_with_units, 10, "10.00 B");
}

TEST_F(MetricsTest, GaugeMetrics) {
  MetricGroup metrics("GaugeMetrics");
  AddMetricDef("gauge", TMetricKind::GAUGE, TUnit::NONE);
  IntGauge* int_gauge = metrics.AddGauge("gauge", 0);
  AssertValue(int_gauge, 0, "0");
  int_gauge->Increment(-1);
  AssertValue(int_gauge, -1, "-1");
  int_gauge->Increment(10);
  AssertValue(int_gauge, 9, "9");
  int_gauge->SetValue(3456);
  AssertValue(int_gauge, 3456, "3456");

  AddMetricDef("gauge_with_units", TMetricKind::GAUGE, TUnit::TIME_S);
  IntGauge* int_gauge_with_units =
      metrics.AddGauge("gauge_with_units", 10);
  AssertValue(int_gauge_with_units, 10, "10s000ms");
}

TEST_F(MetricsTest, AtomicHighWaterMarkGauge) {
  MetricGroup metrics("IntHWMGauge");
  AddMetricDef("gauge", TMetricKind::GAUGE, TUnit::NONE);
  AddMetricDef("hwm_gauge", TMetricKind::GAUGE, TUnit::NONE);
  AtomicHighWaterMarkGauge* int_hwm_gauge = metrics.AddHWMGauge("hwm_gauge", "gauge", 0);
  IntGauge* int_gauge = int_hwm_gauge->current_value_;
  AssertValue(int_hwm_gauge, 0, "0");
  AssertValue(int_gauge, 0, "0");
  int_hwm_gauge->Increment(-1);
  AssertValue(int_hwm_gauge, 0, "0");
  AssertValue(int_gauge, -1, "-1");
  int_hwm_gauge->Increment(10);
  AssertValue(int_hwm_gauge, 9, "9");
  AssertValue(int_gauge, 9, "9");
  int_hwm_gauge->SetValue(3456);
  AssertValue(int_hwm_gauge, 3456, "3456");
  AssertValue(int_gauge, 3456, "3456");
  int_hwm_gauge->SetValue(100);
  AssertValue(int_hwm_gauge, 3456, "3456");
  AssertValue(int_gauge, 100, "100");

  AddMetricDef("hwm_gauge_with_units", TMetricKind::GAUGE, TUnit::BYTES);
  AddMetricDef("gauge_with_units", TMetricKind::GAUGE, TUnit::BYTES);
  AtomicHighWaterMarkGauge* int_hwm_gauge_with_units =
      metrics.AddHWMGauge("hwm_gauge_with_units", "gauge_with_units", 10);
  IntGauge* int_gauge_with_units = int_hwm_gauge_with_units->current_value_;
  AssertValue(int_hwm_gauge_with_units, 10, "10.00 B");
  AssertValue(int_gauge_with_units, 10, "10.00 B");
}

TEST_F(MetricsTest, SumGauge) {
  MetricGroup metrics("SumGauge");
  AddMetricDef("gauge1", TMetricKind::GAUGE, TUnit::NONE);
  AddMetricDef("gauge2", TMetricKind::GAUGE, TUnit::NONE);
  AddMetricDef("sum", TMetricKind::GAUGE, TUnit::NONE);
  IntGauge* gauge1 = metrics.AddGauge("gauge1", 0);
  IntGauge* gauge2 = metrics.AddGauge("gauge2", 0);

  vector<IntGauge*> gauges({gauge1, gauge2});
  IntGauge* sum_gauge =
      metrics.RegisterMetric(new SumGauge(MetricDefs::Get("sum"), gauges));

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
  bool_property->SetValue(true);
  AssertValue(bool_property, true, "true");

  AddMetricDef("string_property", TMetricKind::PROPERTY, TUnit::NONE);
  StringProperty* string_property = metrics.AddProperty("string_property",
      string("string1"));
  AssertValue(string_property, "string1", "string1");
  string_property->SetValue("string2");
  AssertValue(string_property, "string2", "string2");
}

TEST_F(MetricsTest, NonFiniteValues) {
  MetricGroup metrics("NanValues");
  AddMetricDef("inf_value", TMetricKind::GAUGE, TUnit::NONE);
  double inf = numeric_limits<double>::infinity();
  DoubleGauge* gauge = metrics.AddDoubleGauge("inf_value", inf);
  AssertValue(gauge, inf, "inf");
  double nan = numeric_limits<double>::quiet_NaN();
  gauge->SetValue(nan);
  EXPECT_TRUE(std::isnan(gauge->GetValue()));
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
  StatsMetric<uint64_t, StatsType::MAX | StatsType::MEAN>*
      stats_metric =
      StatsMetric<uint64_t, StatsType::MAX | StatsType::MEAN>::CreateAndRegister(&metrics,
          "impala-server.io.mgr.cached-file-handles-hit-ratio");
  EXPECT_EQ(stats_metric->ToHumanReadable(), "");
  stats_metric->Update(3);
  stats_metric->Update(10);
  stats_metric->Update(2);

  EXPECT_EQ(stats_metric->ToHumanReadable(), "last: 2, max: 10, mean: 5.000000");
}

TEST_F(MetricsTest, MemMetric) {
#if !defined(ADDRESS_SANITIZER) && !defined(THREAD_SANITIZER)
  MetricGroup metrics("MemMetrics");
  ASSERT_OK(RegisterMemoryMetrics(&metrics, false, nullptr, nullptr));
  // Smoke test to confirm that tcmalloc metrics are returning reasonable values.
  IntGauge* bytes_in_use =
      metrics.FindMetricForTesting<IntGauge>("tcmalloc.bytes-in-use");
  ASSERT_TRUE(bytes_in_use != NULL);

  uint64_t cur_in_use = bytes_in_use->GetValue();
  EXPECT_GT(cur_in_use, 0);

  // Allocate 100MB to increase the number of bytes used. TCMalloc may also give up some
  // bytes during this allocation, so this allocation is deliberately large to ensure that
  // the bytes used metric goes up net.
  scoped_ptr<vector<uint64_t>> chunk(new vector<uint64_t>(100 * 1024 * 1024));
  EXPECT_GT(bytes_in_use->GetValue(), cur_in_use);

  IntGauge* total_bytes_reserved =
      metrics.FindMetricForTesting<IntGauge>("tcmalloc.total-bytes-reserved");
  ASSERT_TRUE(total_bytes_reserved != NULL);
  ASSERT_GT(total_bytes_reserved->GetValue(), 0);

  IntGauge* pageheap_free_bytes =
      metrics.FindMetricForTesting<IntGauge>("tcmalloc.pageheap-free-bytes");
  ASSERT_TRUE(pageheap_free_bytes != NULL);

  IntGauge* pageheap_unmapped_bytes =
      metrics.FindMetricForTesting<IntGauge>("tcmalloc.pageheap-unmapped-bytes");
  EXPECT_TRUE(pageheap_unmapped_bytes != NULL);
#endif
}

TEST_F(MetricsTest, JvmMemoryMetrics) {
  MetricGroup metrics("JvmMemoryMetrics");
  ASSERT_OK(RegisterMemoryMetrics(&metrics, true, nullptr, nullptr));
  IntGauge* jvm_total_used =
      metrics.GetOrCreateChildGroup("jvm")->FindMetricForTesting<IntGauge>(
          "jvm.total.current-usage-bytes");
  ASSERT_TRUE(jvm_total_used != NULL);
  EXPECT_GT(jvm_total_used->GetValue(), 0);
  IntGauge* jvm_peak_total_used =
      metrics.GetOrCreateChildGroup("jvm")->FindMetricForTesting<IntGauge>(
          "jvm.total.peak-current-usage-bytes");
  ASSERT_TRUE(jvm_peak_total_used != NULL);
  EXPECT_GT(jvm_peak_total_used->GetValue(), 0);
}

void AssertJson(const Value& val, const string& name, const string& value,
    const string& description, const string& kind="", const string& units="") {
  EXPECT_EQ(val["name"].GetString(), name);
  EXPECT_EQ(val["human_readable"].GetString(), value);
  EXPECT_EQ(val["description"].GetString(), description);
  if (!kind.empty()) {
    EXPECT_EQ(val["kind"].GetString(), kind);
  }
  if (!units.empty()) {
    EXPECT_EQ(val["units"].GetString(), units);
  }
}

TEST_F(MetricsTest, CountersJson) {
  MetricGroup metrics("CounterMetrics");
  AddMetricDef("counter", TMetricKind::COUNTER, TUnit::UNIT, "description");
  metrics.AddCounter("counter", 0);
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
  metrics.AddGauge("gauge", 10);
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

  EXPECT_EQ(metric->ToHumanReadable(),
      "Count: 10002, sum: 13h53m, min / max: 0 / 10s001ms, "
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
  std::unordered_map<string, int64_t> group_to_num_metrics_map = {
      {val["child_groups"][0u]["name"].GetString(),
          val["child_groups"][0u]["metrics"].Size()},
      {val["child_groups"][1]["name"].GetString(),
          val["child_groups"][1]["metrics"].Size()}};
  EXPECT_TRUE(group_to_num_metrics_map.find("child1") != group_to_num_metrics_map.end());
  EXPECT_EQ(group_to_num_metrics_map["child1"], 0);
  EXPECT_TRUE(group_to_num_metrics_map.find("child2") != group_to_num_metrics_map.end());
  EXPECT_EQ(group_to_num_metrics_map["child2"], 1);

  find_result = metrics.FindChildGroup("child1");
  ASSERT_NE(find_result, reinterpret_cast<MetricGroup*>(NULL));
  Value val2;
  find_result->ToJson(true, &document, &val2);

  EXPECT_EQ(val2["metrics"].Size(), 0);
  EXPECT_EQ(val2["name"].GetString(), string("child1"));
}

TEST_F(MetricsTest, RegisterAndRemoveMetric) {
  MetricGroup metrics("Metrics");

  AddMetricDef("gauge", TMetricKind::GAUGE, TUnit::NONE);
  IntGauge* int_gauge = metrics.AddGauge("gauge", 0);
  AssertValue(int_gauge, 0, "0");
  int_gauge->Increment(-1);
  AssertValue(int_gauge, -1, "-1");

  // Now try removing the metric from the metric group.
  int_gauge = nullptr;
  metrics.RemoveMetric("gauge");
  EXPECT_EQ(metrics.FindMetricForTesting<IntGauge*>("gauge"), nullptr);
}

// Test the mapping of Impala's metric names into prometheus names.
TEST_F(MetricsTest, PrometheusMetricNames) {
  // Test that metrics get prefixed with impala_ if needed and don't get
  // prefixed if they already start with impala.
  EXPECT_EQ("impala_metric", MetricGroup::ImpalaToPrometheusName("impala_metric"));
  EXPECT_EQ(
      "impala_an_impala_metric", MetricGroup::ImpalaToPrometheusName("an_impala_metric"));
  EXPECT_EQ("impala_IMPALA_METRIC", MetricGroup::ImpalaToPrometheusName("IMPALA_METRIC"));
  EXPECT_EQ("impala_random_metric", MetricGroup::ImpalaToPrometheusName("random_metric"));
  EXPECT_EQ("impala_impal_random_metric",
      MetricGroup::ImpalaToPrometheusName("impal_random_metric"));
  EXPECT_EQ(
      "impala_impalas_metric", MetricGroup::ImpalaToPrometheusName("impalas_metric"));
  EXPECT_EQ("impala_impalametric", MetricGroup::ImpalaToPrometheusName("impalametric"));
  EXPECT_EQ("impala_server_metric",
      MetricGroup::ImpalaToPrometheusName("impala-server-metric"));

  // Test that . and - get transformed to _.
  EXPECT_EQ("impala_metric_name_with_punctuation_",
      MetricGroup::ImpalaToPrometheusName("metric-name.with-punctuation_"));

  // Other special characters are unmodified
  EXPECT_EQ("impala_!@#$%*", MetricGroup::ImpalaToPrometheusName("!@#$%*"));
}

void AssertPrometheus(const std::stringstream& val, const string& name,
    const string& value, const string& desc, const string& kind = "") {
  std::stringstream exp_val;
  // convert to all values to expected format
  exp_val << "# HELP " << name << " " << desc << "\n"
          << "# TYPE " << name << " " << kind << "\n";
  if (name == "impala_stats_metric" || name == "impala_histogram_metric") {
    exp_val << value + "\n";
  } else {
    exp_val << name << " " << value + "\n";
  }
  EXPECT_EQ(val.str(), exp_val.str());
}

TEST_F(MetricsTest, CountersPrometheus) {
  MetricGroup metrics("CounterMetrics");
  AddMetricDef("counter", TMetricKind::COUNTER, TUnit::UNIT, "description");
  metrics.AddCounter("counter", 0);
  std::stringstream counter_val;
  metrics.ToPrometheus(true, &counter_val);
  AssertPrometheus(counter_val, "impala_counter", "0", "description", "counter");
}

TEST_F(MetricsTest, CountersBytesPrometheus) {
  MetricGroup metrics("CounterMetrics");
  AddMetricDef("counter", TMetricKind::COUNTER, TUnit::BYTES, "description");
  metrics.AddCounter("counter", 555);
  std::stringstream counter_val;
  metrics.ToPrometheus(true, &counter_val);
  AssertPrometheus(counter_val, "impala_counter", "555", "description", "counter");
}

TEST_F(MetricsTest, CountersNonePrometheus) {
  MetricGroup metrics("CounterMetrics");
  AddMetricDef("counter", TMetricKind::COUNTER, TUnit::NONE, "description");
  metrics.AddCounter("counter", 0);
  std::stringstream counter_val;
  metrics.ToPrometheus(true, &counter_val);
  AssertPrometheus(counter_val, "impala_counter", "0", "description", "counter");
}

TEST_F(MetricsTest, CountersTimeMSPrometheus) {
  MetricGroup metrics("CounterMetrics");
  AddMetricDef("counter", TMetricKind::COUNTER, TUnit::TIME_MS, "description");
  metrics.AddCounter("counter", 4354364);
  std::stringstream counter_val;
  metrics.ToPrometheus(true, &counter_val);
  AssertPrometheus(counter_val, "impala_counter", "4354.36", "description", "counter");
}

TEST_F(MetricsTest, CountersTimeNSPrometheus) {
  MetricGroup metrics("CounterMetrics");
  AddMetricDef("counter", TMetricKind::COUNTER, TUnit::TIME_NS, "description");
  metrics.AddCounter("counter", 4354364234);
  std::stringstream counter_val;
  metrics.ToPrometheus(true, &counter_val);
  AssertPrometheus(counter_val, "impala_counter", "4.35436", "description", "counter");
}

TEST_F(MetricsTest, CountersTimeSPrometheus) {
  MetricGroup metrics("CounterMetrics");
  AddMetricDef("counter", TMetricKind::COUNTER, TUnit::TIME_S, "description");
  metrics.AddCounter("counter", 120);
  std::stringstream counter_val;
  metrics.ToPrometheus(true, &counter_val);
  AssertPrometheus(counter_val, "impala_counter", "120", "description", "counter");
}

TEST_F(MetricsTest, GaugesPrometheus) {
  MetricGroup metrics("GaugeMetrics");
  AddMetricDef("gauge", TMetricKind::GAUGE, TUnit::NONE);
  metrics.AddGauge("gauge", 10);
  std::stringstream gauge_val;
  metrics.ToPrometheus(true, &gauge_val);
  AssertPrometheus(gauge_val, "impala_gauge", "10", "", "gauge");
}

TEST_F(MetricsTest, GaugesBytesPrometheus) {
  MetricGroup metrics("GaugeMetrics");
  AddMetricDef("gauge", TMetricKind::GAUGE, TUnit::BYTES);
  metrics.AddGauge("gauge", 150000);
  std::stringstream gauge_val;
  metrics.ToPrometheus(true, &gauge_val);
  AssertPrometheus(gauge_val, "impala_gauge", "150000", "", "gauge");
}

TEST_F(MetricsTest, GaugesTimeMSPrometheus) {
  MetricGroup metrics("GaugeMetrics");
  AddMetricDef("gauge", TMetricKind::GAUGE, TUnit::TIME_MS);
  metrics.AddGauge("gauge", 10000);
  std::stringstream gauge_val;
  metrics.ToPrometheus(true, &gauge_val);
  AssertPrometheus(gauge_val, "impala_gauge", "10", "", "gauge");
}

TEST_F(MetricsTest, GaugesTimeNSPrometheus) {
  MetricGroup metrics("GaugeMetrics");
  AddMetricDef("gauge", TMetricKind::GAUGE, TUnit::TIME_NS);
  metrics.AddGauge("gauge", 2334123456);
  std::stringstream gauge_val;
  metrics.ToPrometheus(true, &gauge_val);
  AssertPrometheus(gauge_val, "impala_gauge", "2.33412", "", "gauge");
}

TEST_F(MetricsTest, GaugesTimeSPrometheus) {
  MetricGroup metrics("GaugeMetrics");
  AddMetricDef("gauge", TMetricKind::GAUGE, TUnit::TIME_S);
  metrics.AddGauge("gauge", 1500);
  std::stringstream gauge_val;
  metrics.ToPrometheus(true, &gauge_val);
  AssertPrometheus(gauge_val, "impala_gauge", "1500", "", "gauge");
}

TEST_F(MetricsTest, GaugesUnitPrometheus) {
  MetricGroup metrics("GaugeMetrics");
  AddMetricDef("gauge", TMetricKind::GAUGE, TUnit::UNIT);
  metrics.AddGauge("gauge", 111);
  std::stringstream gauge_val;
  metrics.ToPrometheus(true, &gauge_val);
  AssertPrometheus(gauge_val, "impala_gauge", "111", "", "gauge");
}

TEST_F(MetricsTest, StatsMetricsPrometheus) {
  MetricGroup metrics("StatsMetrics");
  AddMetricDef("stats_metric", TMetricKind::STATS, TUnit::UNIT);
  StatsMetric<double>* metric =
      StatsMetric<double>::CreateAndRegister(&metrics, "stats_metric");
  metric->Update(10.0);
  metric->Update(20.0);
  std::stringstream stats_val;
  metrics.ToPrometheus(true, &stats_val);
  AssertPrometheus(stats_val, "impala_stats_metric",
      "impala_stats_metric_total 2\n"
      "impala_stats_metric_last 20\n"
      "impala_stats_metric_min 10\n"
      "impala_stats_metric_max 20\n"
      "impala_stats_metric_mean 15\n"
      "impala_stats_metric_stddev 5\n",
      "", "counter");
}

TEST_F(MetricsTest, StatsMetricsBytesPrometheus) {
  MetricGroup metrics("StatsMetrics");
  AddMetricDef("stats_metric", TMetricKind::STATS, TUnit::BYTES);
  StatsMetric<double>* metric =
      StatsMetric<double>::CreateAndRegister(&metrics, "stats_metric");
  metric->Update(10.0);
  metric->Update(2230.1234567);
  std::stringstream stats_val;
  metrics.ToPrometheus(true, &stats_val);
  AssertPrometheus(stats_val, "impala_stats_metric",
      "impala_stats_metric_total 2\n"
      "impala_stats_metric_last 2230.12\n"
      "impala_stats_metric_min 10\n"
      "impala_stats_metric_max 2230.12\n"
      "impala_stats_metric_mean 1120.06\n"
      "impala_stats_metric_stddev 1110.06\n",
      "", "counter");
}

TEST_F(MetricsTest, StatsMetricsNonePrometheus) {
  MetricGroup metrics("StatsMetrics");
  AddMetricDef("stats_metric", TMetricKind::STATS, TUnit::NONE);
  StatsMetric<double>* metric =
      StatsMetric<double>::CreateAndRegister(&metrics, "stats_metric");
  metric->Update(10.0);
  metric->Update(20.0);
  std::stringstream stats_val;
  metrics.ToPrometheus(true, &stats_val);
  AssertPrometheus(stats_val, "impala_stats_metric",
      "impala_stats_metric_total 2\n"
      "impala_stats_metric_last 20\n"
      "impala_stats_metric_min 10\n"
      "impala_stats_metric_max 20\n"
      "impala_stats_metric_mean 15\n"
      "impala_stats_metric_stddev 5\n",
      "", "counter");
}

TEST_F(MetricsTest, StatsMetricsTimeMSPrometheus) {
  MetricGroup metrics("StatsMetrics");
  AddMetricDef("stats_metric", TMetricKind::STATS, TUnit::TIME_MS);
  StatsMetric<double>* metric =
      StatsMetric<double>::CreateAndRegister(&metrics, "stats_metric");
  metric->Update(10.0);
  metric->Update(20.0);
  std::stringstream stats_val;
  metrics.ToPrometheus(true, &stats_val);
  AssertPrometheus(stats_val, "impala_stats_metric",
      "impala_stats_metric_total 2\n"
      "impala_stats_metric_last 0.02\n"
      "impala_stats_metric_min 0.01\n"
      "impala_stats_metric_max 0.02\n"
      "impala_stats_metric_mean 0.015\n"
      "impala_stats_metric_stddev 0.005\n",
      "", "counter");
}

TEST_F(MetricsTest, StatsMetricsTimeNSPrometheus) {
  MetricGroup metrics("StatsMetrics");
  AddMetricDef("stats_metric", TMetricKind::STATS, TUnit::TIME_NS);
  StatsMetric<double>* metric =
      StatsMetric<double>::CreateAndRegister(&metrics, "stats_metric");
  metric->Update(10.12345);
  metric->Update(20.567);
  std::stringstream stats_val;
  metrics.ToPrometheus(true, &stats_val);
  AssertPrometheus(stats_val, "impala_stats_metric",
      "impala_stats_metric_total 2\n"
      "impala_stats_metric_last 2.0567e-08\n"
      "impala_stats_metric_min 1.01235e-08\n"
      "impala_stats_metric_max 2.0567e-08\n"
      "impala_stats_metric_mean 1.53452e-08\n"
      "impala_stats_metric_stddev 5.22178e-09\n",
      "", "counter");
}

TEST_F(MetricsTest, StatsMetricsTimeSPrometheus) {
  MetricGroup metrics("StatsMetrics");
  AddMetricDef("stats_metric", TMetricKind::STATS, TUnit::TIME_S);
  StatsMetric<double>* metric =
      StatsMetric<double>::CreateAndRegister(&metrics, "stats_metric");
  metric->Update(10.22);
  metric->Update(20.22);
  std::stringstream stats_val;
  metrics.ToPrometheus(true, &stats_val);
  AssertPrometheus(stats_val, "impala_stats_metric",
      "impala_stats_metric_total 2\n"
      "impala_stats_metric_last 20.22\n"
      "impala_stats_metric_min 10.22\n"
      "impala_stats_metric_max 20.22\n"
      "impala_stats_metric_mean 15.22\n"
      "impala_stats_metric_stddev 5\n",
      "", "counter");
}

TEST_F(MetricsTest, HistogramPrometheus) {
  MetricGroup metrics("HistoMetrics");
  TMetricDef metric_def =
      MakeTMetricDef("histogram-metric", TMetricKind::HISTOGRAM, TUnit::TIME_MS);
  constexpr int MAX_VALUE = 10000;
  HistogramMetric* metric =
      metrics.RegisterMetric(new HistogramMetric(metric_def, MAX_VALUE, 3));

  // Add value beyond limit to make sure it's recorded accurately.
  for (int i = 0; i <= MAX_VALUE + 1; ++i) metric->Update(i);

  std::stringstream val;
  metrics.ToPrometheus(true, &val);
  AssertPrometheus(val, "impala_histogram_metric",
      "impala_histogram_metric{quantile=\"0.2\"} 2.5\n"
      "impala_histogram_metric{quantile=\"0.5\"} 5\n"
      "impala_histogram_metric{quantile=\"0.7\"} 7.5\n"
      "impala_histogram_metric{quantile=\"0.9\"} 9\n"
      "impala_histogram_metric{quantile=\"0.95\"} 9.496\n"
      "impala_histogram_metric{quantile=\"0.999\"} 9.984\n"
      "impala_histogram_metric_count 10002\n"
      "impala_histogram_metric_sum 50015",
      "", "summary");
}

TEST_F(MetricsTest, HistogramTimeNSPrometheus) {
  MetricGroup metrics("HistoMetrics");
  TMetricDef metric_def =
      MakeTMetricDef("histogram-metric", TMetricKind::HISTOGRAM, TUnit::TIME_NS);
  constexpr int MAX_VALUE = 10000;
  HistogramMetric* metric =
      metrics.RegisterMetric(new HistogramMetric(metric_def, MAX_VALUE, 3));

  // Add value beyond limit to make sure it's recorded accurately.
  for (int i = 0; i <= MAX_VALUE + 1; ++i) metric->Update(i);

  std::stringstream val;
  metrics.ToPrometheus(true, &val);
  AssertPrometheus(val, "impala_histogram_metric",
      "impala_histogram_metric{quantile=\"0.2\"} 2.5e-06\n"
      "impala_histogram_metric{quantile=\"0.5\"} 5e-06\n"
      "impala_histogram_metric{quantile=\"0.7\"} 7.5e-06\n"
      "impala_histogram_metric{quantile=\"0.9\"} 9e-06\n"
      "impala_histogram_metric{quantile=\"0.95\"} 9.496e-06\n"
      "impala_histogram_metric{quantile=\"0.999\"} 9.984e-06\n"
      "impala_histogram_metric_count 10002\n"
      "impala_histogram_metric_sum 0.050015",
      "", "summary");
}

TEST_F(MetricsTest, HistogramTimeSPrometheus) {
  MetricGroup metrics("HistoMetrics");
  TMetricDef metric_def =
      MakeTMetricDef("histogram-metric", TMetricKind::HISTOGRAM, TUnit::TIME_S);
  constexpr int MAX_VALUE = 10000;
  HistogramMetric* metric =
      metrics.RegisterMetric(new HistogramMetric(metric_def, MAX_VALUE, 3));

  // Add value beyond limit to make sure it's recorded accurately.
  for (int i = 0; i <= MAX_VALUE + 1; ++i) metric->Update(i);

  std::stringstream val;
  metrics.ToPrometheus(true, &val);
  AssertPrometheus(val, "impala_histogram_metric",
      "impala_histogram_metric{quantile=\"0.2\"} 2500\n"
      "impala_histogram_metric{quantile=\"0.5\"} 5000\n"
      "impala_histogram_metric{quantile=\"0.7\"} 7500\n"
      "impala_histogram_metric{quantile=\"0.9\"} 9000\n"
      "impala_histogram_metric{quantile=\"0.95\"} 9496\n"
      "impala_histogram_metric{quantile=\"0.999\"} 9984\n"
      "impala_histogram_metric_count 10002\n"
      "impala_histogram_metric_sum 50015001",
      "", "summary");
}

TEST_F(MetricsTest, HistogramBytesPrometheus) {
  MetricGroup metrics("HistoMetrics");
  TMetricDef metric_def =
      MakeTMetricDef("histogram-metric", TMetricKind::HISTOGRAM, TUnit::BYTES);
  constexpr int MAX_VALUE = 10000;
  HistogramMetric* metric =
      metrics.RegisterMetric(new HistogramMetric(metric_def, MAX_VALUE, 3));

  // Add value beyond limit to make sure it's recorded accurately.
  for (int i = 0; i <= MAX_VALUE + 1; ++i) metric->Update(i);

  std::stringstream val;
  metrics.ToPrometheus(true, &val);
  AssertPrometheus(val, "impala_histogram_metric",
      "impala_histogram_metric{quantile=\"0.2\"} 2500\n"
      "impala_histogram_metric{quantile=\"0.5\"} 5000\n"
      "impala_histogram_metric{quantile=\"0.7\"} 7500\n"
      "impala_histogram_metric{quantile=\"0.9\"} 9000\n"
      "impala_histogram_metric{quantile=\"0.95\"} 9496\n"
      "impala_histogram_metric{quantile=\"0.999\"} 9984\n"
      "impala_histogram_metric_count 10002\n"
      "impala_histogram_metric_sum 50015001",
      "", "summary");
}

TEST_F(MetricsTest, HistogramUnitPrometheus) {
  MetricGroup metrics("HistoMetrics");
  TMetricDef metric_def =
      MakeTMetricDef("histogram-metric", TMetricKind::HISTOGRAM, TUnit::UNIT);
  constexpr int MAX_VALUE = 10000;
  HistogramMetric* metric =
      metrics.RegisterMetric(new HistogramMetric(metric_def, MAX_VALUE, 3));

  // Add value beyond limit to make sure it's recorded accurately.
  for (int i = 0; i <= MAX_VALUE + 1; ++i) metric->Update(i);

  std::stringstream val;
  metrics.ToPrometheus(true, &val);
  AssertPrometheus(val, "impala_histogram_metric",
      "impala_histogram_metric{quantile=\"0.2\"} 2500\n"
      "impala_histogram_metric{quantile=\"0.5\"} 5000\n"
      "impala_histogram_metric{quantile=\"0.7\"} 7500\n"
      "impala_histogram_metric{quantile=\"0.9\"} 9000\n"
      "impala_histogram_metric{quantile=\"0.95\"} 9496\n"
      "impala_histogram_metric{quantile=\"0.999\"} 9984\n"
      "impala_histogram_metric_count 10002\n"
      "impala_histogram_metric_sum 50015001",
      "", "summary");
}

TEST_F(MetricsTest, MetricGroupPrometheus) {
  std::stringstream exp_val_counter1, exp_val_counter2, exp_val_child_counter;
  exp_val_counter1 << "# HELP impala_counter1 description\n"
                      "# TYPE impala_counter1 counter\n"
                      "impala_counter1 2048\n";
  exp_val_counter2 << "# HELP impala_counter2 description\n"
                      "# TYPE impala_counter2 counter\n"
                      "impala_counter2 2048\n";
  exp_val_child_counter << "# HELP impala_child_counter description\n"
                           "# TYPE impala_child_counter counter\n"
                           "impala_child_counter 0\n";
  MetricGroup metrics("PrometheusTest");
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

  std::stringstream val;
  metrics.ToPrometheus(true, &val);
  EXPECT_STR_CONTAINS(val.str(), exp_val_counter1.str());
  EXPECT_STR_CONTAINS(val.str(), exp_val_counter2.str());
  EXPECT_STR_CONTAINS(val.str(), exp_val_child_counter.str());
}

// test with null metrics
TEST_F(MetricsTest, StatsMetricsNullPrometheus) {
  MetricGroup nullMetrics("StatsMetrics");
  AddMetricDef("", TMetricKind::STATS, TUnit::TIME_S);
  std::stringstream stats_val;
  nullMetrics.ToPrometheus(true, &stats_val);
  EXPECT_EQ("", stats_val.str());

  MetricGroup metrics("Metrics");
  AddMetricDef("test", TMetricKind::STATS, TUnit::TIME_S);
  metrics.ToPrometheus(true, &stats_val);
  EXPECT_EQ("", stats_val.str());
}
}

