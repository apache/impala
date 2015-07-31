// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "util/metrics.h"
#include "util/collection-metrics.h"
#include "util/memory-metrics.h"

#include <gtest/gtest.h>
#include <boost/scoped_ptr.hpp>
#include <limits>
#include <map>

#include "util/jni-util.h"
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
  IntCounter* int_counter = metrics.AddCounter("counter", 0L);
  AssertValue(int_counter, 0, "0");
  int_counter->Increment(1);
  AssertValue(int_counter, 1, "1");
  int_counter->Increment(10);
  AssertValue(int_counter, 11, "11");
  int_counter->set_value(3456);
  AssertValue(int_counter, 3456, "3.46K");

  AddMetricDef("counter_with_units", TMetricKind::COUNTER, TUnit::BYTES);
  IntCounter* int_counter_with_units =
      metrics.AddCounter("counter_with_units", 10L);
  AssertValue(int_counter_with_units, 10, "10.00 B");
}

TEST_F(MetricsTest, GaugeMetrics) {
  MetricGroup metrics("GaugeMetrics");
  AddMetricDef("gauge", TMetricKind::GAUGE, TUnit::NONE);
  IntGauge* int_gauge = metrics.AddGauge("gauge", 0L);
  AssertValue(int_gauge, 0, "0");
  int_gauge->Increment(-1);
  AssertValue(int_gauge, -1, "-1");
  int_gauge->Increment(10);
  AssertValue(int_gauge, 9, "9");
  int_gauge->set_value(3456);
  AssertValue(int_gauge, 3456, "3456");

  AddMetricDef("gauge_with_units", TMetricKind::GAUGE, TUnit::TIME_S);
  IntGauge* int_gauge_with_units =
      metrics.AddGauge("gauge_with_units", 10L);
  AssertValue(int_gauge_with_units, 10, "10s000ms");
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
  EXPECT_TRUE(isnan(gauge->value()));
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
  RegisterMemoryMetrics(&metrics, false);
  // Smoke test to confirm that tcmalloc metrics are returning reasonable values.
  UIntGauge* bytes_in_use =
      metrics.FindMetricForTesting<UIntGauge>("tcmalloc.bytes-in-use");
  DCHECK(bytes_in_use != NULL);

  uint64_t cur_in_use = bytes_in_use->value();
  EXPECT_GT(cur_in_use, 0);

  // Allocate 100MB to increase the number of bytes used. TCMalloc may also give up some
  // bytes during this allocation, so this allocation is deliberately large to ensure that
  // the bytes used metric goes up net.
  scoped_ptr<vector<uint64_t> > chunk(new vector<uint64_t>(100 * 1024 * 1024));
  EXPECT_GT(bytes_in_use->value(), cur_in_use);

  UIntGauge* total_bytes_reserved =
      metrics.FindMetricForTesting<UIntGauge>("tcmalloc.total-bytes-reserved");
  DCHECK(total_bytes_reserved != NULL);
  DCHECK_GT(total_bytes_reserved->value(), 0);

  UIntGauge* pageheap_free_bytes =
      metrics.FindMetricForTesting<UIntGauge>("tcmalloc.pageheap-free-bytes");
  DCHECK(pageheap_free_bytes != NULL);

  UIntGauge* pageheap_unmapped_bytes =
      metrics.FindMetricForTesting<UIntGauge>("tcmalloc.pageheap-unmapped-bytes");
  EXPECT_TRUE(pageheap_unmapped_bytes != NULL);
#endif
}

TEST_F(MetricsTest, JvmMetrics) {
  MetricGroup metrics("JvmMetrics");
  RegisterMemoryMetrics(&metrics, true);
  UIntGauge* jvm_total_used =
      metrics.GetChildGroup("jvm")->FindMetricForTesting<UIntGauge>(
          "jvm.total.current-usage-bytes");
  ASSERT_TRUE(jvm_total_used != NULL);
  EXPECT_GT(jvm_total_used->value(), 0);
  UIntGauge* jvm_peak_total_used =
      metrics.GetChildGroup("jvm")->FindMetricForTesting<UIntGauge>(
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
  metrics.AddCounter("counter", 0L);
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
  metrics.AddGauge("gauge", 10L);
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

  metrics.GetChildGroup("child1");
  AddMetricDef("child_counter", TMetricKind::COUNTER, TUnit::BYTES, "description");
  metrics.GetChildGroup("child2")->AddCounter("child_counter", 0);

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
}

}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitThreading();
  impala::JniUtil::Init();

  return RUN_ALL_TESTS();
}
