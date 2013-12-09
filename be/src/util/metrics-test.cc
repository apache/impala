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
#include "util/non-primitive-metrics.h"
#include "util/memory-metrics.h"

#include <gtest/gtest.h>
#include <boost/scoped_ptr.hpp>

#include "util/jni-util.h"
#include "util/thread.h"

using namespace boost;
using namespace std;

namespace impala {

class MetricsTest : public testing::Test {
 public:
  Metrics* metrics() { return metrics_.get(); }
  MetricsTest() : metrics_(new Metrics()) {
    bool_metric_ = metrics_->CreateAndRegisterPrimitiveMetric("bool",
        false);
    int_metric_ = metrics_->CreateAndRegisterPrimitiveMetric("int", 0L);
    double_metric_ = metrics_->CreateAndRegisterPrimitiveMetric("double",
        1.23);
    string_metric_ = metrics_->CreateAndRegisterPrimitiveMetric("string",
        string("hello world"));
    inf_metric_ = metrics_->CreateAndRegisterPrimitiveMetric("inf_double", 0.0);
    stats_metric_ = metrics_->RegisterMetric(new StatsMetric<double>("stats"));

    vector<int> items;
    items.push_back(1); items.push_back(2); items.push_back(3);
    list_metric_ = metrics_->RegisterMetric(new ListMetric<int>("list", items));
    set<int> item_set;
    item_set.insert(4); item_set.insert(5); item_set.insert(6);
    set_metric_ = metrics_->RegisterMetric(new SetMetric<int>("set", item_set));

    set<string> string_set;
    string_set.insert("one"); string_set.insert("two");
    string_set_metric_ = metrics_->RegisterMetric(new SetMetric<string>("string_set",
                                                                        string_set));

    RegisterMemoryMetrics(metrics_.get(), true);
  }
  Metrics::BooleanMetric* bool_metric_;
  Metrics::IntMetric* int_metric_;
  Metrics::DoubleMetric* double_metric_;
  Metrics::DoubleMetric* inf_metric_;
  Metrics::StringMetric* string_metric_;

  ListMetric<int>* list_metric_;
  SetMetric<int>* set_metric_;
  SetMetric<string>* string_set_metric_; // For quote testing
  StatsMetric<double>* stats_metric_;

 private:
  boost::scoped_ptr<Metrics> metrics_;
};

TEST_F(MetricsTest, IntMetrics) {
  EXPECT_NE(metrics()->DebugString().find("int:0"), string::npos);
  int_metric_->Update(3);
  EXPECT_NE(metrics()->DebugString().find("int:3"), string::npos);
}

TEST_F(MetricsTest, DoubleMetrics) {
  EXPECT_NE(metrics()->DebugString().find("double:1.23"), string::npos);
  double_metric_->Update(2.34);
  EXPECT_NE(metrics()->DebugString().find("double:2.34"), string::npos);
}

TEST_F(MetricsTest, StringMetrics) {
  EXPECT_NE(metrics()->DebugString().find("string:hello world"), string::npos);
  string_metric_->Update("foo bar");
  EXPECT_NE(metrics()->DebugString().find("string:foo bar"), string::npos);
}

TEST_F(MetricsTest, BooleanMetrics) {
  EXPECT_NE(metrics()->DebugString().find("bool:0"), string::npos);
  bool_metric_->Update(true);
  EXPECT_NE(metrics()->DebugString().find("bool:1"), string::npos);
}

TEST_F(MetricsTest, ListMetrics) {
  EXPECT_NE(metrics()->DebugString().find("list:[1, 2, 3]"), string::npos);
  list_metric_->Update(vector<int>());
  EXPECT_NE(metrics()->DebugString().find("list:[]"), string::npos);
}

TEST_F(MetricsTest, SetMetrics) {
  EXPECT_NE(metrics()->DebugString().find("set:[4, 5, 6]"), string::npos);
  set_metric_->Add(7);
  set_metric_->Add(7);
  set_metric_->Remove(4);
  set_metric_->Remove(4);
  EXPECT_NE(metrics()->DebugString().find("set:[5, 6, 7]"), string::npos);
}

TEST_F(MetricsTest, TestAndSet) {
  int_metric_->Update(1);
  // Expect update to fail
  EXPECT_EQ(int_metric_->TestAndSet(5, 0), 1);
  EXPECT_EQ(int_metric_->value(), 1);

  // Successful update
  EXPECT_EQ(int_metric_->TestAndSet(5, 1), 1);
  EXPECT_EQ(int_metric_->value(), 5);
}

TEST_F(MetricsTest, Increment) {
  int_metric_->Update(1);
  EXPECT_EQ(int_metric_->Increment(10), 11);
  EXPECT_EQ(int_metric_->value(), 11);
}

TEST_F(MetricsTest, JsonQuoting) {
  // Strings should be quoted in Json output
  EXPECT_NE(metrics()->DebugStringJson().find("\"string\": \"hello world\""),
            string::npos);

  // Other types should not be quoted
  EXPECT_NE(metrics()->DebugStringJson().find("\"bool\": 0"), string::npos);

  // Strings in sets should be quoted
  EXPECT_NE(metrics()->DebugStringJson().find("\"string_set\": [\"one\", \"two\"]"),
            string::npos);

  // Other types in sets should not be quoted
  EXPECT_NE(metrics()->DebugStringJson().find("\"set\": [4, 5, 6]"), string::npos);
}

TEST_F(MetricsTest, NonfiniteDoubles) {
  inf_metric_->Update(1.0 / 0.0);
  EXPECT_NE(metrics()->DebugString().find("inf_double:inf"), string::npos);
  EXPECT_NE(metrics()->DebugStringJson().find("\"inf_double\": null"), string::npos);

  inf_metric_->Update(0.0 / 0.0);
  // 0.0 / 0.0 can either be nan or -nan (compiler dependant)
  EXPECT_TRUE(metrics()->DebugString().find("inf_double:-nan") != string::npos ||
              metrics()->DebugString().find("inf_double:nan") != string::npos);
  EXPECT_NE(metrics()->DebugStringJson().find("\"inf_double\": null"), string::npos);
}

TEST_F(MetricsTest, StatsMetric) {
  // Uninitialised stats metrics don't print anything other than the count
  EXPECT_NE(metrics()->DebugStringJson().find("\"stats\": { \"count\": 0 }"),
            string::npos);

  EXPECT_NE(metrics()->DebugString().find("stats: count: 0"), string::npos);
  EXPECT_EQ(metrics()->DebugString().find("stats: count: 0, mean:"), string::npos);

  stats_metric_->Update(0.0);
  stats_metric_->Update(1.0);
  stats_metric_->Update(2.0);

  EXPECT_NE(metrics()->DebugString().find(
      "stats: count: 3, last: 2, min: 0, max: 2, mean: 1, stddev: 0.816497"),
      string::npos);
}

TEST_F(MetricsTest, MemMetric) {
#ifndef ADDRESS_SANITIZER
  // Smoke test to confirm that tcmalloc metrics are returning reasonable values.
  Metrics::Metric<uint64_t>* bytes_in_use =
      metrics()->GetMetric<Metrics::Metric<uint64_t> >("tcmalloc.bytes-in-use");
  EXPECT_TRUE(bytes_in_use != NULL);

  uint64_t cur_in_use = bytes_in_use->value();
  scoped_ptr<uint64_t> chunk(new uint64_t);
  EXPECT_GT(cur_in_use, 0);
  EXPECT_GT(bytes_in_use->value(), cur_in_use);

  Metrics::Metric<uint64_t>* total_bytes_reserved =
      metrics()->GetMetric<Metrics::Metric<uint64_t> >("tcmalloc.total-bytes-reserved");
  EXPECT_TRUE(total_bytes_reserved != NULL);
  EXPECT_GT(total_bytes_reserved->value(), 0);

  Metrics::Metric<uint64_t>* pageheap_free_bytes =
      metrics()->GetMetric<Metrics::Metric<uint64_t> >("tcmalloc.pageheap-free-bytes");
  EXPECT_TRUE(pageheap_free_bytes != NULL);

  Metrics::Metric<uint64_t>* pageheap_unmapped_bytes =
      metrics()->GetMetric<Metrics::Metric<uint64_t> >(
          "tcmalloc.pageheap-unmapped-bytes");
  EXPECT_TRUE(pageheap_unmapped_bytes != NULL);
#endif
}

TEST_F(MetricsTest, JvmMetrics) {
  Metrics::Metric<uint64_t>* jvm_total_used =
      metrics()->GetMetric<Metrics::Metric<uint64_t> >("jvm.total.current-usage-bytes");
  ASSERT_TRUE(jvm_total_used != NULL);
  EXPECT_GT(jvm_total_used->value(), 0);
  Metrics::Metric<uint64_t>* jvm_peak_total_used =
      metrics()->GetMetric<Metrics::Metric<uint64_t> >(
          "jvm.total.peak-current-usage-bytes");
  ASSERT_TRUE(jvm_peak_total_used != NULL);
  EXPECT_GT(jvm_peak_total_used->value(), 0);
}

}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitThreading();
  impala::JniUtil::Init();

  return RUN_ALL_TESTS();
}
