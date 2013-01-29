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
#include <gtest/gtest.h>
#include <boost/scoped_ptr.hpp>

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
  }
  Metrics::BooleanMetric* bool_metric_;
  Metrics::IntMetric* int_metric_;
  Metrics::DoubleMetric* double_metric_;
  Metrics::StringMetric* string_metric_;

  ListMetric<int>* list_metric_;
  SetMetric<int>* set_metric_;
  SetMetric<string>* string_set_metric_; // For quote testing

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
}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
