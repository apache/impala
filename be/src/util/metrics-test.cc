// Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#include "util/metrics.h"
#include <gtest/gtest.h>
#include <boost/scoped_ptr.hpp>

using namespace boost;
using namespace std;

namespace impala {

class MetricsTest : public testing::Test {
 public:
  Metrics* metrics() { return metrics_.get(); }
  MetricsTest() : metrics_(new Metrics()) { 
    bool_metric_ = metrics_->CreateMetric("bool",
        false);
    int_metric_ = metrics_->CreateMetric("int", 0L);
    double_metric_ = metrics_->CreateMetric("double",
        1.23);
    string_metric_ = metrics_->CreateMetric("string", 
        string("hello world"));
  }
  Metrics::BooleanMetric* bool_metric_;
  Metrics::IntMetric* int_metric_;
  Metrics::DoubleMetric* double_metric_;
  Metrics::StringMetric* string_metric_;
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
}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
