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

#include "runtime/io/footer-cache.h"

#include <gtest/gtest.h>
#include <memory>
#include <string>

#include "common/status.h"
#include "gen-cpp/parquet_types.h"
#include "testutil/gtest-util.h"
#include "util/metrics.h"

namespace impala {
namespace io {

class FooterCacheTest : public ::testing::Test {
 protected:
  void SetUp() override {
    metrics_.reset(new MetricGroup("footer-cache-test"));
  }
  void TearDown() override {}
  
  std::unique_ptr<MetricGroup> metrics_;
};

// Test basic get/put operations with Parquet FileMetaData objects
TEST_F(FooterCacheTest, BasicGetPutParquet) {
  FooterCache cache(metrics_.get());
  ASSERT_OK(cache.Init(1024 * 1024, 4));  // 1MB capacity, 4 partitions

  std::string filename = "/data/test.parquet";
  int64_t mtime = 12345;

  // Create a test FileMetaData object
  auto test_metadata = std::make_shared<parquet::FileMetaData>();
  test_metadata->version = 1;
  test_metadata->num_rows = 1000;
  test_metadata->created_by = "test";

  // Initially, cache should be empty
  FooterCacheValue result = cache.GetFooter(filename, mtime);
  EXPECT_TRUE(std::holds_alternative<std::monostate>(result));

  // Put Parquet footer into cache
  Status status = cache.PutParquetFooter(filename, mtime, test_metadata);
  ASSERT_OK(status);

  // Now we should get it back
  result = cache.GetFooter(filename, mtime);
  EXPECT_TRUE(std::holds_alternative<std::shared_ptr<parquet::FileMetaData>>(result));
  auto cached_metadata = std::get<std::shared_ptr<parquet::FileMetaData>>(result);
  EXPECT_TRUE(cached_metadata != nullptr);
  EXPECT_EQ(cached_metadata->version, 1);
  EXPECT_EQ(cached_metadata->num_rows, 1000);
  EXPECT_EQ(cached_metadata->created_by, "test");
}

// Test basic get/put operations with ORC serialized tail
TEST_F(FooterCacheTest, BasicGetPutOrc) {
  FooterCache cache(metrics_.get());
  ASSERT_OK(cache.Init(1024 * 1024, 4));  // 1MB capacity, 4 partitions

  std::string filename = "/data/test.orc";
  int64_t mtime = 67890;

  // Create test ORC tail data
  std::string test_tail = "ORC serialized tail data";

  // Initially, cache should be empty
  FooterCacheValue result = cache.GetFooter(filename, mtime);
  EXPECT_TRUE(std::holds_alternative<std::monostate>(result));

  // Put ORC footer into cache
  Status status = cache.PutOrcFooter(filename, mtime, test_tail);
  ASSERT_OK(status);

  // Now we should get it back
  result = cache.GetFooter(filename, mtime);
  EXPECT_TRUE(std::holds_alternative<std::string>(result));
  std::string cached_tail = std::get<std::string>(result);
  EXPECT_EQ(cached_tail, test_tail);
}

// Test that mtime mismatch returns empty variant
TEST_F(FooterCacheTest, MtimeMismatch) {
  FooterCache cache(metrics_.get());
  ASSERT_OK(cache.Init(1024 * 1024, 4));

  std::string filename = "/data/test.parquet";
  int64_t mtime1 = 12345;
  int64_t mtime2 = 67890;

  auto test_metadata = std::make_shared<parquet::FileMetaData>();
  test_metadata->version = 1;
  test_metadata->num_rows = 500;

  // Put with mtime1
  ASSERT_OK(cache.PutParquetFooter(filename, mtime1, test_metadata));

  // Get with mtime1 should succeed
  FooterCacheValue result = cache.GetFooter(filename, mtime1);
  EXPECT_TRUE(std::holds_alternative<std::shared_ptr<parquet::FileMetaData>>(result));
  auto cached_metadata = std::get<std::shared_ptr<parquet::FileMetaData>>(result);
  EXPECT_EQ(cached_metadata->num_rows, 500);

  // Get with mtime2 should return empty variant (different mtime)
  result = cache.GetFooter(filename, mtime2);
  EXPECT_TRUE(std::holds_alternative<std::monostate>(result));
}

// Test LRU eviction with metrics
TEST_F(FooterCacheTest, LruEviction) {
  FooterCache cache(metrics_.get());
  ASSERT_OK(cache.Init(500, 1));  // Small capacity to trigger eviction, single partition

  // Get eviction counter
  IntCounter* evicted = metrics_->FindMetricForTesting<IntCounter>(
      "impala-server.io-mgr.footer-cache.entries-evicted");
  ASSERT_TRUE(evicted != nullptr);
  int64_t initial_evictions = evicted->GetValue();

  // Insert entries until cache is full and eviction occurs
  for (int i = 0; i < 5; ++i) {
    std::string filename = "/data/file" + std::to_string(i) + ".parquet";
    auto metadata = std::make_shared<parquet::FileMetaData>();
    metadata->version = 1;
    metadata->num_rows = i * 100;
    ASSERT_OK(cache.PutParquetFooter(filename, 100, metadata));
  }

  // Verify that eviction occurred (eviction counter increased)
  int64_t final_evictions = evicted->GetValue();
  EXPECT_GT(final_evictions, initial_evictions);
}
// Test metrics tracking
TEST_F(FooterCacheTest, MetricsTracking) {
  FooterCache cache(metrics_.get());
  ASSERT_OK(cache.Init(1024 * 1024, 4));

  std::string filename = "/data/test.parquet";
  int64_t mtime = 12345;
  auto test_metadata = std::make_shared<parquet::FileMetaData>();
  test_metadata->version = 1;
  test_metadata->num_rows = 1000;

  // Get metrics
  IntCounter* hits = metrics_->FindMetricForTesting<IntCounter>("impala-server.io-mgr.footer-cache.hits");
  IntCounter* misses = metrics_->FindMetricForTesting<IntCounter>("impala-server.io-mgr.footer-cache.misses");
  IntGauge* entries_in_use = metrics_->FindMetricForTesting<IntGauge>("impala-server.io-mgr.footer-cache.entries-in-use");

  ASSERT_TRUE(hits != nullptr);
  ASSERT_TRUE(misses != nullptr);
  ASSERT_TRUE(entries_in_use != nullptr);

  // Initial miss
  FooterCacheValue result = cache.GetFooter(filename, mtime);
  EXPECT_TRUE(std::holds_alternative<std::monostate>(result));
  EXPECT_EQ(misses->GetValue(), 1);
  EXPECT_EQ(hits->GetValue(), 0);

  // Put footer
  ASSERT_OK(cache.PutParquetFooter(filename, mtime, test_metadata));
  EXPECT_EQ(entries_in_use->GetValue(), 1);

  // Cache hit
  result = cache.GetFooter(filename, mtime);
  EXPECT_TRUE(std::holds_alternative<std::shared_ptr<parquet::FileMetaData>>(result));
  auto cached_metadata = std::get<std::shared_ptr<parquet::FileMetaData>>(result);
  EXPECT_EQ(cached_metadata->num_rows, 1000);
  EXPECT_EQ(hits->GetValue(), 1);
  EXPECT_EQ(misses->GetValue(), 1);
}

// Test partitioning (different files go to different partitions)
TEST_F(FooterCacheTest, Partitioning) {
  FooterCache cache(metrics_.get());
  ASSERT_OK(cache.Init(1024 * 1024, 16));  // 1MB, 16 partitions

  EXPECT_EQ(cache.NumPartitions(), 16);
  EXPECT_EQ(cache.Capacity(), 1024 * 1024);

  // Insert files - they should be distributed across partitions
  for (int i = 0; i < 32; ++i) {
    std::string filename = "/data/file" + std::to_string(i) + ".parquet";
    auto metadata = std::make_shared<parquet::FileMetaData>();
    metadata->version = 1;
    metadata->num_rows = i * 100;
    ASSERT_OK(cache.PutParquetFooter(filename, 100, metadata));
  }

  // All should be retrievable
  for (int i = 0; i < 32; ++i) {
    std::string filename = "/data/file" + std::to_string(i) + ".parquet";
    FooterCacheValue result = cache.GetFooter(filename, 100);
    EXPECT_TRUE(std::holds_alternative<std::shared_ptr<parquet::FileMetaData>>(result));
    auto cached_metadata = std::get<std::shared_ptr<parquet::FileMetaData>>(result);
    EXPECT_EQ(cached_metadata->num_rows, i * 100);
  }
}

}  // namespace io
}  // namespace impala
