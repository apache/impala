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

#include <gtest/gtest.h>

#include "util/open-file-options-util.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

namespace impala {

// Fully-qualified paths are used throughout so IsS3APath() never consults the
// (unavailable) ExecEnv default filesystem.
static const char* S3_PARQUET = "s3a://bucket/data/f.parq";
static const char* S3_TEXT = "s3a://bucket/data/f.txt";
static const char* HDFS_PARQUET = "hdfs://nn/data/f.parq";

static const string READ_POLICY_KEY = "fs.option.openfile.read.policy";
static const string RANDOM_POLICY = "random";
static const string LENGTH_KEY = "fs.option.openfile.length";

// Returns the value for 'key' in 'opts', or "" if absent.
static string Get(const io::OpenFileOptions& opts, const string& key) {
  for (const auto& kv : opts) {
    if (kv.first == key) return kv.second;
  }
  return "";
}

static bool Has(const io::OpenFileOptions& opts, const string& key) {
  for (const auto& kv : opts) {
    if (kv.first == key) return true;
  }
  return false;
}

TEST(OpenFileOptionsUtilTest, S3AGetsRandomReadPolicy) {
  // The policy does not depend on the file format - there is no format input. Both a
  // columnar and a row-oriented path are checked only to document that intent.
  io::OpenFileOptions opts = BuildOpenFileOptions(S3_PARQUET, 1024);
  EXPECT_EQ(RANDOM_POLICY, Get(opts, READ_POLICY_KEY));
  EXPECT_EQ("1024", Get(opts, LENGTH_KEY));

  opts = BuildOpenFileOptions(S3_TEXT, 1024);
  EXPECT_EQ(RANDOM_POLICY, Get(opts, READ_POLICY_KEY));
  EXPECT_EQ("1024", Get(opts, LENGTH_KEY));
}

TEST(OpenFileOptionsUtilTest, NonS3AGetsNoReadPolicy) {
  // Non-S3A -> read policy irrelevant, only the length.
  io::OpenFileOptions opts = BuildOpenFileOptions(HDFS_PARQUET, 4096);
  EXPECT_FALSE(Has(opts, READ_POLICY_KEY));
  EXPECT_EQ("4096", Get(opts, LENGTH_KEY));
}

TEST(OpenFileOptionsUtilTest, LengthOnlySetWhenPositive) {
  // Zero/stale length must not be sent (would truncate reads); policy still set.
  io::OpenFileOptions zero = BuildOpenFileOptions(S3_PARQUET, 0);
  EXPECT_FALSE(Has(zero, LENGTH_KEY));
  EXPECT_EQ(RANDOM_POLICY, Get(zero, READ_POLICY_KEY));

  // Non-S3A + non-positive length -> no options at all.
  io::OpenFileOptions negative = BuildOpenFileOptions(HDFS_PARQUET, -1);
  EXPECT_TRUE(negative.empty());
}

TEST(OpenFileOptionsUtilTest, NullPathIsSafe) {
  io::OpenFileOptions opts = BuildOpenFileOptions(nullptr, 512);
  EXPECT_FALSE(Has(opts, READ_POLICY_KEY));
  EXPECT_EQ("512", Get(opts, LENGTH_KEY));
}

}
