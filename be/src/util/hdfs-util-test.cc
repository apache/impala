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

#include "hdfs-util.h"

#include "testutil/gtest-util.h"
#include "common/init.h"
#include "common/logging.h"
#include "util/test-info.h"
#include "runtime/exec-env.h"
#include "service/fe-support.h"

using namespace impala;

DECLARE_bool(enable_webserver);

TEST(HdfsUtilTest, CheckFilesystemsMatch) {
  // We do not want to start the webserver.
  FLAGS_enable_webserver = false;
  ExecEnv* exec_env = new ExecEnv();

  // We do this to retrieve the default FS from the frontend.
  // It doesn't matter if initializing the ExecEnv fails.
  discard_result(exec_env->Init());

  // Tests with both paths qualified.
  EXPECT_TRUE(FilesystemsMatch("s3a://dummybucket/temp_dir/temp_path",
                               "s3a://dummybucket/temp_dir_2/temp_path_2"));
  EXPECT_FALSE(FilesystemsMatch("s3a://dummybucket/temp_dir/temp_path",
                                "s3a://dummybucket_2/temp_dir_2/temp_path_2"));
  EXPECT_FALSE(FilesystemsMatch("s3a://dummybucket/temp_dir/temp_path",
                                "hdfs://namenode/temp_dir2/temp_path_2"));
  EXPECT_FALSE(FilesystemsMatch("hdfs://namenode/temp_dir/temp_path",
                                "hdfs://namenode_2/temp_dir2/temp_path_2"));
  EXPECT_TRUE(FilesystemsMatch("hdfs://namenode:9999/temp_dir/temp_path",
                                "hdfs://namenode:9999/temp_dir2/temp_path_2"));
  EXPECT_FALSE(FilesystemsMatch("hdfs://namenode:9999/temp_dir/temp_path",
                                "hdfs://namenode:8888/temp_dir2/temp_path_2"));
  EXPECT_TRUE(FilesystemsMatch("file:/path/to/dir/filename.parq",
                               "file:///path/to/dir/filename.parq"));
  EXPECT_TRUE(FilesystemsMatch("file:/path/to/dir/filename.parq",
                               "file:/path_2/to/dir/filename.parq"));
  EXPECT_TRUE(FilesystemsMatch("file:///path/to/dir/filename.parq",
                               "file:/path_2/to/dir/filename.parq"));
  EXPECT_FALSE(FilesystemsMatch("file:/path/to/dir/filename.parq",
                                "file2://path/to/dir/filename.parq"));
  EXPECT_FALSE(FilesystemsMatch("hdfs://", "s3a://dummybucket/temp_dir/temp_path"));
  EXPECT_TRUE(FilesystemsMatch("hdfs://namenode", "hdfs://namenode/"));

  // Tests with both paths paths unqualified.
  EXPECT_TRUE(FilesystemsMatch("tempdir/temppath", "tempdir2/temppath2"));

  // Tests with one path qualified and the other unqualified.
  const char* default_fs = exec_env->default_fs().c_str();
  EXPECT_TRUE(FilesystemsMatch(default_fs, "temp_dir/temp_path"));
  EXPECT_TRUE(FilesystemsMatch("temp_dir/temp_path", default_fs));
  EXPECT_FALSE(FilesystemsMatch("badscheme://namenode/temp_dir/temp_path",
                                "temp_dir/temp_path"));
  EXPECT_FALSE(FilesystemsMatch("badscheme://namenode:1234/temp_dir/temp_path",
                                "temp_dir/temp_path"));
}

TEST(HdfsUtilTest, CheckGetBaseName) {
  // We do not want to start the webserver.
  FLAGS_enable_webserver = false;

  EXPECT_EQ(".", GetBaseName("s3a://"));
  EXPECT_EQ(".", GetBaseName("s3a://dummybucket"));
  EXPECT_EQ(".", GetBaseName("s3a://dummybucket/"));
  EXPECT_EQ(".", GetBaseName("s3a://dummybucket//"));
  EXPECT_EQ("c", GetBaseName("s3a://dummybucket/c/"));
  EXPECT_EQ("c", GetBaseName("s3a://dummybucket/a/b/c"));
  EXPECT_EQ("c", GetBaseName("s3a://dummybucket/a/b/c///"));

  EXPECT_EQ(".", GetBaseName("s3a:///"));
  EXPECT_EQ("c", GetBaseName("s3a:///c"));
  EXPECT_EQ("c", GetBaseName("s3a:///a/b//c/"));

  EXPECT_EQ(".", GetBaseName("hdfs://"));
  EXPECT_EQ(".", GetBaseName("hdfs://localhost"));
  EXPECT_EQ(".", GetBaseName("hdfs://localhost:8020"));
  EXPECT_EQ(".", GetBaseName("hdfs://localhost:8020/"));
  EXPECT_EQ(".", GetBaseName("hdfs://localhost:8020//"));
  EXPECT_EQ("c", GetBaseName("hdfs://localhost:8020/c"));
  EXPECT_EQ("c", GetBaseName("hdfs://localhost:8020/c/"));
  EXPECT_EQ("c", GetBaseName("hdfs://localhost:8020/a//b/c"));
  EXPECT_EQ("c", GetBaseName("hdfs://localhost:8020//a//b/c/"));
  EXPECT_EQ("c", GetBaseName("hdfs://localhost:8020//a//b/c///"));

  EXPECT_EQ(".", GetBaseName("hdfs:///"));
  EXPECT_EQ("c", GetBaseName("hdfs:///c"));
  EXPECT_EQ("c", GetBaseName("hdfs:///a/b//c/"));

  EXPECT_EQ(".", GetBaseName(""));
  EXPECT_EQ(".", GetBaseName("/"));
  EXPECT_EQ("c", GetBaseName("c"));
  EXPECT_EQ("c", GetBaseName("/c"));
  EXPECT_EQ("c", GetBaseName("/a//b/c/"));
}
