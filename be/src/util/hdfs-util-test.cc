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

namespace impala {

TEST(HdfsUtilTest, CheckFilesystemsAndBucketsMatch) {
  // We do this to retrieve the default FS from the frontend without starting the rest
  // of the ExecEnv services.
  ExecEnv exec_env;
  ASSERT_OK(exec_env.InitHadoopConfig());

  // Tests with both paths qualified.
  EXPECT_TRUE(FilesystemsAndBucketsMatch("s3a://dummybucket/temp_dir/temp_path",
                                         "s3a://dummybucket/temp_dir_2/temp_path_2"));
  EXPECT_FALSE(FilesystemsAndBucketsMatch("s3a://dummybucket/temp_dir/temp_path",
                                          "s3a://dummybucket_2/temp_dir_2/temp_path_2"));
  EXPECT_FALSE(FilesystemsAndBucketsMatch("s3a://dummybucket/temp_dir/temp_path",
                                          "hdfs://namenode/temp_dir2/temp_path_2"));
  EXPECT_FALSE(FilesystemsAndBucketsMatch("hdfs://namenode/temp_dir/temp_path",
                                          "hdfs://namenode_2/temp_dir2/temp_path_2"));
  EXPECT_TRUE(FilesystemsAndBucketsMatch("hdfs://namenode:9999/temp_dir/temp_path",
                                         "hdfs://namenode:9999/temp_dir2/temp_path_2"));
  EXPECT_FALSE(FilesystemsAndBucketsMatch("hdfs://namenode:9999/temp_dir/temp_path",
                                          "hdfs://namenode:8888/temp_dir2/temp_path_2"));
  EXPECT_TRUE(FilesystemsAndBucketsMatch("file:/path/to/dir/filename.parq",
                                         "file:///path/to/dir/filename.parq"));
  EXPECT_TRUE(FilesystemsAndBucketsMatch("file:/path/to/dir/filename.parq",
                                         "file:/path_2/to/dir/filename.parq"));
  EXPECT_TRUE(FilesystemsAndBucketsMatch("file:///path/to/dir/filename.parq",
                                         "file:/path_2/to/dir/filename.parq"));
  EXPECT_FALSE(FilesystemsAndBucketsMatch("file:/path/to/dir/filename.parq",
                                          "file2://path/to/dir/filename.parq"));
  EXPECT_FALSE(FilesystemsAndBucketsMatch("hdfs://",
                                          "s3a://dummybucket/temp_dir/temp_path"));
  EXPECT_TRUE(FilesystemsAndBucketsMatch("hdfs://namenode", "hdfs://namenode/"));
  EXPECT_TRUE(FilesystemsAndBucketsMatch("o3fs://bucket.volume.namenode:9862/path1",
                                         "o3fs://bucket.volume.namenode:9862/path2"));
  EXPECT_FALSE(FilesystemsAndBucketsMatch("o3fs://bucket1.volume.namenode:9862/path1",
                                          "o3fs://bucket2.volume.namenode:9862/path2"));

  // Provide unqualified paths for testing. With ofs, unqualified paths still include the
  // volume and bucket name which causes tests to fail. Add a prefix so they pass.
  std::string relpath1 = "tempdir/temppath";
  std::string relpath2 = "tempdir2/temppath2";
  std::string default_fs = exec_env.default_fs();
  if (default_fs.rfind(FILESYS_PREFIX_OFS, 0) == 0) {
    relpath1 = "volume/bucket/" + relpath1;
    relpath2 = "volume/bucket/" + relpath2;
    default_fs += "/volume/bucket";
  }

  // Tests with both paths paths unqualified.
  EXPECT_TRUE(FilesystemsAndBucketsMatch(relpath1.c_str(), relpath2.c_str()));

  // Tests with one path qualified and the other unqualified.
  EXPECT_TRUE(FilesystemsAndBucketsMatch(default_fs.c_str(), relpath1.c_str()));
  EXPECT_TRUE(FilesystemsAndBucketsMatch(relpath1.c_str(), default_fs.c_str()));
  EXPECT_FALSE(FilesystemsAndBucketsMatch("badscheme://namenode/temp_dir/temp_path",
                                "temp_dir/temp_path"));
  EXPECT_FALSE(FilesystemsAndBucketsMatch("badscheme://namenode:1234/temp_dir/temp_path",
                                "temp_dir/temp_path"));

  // Tests for ofs with volume/bucket
  EXPECT_TRUE(FilesystemsAndBucketsMatch("ofs://namenode:9862/volume/bucket/path1",
                                         "ofs://namenode:9862/volume/bucket/path2"));
  EXPECT_FALSE(FilesystemsAndBucketsMatch("ofs://namenode:9862/volume/bucket1/path1",
                                          "ofs://namenode:9862/volume/bucket2/path2"));
  EXPECT_FALSE(FilesystemsAndBucketsMatch("ofs://namenode:9862/volume1/bucket/path1",
                                          "ofs://namenode:9862/volume2/bucket/path2"));
  EXPECT_FALSE(FilesystemsAndBucketsMatch("ofs://namenode:9862/volume1/bucket1/path1",
                                          "ofs://namenode:9862/volume2/bucket2/path2"));
}

TEST(HdfsUtilTest, CheckGetBaseName) {
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
};
