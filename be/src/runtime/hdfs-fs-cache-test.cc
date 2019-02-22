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

#include <string>
#include <gutil/strings/substitute.h>

#include "runtime/hdfs-fs-cache.h"
#include "testutil/gtest-util.h"
#include "common/names.h"

namespace impala {
void ValidateNameNode(const string& path, const string& expected_namenode,
    const string& expected_error) {
  string err;
  string namenode = HdfsFsCache::GetNameNodeFromPath(path, &err);
  if (err.empty()) {
    EXPECT_EQ(namenode, expected_namenode);
  } else {
    EXPECT_EQ(err, expected_error);
  }
}

TEST(HdfsFsCacheTest, Basic) {
  // Validate qualified paths for various FileSystems
  ValidateNameNode(string("hdfs://localhost:25000/usr/hive"),
      string("hdfs://localhost:25000/"), string(""));
  ValidateNameNode(string("hdfs://nameservice1/usr/hive"),
      string("hdfs://nameservice1/"), string(""));
  ValidateNameNode(string("s3a://hdfsbucket/usr/hive"),
      string("s3a://hdfsbucket/"), string(""));
  ValidateNameNode(string("hdfs://testserver/"), string("hdfs://testserver/"),
      string(""));

  // Validate local path
  ValidateNameNode(string("file:///usr/hive"), string("file:///"), string(""));
  ValidateNameNode(string("file:/usr/hive"), string("file:///"), string(""));

  // Validate unqualified path
  ValidateNameNode(string("/usr/hive"), string("default"), string(""));

  // Validate invalid path
  string path("://usr/hive");
  ValidateNameNode(string("://usr/hive"), string(""),
      Substitute("Path missing scheme: $0", path));
  path = string("hdfs://test_invalid_path");
  ValidateNameNode(path, string(""),
      Substitute("Path missing '/' after authority: $0", path));
}

}

