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

#include "filesystem-util.h"

#include <boost/filesystem.hpp>
#include <sys/stat.h>

#include "common/logging.h"
#include "testutil/gtest-util.h"
#include "util/test-info.h"

#include "common/names.h"

using namespace impala;
namespace filesystem = boost::filesystem;
using filesystem::path;

TEST(FilesystemUtil, rlimit) {
  ASSERT_LT(0ul, FileSystemUtil::MaxNumFileHandles());
}

TEST(FilesystemUtil, RemoveAndCreateDirectory) {
  // Setup a temporary directory with one subdir
  path dir = filesystem::unique_path();
  path subdir1 = dir / "impala1";
  path subdir2 = dir / "impala2";
  path subdir3 = dir / "a" / "longer" / "path";
  filesystem::create_directories(subdir1);
  // Test error cases by removing write permissions on root dir to prevent
  // creation/deletion of subdirs
  chmod(dir.string().c_str(), 0);
  EXPECT_FALSE(FileSystemUtil::RemoveAndCreateDirectory(subdir1.string()).ok());
  EXPECT_FALSE(FileSystemUtil::RemoveAndCreateDirectory(subdir2.string()).ok());
  // Test success cases by adding write permissions back
  chmod(dir.string().c_str(), S_IRWXU);
  EXPECT_OK(FileSystemUtil::RemoveAndCreateDirectory(subdir1.string()));
  EXPECT_OK(FileSystemUtil::RemoveAndCreateDirectory(subdir2.string()));
  // Check that directories were created
  EXPECT_TRUE(filesystem::exists(subdir1) && filesystem::is_directory(subdir1));
  EXPECT_TRUE(filesystem::exists(subdir2) && filesystem::is_directory(subdir2));
  // Exercise VerifyIsDirectory
  EXPECT_OK(FileSystemUtil::VerifyIsDirectory(subdir1.string()));
  EXPECT_OK(FileSystemUtil::VerifyIsDirectory(subdir2.string()));
  EXPECT_FALSE(FileSystemUtil::VerifyIsDirectory(subdir3.string()).ok());
  // Check that nested directories can be created
  EXPECT_OK(FileSystemUtil::RemoveAndCreateDirectory(subdir3.string()));
  EXPECT_TRUE(filesystem::exists(subdir3) && filesystem::is_directory(subdir3));
  // Cleanup
  filesystem::remove_all(dir);
}

TEST(FilesystemUtil, Paths) {
  // Canonical path must not be empty
  EXPECT_FALSE(FileSystemUtil::IsCanonicalPath(""));
  // Canonical paths must be absolute
  EXPECT_FALSE(FileSystemUtil::IsCanonicalPath("a/b"));
  // Canonical paths must not contain "//", "..", "." components
  EXPECT_FALSE(FileSystemUtil::IsCanonicalPath(".."));
  EXPECT_FALSE(FileSystemUtil::IsCanonicalPath("/.."));
  EXPECT_FALSE(FileSystemUtil::IsCanonicalPath("/a/b/.."));
  EXPECT_FALSE(FileSystemUtil::IsCanonicalPath("/a/b/../c"));
  EXPECT_FALSE(FileSystemUtil::IsCanonicalPath("."));
  EXPECT_FALSE(FileSystemUtil::IsCanonicalPath("/."));
  EXPECT_FALSE(FileSystemUtil::IsCanonicalPath("/a/b/."));
  EXPECT_FALSE(FileSystemUtil::IsCanonicalPath("/a/b/./c"));
  EXPECT_FALSE(FileSystemUtil::IsCanonicalPath("/a//b"));
  // Canonical paths must not end with '/'
  EXPECT_FALSE(FileSystemUtil::IsCanonicalPath("/a/b/"));

  // The following are valid canonical paths
  EXPECT_TRUE(FileSystemUtil::IsCanonicalPath("/"));
  EXPECT_TRUE(FileSystemUtil::IsCanonicalPath("/a"));
  EXPECT_TRUE(FileSystemUtil::IsCanonicalPath("/ab/cd/efg"));

  // The following should fail as "/a/b" is not a prefix of "/a/bc"
  EXPECT_FALSE(FileSystemUtil::IsPrefixPath("/a/b", "/a/bc"));

  // The following calls should succed.
  EXPECT_TRUE(FileSystemUtil::IsPrefixPath("/", "/"));
  EXPECT_TRUE(FileSystemUtil::IsPrefixPath("/", "/a/bc/def"));
  EXPECT_TRUE(FileSystemUtil::IsPrefixPath("/a", "/a/bc/def"));
  EXPECT_TRUE(FileSystemUtil::IsPrefixPath("/a/bc", "/a/bc/def"));
  EXPECT_TRUE(FileSystemUtil::IsPrefixPath("/a/bc/def", "/a/bc/def"));

  // 'relpath' should be empty if path equals to the start directory.
  string relpath;
  EXPECT_TRUE(FileSystemUtil::GetRelativePath("/", "/", &relpath));
  EXPECT_EQ(string(""), relpath);
  EXPECT_TRUE(FileSystemUtil::GetRelativePath("/a/bc/def", "/a/bc/def", &relpath));
  EXPECT_EQ(string(""), relpath);

  // The following should fail as "/a/b" is not a prefix of "/a/bc" path.
  EXPECT_FALSE(FileSystemUtil::GetRelativePath("/a/bc", "/a/b", &relpath));

  // The following calls should succeed.
  EXPECT_TRUE(FileSystemUtil::GetRelativePath("/a/bc/def", "/", &relpath));
  EXPECT_EQ(string("a/bc/def"), relpath);

  EXPECT_TRUE(FileSystemUtil::GetRelativePath("/a/bc/def", "/a", &relpath));
  EXPECT_EQ(string("bc/def"), relpath);

  EXPECT_TRUE(FileSystemUtil::GetRelativePath("/a/bc/def", "/a/bc", &relpath));
  EXPECT_EQ(string("def"), relpath);
}

