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
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "common/logging.h"
#include "testutil/gtest-util.h"
#include "util/scope-exit-trigger.h"
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

TEST(FilesystemUtil, FindFileInPath) {
  path dir = filesystem::unique_path();

  // Paths to existent and non-existent dirs.
  path subdir1 = dir / "impala1";
  path subdir2 = dir / "impala2";
  filesystem::create_directories(subdir1);

  // Paths to existent and non-existent file.
  path file1 = dir / "a_file1";
  path file2 = dir / "a_file2";
  ASSERT_OK(FileSystemUtil::CreateFile(file1.string()));

  path file3 = subdir1 / "a_file3";
  ASSERT_OK(FileSystemUtil::CreateFile(file3.string()));

  string path = FileSystemUtil::FindFileInPath(dir.string(), "a.*1");
  EXPECT_EQ(file1.string(), path);
  path = FileSystemUtil::FindFileInPath(dir.string(), "a.*2");
  EXPECT_EQ("", path);
  path = FileSystemUtil::FindFileInPath(dir.string(), "a.*3");
  EXPECT_EQ("", path);
  path = FileSystemUtil::FindFileInPath(dir.string(), "");
  EXPECT_EQ("", path);

  path = FileSystemUtil::FindFileInPath(dir.string() + "/*", "a_file1");
  EXPECT_EQ(file1.string(), path);
  path = FileSystemUtil::FindFileInPath(dir.string(), "a_file2");
  EXPECT_EQ("", path);

  path = FileSystemUtil::FindFileInPath(dir.string(), "impala1");
  EXPECT_EQ(subdir1.string(), path);
  path = FileSystemUtil::FindFileInPath(dir.string(), "impala2");
  EXPECT_EQ("", path);

  path = FileSystemUtil::FindFileInPath(subdir1.string(), "a.*3");
  EXPECT_EQ(file3.string(), path);
  path = FileSystemUtil::FindFileInPath(subdir1.string(), "anything");
  EXPECT_EQ("", path);
  path = FileSystemUtil::FindFileInPath(subdir2.string(), ".*");
  EXPECT_EQ("", path);

  path = FileSystemUtil::FindFileInPath(file1.string(), "a.*1");
  EXPECT_EQ(file1.string(), path);
  path = FileSystemUtil::FindFileInPath(file1.string(), "a.*2");
  EXPECT_EQ("", path);
  path = FileSystemUtil::FindFileInPath(file2.string(), "a.*2");
  EXPECT_EQ("", path);

  // Cleanup
  filesystem::remove_all(dir);
}

// This test exercises the handling of different directory entry types by GetEntryNames().
TEST(FilesystemUtil, DirEntryTypes) {
  // Setup a temporary directory with one subdir
  path base_dir = filesystem::unique_path();
  path dir = base_dir / "impala-dir";
  path subdir = dir / "impala-subdir";
  path file = dir / "impala-file";

  // Always cleanup on exit.
  auto remove_dir = MakeScopeExitTrigger([&dir]() { filesystem::remove_all(dir); });

  // Create the test directory and file.
  ASSERT_OK(FileSystemUtil::RemoveAndCreateDirectory(subdir.string()));
  ASSERT_OK(FileSystemUtil::CreateFile(file.string()));

  // Check if the system supports listing directory entries' types for 'impala-dir'.
  // Some filesystem may not have full support for it on older platforms.
  DIR* dir_stream = opendir(dir.c_str());
  ASSERT_TRUE(dir_stream != nullptr);
  auto close_dir = MakeScopeExitTrigger([&dir_stream]() { closedir(dir_stream); });
  const dirent* dir_entry;
  while ((dir_entry = readdir(dir_stream)) != nullptr) {
    const char* entry_name = dir_entry->d_name;
    if ((strcmp(entry_name, "impala-subdir") == 0 && dir_entry->d_type != DT_DIR) ||
        (strcmp(entry_name, "impala-file") == 0 && dir_entry->d_type != DT_REG)) {
      LOG(WARNING) << Substitute("readdir() failed to list directory entry types of $0. "
          "Skipping DirEntryType test.", dir.string());
      return;
    }
  }

  // Verify that all directory entires are listed with the default parameters.
  vector<string> entries;
  ASSERT_OK(FileSystemUtil::Directory::GetEntryNames(dir.string(), &entries));
  ASSERT_EQ(entries.size(), 2);
  for (const string& entry : entries) {
    EXPECT_TRUE(entry == "impala-subdir" || entry == "impala-file");
  }

  // Verify that only directory type entries are listed with DIR_ENTRY_DIR.
  entries.clear();
  ASSERT_OK(FileSystemUtil::Directory::GetEntryNames(dir.string(), &entries, 0,
      FileSystemUtil::Directory::DIR_ENTRY_DIR));
  ASSERT_EQ(entries.size(), 1);
  EXPECT_TRUE(entries[0] == "impala-subdir");

  // Verify that only file type entries are listed with DIR_ENTRY_REG.
  entries.clear();
  ASSERT_OK(FileSystemUtil::Directory::GetEntryNames(dir.string(), &entries, 0,
      FileSystemUtil::Directory::DIR_ENTRY_REG));
  ASSERT_EQ(entries.size(), 1);
  EXPECT_TRUE(entries[0] == "impala-file");
}

TEST(FilesystemUtil, Regexes) {
  // Setup a temporary directory with one subdir
  path base_dir = filesystem::unique_path();
  path dir = base_dir / "regex-test-dir";
  path foo1 = dir / "foo1";
  path foo2 = dir / "foo2";
  path foo_subdir = dir / "foo_subdir";
  path bar1 = dir / "bar1";
  path bar_subdir = dir / "bar_subdir";

  // Always cleanup on exit.
  auto remove_dir = MakeScopeExitTrigger([&dir]() { filesystem::remove_all(dir); });

  ASSERT_OK(FileSystemUtil::RemoveAndCreateDirectory(dir.string()));
  ASSERT_OK(FileSystemUtil::CreateFile(foo1.string()));
  ASSERT_OK(FileSystemUtil::CreateFile(foo2.string()));
  ASSERT_OK(FileSystemUtil::RemoveAndCreateDirectory(foo_subdir.string()));
  ASSERT_OK(FileSystemUtil::CreateFile(bar1.string()));
  ASSERT_OK(FileSystemUtil::RemoveAndCreateDirectory(bar_subdir.string()));

  vector<string> entries;
  ASSERT_OK(FileSystemUtil::Directory::GetEntryNames(dir.string(), &entries, 0,
      FileSystemUtil::Directory::DIR_ENTRY_ANY, "foo.*"));
  ASSERT_EQ(entries.size(), 3);
  for (const string& entry : entries) {
    EXPECT_TRUE(entry == "foo1" || entry == "foo2" || entry == "foo_subdir");
  }

  entries.clear();
  ASSERT_OK(FileSystemUtil::Directory::GetEntryNames(dir.string(), &entries, 0,
      FileSystemUtil::Directory::DIR_ENTRY_ANY, "foo[0-9]"));
  ASSERT_EQ(entries.size(), 2);
  for (const string& entry : entries) {
    EXPECT_TRUE(entry == "foo1" || entry == "foo2");
  }
}

TEST(FilesystemUtil, PathExists) {
  path dir = filesystem::unique_path();

  // Paths to existent and non-existent dirs.
  path subdir1 = dir / "impala1";
  path subdir2 = dir / "impala2";
  filesystem::create_directories(subdir1);
  bool exists;
  EXPECT_OK(FileSystemUtil::PathExists(subdir1.string(), &exists));
  EXPECT_TRUE(exists);
  EXPECT_OK(FileSystemUtil::PathExists(subdir2.string(), &exists));
  EXPECT_FALSE(exists);

  // Paths to existent and non-existent file.
  path file1 = dir / "a_file1";
  path file2 = dir / "a_file2";
  ASSERT_OK(FileSystemUtil::CreateFile(file1.string()));
  EXPECT_OK(FileSystemUtil::PathExists(file1.string(), &exists));
  EXPECT_TRUE(exists);
  EXPECT_OK(FileSystemUtil::PathExists(file2.string(), &exists));
  EXPECT_FALSE(exists);

  // Cleanup
  filesystem::remove_all(dir);
}
