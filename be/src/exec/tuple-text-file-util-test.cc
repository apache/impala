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

#include <fstream>
#include <gtest/gtest.h>

#include "exec/tuple-text-file-util.h"
#include "runtime/test-env.h"
#include "util/filesystem-util.h"

using namespace std;

namespace impala {
class TupleTextFileUtilTest : public ::testing::Test {
 protected:
  void SetUp() override {
    temp_dir_ = "/tmp/tuple_text_file_util_test";
    ASSERT_OK(FileSystemUtil::RemoveAndCreateDirectory(temp_dir_));
  }

  void TearDown() override {
    vector<string> temp_dirs;
    temp_dirs.push_back(temp_dir_);
    ASSERT_OK(FileSystemUtil::RemovePaths(temp_dirs));
  }

  string CreateTestFile(const string& filename, const vector<string>& lines) {
    string full_path = temp_dir_ + "/" + filename;
    ofstream file(full_path);
    for (const auto& line : lines) {
      file << line << endl;
    }
    file.close();
    return full_path;
  }

  string temp_dir_;
};

TEST_F(TupleTextFileUtilTest, VerifyDebugDumpCache_IdenticalFiles) {
  vector<string> lines = {"Line 1", "Line 2", "Line 3"};
  string file1 = CreateTestFile("file1.txt", lines);
  string file2 = CreateTestFile("file2.txt", lines);

  bool passed = false;
  ASSERT_OK(TupleTextFileUtil::VerifyDebugDumpCache(file1, file2, &passed));
  EXPECT_TRUE(passed);
}

TEST_F(TupleTextFileUtilTest, VerifyDebugDumpCache_DifferentFiles) {
  vector<string> lines1 = {"Line 1", "Line 2", "Line 3"};
  vector<string> lines2 = {"Line 1", "Different Line", "Line 3"};
  string file1 = CreateTestFile("file1.txt", lines1);
  string file2 = CreateTestFile("file2.txt", lines2);

  bool passed = false;
  Status status = TupleTextFileUtil::VerifyDebugDumpCache(file1, file2, &passed);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), TErrorCode::TUPLE_CACHE_INCONSISTENCY);
  EXPECT_FALSE(passed);
}

TEST_F(TupleTextFileUtilTest, VerifyDebugDumpCache_DifferentOrders) {
  vector<string> lines1 = {"Line 1", "Line 2", "Line 3"};
  vector<string> lines2 = {"Line 1", "Line 3", "Line 2"};
  string file1 = CreateTestFile("file1.txt", lines1);
  string file2 = CreateTestFile("file2.txt", lines2);

  bool passed = true;
  ASSERT_OK(TupleTextFileUtil::VerifyDebugDumpCache(file1, file2, &passed));
  EXPECT_FALSE(passed);
}

TEST_F(TupleTextFileUtilTest, VerifyDebugDumpCache_NonexistentFile) {
  vector<string> lines = {"Line 1", "Line 2", "Line 3"};
  string file1 = CreateTestFile("file1.txt", lines);
  string file2 = temp_dir_ + "/nonexistent.txt";

  bool passed = true;
  Status status = TupleTextFileUtil::VerifyDebugDumpCache(file1, file2, &passed);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), TErrorCode::TUPLE_CACHE_INCONSISTENCY);
}

TEST_F(TupleTextFileUtilTest, VerifyRows_IdenticalFiles) {
  vector<string> lines = {"Row 1", "Row 2", "Row 3"};
  string ref_file = CreateTestFile("ref.txt", lines);
  string cmp_file = CreateTestFile("cmp.txt", lines);

  ASSERT_OK(TupleTextFileUtil::VerifyRows(cmp_file, ref_file));
  ASSERT_OK(TupleTextFileUtil::VerifyRows(ref_file, cmp_file));
}

TEST_F(TupleTextFileUtilTest, VerifyRows_DifferentRowCount) {
  vector<string> ref_lines = {"Row 1", "Row 2", "Row 3"};
  vector<string> cmp_lines = {"Row 1", "Row 2"};
  string ref_file = CreateTestFile("ref.txt", ref_lines);
  string cmp_file = CreateTestFile("cmp.txt", cmp_lines);

  Status status = TupleTextFileUtil::VerifyRows(cmp_file, ref_file);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), TErrorCode::TUPLE_CACHE_INCONSISTENCY);
  status = TupleTextFileUtil::VerifyRows(ref_file, cmp_file);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), TErrorCode::TUPLE_CACHE_INCONSISTENCY);
}

TEST_F(TupleTextFileUtilTest, VerifyRows_DifferentContent) {
  vector<string> ref_lines = {"Row 1", "Row 2", "Row 3"};
  vector<string> cmp_lines = {"Row 1", "Different Row", "Row 3"};
  string ref_file = CreateTestFile("ref.txt", ref_lines);
  string cmp_file = CreateTestFile("cmp.txt", cmp_lines);

  Status status = TupleTextFileUtil::VerifyRows(cmp_file, ref_file);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), TErrorCode::TUPLE_CACHE_INCONSISTENCY);
  status = TupleTextFileUtil::VerifyRows(ref_file, cmp_file);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), TErrorCode::TUPLE_CACHE_INCONSISTENCY);
}

TEST_F(TupleTextFileUtilTest, VerifyRows_DifferentOrders) {
  vector<string> ref_lines = {"Row 1", "Row 2", "Row 3"};
  vector<string> cmp_lines = {"Row 1", "Row 3", "Row 2"};
  string ref_file = CreateTestFile("ref.txt", ref_lines);
  string cmp_file = CreateTestFile("cmp.txt", cmp_lines);

  ASSERT_OK(TupleTextFileUtil::VerifyRows(cmp_file, ref_file));
  ASSERT_OK(TupleTextFileUtil::VerifyRows(ref_file, cmp_file));
}

TEST_F(TupleTextFileUtilTest, VerifyRows_NonexistentFile) {
  vector<string> lines = {"Row 1", "Row 2", "Row 3"};
  string ref_file = CreateTestFile("ref.txt", lines);
  string cmp_file = temp_dir_ + "/nonexistent.txt";

  Status status = TupleTextFileUtil::VerifyRows(cmp_file, ref_file);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), TErrorCode::DISK_IO_ERROR);
  status = TupleTextFileUtil::VerifyRows(ref_file, cmp_file);
  EXPECT_FALSE(status.ok());
  EXPECT_EQ(status.code(), TErrorCode::DISK_IO_ERROR);
}
} // namespace impala
