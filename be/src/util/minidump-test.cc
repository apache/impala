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

#include "util/minidump.h"

#include <boost/algorithm/string.hpp>
#include <boost/regex.hpp>
#include <gtest/gtest.h>

#include "client/linux/handler/minidump_descriptor.h"
#include "common/init.h"
#include "common/thread-debug-info.h"
#include "util/test-info.h"

namespace impala {

bool DumpCallback(const google_breakpad::MinidumpDescriptor& descriptor, void* context,
    bool succeeded);

TEST(Minidump, DumpCallback) {
  testing::internal::CaptureStdout();
  testing::internal::CaptureStderr();

  google_breakpad::MinidumpDescriptor descriptor("/tmp/arbitrary/path");
  descriptor.UpdatePath();
  DumpCallback(descriptor, nullptr, true);

  std::string stdout = testing::internal::GetCapturedStdout();
  std::string stderr = testing::internal::GetCapturedStderr();
  boost::regex wrote_minidump("Wrote minidump to /tmp/arbitrary/path/.*\\.dmp");

  for (std::string output : {stdout, stderr}) {
    std::vector<std::string> lines;
    boost::split(lines, output, boost::is_any_of("\n\r"), boost::token_compress_on);
    EXPECT_EQ(3, lines.size()) << output;
    EXPECT_EQ("Minidump with no thread info available.", lines[0])
        << lines[0] << "\nOutput:\n" << output;
    EXPECT_TRUE(boost::regex_match(lines[1], wrote_minidump))
        << lines[1] << "\nOutput:\n" << output;
    EXPECT_EQ("", lines[2]) << output;
  }
}

TEST(Minidump, DumpCallbackWithThread) {
  testing::internal::CaptureStdout();
  testing::internal::CaptureStderr();

  ThreadDebugInfo tdi;
  TUniqueId query, instance;
  std::tie(query.hi, query.lo, instance.hi, instance.lo) = std::make_tuple(1, 2, 3, 4);
  tdi.SetQueryId(query);
  tdi.SetInstanceId(instance);
  google_breakpad::MinidumpDescriptor descriptor("/tmp/arbitrary/path");
  descriptor.UpdatePath();
  DumpCallback(descriptor, nullptr, true);

  std::string stdout = testing::internal::GetCapturedStdout();
  std::string stderr = testing::internal::GetCapturedStderr();
  boost::regex minidump_in_thread("Minidump in thread \\[.*\\] running query "
      "0{15}1:0{15}2, fragment instance 0{15}3:0{15}4");
  boost::regex wrote_minidump("Wrote minidump to /tmp/arbitrary/path/.*\\.dmp");

  for (std::string output : {stdout, stderr}) {
    std::vector<std::string> lines;
    boost::split(lines, output, boost::is_any_of("\n\r"), boost::token_compress_on);
    EXPECT_EQ(3, lines.size()) << output;
    EXPECT_TRUE(boost::regex_match(lines[0], minidump_in_thread))
        << lines[0] << "\nOutput:\n" << output;
    EXPECT_TRUE(boost::regex_match(lines[1], wrote_minidump))
        << lines[1] << "\nOutput:\n" << output;
    EXPECT_EQ("", lines[2]) << output;
  }
}

} // namespace impala

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, /*init_jvm*/ false, impala::TestInfo::BE_TEST);
  return RUN_ALL_TESTS();
}
