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

#include "os-util.h"

#include <cstdlib>
#include <unordered_map>

#include "testutil/gtest-util.h"

using namespace std;
using namespace impala;

void UseEnvironment(const unordered_map<string, string>& env, function<void(void)> fn) {
  for (const auto& e: env) {
    setenv(e.first.c_str(), e.second.c_str(), true);
  }
  fn();
  for (const auto& e: env) {
    unsetenv(e.first.c_str());
  }
}

TEST(OsUtil, RunShellProcess) {
  UseEnvironment({{"JAVA_TOOL_OPTIONS", "-Xmx1G"}}, []{
    string msg;
    RunShellProcess("echo $JAVA_TOOL_OPTIONS", &msg, true, {"JAVA_TOOL_OPTIONS"});
    ASSERT_EQ("", msg);
  });

  UseEnvironment({{"JAVA_TOOL_OPTIONS", "-Xmx1G"}, {"FOOBAR", "foobar"}}, []{
    string msg;
    RunShellProcess("echo $JAVA_TOOL_OPTIONS$FOOBAR", &msg, true, {"JAVA_TOOL_OPTIONS"});
    ASSERT_EQ("foobar", msg);
  });

  UseEnvironment({{"JAVA_TOOL_OPTIONS", "-Xmx1G"}, {"FOOBAR", "foobar"}}, []{
    string msg;
    RunShellProcess("echo $JAVA_TOOL_OPTIONS$FOOBAR", &msg, true, {"JAVA_TOOL_OPTIONS",
        "FOOBAR"});
    ASSERT_EQ("", msg);
  });

  UseEnvironment({{"JAVA_TOOL_OPTIONS", "-Xmx1G"}}, []{
    string msg;
    RunShellProcess("echo $JAVA_TOOL_OPTIONS", &msg, true);
    ASSERT_EQ("-Xmx1G", msg);
  });
}

