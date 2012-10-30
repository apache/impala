// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <stdlib.h>
#include <stdio.h>
#include <iostream>

#include <gtest/gtest.h>
#include "util/debug-util.h"

using namespace std;

namespace impala {

string RecursionStack(int level) {
  if (level == 0) return GetStackTrace();
  return RecursionStack(level - 1);
}

TEST(DebugUtil, StackDump) { 
  cout << "Stack: " << endl << GetStackTrace() << endl;
  cout << "Stack Recursion: " << endl << RecursionStack(5) << endl;
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

