// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

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

