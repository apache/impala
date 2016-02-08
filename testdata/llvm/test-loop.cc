// Copyright (c) 2012 Cloudera, Inc.  All right reserved.

// This is a test-only file.  It was used to generate test-loop.bc
// which is used by the unit test to exercise loading precompiled
// ir.
#include <stdio.h>

__attribute__ ((noinline)) void DefaultImplementation() {
  printf("Default\n");
}

void TestLoop(int n) {
  for (int i = 0; i < n; ++i) {
    DefaultImplementation();
  }
}

// Unused function to make sure printf declaration is included in IR module. Used by
// LlvmCodegen::CodegenDebugTrace().
void printf_dummy_fn() {
  printf("dummy");
}
