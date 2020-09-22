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

// This is a test-only file.  It was used to generate test-loop.bc
// which is used by the unit test to exercise loading precompiled
// ir.
#include <stdio.h>
#include <cstdint>

/// The default implementation does nothing with the pointer argument. The codegen
/// implementation that it will be replaced with will increment it.
__attribute__ ((noinline)) void DefaultImplementation(int64_t* ptr) {
  // We need to use 'ptr' otherwise clang marks it as 'readnone' and doesn't actually pass
  // the value from the caller. We need the value to be passed because the codegen'd
  // function this function will be replaced with does use the argument.
  printf("Default, value is %ld.\n", *ptr);
}

void TestLoop(int n, int64_t* counter) {
  for (int i = 0; i < n; ++i) {
    DefaultImplementation(counter);
  }
}

// Unused function to make sure printf declaration is included in IR module. Used by
// LlvmCodegen::CodegenDebugTrace().
void printf_dummy_fn() {
  printf("dummy");
}
