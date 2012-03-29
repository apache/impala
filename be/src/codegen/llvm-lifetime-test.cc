// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <string>
#include <gtest/gtest.h>

#include "codegen/llvm-codegen.h"

#include <boost/thread/thread.hpp>

using namespace std;
using namespace boost;

namespace impala {

// Simple test to just make and destroy llvmcodegen objects.  LLVM 
// has non-obvious object ownership transfers and this sanity checks that.

void LifetimeTest() {
  Status status;
  for (int i = 0; i < 10; ++i) {
    LlvmCodeGen object1("Test");
    LlvmCodeGen object2("Test");
    LlvmCodeGen object3("Test");
    
    status = object1.Init();
    ASSERT_TRUE(status.ok());

    status = object2.Init();
    ASSERT_TRUE(status.ok());

    status = object3.Init();
    ASSERT_TRUE(status.ok());
  }
}

TEST(LlvmLifetimeTest, Basic) {
  LifetimeTest();
}

TEST(LlvmMultithreadedLifetimeTest, Basic) {
  const int NUM_THREADS = 10;
  thread_group thread_group;
  for (int i = 0; i < NUM_THREADS; ++i) {
    thread_group.add_thread(new thread(&LifetimeTest));
  }
  thread_group.join_all();
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::LlvmCodeGen::InitializeLlvm();
  return RUN_ALL_TESTS();
}

