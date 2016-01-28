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


#include <string>
#include <boost/bind.hpp>

#include "testutil/gtest-util.h"
#include "runtime/parallel-executor.h"
#include "util/thread.h"

#include "common/names.h"

using namespace impala;

namespace impala {

class ParallelExecutorTest {
 public:
  Status UpdateFunction(void* value) {
    long arg = reinterpret_cast<long>(value);
    EXPECT_FALSE(updates_found_[arg]);
    updates_found_[arg] = true;

    double result = 0;
    // Run something random to keep this cpu a little busy
    for (int i = 0; i < 10000; ++i) {
      for (int j = 0; j < 200; ++j) {
        result += sin(i) + cos(j);
      }
    }

    return Status::OK();
  }

  ParallelExecutorTest(int num_updates) {
    updates_found_.resize(num_updates);
  }

  void Validate() {
    for (int i = 0; i < updates_found_.size(); ++i) {
      EXPECT_TRUE(updates_found_[i]);
    }
  }

 private:
  vector<int> updates_found_;
};

TEST(ParallelExecutorTest, Basic) {
  int num_work_items = 100;
  ParallelExecutorTest test_caller(num_work_items);

  vector<long> args;
  for (int i = 0; i < num_work_items; ++i) {
    args.push_back(i);
  }

  EXPECT_OK(ParallelExecutor::Exec(
      bind<Status>(mem_fn(&ParallelExecutorTest::UpdateFunction), &test_caller, _1),
      reinterpret_cast<void**>(&args[0]), args.size()));

  test_caller.Validate();
}

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  InitThreading();
  return RUN_ALL_TESTS();
}
