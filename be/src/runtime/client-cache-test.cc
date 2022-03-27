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

#include <stdint.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include "testutil/gtest-util.h"
#include "common/init.h"
#include "codegen/llvm-codegen.h"
#include "runtime/test-env.h"
#include "service/fe-support.h"
#include "runtime/client-cache.h"
#include "statestore/statestore-service-client-wrapper.h"

using namespace std;

namespace impala {
class ClientCacheTest : public testing::Test {
 protected:
  // Pool for objects to be destroyed during test teardown.
  ObjectPool pool_;

  virtual void SetUp() {}

  virtual void TearDown() { pool_.Clear(); }

  void InitAddr(TNetworkAddress** addr) {
    *addr = pool_.Add(new TNetworkAddress());
    (*addr)->__set_hostname("127.0.0.1");
    (*addr)->__set_port(25000);
  }

  // Create a new client by the client cache.
  Status GetClient(shared_ptr<ClientCache<StatestoreServiceClientWrapper>>& cc,
      TNetworkAddress* addr, StatestoreServiceClientWrapper** ck) {
    return cc->GetClient(*addr, ck);
  }

  // Destroy the client.
  void DestroyClient(shared_ptr<ClientCache<StatestoreServiceClientWrapper>>& cc,
      StatestoreServiceClientWrapper** ck) {
    cc->DestroyClient(ck);
  }

  // Return the virtual memory size that the process is using.
  // Only works for Linux.
  uint64_t GetProcessVMSize() {
    // vm size, https://man7.org/linux/man-pages/man5/proc.5.html
    const int vm_size_pos = 22;
    ifstream stream("/proc/self/stat");
    string line;
    string space_delimiter = " ";
    vector<string> words{};
    if (getline(stream, line)) {
      size_t pos = 0;
      while ((pos = line.find(space_delimiter)) != string::npos) {
        words.push_back(line.substr(0, pos));
        line.erase(0, pos + space_delimiter.length());
      }
    }
    uint64_t value = 0;
    if (words.size() <= vm_size_pos) return value;
    istringstream iss(words[vm_size_pos]);
    iss >> value;
    return value;
  }
};

// Testcase for IMPALA-11176 to verify the memory leak issue.
// The memory is not supposed to increase after multiple rounds of client creation and
// destruction.
TEST_F(ClientCacheTest, MemLeak) {
// IMPALA-11196 Testcase will fail in ASAN and TSAN build, therefore disabled.
#if !defined(ADDRESS_SANITIZER) && !defined(THREAD_SANITIZER)
  int64_t create_num = 1000;
  TNetworkAddress* addr;
  InitAddr(&addr);
  uint64_t mem_before = GetProcessVMSize();
  EXPECT_GT(mem_before, 0);
  // Create and destroy the client, the virtual memory usage of the process should not
  // increase.
  while (create_num-- > 0) {
    StatestoreServiceClientWrapper* ck = nullptr;
    shared_ptr<ClientCache<StatestoreServiceClientWrapper>> client_cache =
        std::make_shared<ClientCache<StatestoreServiceClientWrapper>>();
    Status status = GetClient(client_cache, addr, &ck);
    EXPECT_TRUE(status.ok());
    DestroyClient(client_cache, &ck);
  }
  uint64_t mem_after = GetProcessVMSize();
  EXPECT_GE(mem_before, mem_after);
#endif
}
} // namespace impala

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport();
  ABORT_IF_ERROR(impala::LlvmCodeGen::InitializeLlvm());
  return RUN_ALL_TESTS();
}
