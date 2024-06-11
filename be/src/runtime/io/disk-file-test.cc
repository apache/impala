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

#include <gtest/gtest.h>

#include "common/names.h"
#include "runtime/io/disk-io-mgr-internal.h"
#include "testutil/death-test-util.h"

using namespace std;

namespace impala {
namespace io {
class DiskFileTest : public testing::Test {
 public:
  void ValidateMemBlockStatus(MemBlockStatus last_status);
  void ValidateMemBlockStatusTransition(MemBlock& block, MemBlockStatus old_status,
      MemBlockStatus new_status, bool expect_success);
};

typedef DiskFileTest DiskFileDeathTest;

// last_status is the MemBlock's last status it is going to reach other than
// MemBlockStatus::DISABLED.
void DiskFileTest::ValidateMemBlockStatus(MemBlockStatus last_status) {
  const int block_id = 0;
  const int64_t block_size = 1024;
  bool expect_reserved = last_status >= MemBlockStatus::RESERVED;
  bool expect_alloc = last_status >= MemBlockStatus::ALLOC;
  bool reserved = false;
  bool alloc = false;
  MemBlock block(block_id);
  ASSERT_TRUE(block.data() == nullptr);
  ASSERT_TRUE(block.IsStatus(MemBlockStatus::UNINIT));
  if (last_status == MemBlockStatus::UNINIT) goto end;
  block.SetStatus(MemBlockStatus::RESERVED);
  ASSERT_TRUE(block.IsStatus(MemBlockStatus::RESERVED));
  if (last_status == MemBlockStatus::RESERVED) goto end;
  {
    unique_lock<SpinLock> read_buffer_lock(*(block.GetLock()));
    EXPECT_OK(block.AllocLocked(read_buffer_lock, block_size));
  }
  ASSERT_TRUE(block.IsStatus(MemBlockStatus::ALLOC));
  ASSERT_TRUE(block.data() != nullptr);
  if (last_status == MemBlockStatus::ALLOC) goto end;
  ASSERT_EQ(last_status, MemBlockStatus::WRITTEN);
  memset(block.data(), 1, block_size);
  block.SetStatus(MemBlockStatus::WRITTEN);
  ASSERT_TRUE(block.IsStatus(MemBlockStatus::WRITTEN));
  for (int i = 0; i < block_size; i++) {
    EXPECT_EQ(block.data()[i], 1);
  }
end:
  block.Delete(&reserved, &alloc);
  ASSERT_EQ(reserved, expect_reserved);
  ASSERT_EQ(alloc, expect_alloc);
  ASSERT_TRUE(block.IsStatus(MemBlockStatus::DISABLED));
  ASSERT_TRUE(block.data() == nullptr);
}

void DiskFileTest::ValidateMemBlockStatusTransition(MemBlock& block,
    MemBlockStatus old_status, MemBlockStatus new_status, bool expect_success) {
  block.status_ = old_status;
  if (expect_success) {
    block.SetStatus(new_status);
    ASSERT_TRUE(block.IsStatus(new_status));
  } else {
    IMPALA_ASSERT_DEBUG_DEATH(block.SetStatus(new_status), "");
  }
}

// Test the basic flow of a MemBlock.
TEST_F(DiskFileTest, MemBlockTest) {
  ValidateMemBlockStatus(MemBlockStatus::UNINIT);
  ValidateMemBlockStatus(MemBlockStatus::RESERVED);
  ValidateMemBlockStatus(MemBlockStatus::ALLOC);
  ValidateMemBlockStatus(MemBlockStatus::WRITTEN);
}

// Test the MemBlock status transition.
TEST_F(DiskFileDeathTest, MemBlockStatusTransition) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");

  MemBlock block(0);
  ValidateMemBlockStatusTransition(
      block, MemBlockStatus::UNINIT, MemBlockStatus::UNINIT, false);
  ValidateMemBlockStatusTransition(
      block, MemBlockStatus::UNINIT, MemBlockStatus::RESERVED, true);
  ValidateMemBlockStatusTransition(
      block, MemBlockStatus::UNINIT, MemBlockStatus::ALLOC, false);
  ValidateMemBlockStatusTransition(
      block, MemBlockStatus::UNINIT, MemBlockStatus::WRITTEN, false);
  ValidateMemBlockStatusTransition(
      block, MemBlockStatus::UNINIT, MemBlockStatus::DISABLED, true);

  ValidateMemBlockStatusTransition(
      block, MemBlockStatus::RESERVED, MemBlockStatus::UNINIT, false);
  ValidateMemBlockStatusTransition(
      block, MemBlockStatus::RESERVED, MemBlockStatus::RESERVED, false);
  ValidateMemBlockStatusTransition(
      block, MemBlockStatus::RESERVED, MemBlockStatus::ALLOC, true);
  ValidateMemBlockStatusTransition(
      block, MemBlockStatus::RESERVED, MemBlockStatus::WRITTEN, false);
  ValidateMemBlockStatusTransition(
      block, MemBlockStatus::RESERVED, MemBlockStatus::DISABLED, true);

  ValidateMemBlockStatusTransition(
      block, MemBlockStatus::ALLOC, MemBlockStatus::UNINIT, false);
  ValidateMemBlockStatusTransition(
      block, MemBlockStatus::ALLOC, MemBlockStatus::RESERVED, false);
  ValidateMemBlockStatusTransition(
      block, MemBlockStatus::ALLOC, MemBlockStatus::ALLOC, false);
  ValidateMemBlockStatusTransition(
      block, MemBlockStatus::ALLOC, MemBlockStatus::WRITTEN, true);
  ValidateMemBlockStatusTransition(
      block, MemBlockStatus::ALLOC, MemBlockStatus::DISABLED, true);

  ValidateMemBlockStatusTransition(
      block, MemBlockStatus::WRITTEN, MemBlockStatus::UNINIT, false);
  ValidateMemBlockStatusTransition(
      block, MemBlockStatus::WRITTEN, MemBlockStatus::RESERVED, false);
  ValidateMemBlockStatusTransition(
      block, MemBlockStatus::WRITTEN, MemBlockStatus::ALLOC, false);
  ValidateMemBlockStatusTransition(
      block, MemBlockStatus::WRITTEN, MemBlockStatus::WRITTEN, false);
  ValidateMemBlockStatusTransition(
      block, MemBlockStatus::WRITTEN, MemBlockStatus::DISABLED, true);

  ValidateMemBlockStatusTransition(
      block, MemBlockStatus::DISABLED, MemBlockStatus::UNINIT, false);
  ValidateMemBlockStatusTransition(
      block, MemBlockStatus::DISABLED, MemBlockStatus::RESERVED, false);
  ValidateMemBlockStatusTransition(
      block, MemBlockStatus::DISABLED, MemBlockStatus::ALLOC, false);
  ValidateMemBlockStatusTransition(
      block, MemBlockStatus::DISABLED, MemBlockStatus::WRITTEN, false);
  ValidateMemBlockStatusTransition(
      block, MemBlockStatus::DISABLED, MemBlockStatus::DISABLED, true);
}
} // namespace io
} // namespace impala
