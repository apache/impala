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

#include <boost/scoped_ptr.hpp>
#include <boost/bind.hpp>
#include <boost/thread/thread.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include <gtest/gtest.h>

#include "common/init.h"
#include "codegen/llvm-codegen.h"
#include "runtime/disk-io-mgr.h"
#include "runtime/buffered-block-mgr.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "runtime/runtime-state.h"
#include "runtime/tmp-file-mgr.h"
#include "util/disk-info.h"
#include "util/cpu-info.h"
#include "util/promise.h"

#include "gen-cpp/Types_types.h"
#include "gen-cpp/ImpalaInternalService_types.h"

using namespace boost;
using namespace std;

namespace impala {

class BufferedBlockMgrTest : public ::testing::Test {
 protected:
  const static int block_size_ = 1024;

  virtual void SetUp() {
    exec_env_.reset(new ExecEnv);
    exec_env_->InitForFeTests();
    io_mgr_tracker_.reset(new MemTracker(-1));
    block_mgr_parent_tracker_.reset(new MemTracker(-1));
    exec_env_->disk_io_mgr()->Init(io_mgr_tracker_.get());
    runtime_state_.reset(
        new RuntimeState(TPlanFragmentInstanceCtx(), "", exec_env_.get()));
  }

  virtual void TearDown() {
    block_mgr_parent_tracker_.reset();
    runtime_state_.reset();
    exec_env_.reset();
    io_mgr_tracker_.reset();
  }

  static void GetFreeBlock(BufferedBlockMgr* block_mgr,
      BufferedBlockMgr::Block** new_block, Promise<bool>* promise) {
    block_mgr->GetFreeBlock(new_block);
    promise->Set(true);
  }

  static void ValidateBlock(BufferedBlockMgr::Block* block, int32_t data) {
    EXPECT_TRUE(block->valid_data_len() == sizeof(int32_t));
    EXPECT_TRUE(*reinterpret_cast<int32_t*>(block->buffer()) == data);
  }

  BufferedBlockMgr* CreateMgr(int num_buffers) {
    BufferedBlockMgr* block_mgr = BufferedBlockMgr::Create(runtime_state_.get(),
        block_mgr_parent_tracker_.get(), runtime_state_->runtime_profile(),
        num_buffers * block_size_, block_size_);
    int available_buffers = block_mgr->max_available_buffers();
    EXPECT_TRUE(available_buffers == (num_buffers - TmpFileMgr::num_tmp_devices()));
    return block_mgr;
  }

  void AllocateBlocks(BufferedBlockMgr* block_mgr, int num_blocks,
      vector<BufferedBlockMgr::Block*>* blocks) {
    int32_t* data;
    Status status;
    BufferedBlockMgr::Block* new_block;
    for (int i = 0; i < num_blocks; ++i) {
      status = block_mgr->GetFreeBlock(&new_block);
      data = new_block->Allocate<int32_t>(sizeof(int32_t));
      *data = blocks->size();
      EXPECT_TRUE(status.ok());
      blocks->push_back(new_block);
    }
  }

  scoped_ptr<ExecEnv> exec_env_;
  scoped_ptr<RuntimeState> runtime_state_;
  scoped_ptr<MemTracker> block_mgr_parent_tracker_;
  scoped_ptr<MemTracker> io_mgr_tracker_;
};

// Test that pinning more blocks than the max available buffers is blocking.
TEST_F(BufferedBlockMgrTest, PinWait) {
  int num_blocks = 5;
  scoped_ptr<BufferedBlockMgr> block_mgr(CreateMgr(num_blocks));

  vector<BufferedBlockMgr::Block*> blocks;
  AllocateBlocks(block_mgr.get(), num_blocks, &blocks);

  BufferedBlockMgr::Block* new_block;
  Promise<bool> got_block;
  // Test that pinning an additional block waits until a block is unpinned.
  thread extra_thread(GetFreeBlock, block_mgr.get(), &new_block, &got_block);
  bool timed_out;
  bool done = got_block.Get(500, &timed_out);
  EXPECT_TRUE(timed_out);
  EXPECT_TRUE(block_mgr_parent_tracker_->consumption() == num_blocks * block_size_);

  blocks.back()->Unpin();
  done = got_block.Get();
  EXPECT_TRUE(done);
  extra_thread.join();

  block_mgr->Close();
  EXPECT_TRUE(block_mgr_parent_tracker_->consumption() == 0);
}

// Test the eviction policy of the block mgr. No writes issued until more than
// the max available buffers are allocated. Writes must be issued in LIFO order.
TEST_F(BufferedBlockMgrTest, Eviction) {
  int max_num_buffers = 5;
  scoped_ptr<BufferedBlockMgr> block_mgr(CreateMgr(max_num_buffers));

  // Check counters.
  RuntimeProfile* profile = runtime_state_->runtime_profile();
  RuntimeProfile::Counter* buffered_pin = profile->GetCounter("BufferedPins");
  RuntimeProfile::Counter* writes_issued = profile->GetCounter("WritesIssued");

  int available_buffers = block_mgr->max_available_buffers();
  vector<BufferedBlockMgr::Block*> blocks;
  AllocateBlocks(block_mgr.get(), available_buffers, &blocks);

  EXPECT_TRUE(block_mgr_parent_tracker_->consumption() == max_num_buffers * block_size_);
  BOOST_FOREACH(BufferedBlockMgr::Block* block, blocks) {
    block->Unpin();
  }

  Status status;
  // Re-pinning all blocks
  for (int i = 0; i < blocks.size(); ++i) {
    status = blocks[i]->Pin();
    EXPECT_TRUE(status.ok());
    ValidateBlock(blocks[i], i);
  }
  int buffered_pins_expected = blocks.size();
  // All blocks must have been in memory and no writes issued.
  EXPECT_TRUE(buffered_pin->value() == buffered_pins_expected);
  EXPECT_TRUE(writes_issued->value() == 0);

  // Unpin all blocks
  BOOST_FOREACH(BufferedBlockMgr::Block* block, blocks) {
    block->Unpin();
  }
  // Get two new blocks.
  AllocateBlocks(block_mgr.get(), 2, &blocks);
  // Exactly two writes must be issued. The first (num_blocks - 2) must be in memory.
  EXPECT_TRUE(writes_issued->value() == 2);
  for (int i = 0; i < (available_buffers - 2); ++i) {
    status = blocks[i]->Pin();
    EXPECT_TRUE(status.ok());
    ValidateBlock(blocks[i], i);
  }
  buffered_pins_expected += (available_buffers - 2);
  EXPECT_TRUE(buffered_pin->value() == buffered_pins_expected);

  block_mgr->Close();
}

// Test deletion and reuse of blocks.
TEST_F(BufferedBlockMgrTest, Deletion) {
  int max_num_buffers = 5;
  scoped_ptr<BufferedBlockMgr> block_mgr(CreateMgr(max_num_buffers));

  // Check counters.
  RuntimeProfile* profile = runtime_state_->runtime_profile();
  RuntimeProfile::Counter* recycled_cnt = profile->GetCounter("RecycledBlocks");
  RuntimeProfile::Counter* created_cnt = profile->GetCounter("NumCreatedBlocks");

  int available_buffers = block_mgr->max_available_buffers();
  vector<BufferedBlockMgr::Block*> blocks;
  AllocateBlocks(block_mgr.get(), available_buffers, &blocks);
  EXPECT_TRUE(created_cnt->value() == available_buffers);

  Status status;
  BOOST_FOREACH(BufferedBlockMgr::Block* block, blocks) {
    block->Delete();
  }
  AllocateBlocks(block_mgr.get(), available_buffers, &blocks);
  EXPECT_TRUE(created_cnt->value() == available_buffers);
  EXPECT_TRUE(recycled_cnt->value() == available_buffers);

  block_mgr->Close();
  EXPECT_TRUE(block_mgr_parent_tracker_->consumption() == 0);
}

// Test that all APIs return cancelled after close.
TEST_F(BufferedBlockMgrTest, Close) {
  int max_num_buffers = 5;
  scoped_ptr<BufferedBlockMgr> block_mgr(CreateMgr(max_num_buffers));

  int available_buffers = block_mgr->max_available_buffers();
  vector<BufferedBlockMgr::Block*> blocks;
  AllocateBlocks(block_mgr.get(), available_buffers, &blocks);

  block_mgr->Close();
  EXPECT_TRUE(block_mgr_parent_tracker_->consumption() == 0);

  Status status;
  BufferedBlockMgr::Block* new_block;
  status = block_mgr->GetFreeBlock(&new_block);
  EXPECT_TRUE(status.IsCancelled());
  status = blocks[0]->Unpin();
  EXPECT_TRUE(status.IsCancelled());
  status = blocks[1]->Delete();
  EXPECT_TRUE(status.IsCancelled());
}

// Test that randomly issues GetFreeBlock(), Pin(), Unpin() and Delete() calls.
// All calls made are legal - error conditions are not tested.
TEST_F(BufferedBlockMgrTest, Random) {
  const int num_buffers = 10;
  const int num_iterations = 100000;
  unordered_map<BufferedBlockMgr::Block*, int> pinned_block_map;
  vector<pair<BufferedBlockMgr::Block*, int32_t> > pinned_blocks;
  unordered_map<BufferedBlockMgr::Block*, int> unpinned_block_map;
  vector<pair<BufferedBlockMgr::Block*, int32_t> > unpinned_blocks;

  typedef enum { Pin, New, Unpin, Delete } ApiFunction;
  ApiFunction api_function;
  scoped_ptr<BufferedBlockMgr> block_mgr(CreateMgr(num_buffers));
  pinned_blocks.reserve(num_buffers);
  BufferedBlockMgr::Block* new_block;
  Status status;
  for (int i = 0; i < num_iterations; ++i) {
    if ((i % 20000) == 0) LOG (ERROR) << " Iteration " << i << endl;
    if (pinned_blocks.size() == 0 && unpinned_blocks.size() == 0) {
      api_function = New;
    } else if (pinned_blocks.size() == 0) {
      // Pin or New. Can't unpin or delete.
      api_function = static_cast<ApiFunction>(rand() % 2);
    } else if (pinned_blocks.size() >= num_buffers) {
      // Unpin or delete. Can't pin or get new.
      api_function = static_cast<ApiFunction>(2 + (rand() % 2));
    } else if (unpinned_blocks.size() == 0) {
      // Can't pin. Unpin, new or delete.
      api_function = static_cast<ApiFunction>(1 + (rand() % 3));
    } else {
      // Any api function.
      api_function = static_cast<ApiFunction>(rand() % 4);
    }

    pair<BufferedBlockMgr::Block*, int32_t> block_data;
    int rand_pick;
    int32_t* data;
    switch (api_function) {
      case New:
        status = block_mgr->GetFreeBlock(&new_block);
        EXPECT_TRUE(status.ok());
        data = new_block->Allocate<int32_t>(sizeof(int32_t));
        *data = rand();
        block_data = make_pair(new_block, *data);

        pinned_blocks.push_back(block_data);
        pinned_block_map.insert(make_pair(block_data.first, pinned_blocks.size() - 1));
        break;
      case Pin:
        rand_pick = rand() % unpinned_blocks.size();
        block_data = unpinned_blocks[rand_pick];
        status = block_data.first->Pin();
        EXPECT_TRUE(status.ok());
        ValidateBlock(block_data.first, block_data.second);
        unpinned_blocks[rand_pick] = unpinned_blocks.back();
        unpinned_blocks.pop_back();
        unpinned_block_map[unpinned_blocks[rand_pick].first] = rand_pick;

        pinned_blocks.push_back(block_data);
        pinned_block_map.insert(make_pair(block_data.first, pinned_blocks.size() - 1));
        break;
      case Unpin:
        rand_pick = rand() % pinned_blocks.size();
        block_data = pinned_blocks[rand_pick];
        status = block_data.first->Unpin();
        EXPECT_TRUE(status.ok());
        pinned_blocks[rand_pick] = pinned_blocks.back();
        pinned_blocks.pop_back();
        pinned_block_map[pinned_blocks[rand_pick].first] = rand_pick;

        unpinned_blocks.push_back(block_data);
        unpinned_block_map.insert(make_pair(block_data.first,
            unpinned_blocks.size() - 1));
        break;
      case Delete:
        rand_pick = rand() % pinned_blocks.size();
        block_data = pinned_blocks[rand_pick];
        status = block_data.first->Delete();
        EXPECT_TRUE(status.ok());
        pinned_blocks[rand_pick] = pinned_blocks.back();
        pinned_blocks.pop_back();
        pinned_block_map[pinned_blocks[rand_pick].first] = rand_pick;
        break;
    } // end switch (apiFunction)
  } // end for ()

  block_mgr->Close();
  EXPECT_TRUE(block_mgr_parent_tracker_->consumption() == 0);
}

}

int main(int argc, char **argv) {
  impala::InitCommonRuntime(argc, argv, true);
  impala::LlvmCodeGen::InitializeLlvm();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
