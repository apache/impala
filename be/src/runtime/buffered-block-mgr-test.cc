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
#include <boost/filesystem.hpp>
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
#include "service/fe-support.h"
#include "util/disk-info.h"
#include "util/cpu-info.h"
#include "util/promise.h"
#include "util/test-info.h"
#include "util/time.h"

#include "gen-cpp/Types_types.h"
#include "gen-cpp/ImpalaInternalService_types.h"

using namespace boost;
using namespace boost::filesystem;
using namespace std;

// Note: This is the default scratch dir created by impala.
// FLAGS_scratch_dirs + TmpFileMgr::TMP_SUB_DIR_NAME.
const string SCRATCH_DIR = "/tmp/impala-scratch";

DECLARE_bool(disk_spill_encryption);

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

  static void GetFreeBlock(BufferedBlockMgr* block_mgr, BufferedBlockMgr::Client* client,
      BufferedBlockMgr::Block** new_block, Promise<bool>* promise) {
    block_mgr->GetNewBlock(client, NULL, new_block);
    promise->Set(true);
  }

  static void ValidateBlock(BufferedBlockMgr::Block* block, int32_t data) {
    EXPECT_TRUE(block->valid_data_len() == sizeof(int32_t));
    EXPECT_TRUE(*reinterpret_cast<int32_t*>(block->buffer()) == data);
  }

  static int32_t* MakeRandomSizeData(BufferedBlockMgr::Block* block) {
    // Format is int32_t size, followed by size bytes of data
    int32_t size = (rand() % 252) + 4; // So blocks have 4-256 bytes of data
    uint8_t* data = block->Allocate<uint8_t>(size);
    *(reinterpret_cast<int32_t*>(data)) = size;
    int i;
    for (i = 4; i < size-5; ++i) {
      data[i] = i;
    }
    for (; i < size; ++i) {  // End marker of at least 5 0xff's
      data[i] = 0xff;
    }
    return reinterpret_cast<int32_t*>(data);  // Really returns a pointer to size
  }

  static void ValidateRandomSizeData(BufferedBlockMgr::Block* block, int32_t size) {
    int32_t bsize = *(reinterpret_cast<int32_t*>(block->buffer()));
    uint8_t* data = reinterpret_cast<uint8_t*>(block->buffer());
    int i;
    EXPECT_EQ(block->valid_data_len(), size);
    EXPECT_EQ(size, bsize);
    for (i = 4; i < size-5; ++i) {
      EXPECT_EQ(data[i], i);
    }
    for (; i < size; ++i) {
      EXPECT_EQ(data[i], 0xff);
    }
  }

  shared_ptr<BufferedBlockMgr> CreateMgr(int max_buffers) {
    shared_ptr<BufferedBlockMgr> mgr;
    BufferedBlockMgr::Create(runtime_state_.get(),
        block_mgr_parent_tracker_.get(), runtime_state_->runtime_profile(),
        max_buffers * block_size_, block_size_, &mgr);
    EXPECT_TRUE(mgr != NULL);
    EXPECT_TRUE(block_mgr_parent_tracker_->consumption() == 0);
    return mgr;
  }

  void AllocateBlocks(BufferedBlockMgr* block_mgr, BufferedBlockMgr::Client* client,
      int num_blocks, vector<BufferedBlockMgr::Block*>* blocks) {
    int32_t* data;
    Status status;
    BufferedBlockMgr::Block* new_block;
    for (int i = 0; i < num_blocks; ++i) {
      status = block_mgr->GetNewBlock(client, NULL, &new_block);
      EXPECT_TRUE(status.ok());
      EXPECT_TRUE(new_block != NULL);
      data = new_block->Allocate<int32_t>(sizeof(int32_t));
      *data = blocks->size();
      blocks->push_back(new_block);
    }
  }

  // Test that randomly issues GetFreeBlock(), Pin(), Unpin(), Delete() and Close()
  // calls. All calls made are legal - error conditions are not expected until the
  // first call to Close().  This is called 2 times with encryption+integrity on/off
  void TestRandomInternal() {
    const int num_buffers = 10;
    const int num_iterations = 100000;
    const int iters_before_close = num_iterations - 5000;
    bool close_called = false;
    unordered_map<BufferedBlockMgr::Block*, int> pinned_block_map;
    vector<pair<BufferedBlockMgr::Block*, int32_t> > pinned_blocks;
    unordered_map<BufferedBlockMgr::Block*, int> unpinned_block_map;
    vector<pair<BufferedBlockMgr::Block*, int32_t> > unpinned_blocks;

    typedef enum { Pin, New, Unpin, Delete, Close } ApiFunction;
    ApiFunction api_function;
    shared_ptr<BufferedBlockMgr> block_mgr = CreateMgr(num_buffers);
    BufferedBlockMgr::Client* client;
    Status status = block_mgr->RegisterClient(0, NULL, runtime_state_.get(), &client);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(client != NULL);

    pinned_blocks.reserve(num_buffers);
    BufferedBlockMgr::Block* new_block;
    for (int i = 0; i < num_iterations; ++i) {
      if ((i % 20000) == 0) LOG (ERROR) << " Iteration " << i << endl;
      if (i > iters_before_close && (rand() % 5 == 0)) {
        api_function = Close;
      } else if (pinned_blocks.size() == 0 && unpinned_blocks.size() == 0) {
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
      bool pinned;
      switch (api_function) {
        case New:
          status = block_mgr->GetNewBlock(client, NULL, &new_block);
          if (close_called) {
            EXPECT_TRUE(new_block == NULL);
            EXPECT_TRUE(status.IsCancelled());
            continue;
          }
          EXPECT_TRUE(status.ok());
          EXPECT_TRUE(new_block != NULL);
          data = MakeRandomSizeData(new_block);
          block_data = make_pair(new_block, *data);

          pinned_blocks.push_back(block_data);
          pinned_block_map.insert(make_pair(block_data.first, pinned_blocks.size() - 1));
          break;
        case Pin:
          rand_pick = rand() % unpinned_blocks.size();
          block_data = unpinned_blocks[rand_pick];
          status = block_data.first->Pin(&pinned);
          if (close_called) {
            EXPECT_TRUE(status.IsCancelled());
            EXPECT_FALSE(pinned);
            continue;
          }
          EXPECT_TRUE(status.ok());
          EXPECT_TRUE(pinned);
          ValidateRandomSizeData(block_data.first, block_data.second);
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
          if (close_called) {
            EXPECT_TRUE(status.IsCancelled());
            continue;
          }
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
          if (close_called) {
            EXPECT_TRUE(status.IsCancelled());
            continue;
          }
          EXPECT_TRUE(status.ok());
          pinned_blocks[rand_pick] = pinned_blocks.back();
          pinned_blocks.pop_back();
          pinned_block_map[pinned_blocks[rand_pick].first] = rand_pick;
          break;
        case Close:
          block_mgr->Cancel();
          close_called = true;
          break;
      } // end switch (apiFunction)
    } // end for ()

    block_mgr.reset();
    EXPECT_TRUE(block_mgr_parent_tracker_->consumption() == 0);
  }

  scoped_ptr<ExecEnv> exec_env_;
  scoped_ptr<RuntimeState> runtime_state_;
  scoped_ptr<MemTracker> block_mgr_parent_tracker_;
  scoped_ptr<MemTracker> io_mgr_tracker_;
};

TEST_F(BufferedBlockMgrTest, GetNewBlock) {
  int max_num_blocks = 5;
  shared_ptr<BufferedBlockMgr> block_mgr = CreateMgr(max_num_blocks);
  BufferedBlockMgr::Client* client;
  Status status = block_mgr->RegisterClient(0, NULL, runtime_state_.get(), &client);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(block_mgr_parent_tracker_->consumption() == 0);

  // Allocate blocks until max_num_blocks, they should all succeed and memory
  // usage should go up.
  BufferedBlockMgr::Block* new_block;
  BufferedBlockMgr::Block* first_block = NULL;
  for (int i = 0; i < max_num_blocks; ++i) {
    status = block_mgr->GetNewBlock(client, NULL, &new_block);
    EXPECT_TRUE(new_block != NULL);
    EXPECT_EQ(block_mgr->bytes_allocated(), (i + 1) * block_size_);
    if (first_block == NULL) first_block = new_block;
  }

  // Trying to allocate a new one should fail.
  status = block_mgr->GetNewBlock(client, NULL, &new_block);
  EXPECT_TRUE(new_block == NULL);
  EXPECT_EQ(block_mgr->bytes_allocated(), max_num_blocks * block_size_);

  // We can allocate a new block by transferring an already allocated one.
  uint8_t* old_buffer = first_block->buffer();
  status = block_mgr->GetNewBlock(client, first_block, &new_block);
  EXPECT_TRUE(new_block != NULL);
  EXPECT_TRUE(old_buffer == new_block->buffer());
  EXPECT_EQ(block_mgr->bytes_allocated(), max_num_blocks * block_size_);
  EXPECT_TRUE(!first_block->is_pinned());

  // Trying to allocate a new one should still fail.
  status = block_mgr->GetNewBlock(client, NULL, &new_block);
  EXPECT_TRUE(new_block == NULL);
  EXPECT_EQ(block_mgr->bytes_allocated(), max_num_blocks * block_size_);

  EXPECT_EQ(block_mgr->writes_issued(), 1);;
  block_mgr.reset();
  EXPECT_TRUE(block_mgr_parent_tracker_->consumption() == 0);
}

TEST_F(BufferedBlockMgrTest, GetNewBlockSmallBlocks) {
  int max_num_blocks = 3;
  shared_ptr<BufferedBlockMgr> block_mgr = CreateMgr(max_num_blocks);
  BufferedBlockMgr::Client* client;
  MemTracker tracker;
  Status status = block_mgr->RegisterClient(0, &tracker, runtime_state_.get(), &client);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(block_mgr_parent_tracker_->consumption() == 0);

  vector<BufferedBlockMgr::Block*> blocks;

  // Allocate a small block.
  BufferedBlockMgr::Block* new_block = NULL;
  status = block_mgr->GetNewBlock(client, NULL, &new_block, 128);
  EXPECT_TRUE(new_block != NULL);
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(block_mgr->bytes_allocated(), 0);
  EXPECT_EQ(block_mgr_parent_tracker_->consumption(), 0);
  EXPECT_EQ(tracker.consumption(), 128);
  EXPECT_TRUE(new_block->is_pinned());
  EXPECT_EQ(new_block->BytesRemaining(), 128);
  EXPECT_TRUE(new_block->buffer() != NULL);
  blocks.push_back(new_block);

  // Allocate a normal block
  status = block_mgr->GetNewBlock(client, NULL, &new_block);
  EXPECT_TRUE(new_block != NULL);
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(block_mgr->bytes_allocated(), block_mgr->max_block_size());
  EXPECT_EQ(block_mgr_parent_tracker_->consumption(), block_mgr->max_block_size());
  EXPECT_EQ(tracker.consumption(), 128 + block_mgr->max_block_size());
  EXPECT_TRUE(new_block->is_pinned());
  EXPECT_EQ(new_block->BytesRemaining(), block_mgr->max_block_size());
  EXPECT_TRUE(new_block->buffer() != NULL);
  blocks.push_back(new_block);

  // Allocate another small block.
  status = block_mgr->GetNewBlock(client, NULL, &new_block, 512);
  EXPECT_TRUE(new_block != NULL);
  EXPECT_TRUE(status.ok());
  EXPECT_EQ(block_mgr->bytes_allocated(), block_mgr->max_block_size());
  EXPECT_EQ(block_mgr_parent_tracker_->consumption(), block_mgr->max_block_size());
  EXPECT_EQ(tracker.consumption(), 128 + 512 + block_mgr->max_block_size());
  EXPECT_TRUE(new_block->is_pinned());
  EXPECT_EQ(new_block->BytesRemaining(), 512);
  EXPECT_TRUE(new_block->buffer() != NULL);
  blocks.push_back(new_block);

  // Should be able to unpin and pin the middle block
  status = blocks[1]->Unpin();
  EXPECT_TRUE(status.ok());

  bool pinned;
  status = blocks[1]->Pin(&pinned);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(pinned);

  for (int i = 0; i < blocks.size(); ++i) {
    blocks[i]->Delete();
  }
  block_mgr.reset();
  EXPECT_TRUE(block_mgr_parent_tracker_->consumption() == 0);
}

// Test that pinning more blocks than the max available buffers.
TEST_F(BufferedBlockMgrTest, Pin) {
  int max_num_blocks = 5;
  shared_ptr<BufferedBlockMgr> block_mgr = CreateMgr(max_num_blocks);
  BufferedBlockMgr::Client* client;
  Status status = block_mgr->RegisterClient(0, NULL, runtime_state_.get(), &client);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(client != NULL);

  vector<BufferedBlockMgr::Block*> blocks;
  AllocateBlocks(block_mgr.get(), client, max_num_blocks, &blocks);

  // Unpin them all.
  for (int i = 0; i < blocks.size(); ++i) {
    status = blocks[i]->Unpin();
    EXPECT_TRUE(status.ok());
  }

  // Allocate more, this should work since we just unpinned some blocks.
  AllocateBlocks(block_mgr.get(), client, max_num_blocks, &blocks);

  // Try to pin a unpinned block, this should not be possible.
  bool pinned;
  status = blocks[0]->Pin(&pinned);
  EXPECT_TRUE(status.ok());
  EXPECT_FALSE(pinned);

  // Unpin all blocks.
  for (int i = 0; i < blocks.size(); ++i) {
    status = blocks[i]->Unpin();
    EXPECT_TRUE(status.ok());
  }

  // Should be able to pin max_num_blocks blocks.
  for (int i = 0; i < max_num_blocks; ++i) {
    status = blocks[i]->Pin(&pinned);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(pinned);
  }

  // Can't pin any more though.
  status = blocks[max_num_blocks]->Pin(&pinned);
  EXPECT_TRUE(status.ok());
  EXPECT_FALSE(pinned);

  block_mgr.reset();
  EXPECT_TRUE(block_mgr_parent_tracker_->consumption() == 0);
}

// Test the eviction policy of the block mgr. No writes issued until more than
// the max available buffers are allocated. Writes must be issued in LIFO order.
TEST_F(BufferedBlockMgrTest, Eviction) {
  int max_num_buffers = 5;
  shared_ptr<BufferedBlockMgr> block_mgr = CreateMgr(max_num_buffers);
  BufferedBlockMgr::Client* client;
  Status status = block_mgr->RegisterClient(0, NULL, runtime_state_.get(), &client);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(client != NULL);

  // Check counters.
  RuntimeProfile* profile = block_mgr->profile();
  RuntimeProfile::Counter* buffered_pin = profile->GetCounter("BufferedPins");

  vector<BufferedBlockMgr::Block*> blocks;
  AllocateBlocks(block_mgr.get(), client, max_num_buffers, &blocks);

  EXPECT_EQ(block_mgr->bytes_allocated(), max_num_buffers * block_size_);
  BOOST_FOREACH(BufferedBlockMgr::Block* block, blocks) {
    block->Unpin();
  }

  // Re-pinning all blocks
  for (int i = 0; i < blocks.size(); ++i) {
    bool pinned;
    status = blocks[i]->Pin(&pinned);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(pinned);
    ValidateBlock(blocks[i], i);
  }
  int buffered_pins_expected = blocks.size();
  EXPECT_EQ(buffered_pin->value(), buffered_pins_expected);

  // Unpin all blocks
  BOOST_FOREACH(BufferedBlockMgr::Block* block, blocks) {
    block->Unpin();
  }
  // Get two new blocks.
  AllocateBlocks(block_mgr.get(), client, 2, &blocks);
  // At least two writes must be issued. The first (num_blocks - 2) must be in memory.
  EXPECT_GE(block_mgr->writes_issued(), 2);
  for (int i = 0; i < (max_num_buffers - 2); ++i) {
    bool pinned;
    status = blocks[i]->Pin(&pinned);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(pinned);
    ValidateBlock(blocks[i], i);
  }
  EXPECT_GE(buffered_pin->value(),  buffered_pins_expected);

  block_mgr.reset();
  EXPECT_TRUE(block_mgr_parent_tracker_->consumption() == 0);
}

// Test deletion and reuse of blocks.
TEST_F(BufferedBlockMgrTest, Deletion) {
  int max_num_buffers = 5;
  shared_ptr<BufferedBlockMgr> block_mgr = CreateMgr(max_num_buffers);
  BufferedBlockMgr::Client* client;
  Status status = block_mgr->RegisterClient(0, NULL, runtime_state_.get(), &client);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(client != NULL);

  // Check counters.
  RuntimeProfile* profile = block_mgr->profile();
  RuntimeProfile::Counter* recycled_cnt = profile->GetCounter("BlocksRecycled");
  RuntimeProfile::Counter* created_cnt = profile->GetCounter("BlocksCreated");

  vector<BufferedBlockMgr::Block*> blocks;
  AllocateBlocks(block_mgr.get(), client, max_num_buffers, &blocks);
  EXPECT_TRUE(created_cnt->value() == max_num_buffers);

  BOOST_FOREACH(BufferedBlockMgr::Block* block, blocks) {
    block->Delete();
  }
  AllocateBlocks(block_mgr.get(), client, max_num_buffers, &blocks);
  EXPECT_TRUE(created_cnt->value() == max_num_buffers);
  EXPECT_TRUE(recycled_cnt->value() == max_num_buffers);

  block_mgr.reset();
  EXPECT_TRUE(block_mgr_parent_tracker_->consumption() == 0);
}

// Test that all APIs return cancelled after close.
TEST_F(BufferedBlockMgrTest, Close) {
  int max_num_buffers = 5;
  shared_ptr<BufferedBlockMgr> block_mgr = CreateMgr(max_num_buffers);
  BufferedBlockMgr::Client* client;
  Status status = block_mgr->RegisterClient(0, NULL, runtime_state_.get(), &client);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(client != NULL);

  vector<BufferedBlockMgr::Block*> blocks;
  AllocateBlocks(block_mgr.get(), client, max_num_buffers, &blocks);

  block_mgr->Cancel();

  BufferedBlockMgr::Block* new_block;
  status = block_mgr->GetNewBlock(client, NULL, &new_block);
  EXPECT_TRUE(new_block == NULL);
  EXPECT_TRUE(status.IsCancelled());
  status = blocks[0]->Unpin();
  EXPECT_TRUE(status.IsCancelled());
  bool pinned;
  status = blocks[0]->Pin(&pinned);
  EXPECT_TRUE(status.IsCancelled());
  status = blocks[1]->Delete();
  EXPECT_TRUE(status.IsCancelled());

  block_mgr.reset();
  EXPECT_TRUE(block_mgr_parent_tracker_->consumption() == 0);
}

// Test that the block manager behaves correctly after a write error
// Delete the scratch directory before an operation that would cause a write
// and test that subsequent API calls return 'CANCELLED' correctly.
TEST_F(BufferedBlockMgrTest, WriteError) {
  int max_num_buffers = 2;
  const int write_wait_millis = 500;
  shared_ptr<BufferedBlockMgr> block_mgr = CreateMgr(max_num_buffers);
  BufferedBlockMgr::Client* client;
  Status status = block_mgr->RegisterClient(0, NULL, runtime_state_.get(), &client);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(client != NULL);

  RuntimeProfile* profile = block_mgr->profile();
  RuntimeProfile::Counter* writes_outstanding =
      profile->GetCounter("BlockWritesOutstanding");
  vector<BufferedBlockMgr::Block*> blocks;
  AllocateBlocks(block_mgr.get(), client, max_num_buffers, &blocks);
  // Unpin a block, forcing a write.
  status = blocks[0]->Unpin();
  EXPECT_TRUE(status.ok());
  // Wait for the write to go through.
  SleepForMs(write_wait_millis);
  EXPECT_TRUE(writes_outstanding->value() == 0);

  // Empty the scratch directory.
  int num_files = 0;
  directory_iterator dir_it(SCRATCH_DIR);
  for (; dir_it != directory_iterator(); ++dir_it) {
    ++num_files;
    remove_all(dir_it->path());
  }
  EXPECT_TRUE(num_files > 0);
  status = blocks[1]->Unpin();
  EXPECT_TRUE(status.ok());
  // Allocate one more block, forcing a write and causing an error.
  AllocateBlocks(block_mgr.get(), client, 1, &blocks);
  // Wait for the write to go through.
  SleepForMs(write_wait_millis);
  EXPECT_TRUE(writes_outstanding->value() == 0);

  // Subsequent calls should fail.
  status = blocks[2]->Delete();
  EXPECT_TRUE(status.IsCancelled());
  BufferedBlockMgr::Block* new_block;
  status = block_mgr->GetNewBlock(client, NULL, &new_block);
  EXPECT_TRUE(new_block == NULL);
  EXPECT_TRUE(status.IsCancelled());
  block_mgr.reset();
  EXPECT_TRUE(block_mgr_parent_tracker_->consumption() == 0);
}

// Create two clients with different number of reserved buffers.
TEST_F(BufferedBlockMgrTest, MultipleClients) {
  int client1_buffers = 3;
  int client2_buffers = 5;
  int max_num_buffers = client1_buffers + client2_buffers;

  shared_ptr<BufferedBlockMgr> block_mgr = CreateMgr(max_num_buffers);
  BufferedBlockMgr::Client* client1;
  BufferedBlockMgr::Client* client2;
  Status status;
  bool reserved = false;

  status = block_mgr->RegisterClient(client1_buffers, NULL, runtime_state_.get(),
      &client1);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(client1 != NULL);
  status = block_mgr->RegisterClient(client2_buffers, NULL, runtime_state_.get(),
      &client2);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(client2 != NULL);

  // Reserve client 1's and 2's buffers. They should succeed.
  reserved = block_mgr->TryAcquireTmpReservation(client1, 1);
  EXPECT_TRUE(reserved);
  reserved = block_mgr->TryAcquireTmpReservation(client2, 1);
  EXPECT_TRUE(reserved);

  vector<BufferedBlockMgr::Block*> client1_blocks;
  // Allocate all of client1's reserved blocks, they should all succeed.
  AllocateBlocks(block_mgr.get(), client1, client1_buffers, &client1_blocks);

  // Try allocating one more, that should fail.
  BufferedBlockMgr::Block* block;
  status = block_mgr->GetNewBlock(client1, NULL, &block);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(block == NULL);

  // Trying to reserve should also fail.
  reserved = block_mgr->TryAcquireTmpReservation(client1, 1);
  EXPECT_FALSE(reserved);

  // Allocate all of client2's reserved blocks, these should succeed.
  vector<BufferedBlockMgr::Block*> client2_blocks;
  AllocateBlocks(block_mgr.get(), client2, client2_buffers, &client2_blocks);

  // Try allocating one more from client 2, that should fail.
  status = block_mgr->GetNewBlock(client2, NULL, &block);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(block == NULL);

  // Unpin one block from client 1.
  status = client1_blocks[0]->Unpin();
  EXPECT_TRUE(status.ok());

  // Client 2 should still not be able to allocate.
  status = block_mgr->GetNewBlock(client2, NULL, &block);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(block == NULL);

  // Client 2 should still not be able to reserve.
  reserved = block_mgr->TryAcquireTmpReservation(client2, 1);
  EXPECT_FALSE(reserved);

  // Client 1 should be able to though.
  status = block_mgr->GetNewBlock(client1, NULL, &block);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(block != NULL);

  // Unpin two of client 1's blocks (client 1 should have 3 unpinned blocks now).
  status = client1_blocks[1]->Unpin();
  EXPECT_TRUE(status.ok());
  status = client1_blocks[2]->Unpin();
  EXPECT_TRUE(status.ok());

  // Clear client 1's reservation
  block_mgr->ClearReservations(client1);

  // Client 2 should be able to reserve 1 buffers now (there are 2 left);
  reserved = block_mgr->TryAcquireTmpReservation(client2, 1);
  EXPECT_TRUE(reserved);

  // Client one can only pin 1.
  bool pinned;
  status = client1_blocks[0]->Pin(&pinned);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(pinned);
  // Can't get this one.
  status = client1_blocks[1]->Pin(&pinned);
  EXPECT_TRUE(status.ok());
  EXPECT_FALSE(pinned);

  // Client 2 can pick up the one reserved buffer
  status = block_mgr->GetNewBlock(client2, NULL, &block);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(block != NULL);
  // But not a second
  BufferedBlockMgr::Block* block2;
  status = block_mgr->GetNewBlock(client2, NULL, &block2);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(block2 == NULL);

  // Unpin client 2's block it got from the reservation. Sine this is a tmp
  // reservation, client 1 can pick it up again (it is not longer reserved).
  status = block->Unpin();
  EXPECT_TRUE(status.ok());
  status = client1_blocks[1]->Pin(&pinned);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(pinned);

  block_mgr.reset();
  EXPECT_TRUE(block_mgr_parent_tracker_->consumption() == 0);
}

// Create two clients with different number of reserved buffers and some additional.
TEST_F(BufferedBlockMgrTest, MultipleClientsExtraBuffers) {
  int client1_buffers = 1;
  int client2_buffers = 1;
  int max_num_buffers = client1_buffers + client2_buffers + 2;

  shared_ptr<BufferedBlockMgr> block_mgr = CreateMgr(max_num_buffers);
  BufferedBlockMgr::Client* client1;
  BufferedBlockMgr::Client* client2;
  Status status;
  BufferedBlockMgr::Block* block;

  status = block_mgr->RegisterClient(client1_buffers, NULL, runtime_state_.get(),
      &client1);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(client1 != NULL);
  status = block_mgr->RegisterClient(client2_buffers, NULL, runtime_state_.get(),
      &client2);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(client2 != NULL);

  vector<BufferedBlockMgr::Block*> client1_blocks;
  // Allocate all of client1's reserved blocks, they should all succeed.
  AllocateBlocks(block_mgr.get(), client1, client1_buffers, &client1_blocks);

  // Allocate all of client2's reserved blocks, these should succeed.
  vector<BufferedBlockMgr::Block*> client2_blocks;
  AllocateBlocks(block_mgr.get(), client2, client2_buffers, &client2_blocks);

  // We have two spare buffers now. Each client should be able to allocate it.
  status = block_mgr->GetNewBlock(client1, NULL, &block);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(block != NULL);
  status = block_mgr->GetNewBlock(client2, NULL, &block);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(block != NULL);

  // Now we are completely full, no one should be able to allocate a new block.
  status = block_mgr->GetNewBlock(client1, NULL, &block);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(block == NULL);
  status = block_mgr->GetNewBlock(client2, NULL, &block);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(block == NULL);

  block_mgr.reset();
  EXPECT_TRUE(block_mgr_parent_tracker_->consumption() == 0);
}

// Create two clients causing oversubscription.
TEST_F(BufferedBlockMgrTest, ClientOversubscription) {
  int client1_buffers = 1;
  int client2_buffers = 2;
  int max_num_buffers = 2;

  shared_ptr<BufferedBlockMgr> block_mgr = CreateMgr(max_num_buffers);
  BufferedBlockMgr::Client* client1;
  BufferedBlockMgr::Client* client2;
  Status status;
  BufferedBlockMgr::Block* block;

  status = block_mgr->RegisterClient(client1_buffers, NULL, runtime_state_.get(),
      &client1);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(client1 != NULL);
  status = block_mgr->RegisterClient(client2_buffers, NULL, runtime_state_.get(),
      &client2);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(client2 != NULL);

  // Client one allocates first block, should work.
  status = block_mgr->GetNewBlock(client1, NULL, &block);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(block != NULL);

  // Client two allocates first block, should work.
  status = block_mgr->GetNewBlock(client2, NULL, &block);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(block != NULL);

  // At this point we've used both buffers. Client one reserved one so subsequent
  // calls should fail with no error (but returns no block).
  status = block_mgr->GetNewBlock(client1, NULL, &block);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(block == NULL);

  // Allocate with client two. Since client two reserved 2 buffers, this should fail
  // with MEM_LIMIT_EXCEEDED.
  status = block_mgr->GetNewBlock(client2, NULL, &block);
  EXPECT_TRUE(status.IsMemLimitExceeded());

  block_mgr.reset();
  EXPECT_TRUE(block_mgr_parent_tracker_->consumption() == 0);
}

TEST_F(BufferedBlockMgrTest, Random_plain) {
  FLAGS_disk_spill_encryption = false;
  TestRandomInternal();
}

TEST_F(BufferedBlockMgrTest, Random_integ_enc) {
  FLAGS_disk_spill_encryption = true;
  TestRandomInternal();
}

}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport();
  impala::TmpFileMgr::Init();
  impala::LlvmCodeGen::InitializeLlvm();
  return RUN_ALL_TESTS();
}
