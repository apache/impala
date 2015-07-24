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
#include <gutil/strings/substitute.h>
#include <sys/stat.h>

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
#include "util/filesystem-util.h"
#include "util/promise.h"
#include "util/test-info.h"
#include "util/time.h"

#include "gen-cpp/Types_types.h"
#include "gen-cpp/ImpalaInternalService_types.h"

#include "common/names.h"

using boost::filesystem::directory_iterator;
using boost::filesystem::remove;
using strings::Substitute;

// Note: This is the default scratch dir created by impala.
// FLAGS_scratch_dirs + TmpFileMgr::TMP_SUB_DIR_NAME.
const string SCRATCH_DIR = "/tmp/impala-scratch";

// This suffix is appended to a tmp dir
const string SCRATCH_SUFFIX = "/impala-scratch";

// Number of millieconds to wait to ensure write completes
const static int WRITE_WAIT_MILLIS = 500;

// How often to check for write completion
const static int WRITE_CHECK_INTERVAL_MILLIS = 10;

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
        new RuntimeState(TExecPlanFragmentParams(), "", exec_env_.get()));
    metrics_.reset(new MetricGroup("buffered-block-mgr-test"));
    tmp_file_mgr_.reset(new TmpFileMgr);
    tmp_file_mgr_->Init(metrics_.get());
  }

  virtual void TearDown() {
    block_mgr_parent_tracker_.reset();
    tmp_file_mgr_.reset();
    runtime_state_.reset();
    exec_env_.reset();
    io_mgr_tracker_.reset();
    // Tests modify permissions, so make sure we can delete if they didn't clean up.
    for (int i = 0; i < created_tmp_dirs_.size(); ++i) {
      chmod((created_tmp_dirs_[i] + SCRATCH_SUFFIX).c_str(), S_IRWXU);
    }
    FileSystemUtil::RemovePaths(created_tmp_dirs_);
    created_tmp_dirs_.clear();
  }

  vector<string> InitMultipleTmpDirs(int num_dirs, shared_ptr<TmpFileMgr>* tmp_file_mgr) {
    vector<string> tmp_dirs;
    for (int i = 0; i < num_dirs; ++i) {
      const string& dir = Substitute("/tmp/buffered-block-mgr-test.$0", i);
      // Fix permissions in case old directories were left from previous runs of test.
      chmod((dir + SCRATCH_SUFFIX).c_str(), S_IRWXU);
      EXPECT_TRUE(FileSystemUtil::CreateDirectory(dir).ok());
      tmp_dirs.push_back(dir);
      created_tmp_dirs_.push_back(dir);
    }
    metrics_.reset(new MetricGroup("buffered-block-mgr-test"));
    tmp_file_mgr->reset(new TmpFileMgr);
    (*tmp_file_mgr)->InitCustom(tmp_dirs, false, metrics_.get());
    EXPECT_EQ(num_dirs, (*tmp_file_mgr)->num_active_tmp_devices());
    return tmp_dirs;
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

  shared_ptr<BufferedBlockMgr> CreateMgr(int max_buffers, TmpFileMgr* tmp_file_mgr=NULL) {
    if (tmp_file_mgr == NULL) tmp_file_mgr = tmp_file_mgr_.get();
    shared_ptr<BufferedBlockMgr> mgr;
    BufferedBlockMgr::Create(runtime_state_.get(),
        block_mgr_parent_tracker_.get(), runtime_state_->runtime_profile(),
        tmp_file_mgr, max_buffers * block_size_, block_size_, &mgr);
    EXPECT_TRUE(mgr != NULL);
    EXPECT_TRUE(block_mgr_parent_tracker_->consumption() == 0);
    return mgr;
  }

  static RuntimeState* CreateRuntimeState(int64_t query_id, ExecEnv* exec_env) {
    TExecPlanFragmentParams plan_params = TExecPlanFragmentParams();
    plan_params.fragment_instance_ctx.query_ctx.query_id.hi = 0;
    plan_params.fragment_instance_ctx.query_ctx.query_id.lo = query_id;
    return new RuntimeState(plan_params, "", exec_env);
  }

  shared_ptr<BufferedBlockMgr> CreateQueryMgr(TmpFileMgr* tmp_file_mgr,
      int64_t query_id, int blocks_per_mgr, shared_ptr<RuntimeState>* runtime_state) {
    runtime_state->reset(CreateRuntimeState(query_id, exec_env_.get()));
    EXPECT_TRUE(runtime_state != NULL);
    shared_ptr<BufferedBlockMgr> mgr;
    BufferedBlockMgr::Create(runtime_state->get(),
        block_mgr_parent_tracker_.get(), (*runtime_state)->runtime_profile(),
        tmp_file_mgr, blocks_per_mgr * block_size_, block_size_, &mgr);
    EXPECT_TRUE(mgr != NULL);
    return mgr;
  }

  // Create multiple separate managers, e.g. as if multiple queries were executing.
  // Return created objects for use or later deletion by caller. Returned objects that
  // must be deleted are wrapped in shared_ptrs so that occurs automatically.
  void CreateQueryMgrs(TmpFileMgr* tmp_file_mgr, int num_mgrs, int max_buffers,
      vector<shared_ptr<RuntimeState> >* runtime_states,
      vector<shared_ptr<BufferedBlockMgr> >* mgrs,
      vector<BufferedBlockMgr::Client*>* clients) {
    for (int i = 0; i < num_mgrs; ++i) {
      shared_ptr<RuntimeState> runtime_state;
      shared_ptr<BufferedBlockMgr> mgr = CreateQueryMgr(tmp_file_mgr, i, max_buffers,
          &runtime_state);
      BufferedBlockMgr::Client* client;
      mgr->RegisterClient(0, NULL, runtime_state.get(), &client);
      runtime_states->push_back(runtime_state);
      mgrs->push_back(mgr);
      clients->push_back(client);
    }
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

  // Pin all blocks, expecting they are pinned successfully.
  void PinBlocks(const vector<BufferedBlockMgr::Block*>& blocks) {
    for (int i = 0; i < blocks.size(); ++i) {
      bool pinned;
      EXPECT_TRUE(blocks[i]->Pin(&pinned).ok());
      EXPECT_TRUE(pinned);
    }
  }

  // Pin all blocks, expecting no errors from Unpin() calls.
  void UnpinBlocks(const vector<BufferedBlockMgr::Block*>& blocks) {
    for (int i = 0; i < blocks.size(); ++i) {
      EXPECT_TRUE(blocks[i]->Unpin().ok());
    }
  }

  static void WaitForWrites(const shared_ptr<BufferedBlockMgr>& block_mgr) {
    vector<shared_ptr<BufferedBlockMgr> > block_mgrs;
    block_mgrs.push_back(block_mgr);
    WaitForWrites(block_mgrs);
  }

  // Wait for writes issued through block managers to complete.
  static void WaitForWrites(const vector<shared_ptr<BufferedBlockMgr> >& block_mgrs) {
    int max_attempts = WRITE_WAIT_MILLIS / WRITE_CHECK_INTERVAL_MILLIS;
    for (int i = 0; i < max_attempts; ++i) {
      SleepForMs(WRITE_CHECK_INTERVAL_MILLIS);
      if (AllWritesComplete(block_mgrs)) return;
    }
    EXPECT_TRUE(false) << "Writes did not complete after " << WRITE_WAIT_MILLIS << "ms";
  }

  static bool AllWritesComplete(const vector<shared_ptr<BufferedBlockMgr> >& block_mgrs) {
    for (int i = 0; i < block_mgrs.size(); ++i) {
      RuntimeProfile::Counter* writes_outstanding =
          block_mgrs[i]->profile()->GetCounter("BlockWritesOutstanding");
      if (writes_outstanding->value() != 0) return false;
    }
    return true;
  }

  // Delete the temporary file backing a block - all subsequent writes to the file
  // should fail. Expects backing file has already been allocated.
  static void DeleteBackingFile(BufferedBlockMgr::Block* block) {
    const string& path = block->TmpFilePath();
    EXPECT_GT(path.size(), 0);
    EXPECT_TRUE(remove(path));
    LOG(INFO) << "Injected fault by deleting file " << path;
  }

  // Check that the file backing the block has dir as a prefix of its path.
  static bool BlockInDir(BufferedBlockMgr::Block* block, const string& dir) {
    return block->TmpFilePath().find(dir) == 0;
  }

  // Find a block in the list that is backed by a file with the given directory as prefix
  // of its path.
  static BufferedBlockMgr::Block* FindBlockForDir(
      const vector<BufferedBlockMgr::Block*>& blocks, const string& dir) {
    for (int i = 0; i < blocks.size(); ++i) {
      if (BlockInDir(blocks[i], dir)) return blocks[i];
    }
    return NULL;
  }

  // Test that randomly issues GetFreeBlock(), Pin(), Unpin(), Delete() and Close()
  // calls. All calls made are legal - error conditions are not expected until the
  // first call to Close().  This is called 2 times with encryption+integrity on/off.
  // When executed in single-threaded mode 'tid' should be SINGLE_THREADED_TID.
  static const int SINGLE_THREADED_TID = -1;
  void TestRandomInternalImpl(BufferedBlockMgr* block_mgr, int num_buffers, int tid) {
    DCHECK(block_mgr != NULL);
    const int num_iterations = 100000;
    const int iters_before_close = num_iterations - 5000;
    bool close_called = false;
    unordered_map<BufferedBlockMgr::Block*, int> pinned_block_map;
    vector<pair<BufferedBlockMgr::Block*, int32_t> > pinned_blocks;
    unordered_map<BufferedBlockMgr::Block*, int> unpinned_block_map;
    vector<pair<BufferedBlockMgr::Block*, int32_t> > unpinned_blocks;

    typedef enum { Pin, New, Unpin, Delete, Close } ApiFunction;
    ApiFunction api_function;

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
      int rand_pick = 0;
      int32_t* data = NULL;
      bool pinned = false;
      switch (api_function) {
        case New:
          status = block_mgr->GetNewBlock(client, NULL, &new_block);
          if (close_called || (tid != SINGLE_THREADED_TID && status.IsCancelled())) {
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
          if (close_called || (tid != SINGLE_THREADED_TID && status.IsCancelled())) {
            EXPECT_TRUE(status.IsCancelled());
            // In single-threaded runs the block should not have been pinned.
            // In multi-threaded runs Pin() may return the block pinned but the status to
            // be cancelled. In this case we could move the block from unpinned_blocks
            // to pinned_blocks. We do not do that because after IsCancelled() no actual
            // block operations should take place.
            if (tid == SINGLE_THREADED_TID) EXPECT_FALSE(pinned);
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
          if (close_called || (tid != SINGLE_THREADED_TID && status.IsCancelled())) {
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
          if (close_called || (tid != SINGLE_THREADED_TID && status.IsCancelled())) {
            EXPECT_TRUE(status.IsCancelled());
          } else {
            EXPECT_TRUE(status.ok());
          }
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
  }

  // Single-threaded execution of the TestRandomInternalImpl.
  void TestRandomInternalSingle() {
    const int num_buffers = 10;
    shared_ptr<BufferedBlockMgr> block_mgr = CreateMgr(num_buffers);
    TestRandomInternalImpl(block_mgr.get(), num_buffers, SINGLE_THREADED_TID);
    block_mgr.reset();
    EXPECT_EQ(block_mgr_parent_tracker_->consumption(), 0);
  }

  // Multi-threaded execution of the TestRandomInternalImpl.
  void TestRandomInternalMulti(int num_threads) {
    DCHECK_GT(num_threads, 0);
    const int num_buffers = 10;
    shared_ptr<BufferedBlockMgr> block_mgr = CreateMgr(num_buffers * num_threads);
    thread_group workers;
    for (int i = 0; i < num_threads; ++i) {
      thread* t = new thread(bind(&BufferedBlockMgrTest::TestRandomInternalImpl, this,
                                  block_mgr.get(), num_buffers, i));
      workers.add_thread(t);
    }
    workers.join_all();
    block_mgr.reset();
    EXPECT_EQ(block_mgr_parent_tracker_->consumption(), 0);
  }

  scoped_ptr<ExecEnv> exec_env_;
  scoped_ptr<RuntimeState> runtime_state_;
  scoped_ptr<MemTracker> block_mgr_parent_tracker_;
  scoped_ptr<MemTracker> io_mgr_tracker_;
  scoped_ptr<MetricGroup> metrics_;
  scoped_ptr<TmpFileMgr> tmp_file_mgr_;
  vector<string> created_tmp_dirs_;
};

TEST_F(BufferedBlockMgrTest, GetNewBlock) {
  int max_num_blocks = 5;
  shared_ptr<BufferedBlockMgr> block_mgr = CreateMgr(max_num_blocks);
  BufferedBlockMgr::Client* client;
  EXPECT_TRUE(block_mgr->RegisterClient(0, NULL, runtime_state_.get(), &client).ok());
  EXPECT_EQ(block_mgr_parent_tracker_->consumption(), 0);

  // Allocate blocks until max_num_blocks, they should all succeed and memory
  // usage should go up.
  BufferedBlockMgr::Block* new_block;
  BufferedBlockMgr::Block* first_block = NULL;
  for (int i = 0; i < max_num_blocks; ++i) {
    EXPECT_TRUE(block_mgr->GetNewBlock(client, NULL, &new_block).ok());
    EXPECT_TRUE(new_block != NULL);
    EXPECT_EQ(block_mgr->bytes_allocated(), (i + 1) * block_size_);
    if (first_block == NULL) first_block = new_block;
  }

  // Trying to allocate a new one should fail.
  EXPECT_TRUE(block_mgr->GetNewBlock(client, NULL, &new_block).ok());
  EXPECT_TRUE(new_block == NULL);
  EXPECT_EQ(block_mgr->bytes_allocated(), max_num_blocks * block_size_);

  // We can allocate a new block by transferring an already allocated one.
  uint8_t* old_buffer = first_block->buffer();
  EXPECT_TRUE(block_mgr->GetNewBlock(client, first_block, &new_block).ok());
  EXPECT_TRUE(new_block != NULL);
  EXPECT_TRUE(old_buffer == new_block->buffer());
  EXPECT_EQ(block_mgr->bytes_allocated(), max_num_blocks * block_size_);
  EXPECT_TRUE(!first_block->is_pinned());

  // Trying to allocate a new one should still fail.
  EXPECT_TRUE(block_mgr->GetNewBlock(client, NULL, &new_block).ok());
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
  EXPECT_TRUE(block_mgr->RegisterClient(0, &tracker, runtime_state_.get(), &client).ok());
  EXPECT_TRUE(block_mgr_parent_tracker_->consumption() == 0);

  vector<BufferedBlockMgr::Block*> blocks;

  // Allocate a small block.
  BufferedBlockMgr::Block* new_block = NULL;
  EXPECT_TRUE(block_mgr->GetNewBlock(client, NULL, &new_block, 128).ok());
  EXPECT_TRUE(new_block != NULL);
  EXPECT_EQ(block_mgr->bytes_allocated(), 0);
  EXPECT_EQ(block_mgr_parent_tracker_->consumption(), 0);
  EXPECT_EQ(tracker.consumption(), 128);
  EXPECT_TRUE(new_block->is_pinned());
  EXPECT_EQ(new_block->BytesRemaining(), 128);
  EXPECT_TRUE(new_block->buffer() != NULL);
  blocks.push_back(new_block);

  // Allocate a normal block
  EXPECT_TRUE(block_mgr->GetNewBlock(client, NULL, &new_block).ok());
  EXPECT_TRUE(new_block != NULL);
  EXPECT_EQ(block_mgr->bytes_allocated(), block_mgr->max_block_size());
  EXPECT_EQ(block_mgr_parent_tracker_->consumption(), block_mgr->max_block_size());
  EXPECT_EQ(tracker.consumption(), 128 + block_mgr->max_block_size());
  EXPECT_TRUE(new_block->is_pinned());
  EXPECT_EQ(new_block->BytesRemaining(), block_mgr->max_block_size());
  EXPECT_TRUE(new_block->buffer() != NULL);
  blocks.push_back(new_block);

  // Allocate another small block.
  EXPECT_TRUE(block_mgr->GetNewBlock(client, NULL, &new_block, 512).ok());
  EXPECT_TRUE(new_block != NULL);
  EXPECT_EQ(block_mgr->bytes_allocated(), block_mgr->max_block_size());
  EXPECT_EQ(block_mgr_parent_tracker_->consumption(), block_mgr->max_block_size());
  EXPECT_EQ(tracker.consumption(), 128 + 512 + block_mgr->max_block_size());
  EXPECT_TRUE(new_block->is_pinned());
  EXPECT_EQ(new_block->BytesRemaining(), 512);
  EXPECT_TRUE(new_block->buffer() != NULL);
  blocks.push_back(new_block);

  // Should be able to unpin and pin the middle block
  EXPECT_TRUE(blocks[1]->Unpin().ok());

  bool pinned;
  EXPECT_TRUE(blocks[1]->Pin(&pinned).ok());
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

// Clear scratch directory. Return # of files deleted.
static int clear_scratch_dir() {
  int num_files = 0;
  directory_iterator dir_it(SCRATCH_DIR);
  for (; dir_it != directory_iterator(); ++dir_it) {
    ++num_files;
    remove_all(dir_it->path());
  }
  return num_files;
}

// Test that the block manager behaves correctly after a write error.  Delete the scratch
// directory before an operation that would cause a write and test that subsequent API
// calls return 'CANCELLED' correctly.
TEST_F(BufferedBlockMgrTest, WriteError) {
  int max_num_buffers = 2;
  shared_ptr<BufferedBlockMgr> block_mgr = CreateMgr(max_num_buffers);
  BufferedBlockMgr::Client* client;
  Status status = block_mgr->RegisterClient(0, NULL, runtime_state_.get(), &client);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(client != NULL);

  vector<BufferedBlockMgr::Block*> blocks;
  AllocateBlocks(block_mgr.get(), client, max_num_buffers, &blocks);
  // Unpin two blocks here, to ensure that backing storage is allocated in tmp file.
  for (int i = 0; i < 2; ++i) {
    status = blocks[i]->Unpin();
    EXPECT_TRUE(status.ok());
  }
  WaitForWrites(block_mgr);
  // Repin the blocks
  for (int i = 0; i < 2; ++i) {
    bool pinned;
    status = blocks[i]->Pin(&pinned);
    EXPECT_TRUE(status.ok());
    EXPECT_TRUE(pinned);
  }
  // Remove the backing storage so that future writes will fail
  int num_files = clear_scratch_dir();
  EXPECT_TRUE(num_files > 0);
  for (int i = 0; i < 2; ++i) {
    status = blocks[i]->Unpin();
    EXPECT_TRUE(status.ok());
  }
  WaitForWrites(block_mgr);
  // Subsequent calls should fail.
  for (int i = 0; i < 2; ++i) {
    status = blocks[i]->Delete();
    EXPECT_TRUE(status.IsCancelled());
  }
  BufferedBlockMgr::Block* new_block;
  status = block_mgr->GetNewBlock(client, NULL, &new_block);
  EXPECT_TRUE(new_block == NULL);
  EXPECT_TRUE(status.IsCancelled());
  block_mgr.reset();
  EXPECT_TRUE(block_mgr_parent_tracker_->consumption() == 0);
}

// Test block manager error handling when temporary file space cannot be allocated to
// back an unpinned buffer.
TEST_F(BufferedBlockMgrTest, TmpFileAllocateError) {
  int max_num_buffers = 2;
  shared_ptr<BufferedBlockMgr> block_mgr = CreateMgr(max_num_buffers);
  BufferedBlockMgr::Client* client;
  Status status = block_mgr->RegisterClient(0, NULL, runtime_state_.get(), &client);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(client != NULL);

  vector<BufferedBlockMgr::Block*> blocks;
  AllocateBlocks(block_mgr.get(), client, max_num_buffers, &blocks);
  // Unpin a block, forcing a write.
  status = blocks[0]->Unpin();
  EXPECT_TRUE(status.ok());
  WaitForWrites(block_mgr);
  // Remove temporary files - subsequent operations will fail.
  int num_files = clear_scratch_dir();
  EXPECT_TRUE(num_files > 0);
  // Current implementation will fail here because it tries to expand the tmp file
  // immediately. This behavior is not contractual but we want to know if it changes
  // accidentally.
  status = blocks[1]->Unpin();
  EXPECT_FALSE(status.ok());
  block_mgr.reset();
  EXPECT_TRUE(block_mgr_parent_tracker_->consumption() == 0);
}

// Test that the block manager is able to blacklist a temporary device correctly after a
// write error. We should not allocate more blocks on that device, but existing blocks
// on the device will remain in use.
TEST_F(BufferedBlockMgrTest, WriteErrorBlacklist) {
  // Set up two buffered block managers with two temporary dirs.
  shared_ptr<TmpFileMgr> tmp_file_mgr;
  vector<string> tmp_dirs = InitMultipleTmpDirs(2, &tmp_file_mgr);
  // Simulate two concurrent queries.
  const int NUM_BLOCK_MGRS = 2;
  const int MAX_NUM_BLOCKS = 4;
  int blocks_per_mgr = MAX_NUM_BLOCKS / NUM_BLOCK_MGRS;
  vector<shared_ptr<RuntimeState> > runtime_states;
  vector<shared_ptr<BufferedBlockMgr> > block_mgrs;
  vector<BufferedBlockMgr::Client*> clients;
  CreateQueryMgrs(tmp_file_mgr.get(), NUM_BLOCK_MGRS, blocks_per_mgr, &runtime_states,
      &block_mgrs, &clients);
  // Allocate files for all 2x2 combinations by unpinning blocks.
  vector<vector<BufferedBlockMgr::Block*> > blocks;
  vector<BufferedBlockMgr::Block*> all_blocks;
  for (int i = 0; i < NUM_BLOCK_MGRS; ++i) {
    vector<BufferedBlockMgr::Block*> mgr_blocks;
    AllocateBlocks(block_mgrs[i].get(), clients[i], blocks_per_mgr, &mgr_blocks);
    UnpinBlocks(mgr_blocks);
    for (int j = 0; j < blocks_per_mgr; ++j) {
      LOG(INFO) << "Manager " << i << " Block " << j << " backed by file "
                << mgr_blocks[j]->TmpFilePath();
    }
    blocks.push_back(mgr_blocks);
    all_blocks.insert(all_blocks.end(), mgr_blocks.begin(), mgr_blocks.end());
  }
  WaitForWrites(block_mgrs);
  int error_mgr = 0;
  int no_error_mgr = 1;
  const string& error_dir = tmp_dirs[0];
  const string& good_dir = tmp_dirs[1];
  // Delete one file from first scratch dir for first block manager.
  BufferedBlockMgr::Block* error_block = FindBlockForDir(blocks[error_mgr], error_dir);
  ASSERT_TRUE(error_block != NULL) << "Expected a tmp file in dir " << error_dir;
  PinBlocks(all_blocks);
  DeleteBackingFile(error_block);
  UnpinBlocks(all_blocks); // Should succeed since tmp file space was already allocated.
  WaitForWrites(block_mgrs);
  EXPECT_TRUE(block_mgrs[error_mgr]->IsCancelled());
  EXPECT_FALSE(block_mgrs[no_error_mgr]->IsCancelled());
  // Temporary device with error should no longer be active.
  vector<TmpFileMgr::DeviceId> active_tmp_devices = tmp_file_mgr->active_tmp_devices();
  EXPECT_EQ(tmp_dirs.size() - 1, active_tmp_devices.size());
  for (int i = 0; i < active_tmp_devices.size(); ++i) {
    const string& device_path = tmp_file_mgr->GetTmpDirPath(active_tmp_devices[i]);
    EXPECT_EQ(string::npos, error_dir.find(device_path));
  }
  // The second block manager should continue using allocated scratch space, since it
  // didn't encounter a write error itself. In future this could change but for now it is
  // the intended behaviour.
  PinBlocks(blocks[no_error_mgr]);
  UnpinBlocks(blocks[no_error_mgr]);
  EXPECT_TRUE(FindBlockForDir(blocks[no_error_mgr], good_dir) != NULL);
  EXPECT_TRUE(FindBlockForDir(blocks[no_error_mgr], error_dir) != NULL);
  // The second block manager should avoid using bad directory for new blocks.
  vector<BufferedBlockMgr::Block*> no_error_new_blocks;
  AllocateBlocks(block_mgrs[no_error_mgr].get(), clients[no_error_mgr], blocks_per_mgr,
      &no_error_new_blocks);
  UnpinBlocks(no_error_new_blocks);
  for (int i = 0; i < no_error_new_blocks.size(); ++i) {
    LOG(INFO) << "Newly created block backed by file "
              << no_error_new_blocks[i]->TmpFilePath();
    EXPECT_TRUE(BlockInDir(no_error_new_blocks[i], good_dir));
  }
  // A new block manager should only use the good dir for backing storage.
  shared_ptr<RuntimeState> new_runtime_state;
  shared_ptr<BufferedBlockMgr> new_block_mgr = CreateQueryMgr(tmp_file_mgr.get(), 9999,
      blocks_per_mgr, &new_runtime_state);
  BufferedBlockMgr::Client* new_client;
  new_block_mgr->RegisterClient(0, NULL, new_runtime_state.get(), &new_client);
  vector<BufferedBlockMgr::Block*> new_mgr_blocks;
  AllocateBlocks(new_block_mgr.get(), new_client, blocks_per_mgr, &new_mgr_blocks);
  UnpinBlocks(new_mgr_blocks);
  for (int i = 0; i < blocks_per_mgr; ++i) {
    LOG(INFO) << "New manager Block " << i << " backed by file "
              << new_mgr_blocks[i]->TmpFilePath();
    EXPECT_TRUE(BlockInDir(new_mgr_blocks[i], good_dir));
  }
}

// Check that allocation error resulting from removal of directory results in blacklisting
// of directory.
TEST_F(BufferedBlockMgrTest, AllocationErrorBlacklist) {
  // Set up two buffered block managers with two temporary dirs.
  shared_ptr<TmpFileMgr> tmp_file_mgr;
  vector<string> tmp_dirs = InitMultipleTmpDirs(2, &tmp_file_mgr);
  // Simulate two concurrent queries.
  int num_block_mgrs = 2;
  int max_num_blocks = 4;
  int blocks_per_mgr = max_num_blocks / num_block_mgrs;
  vector<shared_ptr<RuntimeState> > runtime_states;
  vector<shared_ptr<BufferedBlockMgr> > block_mgrs;
  vector<BufferedBlockMgr::Client*> clients;
  CreateQueryMgrs(tmp_file_mgr.get(), num_block_mgrs, blocks_per_mgr, &runtime_states,
      &block_mgrs, &clients);
  // Allocate files for all 2x2 combinations by unpinning blocks.
  vector<vector<BufferedBlockMgr::Block*> > blocks;
  for (int i = 0; i < num_block_mgrs; ++i) {
    vector<BufferedBlockMgr::Block*> mgr_blocks;
    AllocateBlocks(block_mgrs[i].get(), clients[i], blocks_per_mgr, &mgr_blocks);
    blocks.push_back(mgr_blocks);
  }
  const string& bad_dir = tmp_dirs[0];
  const string& bad_scratch_subdir = bad_dir + SCRATCH_SUFFIX;
  const string& good_dir = tmp_dirs[1];
  const string& good_scratch_subdir = good_dir + SCRATCH_SUFFIX;
  chmod(bad_scratch_subdir.c_str(), 0);
  // The block mgr should attempt to allocate space in bad dir for one block, which will
  // cause an error when it tries to create/expand the file. It should recover and just
  // use the good dir.
  UnpinBlocks(blocks[0]);
  EXPECT_EQ(1, tmp_file_mgr->num_active_tmp_devices());
  const string& active_tmp_dir = tmp_file_mgr->GetTmpDirPath(
      tmp_file_mgr->active_tmp_devices()[0]);
  EXPECT_EQ(good_scratch_subdir, active_tmp_dir);
  chmod(bad_scratch_subdir.c_str(), S_IRWXU);
  // Blocks should not be written to bad dir even if writable again.
  UnpinBlocks(blocks[1]);
  for (int i = 0; i < num_block_mgrs; ++i) {
    for (int j = 0; j < blocks_per_mgr; ++j) {
      EXPECT_TRUE(BlockInDir(blocks[i][j], good_dir));
    }
  }
  // All writes should succeed.
  WaitForWrites(block_mgrs);
  for (int i = 0; i < blocks.size(); ++i) {
    for (int j = 0; j < blocks[i].size(); ++j) {
      EXPECT_TRUE(blocks[i][j]->Delete().ok());
    }
  }
}

// Test that block manager fails cleanly when all directories are inaccessible at runtime.
TEST_F(BufferedBlockMgrTest, NoDirsAllocationError) {
  shared_ptr<TmpFileMgr> tmp_file_mgr;
  vector<string> tmp_dirs = InitMultipleTmpDirs(2, &tmp_file_mgr);
  int max_num_buffers = 2;
  shared_ptr<BufferedBlockMgr> block_mgr = CreateMgr(max_num_buffers, tmp_file_mgr.get());
  BufferedBlockMgr::Client* client;
  EXPECT_TRUE(block_mgr->RegisterClient(0, NULL, runtime_state_.get(), &client).ok());
  EXPECT_TRUE(client != NULL);
  vector<BufferedBlockMgr::Block*> blocks;
  AllocateBlocks(block_mgr.get(), client, max_num_buffers, &blocks);
  for (int i = 0; i < tmp_dirs.size(); ++i) {
    const string& tmp_scratch_subdir = tmp_dirs[i] + SCRATCH_SUFFIX;
    chmod(tmp_scratch_subdir.c_str(), 0);
  }
  for (int i = 0; i < blocks.size(); ++i) {
    EXPECT_FALSE(blocks[i]->Unpin().ok());
  }
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

TEST_F(BufferedBlockMgrTest, SingleRandom_plain) {
  FLAGS_disk_spill_encryption = false;
  TestRandomInternalSingle();
}

TEST_F(BufferedBlockMgrTest, Multi2Random_plain) {
  FLAGS_disk_spill_encryption = false;
  TestRandomInternalMulti(2);
}

TEST_F(BufferedBlockMgrTest, Multi4Random_plain) {
  FLAGS_disk_spill_encryption = false;
  TestRandomInternalMulti(4);
}

// TODO: Enable when we improve concurrency of block mgr.
// TEST_F(BufferedBlockMgrTest, Multi8Random_plain) {
//   FLAGS_disk_spill_encryption = false;
//   TestRandomInternalMulti(8);
// }

TEST_F(BufferedBlockMgrTest, SingleRandom_encryption) {
  FLAGS_disk_spill_encryption = true;
  TestRandomInternalSingle();
}

TEST_F(BufferedBlockMgrTest, Multi2Random_encryption) {
  FLAGS_disk_spill_encryption = true;
  TestRandomInternalMulti(2);
}

TEST_F(BufferedBlockMgrTest, Multi4Random_encryption) {
  FLAGS_disk_spill_encryption = true;
  TestRandomInternalMulti(4);
}

// TODO: Enable when we improve concurrency of block mgr.
// TEST_F(BufferedBlockMgrTest, Multi8Random_encryption) {
//   FLAGS_disk_spill_encryption = true;
//   TestRandomInternalMulti(8);
// }

}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport();
  impala::LlvmCodeGen::InitializeLlvm();
  return RUN_ALL_TESTS();
}
