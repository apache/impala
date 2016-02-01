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
#include "runtime/test-env.h"
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
    test_env_.reset(new TestEnv());
    client_tracker_.reset(new MemTracker(-1));
  }

  virtual void TearDown() {
    TearDownMgrs();
    test_env_.reset();
    client_tracker_.reset();

    // Tests modify permissions, so make sure we can delete if they didn't clean up.
    for (int i = 0; i < created_tmp_dirs_.size(); ++i) {
      chmod((created_tmp_dirs_[i] + SCRATCH_SUFFIX).c_str(), S_IRWXU);
    }
    FileSystemUtil::RemovePaths(created_tmp_dirs_);
    created_tmp_dirs_.clear();
  }

  /// Reinitialize test_env_ to have multiple temporary directories.
  vector<string> InitMultipleTmpDirs(int num_dirs) {
    vector<string> tmp_dirs;
    for (int i = 0; i < num_dirs; ++i) {
      const string& dir = Substitute("/tmp/buffered-block-mgr-test.$0", i);
      // Fix permissions in case old directories were left from previous runs of test.
      chmod((dir + SCRATCH_SUFFIX).c_str(), S_IRWXU);
      EXPECT_TRUE(FileSystemUtil::CreateDirectory(dir).ok());
      tmp_dirs.push_back(dir);
      created_tmp_dirs_.push_back(dir);
    }
    test_env_->InitTmpFileMgr(tmp_dirs, false);
    EXPECT_EQ(num_dirs, test_env_->tmp_file_mgr()->num_active_tmp_devices());
    return tmp_dirs;
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

  /// Helper to create a simple block manager.
  BufferedBlockMgr* CreateMgr(int64_t query_id, int max_buffers, int block_size,
      RuntimeState** query_state = NULL) {
    RuntimeState* state;
    EXPECT_TRUE(test_env_->CreateQueryState(query_id, max_buffers, block_size,
        &state).ok());
    if (query_state != NULL) *query_state = state;
    return state->block_mgr();
  }

  BufferedBlockMgr* CreateMgrAndClient(int64_t query_id, int max_buffers, int block_size,
      int reserved_blocks, bool tolerates_oversubscription, MemTracker* tracker,
      BufferedBlockMgr::Client** client, RuntimeState** query_state = NULL) {
    RuntimeState* state;
    BufferedBlockMgr* mgr = CreateMgr(query_id, max_buffers, block_size, &state);
    EXPECT_TRUE(mgr->RegisterClient(reserved_blocks, tolerates_oversubscription, tracker,
        state, client).ok());
    EXPECT_TRUE(client != NULL);
    if (query_state != NULL) *query_state = state;
    return mgr;
  }

  void CreateMgrsAndClients(int64_t start_query_id, int num_mgrs, int buffers_per_mgr,
      int block_size, int reserved_blocks_per_client, bool tolerates_oversubscription,
      MemTracker* tracker, vector<BufferedBlockMgr*>* mgrs,
      vector<BufferedBlockMgr::Client*>* clients) {
    for (int i = 0; i < num_mgrs; ++i) {
      BufferedBlockMgr::Client* client;
      BufferedBlockMgr* mgr = CreateMgrAndClient(start_query_id + i, buffers_per_mgr,
          block_size_, reserved_blocks_per_client, tolerates_oversubscription,
          tracker, &client);
      mgrs->push_back(mgr);
      clients->push_back(client);
    }
  }

  // Destroy all created query states and associated block managers.
  void TearDownMgrs() {
    // Freeing all block managers should clean up all consumed memory.
    test_env_->TearDownQueryStates();
    EXPECT_EQ(test_env_->block_mgr_parent_tracker()->consumption(), 0);
  }

  void AllocateBlocks(BufferedBlockMgr* block_mgr, BufferedBlockMgr::Client* client,
      int num_blocks, vector<BufferedBlockMgr::Block*>* blocks) {
    int32_t* data;
    Status status;
    BufferedBlockMgr::Block* new_block;
    for (int i = 0; i < num_blocks; ++i) {
      status = block_mgr->GetNewBlock(client, NULL, &new_block);
      ASSERT_TRUE(status.ok());
      ASSERT_TRUE(new_block != NULL);
      data = new_block->Allocate<int32_t>(sizeof(int32_t));
      *data = blocks->size();
      blocks->push_back(new_block);
    }
  }

  // Pin all blocks, expecting they are pinned successfully.
  void PinBlocks(const vector<BufferedBlockMgr::Block*>& blocks) {
    for (int i = 0; i < blocks.size(); ++i) {
      bool pinned;
      Status status = blocks[i]->Pin(&pinned);
      EXPECT_TRUE(status.ok()) << status.msg().msg();
      EXPECT_TRUE(pinned);
    }
  }

  // Pin all blocks. By default, expect no errors from Unpin() calls. If
  // expect_cancelled is true, returning cancelled is allowed.
  void UnpinBlocks(const vector<BufferedBlockMgr::Block*>& blocks,
      bool expect_cancelled = false) {
    for (int i = 0; i < blocks.size(); ++i) {
      Status status = blocks[i]->Unpin();
      if (expect_cancelled) {
        EXPECT_TRUE(status.ok() || status.IsCancelled()) << status.msg().msg();
      } else {
        EXPECT_TRUE(status.ok()) << status.msg().msg();
      }
    }
  }

  void DeleteBlocks(const vector<BufferedBlockMgr::Block*>& blocks) {
    for (int i = 0; i < blocks.size(); ++i) {
      blocks[i]->Delete();
    }
  }

  void DeleteBlocks(const vector<pair<BufferedBlockMgr::Block*, int32_t> >& blocks) {
    for (int i = 0; i < blocks.size(); ++i) {
      blocks[i].first->Delete();
    }
  }

  static void WaitForWrites(BufferedBlockMgr* block_mgr) {
    vector<BufferedBlockMgr*> block_mgrs;
    block_mgrs.push_back(block_mgr);
    WaitForWrites(block_mgrs);
  }

  // Wait for writes issued through block managers to complete.
  static void WaitForWrites(const vector<BufferedBlockMgr*>& block_mgrs) {
    int max_attempts = WRITE_WAIT_MILLIS / WRITE_CHECK_INTERVAL_MILLIS;
    for (int i = 0; i < max_attempts; ++i) {
      SleepForMs(WRITE_CHECK_INTERVAL_MILLIS);
      if (AllWritesComplete(block_mgrs)) return;
    }
    EXPECT_TRUE(false) << "Writes did not complete after " << WRITE_WAIT_MILLIS << "ms";
  }

  static bool AllWritesComplete(BufferedBlockMgr* block_mgr) {
    RuntimeProfile::Counter* writes_outstanding =
        block_mgr->profile()->GetCounter("BlockWritesOutstanding");
    return writes_outstanding->value() == 0;
  }

  static bool AllWritesComplete(const vector<BufferedBlockMgr*>& block_mgrs) {
    for (int i = 0; i < block_mgrs.size(); ++i) {
      if (!AllWritesComplete(block_mgrs[i])) return false;
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

  void TestGetNewBlockImpl(int block_size) {
    Status status;
    int max_num_blocks = 5;
    vector<BufferedBlockMgr::Block*> blocks;
    BufferedBlockMgr* block_mgr;
    BufferedBlockMgr::Client* client;
    block_mgr = CreateMgrAndClient(0, max_num_blocks, block_size, 0, false,
        client_tracker_.get(), &client);
    EXPECT_EQ(test_env_->block_mgr_parent_tracker()->consumption(), 0);

    // Allocate blocks until max_num_blocks, they should all succeed and memory
    // usage should go up.
    BufferedBlockMgr::Block* new_block;
    BufferedBlockMgr::Block* first_block = NULL;
    for (int i = 0; i < max_num_blocks; ++i) {
      status = block_mgr->GetNewBlock(client, NULL, &new_block);
      EXPECT_TRUE(new_block != NULL);
      EXPECT_EQ(block_mgr->bytes_allocated(), (i + 1) * block_size);
      if (first_block == NULL) first_block = new_block;
      blocks.push_back(new_block);
    }

    // Trying to allocate a new one should fail.
    status = block_mgr->GetNewBlock(client, NULL, &new_block);
    EXPECT_TRUE(new_block == NULL);
    EXPECT_EQ(block_mgr->bytes_allocated(), max_num_blocks * block_size);

    // We can allocate a new block by transferring an already allocated one.
    uint8_t* old_buffer = first_block->buffer();
    status = block_mgr->GetNewBlock(client, first_block, &new_block);
    EXPECT_TRUE(new_block != NULL);
    EXPECT_TRUE(old_buffer == new_block->buffer());
    EXPECT_EQ(block_mgr->bytes_allocated(), max_num_blocks * block_size);
    EXPECT_TRUE(!first_block->is_pinned());
    blocks.push_back(new_block);

    // Trying to allocate a new one should still fail.
    status = block_mgr->GetNewBlock(client, NULL, &new_block);
    EXPECT_TRUE(new_block == NULL);
    EXPECT_EQ(block_mgr->bytes_allocated(), max_num_blocks * block_size);

    EXPECT_EQ(block_mgr->writes_issued(), 1);

    DeleteBlocks(blocks);
    TearDownMgrs();
  }

  void TestEvictionImpl(int block_size) {
    Status status;
    ASSERT_GT(block_size, 0);
    int max_num_buffers = 5;
    BufferedBlockMgr* block_mgr;
    BufferedBlockMgr::Client* client;
    block_mgr = CreateMgrAndClient(0, max_num_buffers, block_size, 0, false,
        client_tracker_.get(), &client);

    // Check counters.
    RuntimeProfile* profile = block_mgr->profile();
    RuntimeProfile::Counter* buffered_pin = profile->GetCounter("BufferedPins");

    vector<BufferedBlockMgr::Block*> blocks;
    AllocateBlocks(block_mgr, client, max_num_buffers, &blocks);

    EXPECT_EQ(block_mgr->bytes_allocated(), max_num_buffers * block_size);
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
    AllocateBlocks(block_mgr, client, 2, &blocks);
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
    DeleteBlocks(blocks);
    TearDownMgrs();
  }

  // Test that randomly issues GetFreeBlock(), Pin(), Unpin(), Delete() and Close()
  // calls. All calls made are legal - error conditions are not expected until the first
  // call to Close(). This is called 2 times with encryption+integrity on/off.
  // When executed in single-threaded mode 'tid' should be SINGLE_THREADED_TID.
  static const int SINGLE_THREADED_TID = -1;
  void TestRandomInternalImpl(RuntimeState* state, BufferedBlockMgr* block_mgr,
      int num_buffers, int tid) {
    ASSERT_TRUE(block_mgr != NULL);
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
    Status status = block_mgr->RegisterClient(0, false, client_tracker_.get(), state,
        &client);
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
          block_data.first->Delete();
          pinned_blocks[rand_pick] = pinned_blocks.back();
          pinned_blocks.pop_back();
          pinned_block_map[pinned_blocks[rand_pick].first] = rand_pick;
          break;
        case Close:
          block_mgr->Cancel();
          close_called = true;
          break;
      }
    }

    // The client needs to delete all its blocks.
    DeleteBlocks(pinned_blocks);
    DeleteBlocks(unpinned_blocks);
  }

  // Single-threaded execution of the TestRandomInternalImpl.
  void TestRandomInternalSingle(int block_size) {
    ASSERT_GT(block_size, 0);
    ASSERT_TRUE(test_env_.get() != NULL);
    const int max_num_buffers = 100;
    RuntimeState* state;
    BufferedBlockMgr* block_mgr = CreateMgr(0, max_num_buffers, block_size, &state);
    TestRandomInternalImpl(state, block_mgr, max_num_buffers, SINGLE_THREADED_TID);
    TearDownMgrs();
  }

  // Multi-threaded execution of the TestRandomInternalImpl.
  void TestRandomInternalMulti(int num_threads, int block_size) {
    ASSERT_GT(num_threads, 0);
    ASSERT_GT(block_size, 0);
    ASSERT_TRUE(test_env_.get() != NULL);
    const int max_num_buffers = 100;
    RuntimeState* state;
    BufferedBlockMgr* block_mgr = CreateMgr(0, num_threads * max_num_buffers, block_size,
        &state);

    thread_group workers;
    for (int i = 0; i < num_threads; ++i) {
      thread* t = new thread(bind(&BufferedBlockMgrTest::TestRandomInternalImpl, this,
                                  state, block_mgr, max_num_buffers, i));
      workers.add_thread(t);
    }
    workers.join_all();
    TearDownMgrs();
  }

  // Repeatedly call BufferedBlockMgr::Create() and BufferedBlockMgr::~BufferedBlockMgr().
  void CreateDestroyThread(RuntimeState* state) {
    const int num_buffers = 10;
    const int iters = 10000;
    for (int i = 0; i < iters; ++i) {
      shared_ptr<BufferedBlockMgr> mgr;
      Status status = BufferedBlockMgr::Create(state,
          test_env_->block_mgr_parent_tracker(), state->runtime_profile(),
          test_env_->tmp_file_mgr(), block_size_ * num_buffers, block_size_, &mgr);
    }
  }

  // IMPALA-2286: Test for races between BufferedBlockMgr::Create() and
  // BufferedBlockMgr::~BufferedBlockMgr().
  void CreateDestroyMulti() {
    const int num_threads = 8;
    thread_group workers;
    // Create a shared RuntimeState with no BufferedBlockMgr.
    RuntimeState* shared_state = new RuntimeState(TExecPlanFragmentParams(), "",
        test_env_->exec_env());
    for (int i = 0; i < num_threads; ++i) {
      thread* t = new thread(bind(
          &BufferedBlockMgrTest::CreateDestroyThread, this, shared_state));
      workers.add_thread(t);
    }
    workers.join_all();
  }

  // Test that in-flight IO operations are correctly handled on tear down.
  // write: if true, tear down while write operations are in flight, otherwise tear down
  //    during read operations.
  void TestDestructDuringIO(bool write);

  /// Test for IMPALA-2252: race when tearing down runtime state and block mgr after query
  /// cancellation. Simulates query cancellation while writes are in flight. Forces the
  /// block mgr to have a longer lifetime than the runtime state. If write_error is true,
  /// force writes to hit errors. If wait_for_writes is true, wait for writes to complete
  /// before destroying block mgr.
  void TestRuntimeStateTeardown(bool write_error, bool wait_for_writes);

  scoped_ptr<TestEnv> test_env_;
  scoped_ptr<MemTracker> client_tracker_;
  vector<string> created_tmp_dirs_;
};

TEST_F(BufferedBlockMgrTest, GetNewBlock) {
  TestGetNewBlockImpl(1024);
  TestGetNewBlockImpl(8 * 1024);
  TestGetNewBlockImpl(8 * 1024 * 1024);
}

TEST_F(BufferedBlockMgrTest, GetNewBlockSmallBlocks) {
  const int block_size = 1024;
  int max_num_blocks = 3;
  BufferedBlockMgr* block_mgr;
  BufferedBlockMgr::Client* client;
  block_mgr = CreateMgrAndClient(0, max_num_blocks, block_size, 0, false,
      client_tracker_.get(), &client);
  EXPECT_EQ(0, test_env_->block_mgr_parent_tracker()->consumption());

  vector<BufferedBlockMgr::Block*> blocks;

  // Allocate a small block.
  BufferedBlockMgr::Block* new_block = NULL;
  EXPECT_TRUE(block_mgr->GetNewBlock(client, NULL, &new_block, 128).ok());
  EXPECT_TRUE(new_block != NULL);
  EXPECT_EQ(block_mgr->bytes_allocated(), 0);
  EXPECT_EQ(test_env_->block_mgr_parent_tracker()->consumption(), 0);
  EXPECT_EQ(client_tracker_->consumption(), 128);
  EXPECT_TRUE(new_block->is_pinned());
  EXPECT_EQ(new_block->BytesRemaining(), 128);
  EXPECT_TRUE(new_block->buffer() != NULL);
  blocks.push_back(new_block);

  // Allocate a normal block
  EXPECT_TRUE(block_mgr->GetNewBlock(client, NULL, &new_block).ok());
  EXPECT_TRUE(new_block != NULL);
  EXPECT_EQ(block_mgr->bytes_allocated(), block_mgr->max_block_size());
  EXPECT_EQ(test_env_->block_mgr_parent_tracker()->consumption(),
            block_mgr->max_block_size());
  EXPECT_EQ(client_tracker_->consumption(), 128 + block_mgr->max_block_size());
  EXPECT_TRUE(new_block->is_pinned());
  EXPECT_EQ(new_block->BytesRemaining(), block_mgr->max_block_size());
  EXPECT_TRUE(new_block->buffer() != NULL);
  blocks.push_back(new_block);

  // Allocate another small block.
  EXPECT_TRUE(block_mgr->GetNewBlock(client, NULL, &new_block, 512).ok());
  EXPECT_TRUE(new_block != NULL);
  EXPECT_EQ(block_mgr->bytes_allocated(), block_mgr->max_block_size());
  EXPECT_EQ(test_env_->block_mgr_parent_tracker()->consumption(),
            block_mgr->max_block_size());
  EXPECT_EQ(client_tracker_->consumption(), 128 + 512 + block_mgr->max_block_size());
  EXPECT_TRUE(new_block->is_pinned());
  EXPECT_EQ(new_block->BytesRemaining(), 512);
  EXPECT_TRUE(new_block->buffer() != NULL);
  blocks.push_back(new_block);

  // Should be able to unpin and pin the middle block
  EXPECT_TRUE(blocks[1]->Unpin().ok());

  bool pinned;
  EXPECT_TRUE(blocks[1]->Pin(&pinned).ok());
  EXPECT_TRUE(pinned);

  DeleteBlocks(blocks);
  TearDownMgrs();
}

// Test that pinning more blocks than the max available buffers.
TEST_F(BufferedBlockMgrTest, Pin) {
  Status status;
  int max_num_blocks = 5;
  const int block_size = 1024;
  BufferedBlockMgr* block_mgr;
  BufferedBlockMgr::Client* client;
  block_mgr = CreateMgrAndClient(0, max_num_blocks, block_size, 0, false,
      client_tracker_.get(), &client);

  vector<BufferedBlockMgr::Block*> blocks;
  AllocateBlocks(block_mgr, client, max_num_blocks, &blocks);

  // Unpin them all.
  for (int i = 0; i < blocks.size(); ++i) {
    status = blocks[i]->Unpin();
    EXPECT_TRUE(status.ok());
  }

  // Allocate more, this should work since we just unpinned some blocks.
  AllocateBlocks(block_mgr, client, max_num_blocks, &blocks);

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

  DeleteBlocks(blocks);
  TearDownMgrs();
}

// Test the eviction policy of the block mgr. No writes issued until more than
// the max available buffers are allocated. Writes must be issued in LIFO order.
TEST_F(BufferedBlockMgrTest, Eviction) {
  TestEvictionImpl(1024);
  TestEvictionImpl(8 * 1024 * 1024);
}

// Test deletion and reuse of blocks.
TEST_F(BufferedBlockMgrTest, Deletion) {
  int max_num_buffers = 5;
  const int block_size = 1024;
  BufferedBlockMgr* block_mgr;
  BufferedBlockMgr::Client* client;
  block_mgr = CreateMgrAndClient(0, max_num_buffers, block_size, 0, false,
      client_tracker_.get(), &client);

  // Check counters.
  RuntimeProfile* profile = block_mgr->profile();
  RuntimeProfile::Counter* recycled_cnt = profile->GetCounter("BlocksRecycled");
  RuntimeProfile::Counter* created_cnt = profile->GetCounter("BlocksCreated");

  vector<BufferedBlockMgr::Block*> blocks;
  AllocateBlocks(block_mgr, client, max_num_buffers, &blocks);
  EXPECT_TRUE(created_cnt->value() == max_num_buffers);

  DeleteBlocks(blocks);
  blocks.clear();
  AllocateBlocks(block_mgr, client, max_num_buffers, &blocks);
  EXPECT_TRUE(created_cnt->value() == max_num_buffers);
  EXPECT_TRUE(recycled_cnt->value() == max_num_buffers);

  DeleteBlocks(blocks);
  TearDownMgrs();
}

// Delete blocks of various sizes and statuses to exercise the different code paths.
// This relies on internal validation in block manager to detect many errors.
TEST_F(BufferedBlockMgrTest, DeleteSingleBlocks) {
  int max_num_buffers = 16;
  BufferedBlockMgr::Client* client;
  BufferedBlockMgr* block_mgr = CreateMgrAndClient(0, max_num_buffers, block_size_,
      0, false, client_tracker_.get(), &client);

  // Pinned I/O block.
  BufferedBlockMgr::Block* new_block;
  EXPECT_TRUE(block_mgr->GetNewBlock(client, NULL, &new_block).ok());
  EXPECT_TRUE(new_block != NULL);
  EXPECT_TRUE(new_block->is_pinned());
  EXPECT_TRUE(new_block->is_max_size());
  new_block->Delete();
  EXPECT_TRUE(client_tracker_->consumption() == 0);

  // Pinned non-I/O block.
  int small_block_size = 128;
  EXPECT_TRUE(block_mgr->GetNewBlock(client, NULL, &new_block, small_block_size).ok());
  EXPECT_TRUE(new_block != NULL);
  EXPECT_TRUE(new_block->is_pinned());
  EXPECT_EQ(small_block_size, client_tracker_->consumption());
  new_block->Delete();
  EXPECT_EQ(0, client_tracker_->consumption());

  // Unpinned I/O block - delete after written to disk.
  EXPECT_TRUE(block_mgr->GetNewBlock(client, NULL, &new_block).ok());
  EXPECT_TRUE(new_block != NULL);
  EXPECT_TRUE(new_block->is_pinned());
  EXPECT_TRUE(new_block->is_max_size());
  new_block->Unpin();
  EXPECT_FALSE(new_block->is_pinned());
  WaitForWrites(block_mgr);
  new_block->Delete();
  EXPECT_TRUE(client_tracker_->consumption() == 0);

  // Unpinned I/O block - delete before written to disk.
  EXPECT_TRUE(block_mgr->GetNewBlock(client, NULL, &new_block).ok());
  EXPECT_TRUE(new_block != NULL);
  EXPECT_TRUE(new_block->is_pinned());
  EXPECT_TRUE(new_block->is_max_size());
  new_block->Unpin();
  EXPECT_FALSE(new_block->is_pinned());
  new_block->Delete();
  WaitForWrites(block_mgr);
  EXPECT_TRUE(client_tracker_->consumption() == 0);

  TearDownMgrs();
}

// Test that all APIs return cancelled after close.
TEST_F(BufferedBlockMgrTest, Close) {
  int max_num_buffers = 5;
  const int block_size = 1024;
  BufferedBlockMgr* block_mgr;
  BufferedBlockMgr::Client* client;
  block_mgr = CreateMgrAndClient(0, max_num_buffers, block_size, 0, false,
      client_tracker_.get(), &client);

  vector<BufferedBlockMgr::Block*> blocks;
  AllocateBlocks(block_mgr, client, max_num_buffers, &blocks);

  block_mgr->Cancel();

  BufferedBlockMgr::Block* new_block;
  Status status = block_mgr->GetNewBlock(client, NULL, &new_block);
  EXPECT_TRUE(status.IsCancelled());
  EXPECT_TRUE(new_block == NULL);
  status = blocks[0]->Unpin();
  EXPECT_TRUE(status.IsCancelled());
  bool pinned;
  status = blocks[0]->Pin(&pinned);
  EXPECT_TRUE(status.IsCancelled());

  DeleteBlocks(blocks);
  TearDownMgrs();
}

TEST_F(BufferedBlockMgrTest, DestructDuringWrite) {
  const int trials = 20;
  const int max_num_buffers = 5;

  for (int trial = 0; trial < trials; ++trial) {
    BufferedBlockMgr::Client* client;
    BufferedBlockMgr* block_mgr = CreateMgrAndClient(0, max_num_buffers, block_size_,
        0, false, client_tracker_.get(), &client);

    vector<BufferedBlockMgr::Block*> blocks;
    AllocateBlocks(block_mgr, client, max_num_buffers, &blocks);

    // Unpin will initiate writes.
    UnpinBlocks(blocks);

    // Writes should still be in flight when blocks are deleted.
    DeleteBlocks(blocks);

    // Destruct block manager while blocks are deleted and writes are in flight.
    TearDownMgrs();
  }
  // Destroying test environment will check that all writes have completed.
}

void BufferedBlockMgrTest::TestRuntimeStateTeardown(bool write_error,
    bool wait_for_writes) {
  const int max_num_buffers = 10;
  RuntimeState* state;
  BufferedBlockMgr::Client* client;
  CreateMgrAndClient(0, max_num_buffers, block_size_, 0, false, client_tracker_.get(),
      &client, &state);

  // Hold another reference to block mgr so that it can outlive runtime state.
  shared_ptr<BufferedBlockMgr> block_mgr;
  Status status = BufferedBlockMgr::Create(state, test_env_->block_mgr_parent_tracker(),
      state->runtime_profile(), test_env_->tmp_file_mgr(), 0, block_size_, &block_mgr);
  ASSERT_TRUE(status.ok());
  ASSERT_TRUE(block_mgr != NULL);

  vector<BufferedBlockMgr::Block*> blocks;
  AllocateBlocks(block_mgr.get(), client, max_num_buffers, &blocks);

  if (write_error) {
    // Force flushing blocks to disk then remove temporary file to force writes to fail.
    UnpinBlocks(blocks);
    vector<BufferedBlockMgr::Block*> more_blocks;
    AllocateBlocks(block_mgr.get(), client, max_num_buffers, &more_blocks);
    DeleteBlocks(more_blocks);
    PinBlocks(blocks);
    DeleteBackingFile(blocks[0]);
  }

  // Unpin will initiate writes. If the write error propagates fast enough, some Unpin()
  // calls may see a cancelled block mgr.
  UnpinBlocks(blocks, write_error);

  // Tear down while writes are in flight. The block mgr may outlive the runtime state
  // because it may be referenced by other runtime states. This test simulates this
  // scenario by holding onto a reference to the block mgr. This should be safe so
  // long as blocks are properly deleted before the runtime state is torn down.
  DeleteBlocks(blocks);
  test_env_->TearDownQueryStates();

  // Optionally wait for writes to complete after cancellation.
  if (wait_for_writes) WaitForWrites(block_mgr.get());
  block_mgr.reset();

  EXPECT_TRUE(test_env_->block_mgr_parent_tracker()->consumption() == 0);
}

TEST_F(BufferedBlockMgrTest, RuntimeStateTeardown) {
  TestRuntimeStateTeardown(false, false);
}

TEST_F(BufferedBlockMgrTest, RuntimeStateTeardownWait) {
  TestRuntimeStateTeardown(false, true);
}

TEST_F(BufferedBlockMgrTest, RuntimeStateTeardownWriteError) {
  TestRuntimeStateTeardown(true, true);
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
  Status status;
  int max_num_buffers = 2;
  const int block_size = 1024;
  BufferedBlockMgr* block_mgr;
  BufferedBlockMgr::Client* client;
  block_mgr = CreateMgrAndClient(0, max_num_buffers, block_size, 0, false,
      client_tracker_.get(), &client);

  vector<BufferedBlockMgr::Block*> blocks;
  AllocateBlocks(block_mgr, client, max_num_buffers, &blocks);
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
  DeleteBlocks(blocks);
  BufferedBlockMgr::Block* new_block;
  status = block_mgr->GetNewBlock(client, NULL, &new_block);
  EXPECT_TRUE(status.IsCancelled());
  EXPECT_TRUE(new_block == NULL);

  TearDownMgrs();
}

// Test block manager error handling when temporary file space cannot be allocated to
// back an unpinned buffer.
TEST_F(BufferedBlockMgrTest, TmpFileAllocateError) {
  Status status;
  int max_num_buffers = 2;
  BufferedBlockMgr::Client* client;
  BufferedBlockMgr* block_mgr = CreateMgrAndClient(0, max_num_buffers, block_size_, 0,
      false, client_tracker_.get(), &client);

  vector<BufferedBlockMgr::Block*> blocks;
  AllocateBlocks(block_mgr, client, max_num_buffers, &blocks);
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

  DeleteBlocks(blocks);
  TearDownMgrs();
}

// Test that the block manager is able to blacklist a temporary device correctly after a
// write error. We should not allocate more blocks on that device, but existing blocks
// on the device will remain in use.
/// Disabled because blacklisting was disabled as workaround for IMPALA-2305.
TEST_F(BufferedBlockMgrTest, DISABLED_WriteErrorBlacklist) {
  // Set up two buffered block managers with two temporary dirs.
  vector<string> tmp_dirs = InitMultipleTmpDirs(2);
  // Simulate two concurrent queries.
  const int NUM_BLOCK_MGRS = 2;
  const int MAX_NUM_BLOCKS = 4;
  int blocks_per_mgr = MAX_NUM_BLOCKS / NUM_BLOCK_MGRS;
  vector<BufferedBlockMgr*> block_mgrs;
  vector<BufferedBlockMgr::Client*> clients;
  CreateMgrsAndClients(0, NUM_BLOCK_MGRS, blocks_per_mgr, block_size_, 0, false,
      client_tracker_.get(), &block_mgrs, &clients);

  // Allocate files for all 2x2 combinations by unpinning blocks.
  vector<vector<BufferedBlockMgr::Block*> > blocks;
  vector<BufferedBlockMgr::Block*> all_blocks;
  for (int i = 0; i < NUM_BLOCK_MGRS; ++i) {
    vector<BufferedBlockMgr::Block*> mgr_blocks;
    AllocateBlocks(block_mgrs[i], clients[i], blocks_per_mgr, &mgr_blocks);
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
  vector<TmpFileMgr::DeviceId> active_tmp_devices =
    test_env_->tmp_file_mgr()->active_tmp_devices();
  EXPECT_EQ(tmp_dirs.size() - 1, active_tmp_devices.size());
  for (int i = 0; i < active_tmp_devices.size(); ++i) {
    const string& device_path = test_env_->tmp_file_mgr()->GetTmpDirPath(
        active_tmp_devices[i]);
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
  AllocateBlocks(block_mgrs[no_error_mgr], clients[no_error_mgr], blocks_per_mgr,
      &no_error_new_blocks);
  UnpinBlocks(no_error_new_blocks);
  for (int i = 0; i < no_error_new_blocks.size(); ++i) {
    LOG(INFO) << "Newly created block backed by file "
              << no_error_new_blocks[i]->TmpFilePath();
    EXPECT_TRUE(BlockInDir(no_error_new_blocks[i], good_dir));
  }
  // A new block manager should only use the good dir for backing storage.
  BufferedBlockMgr::Client* new_client;
  BufferedBlockMgr* new_block_mgr = CreateMgrAndClient(9999, blocks_per_mgr, block_size_,
      0, false, client_tracker_.get(), &new_client);
  vector<BufferedBlockMgr::Block*> new_mgr_blocks;
  AllocateBlocks(new_block_mgr, new_client, blocks_per_mgr, &new_mgr_blocks);
  UnpinBlocks(new_mgr_blocks);
  for (int i = 0; i < blocks_per_mgr; ++i) {
    LOG(INFO) << "New manager Block " << i << " backed by file "
              << new_mgr_blocks[i]->TmpFilePath();
    EXPECT_TRUE(BlockInDir(new_mgr_blocks[i], good_dir));
  }
}

// Check that allocation error resulting from removal of directory results in blocks
/// being allocated in other directories.
TEST_F(BufferedBlockMgrTest, AllocationErrorHandling) {
  // Set up two buffered block managers with two temporary dirs.
  vector<string> tmp_dirs = InitMultipleTmpDirs(2);
  // Simulate two concurrent queries.
  int num_block_mgrs = 2;
  int max_num_blocks = 4;
  int blocks_per_mgr = max_num_blocks / num_block_mgrs;
  vector<RuntimeState*> runtime_states;
  vector<BufferedBlockMgr*> block_mgrs;
  vector<BufferedBlockMgr::Client*> clients;
  CreateMgrsAndClients(0, num_block_mgrs, blocks_per_mgr, block_size_, 0,
      false, client_tracker_.get(), &block_mgrs, &clients);

  // Allocate files for all 2x2 combinations by unpinning blocks.
  vector<vector<BufferedBlockMgr::Block*> > blocks;
  for (int i = 0; i < num_block_mgrs; ++i) {
    vector<BufferedBlockMgr::Block*> mgr_blocks;
    LOG(INFO) << "Iter " << i;
    AllocateBlocks(block_mgrs[i], clients[i], blocks_per_mgr, &mgr_blocks);
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
  // Directories remain on active list even when they experience errors.
  EXPECT_EQ(2, test_env_->tmp_file_mgr()->num_active_tmp_devices());
  // Blocks should not be written to bad dir even if it remains non-writable.
  UnpinBlocks(blocks[1]);
  // All writes should succeed.
  WaitForWrites(block_mgrs);
  for (int i = 0; i < blocks.size(); ++i) {
    DeleteBlocks(blocks[i]);
  }
}

// Test that block manager fails cleanly when all directories are inaccessible at runtime.
TEST_F(BufferedBlockMgrTest, NoDirsAllocationError) {
  vector<string> tmp_dirs = InitMultipleTmpDirs(2);
  int max_num_buffers = 2;
  BufferedBlockMgr::Client* client;
  BufferedBlockMgr* block_mgr = CreateMgrAndClient(0, max_num_buffers, block_size_,
      0, false, client_tracker_.get(), &client);
  vector<BufferedBlockMgr::Block*> blocks;
  AllocateBlocks(block_mgr, client, max_num_buffers, &blocks);
  for (int i = 0; i < tmp_dirs.size(); ++i) {
    const string& tmp_scratch_subdir = tmp_dirs[i] + SCRATCH_SUFFIX;
    chmod(tmp_scratch_subdir.c_str(), 0);
  }
  for (int i = 0; i < blocks.size(); ++i) {
    EXPECT_FALSE(blocks[i]->Unpin().ok());
  }
  DeleteBlocks(blocks);
}

// Test that block manager can still allocate buffers when spilling is disabled.
TEST_F(BufferedBlockMgrTest, NoTmpDirs) {
  InitMultipleTmpDirs(0);
  int max_num_buffers = 3;
  BufferedBlockMgr::Client* client;
  BufferedBlockMgr* block_mgr = CreateMgrAndClient(0, max_num_buffers, block_size_,
      0, false, client_tracker_.get(), &client);
  vector<BufferedBlockMgr::Block*> blocks;
  AllocateBlocks(block_mgr, client, max_num_buffers, &blocks);
  DeleteBlocks(blocks);
}

// Create two clients with different number of reserved buffers.
TEST_F(BufferedBlockMgrTest, MultipleClients) {
  Status status;
  int client1_buffers = 3;
  int client2_buffers = 5;
  int max_num_buffers = client1_buffers + client2_buffers;
  const int block_size = 1024;
  RuntimeState* runtime_state;
  BufferedBlockMgr* block_mgr = CreateMgr(0, max_num_buffers, block_size, &runtime_state);

  BufferedBlockMgr::Client* client1 = NULL;
  BufferedBlockMgr::Client* client2 = NULL;
  status = block_mgr->RegisterClient(client1_buffers, false, client_tracker_.get(),
      runtime_state, &client1);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(client1 != NULL);
  status = block_mgr->RegisterClient(client2_buffers, false, client_tracker_.get(),
      runtime_state, &client2);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(client2 != NULL);

  // Reserve client 1's and 2's buffers. They should succeed.
  bool reserved = block_mgr->TryAcquireTmpReservation(client1, 1);
  EXPECT_TRUE(reserved);
  reserved = block_mgr->TryAcquireTmpReservation(client2, 1);
  EXPECT_TRUE(reserved);

  vector<BufferedBlockMgr::Block*> client1_blocks;
  // Allocate all of client1's reserved blocks, they should all succeed.
  AllocateBlocks(block_mgr, client1, client1_buffers, &client1_blocks);

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
  AllocateBlocks(block_mgr, client2, client2_buffers, &client2_blocks);

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
  client1_blocks.push_back(block);

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
  client2_blocks.push_back(block);

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

  DeleteBlocks(client1_blocks);
  DeleteBlocks(client2_blocks);
  TearDownMgrs();
}

// Create two clients with different number of reserved buffers and some additional.
TEST_F(BufferedBlockMgrTest, MultipleClientsExtraBuffers) {
  Status status;
  int client1_buffers = 1;
  int client2_buffers = 1;
  int max_num_buffers = client1_buffers + client2_buffers + 2;
  const int block_size = 1024;
  RuntimeState* runtime_state;
  BufferedBlockMgr* block_mgr = CreateMgr(0, max_num_buffers, block_size, &runtime_state);

  BufferedBlockMgr::Client* client1 = NULL;
  BufferedBlockMgr::Client* client2 = NULL;
  BufferedBlockMgr::Block* block = NULL;
  status = block_mgr->RegisterClient(client1_buffers, false, client_tracker_.get(),
      runtime_state, &client1);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(client1 != NULL);
  status = block_mgr->RegisterClient(client2_buffers, false, client_tracker_.get(),
      runtime_state, &client2);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(client2 != NULL);

  vector<BufferedBlockMgr::Block*> client1_blocks;
  // Allocate all of client1's reserved blocks, they should all succeed.
  AllocateBlocks(block_mgr, client1, client1_buffers, &client1_blocks);

  // Allocate all of client2's reserved blocks, these should succeed.
  vector<BufferedBlockMgr::Block*> client2_blocks;
  AllocateBlocks(block_mgr, client2, client2_buffers, &client2_blocks);

  // We have two spare buffers now. Each client should be able to allocate it.
  status = block_mgr->GetNewBlock(client1, NULL, &block);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(block != NULL);
  client1_blocks.push_back(block);
  status = block_mgr->GetNewBlock(client2, NULL, &block);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(block != NULL);
  client2_blocks.push_back(block);

  // Now we are completely full, no one should be able to allocate a new block.
  status = block_mgr->GetNewBlock(client1, NULL, &block);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(block == NULL);
  status = block_mgr->GetNewBlock(client2, NULL, &block);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(block == NULL);

  DeleteBlocks(client1_blocks);
  DeleteBlocks(client2_blocks);
  TearDownMgrs();
}

// Create multiple clients causing oversubscription.
TEST_F(BufferedBlockMgrTest, ClientOversubscription) {
  Status status;
  int client1_buffers = 1;
  int client2_buffers = 2;
  int client3_buffers = 2;
  int max_num_buffers = 2;
  const int block_size = 1024;
  RuntimeState* runtime_state;
  BufferedBlockMgr* block_mgr = CreateMgr(0, max_num_buffers, block_size, &runtime_state);
  vector<BufferedBlockMgr::Block*> blocks;

  BufferedBlockMgr::Client* client1 = NULL;
  BufferedBlockMgr::Client* client2 = NULL;
  BufferedBlockMgr::Client* client3 = NULL;
  BufferedBlockMgr::Block* block = NULL;
  status = block_mgr->RegisterClient(client1_buffers, false, client_tracker_.get(),
      runtime_state, &client1);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(client1 != NULL);
  status = block_mgr->RegisterClient(client2_buffers, false, client_tracker_.get(),
      runtime_state, &client2);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(client2 != NULL);
  status = block_mgr->RegisterClient(client3_buffers, true, client_tracker_.get(),
      runtime_state, &client3);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(client3 != NULL);

  // Client one allocates first block, should work.
  status = block_mgr->GetNewBlock(client1, NULL, &block);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(block != NULL);
  blocks.push_back(block);

  // Client two allocates first block, should work.
  status = block_mgr->GetNewBlock(client2, NULL, &block);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(block != NULL);
  blocks.push_back(block);

  // At this point we've used both buffers. Client one reserved one so subsequent
  // calls should fail with no error (but returns no block).
  status = block_mgr->GetNewBlock(client1, NULL, &block);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(block == NULL);

  // Allocate with client two. Since client two reserved 2 buffers, this should fail
  // with MEM_LIMIT_EXCEEDED.
  status = block_mgr->GetNewBlock(client2, NULL, &block);
  EXPECT_TRUE(status.IsMemLimitExceeded());

  // Allocate with client three. Since client three can tolerate oversubscription,
  // this should fail with no error even though it was a reserved request.
  status = block_mgr->GetNewBlock(client3, NULL, &block);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(block == NULL);

  DeleteBlocks(blocks);
  TearDownMgrs();
}

TEST_F(BufferedBlockMgrTest, SingleRandom_plain) {
  FLAGS_disk_spill_encryption = false;
  TestRandomInternalSingle(1024);
  TestRandomInternalSingle(8 * 1024);
  TestRandomInternalSingle(8 * 1024 * 1024);
}

TEST_F(BufferedBlockMgrTest, Multi2Random_plain) {
  FLAGS_disk_spill_encryption = false;
  TestRandomInternalMulti(2, 1024);
  TestRandomInternalMulti(2, 8 * 1024);
  TestRandomInternalMulti(2, 8 * 1024 * 1024);
}

TEST_F(BufferedBlockMgrTest, Multi4Random_plain) {
  FLAGS_disk_spill_encryption = false;
  TestRandomInternalMulti(4, 1024);
  TestRandomInternalMulti(4, 8 * 1024);
  TestRandomInternalMulti(4, 8 * 1024 * 1024);
}

// TODO: Enable when we improve concurrency/scalability of block mgr.
// TEST_F(BufferedBlockMgrTest, Multi8Random_plain) {
//   FLAGS_disk_spill_encryption = false;
//   TestRandomInternalMulti(8);
// }

TEST_F(BufferedBlockMgrTest, SingleRandom_encryption) {
  FLAGS_disk_spill_encryption = true;
  TestRandomInternalSingle(8 * 1024);
}

TEST_F(BufferedBlockMgrTest, Multi2Random_encryption) {
  FLAGS_disk_spill_encryption = true;
  TestRandomInternalMulti(2, 8 * 1024);
}

TEST_F(BufferedBlockMgrTest, Multi4Random_encryption) {
  FLAGS_disk_spill_encryption = true;
  TestRandomInternalMulti(4, 8 * 1024);
}

// TODO: Enable when we improve concurrency/scalability of block mgr.
// TEST_F(BufferedBlockMgrTest, Multi8Random_encryption) {
//   FLAGS_disk_spill_encryption = true;
//   TestRandomInternalMulti(8);
// }


TEST_F(BufferedBlockMgrTest, CreateDestroyMulti) {
  CreateDestroyMulti();
}

}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport();
  impala::LlvmCodeGen::InitializeLlvm();
  return RUN_ALL_TESTS();
}
