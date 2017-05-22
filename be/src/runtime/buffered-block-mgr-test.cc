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

#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/filesystem.hpp>
#include <boost/regex.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/thread.hpp>
#include <gutil/strings/substitute.h>
#include <sys/stat.h>

#include "codegen/llvm-codegen.h"
#include "common/init.h"
#include "common/object-pool.h"
#include "runtime/buffered-block-mgr.h"
#include "runtime/disk-io-mgr.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "runtime/query-state.h"
#include "runtime/runtime-state.h"
#include "runtime/test-env.h"
#include "runtime/tmp-file-mgr.h"
#include "service/fe-support.h"
#include "testutil/gtest-util.h"
#include "util/cpu-info.h"
#include "util/disk-info.h"
#include "util/error-util.h"
#include "util/filesystem-util.h"
#include "util/promise.h"
#include "util/test-info.h"
#include "util/time.h"

#include "gen-cpp/Types_types.h"
#include "gen-cpp/ImpalaInternalService_types.h"

#include "common/names.h"

using boost::filesystem::directory_iterator;
using boost::filesystem::remove;
using boost::regex;

// Note: This is the default scratch dir created by impala.
// FLAGS_scratch_dirs + TmpFileMgr::TMP_SUB_DIR_NAME.
const string SCRATCH_DIR = "/tmp/impala-scratch";

// This suffix is appended to a tmp dir
const string SCRATCH_SUFFIX = "/impala-scratch";

// Number of millieconds to wait to ensure write completes. We don't know for sure how
// slow the disk will be, so this is much higher than we expect the writes to take.
const static int WRITE_WAIT_MILLIS = 10000;

// How often to check for write completion
const static int WRITE_CHECK_INTERVAL_MILLIS = 10;

DECLARE_bool(disk_spill_encryption);

namespace impala {

class BufferedBlockMgrTest : public ::testing::Test {
 protected:
  const static int block_size_ = 1024;

  virtual void SetUp() {
    test_env_.reset(new TestEnv());
    ASSERT_OK(test_env_->Init());
  }

  virtual void TearDown() {
    TearDownMgrs();
    test_env_.reset();

    // Tests modify permissions, so make sure we can delete if they didn't clean up.
    for (int i = 0; i < created_tmp_dirs_.size(); ++i) {
      chmod((created_tmp_dirs_[i] + SCRATCH_SUFFIX).c_str(), S_IRWXU);
    }
    FileSystemUtil::RemovePaths(created_tmp_dirs_);
    created_tmp_dirs_.clear();
    pool_.Clear();
  }

  /// Reinitialize test_env_ to have multiple temporary directories.
  vector<string> InitMultipleTmpDirs(int num_dirs) {
    vector<string> tmp_dirs;
    for (int i = 0; i < num_dirs; ++i) {
      const string& dir = Substitute("/tmp/buffered-block-mgr-test.$0", i);
      // Fix permissions in case old directories were left from previous runs of test.
      chmod((dir + SCRATCH_SUFFIX).c_str(), S_IRWXU);
      EXPECT_OK(FileSystemUtil::RemoveAndCreateDirectory(dir));
      tmp_dirs.push_back(dir);
      created_tmp_dirs_.push_back(dir);
    }
    test_env_.reset(new TestEnv);
    test_env_->SetTmpFileMgrArgs(tmp_dirs, false);
    EXPECT_OK(test_env_->Init());
    EXPECT_EQ(num_dirs, test_env_->tmp_file_mgr()->NumActiveTmpDevices());
    return tmp_dirs;
  }

  static void ValidateBlock(BufferedBlockMgr::Block* block, int32_t data) {
    ASSERT_EQ(block->valid_data_len(), sizeof(int32_t));
    ASSERT_EQ(*reinterpret_cast<int32_t*>(block->buffer()), data);
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
    ASSERT_EQ(block->valid_data_len(), size);
    ASSERT_EQ(size, bsize);
    for (i = 4; i < size - 5; ++i) {
      ASSERT_EQ(data[i], i);
    }
    for (; i < size; ++i) {
      ASSERT_EQ(data[i], 0xff);
    }
  }

  /// Helper to create a simple block manager.
  BufferedBlockMgr* CreateMgr(int64_t query_id, int max_buffers, int block_size,
      RuntimeState** query_state = NULL, TQueryOptions* query_options = NULL) {
    RuntimeState* state;
    EXPECT_OK(test_env_->CreateQueryStateWithBlockMgr(
        query_id, max_buffers, block_size, query_options, &state));
    if (query_state != NULL) *query_state = state;
    return state->block_mgr();
  }

  /// Create a new client tracker as a child of the RuntimeState's instance tracker.
  MemTracker* NewClientTracker(RuntimeState* state) {
    return pool_.Add(new MemTracker(-1, "client", state->instance_mem_tracker()));
  }

  BufferedBlockMgr* CreateMgrAndClient(int64_t query_id, int max_buffers, int block_size,
      int reserved_blocks, bool tolerates_oversubscription,
      BufferedBlockMgr::Client** client, RuntimeState** query_state = NULL,
      TQueryOptions* query_options = NULL) {
    RuntimeState* state;
    BufferedBlockMgr* mgr =
        CreateMgr(query_id, max_buffers, block_size, &state, query_options);

    MemTracker* client_tracker = NewClientTracker(state);
    EXPECT_OK(mgr->RegisterClient(Substitute("Client for query $0", query_id),
        reserved_blocks, tolerates_oversubscription, client_tracker, state, client));
    EXPECT_TRUE(client != NULL);
    if (query_state != NULL) *query_state = state;
    return mgr;
  }

  void CreateMgrsAndClients(int64_t start_query_id, int num_mgrs, int buffers_per_mgr,
      int block_size, int reserved_blocks_per_client, bool tolerates_oversubscription,
      vector<BufferedBlockMgr*>* mgrs, vector<BufferedBlockMgr::Client*>* clients) {
    for (int i = 0; i < num_mgrs; ++i) {
      BufferedBlockMgr::Client* client;
      BufferedBlockMgr* mgr = CreateMgrAndClient(start_query_id + i, buffers_per_mgr,
          block_size_, reserved_blocks_per_client, tolerates_oversubscription, &client);
      mgrs->push_back(mgr);
      clients->push_back(client);
    }
  }

  // Destroy all created query states and associated block managers.
  void TearDownMgrs() {
    // Tear down the query states, which DCHECKs that the memory consumption of
    // the query's trackers is zero.
    test_env_->TearDownQueries();
  }

  void AllocateBlocks(BufferedBlockMgr* block_mgr, BufferedBlockMgr::Client* client,
      int num_blocks, vector<BufferedBlockMgr::Block*>* blocks) {
    int32_t* data;
    Status status;
    BufferedBlockMgr::Block* new_block;
    for (int i = 0; i < num_blocks; ++i) {
      ASSERT_OK(block_mgr->GetNewBlock(client, NULL, &new_block));
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
      ASSERT_OK(blocks[i]->Pin(&pinned));
      ASSERT_TRUE(pinned);
    }
  }

  // Pin all blocks. By default, expect no errors from Unpin() calls. If
  // expected_error_codes is non-NULL, returning one of the error codes is
  // also allowed.
  void UnpinBlocks(const vector<BufferedBlockMgr::Block*>& blocks,
      const vector<TErrorCode::type>* expected_error_codes = nullptr,
      int delay_between_unpins_ms = 0) {
    for (int i = 0; i < blocks.size(); ++i) {
      Status status = blocks[i]->Unpin();
      if (!status.ok() && expected_error_codes != nullptr) {
        // Check if it's one of the expected errors.
        bool is_expected_error = false;
        for (TErrorCode::type code : *expected_error_codes) {
          if (status.code() == code) {
            is_expected_error = true;
            break;
          }
        }
        ASSERT_TRUE(is_expected_error) << status.msg().msg();
      } else {
        ASSERT_TRUE(status.ok()) << status.msg().msg();
      }
      if (delay_between_unpins_ms > 0) SleepForMs(delay_between_unpins_ms);
    }
  }

  void DeleteBlocks(const vector<BufferedBlockMgr::Block*>& blocks) {
    for (int i = 0; i < blocks.size(); ++i) {
      blocks[i]->Delete();
    }
  }

  void DeleteBlocks(const vector<pair<BufferedBlockMgr::Block*, int32_t>>& blocks) {
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
    ASSERT_TRUE(false) << "Writes did not complete after " << WRITE_WAIT_MILLIS << "ms";
  }

  static bool AllWritesComplete(BufferedBlockMgr* block_mgr) {
    return block_mgr->GetNumWritesOutstanding() == 0;
  }

  static bool AllWritesComplete(const vector<BufferedBlockMgr*>& block_mgrs) {
    for (int i = 0; i < block_mgrs.size(); ++i) {
      if (!AllWritesComplete(block_mgrs[i])) return false;
    }
    return true;
  }

  // Remove permissions for the temporary file at 'path' - all subsequent writes
  // to the file should fail. Expects backing file has already been allocated.
  static void DisableBackingFile(const string& path) {
    EXPECT_GT(path.size(), 0);
    EXPECT_EQ(0, chmod(path.c_str(), 0));
    LOG(INFO) << "Injected fault by removing file permissions " << path;
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
    block_mgr = CreateMgrAndClient(0, max_num_blocks, block_size, 0, false, &client);
    ASSERT_EQ(test_env_->TotalQueryMemoryConsumption(), 0);

    // Allocate blocks until max_num_blocks, they should all succeed and memory
    // usage should go up.
    BufferedBlockMgr::Block* new_block;
    BufferedBlockMgr::Block* first_block = NULL;
    for (int i = 0; i < max_num_blocks; ++i) {
      status = block_mgr->GetNewBlock(client, NULL, &new_block);
      ASSERT_TRUE(new_block != NULL);
      ASSERT_EQ(block_mgr->bytes_allocated(), (i + 1) * block_size);
      if (first_block == NULL) first_block = new_block;
      blocks.push_back(new_block);
    }

    // Trying to allocate a new one should fail.
    ASSERT_OK(block_mgr->GetNewBlock(client, NULL, &new_block));
    ASSERT_TRUE(new_block == NULL);
    ASSERT_EQ(block_mgr->bytes_allocated(), max_num_blocks * block_size);

    // We can allocate a new block by transferring an already allocated one.
    uint8_t* old_buffer = first_block->buffer();
    ASSERT_OK(block_mgr->GetNewBlock(client, first_block, &new_block));
    ASSERT_TRUE(new_block != NULL);
    ASSERT_EQ(old_buffer, new_block->buffer());
    ASSERT_EQ(block_mgr->bytes_allocated(), max_num_blocks * block_size);
    ASSERT_TRUE(!first_block->is_pinned());
    blocks.push_back(new_block);

    // Trying to allocate a new one should still fail.
    ASSERT_OK(block_mgr->GetNewBlock(client, NULL, &new_block));
    ASSERT_TRUE(new_block == NULL);
    ASSERT_EQ(block_mgr->bytes_allocated(), max_num_blocks * block_size);

    ASSERT_EQ(block_mgr->writes_issued(), 1);

    DeleteBlocks(blocks);
    TearDownMgrs();
  }

  void TestEvictionImpl(int block_size) {
    ASSERT_GT(block_size, 0);
    int max_num_buffers = 5;
    BufferedBlockMgr* block_mgr;
    BufferedBlockMgr::Client* client;
    block_mgr = CreateMgrAndClient(0, max_num_buffers, block_size, 0, false, &client);

    // Check counters.
    RuntimeProfile* profile = block_mgr->profile();
    RuntimeProfile::Counter* buffered_pin = profile->GetCounter("BufferedPins");

    vector<BufferedBlockMgr::Block*> blocks;
    AllocateBlocks(block_mgr, client, max_num_buffers, &blocks);

    ASSERT_EQ(block_mgr->bytes_allocated(), max_num_buffers * block_size);
    for (BufferedBlockMgr::Block* block : blocks) block->Unpin();

    // Re-pinning all blocks
    for (int i = 0; i < blocks.size(); ++i) {
      bool pinned;
      ASSERT_OK(blocks[i]->Pin(&pinned));
      ASSERT_TRUE(pinned);
      ValidateBlock(blocks[i], i);
    }
    int buffered_pins_expected = blocks.size();
    ASSERT_EQ(buffered_pin->value(), buffered_pins_expected);

    // Unpin all blocks
    for (BufferedBlockMgr::Block* block : blocks) block->Unpin();
    // Get two new blocks.
    AllocateBlocks(block_mgr, client, 2, &blocks);
    // At least two writes must be issued. The first (num_blocks - 2) must be in memory.
    ASSERT_GE(block_mgr->writes_issued(), 2);
    for (int i = 0; i < (max_num_buffers - 2); ++i) {
      bool pinned;
      ASSERT_OK(blocks[i]->Pin(&pinned));
      ASSERT_TRUE(pinned);
      ValidateBlock(blocks[i], i);
    }
    ASSERT_GE(buffered_pin->value(), buffered_pins_expected);
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
    const int num_iterations = 10000;
    const int iters_before_close = num_iterations - 1000;
    bool close_called = false;
    unordered_map<BufferedBlockMgr::Block*, int> pinned_block_map;
    vector<pair<BufferedBlockMgr::Block*, int32_t>> pinned_blocks;
    unordered_map<BufferedBlockMgr::Block*, int> unpinned_block_map;
    vector<pair<BufferedBlockMgr::Block*, int32_t>> unpinned_blocks;

    typedef enum { Pin, New, Unpin, Delete, Close } ApiFunction;
    ApiFunction api_function;

    BufferedBlockMgr::Client* client;
    ASSERT_OK(
        block_mgr->RegisterClient("", 0, false, NewClientTracker(state), state, &client));
    ASSERT_TRUE(client != NULL);

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
      Status status;
      switch (api_function) {
        case New:
          status = block_mgr->GetNewBlock(client, NULL, &new_block);
          if (close_called || (tid != SINGLE_THREADED_TID && status.IsCancelled())) {
            ASSERT_TRUE(new_block == NULL);
            ASSERT_TRUE(status.IsCancelled());
            continue;
          }
          ASSERT_OK(status);
          ASSERT_TRUE(new_block != NULL);
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
            ASSERT_TRUE(status.IsCancelled());
            // In single-threaded runs the block should not have been pinned.
            // In multi-threaded runs Pin() may return the block pinned but the status to
            // be cancelled. In this case we could move the block from unpinned_blocks
            // to pinned_blocks. We do not do that because after IsCancelled() no actual
            // block operations should take place.
            if (tid == SINGLE_THREADED_TID) ASSERT_FALSE(pinned);
            continue;
          }
          ASSERT_OK(status);
          ASSERT_TRUE(pinned);
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
            ASSERT_TRUE(status.IsCancelled());
            continue;
          }
          ASSERT_OK(status);
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
      Status status = BufferedBlockMgr::Create(state, state->query_mem_tracker(),
          state->runtime_profile(), test_env_->tmp_file_mgr(), block_size_ * num_buffers,
          block_size_, &mgr);
    }
  }

  // IMPALA-2286: Test for races between BufferedBlockMgr::Create() and
  // BufferedBlockMgr::~BufferedBlockMgr().
  void CreateDestroyMulti() {
    const int num_threads = 8;
    thread_group workers;
    // Create a shared RuntimeState with no BufferedBlockMgr.
    RuntimeState shared_state(TQueryCtx(), test_env_->exec_env());

    for (int i = 0; i < num_threads; ++i) {
      thread* t = new thread(
          bind(&BufferedBlockMgrTest::CreateDestroyThread, this, &shared_state));
      workers.add_thread(t);
    }
    workers.join_all();
    shared_state.ReleaseResources();
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

  void TestWriteError(int write_delay_ms);

  scoped_ptr<TestEnv> test_env_;
  ObjectPool pool_;
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
  block_mgr = CreateMgrAndClient(0, max_num_blocks, block_size, 0, false, &client);
  MemTracker* client_tracker = block_mgr->get_tracker(client);
  ASSERT_EQ(0, test_env_->TotalQueryMemoryConsumption());

  vector<BufferedBlockMgr::Block*> blocks;

  // Allocate a small block.
  BufferedBlockMgr::Block* new_block = NULL;
  ASSERT_OK(block_mgr->GetNewBlock(client, NULL, &new_block, 128));
  ASSERT_TRUE(new_block != NULL);
  ASSERT_EQ(block_mgr->bytes_allocated(), 0);
  ASSERT_EQ(block_mgr->mem_tracker()->consumption(), 0);
  ASSERT_EQ(client_tracker->consumption(), 128);
  ASSERT_TRUE(new_block->is_pinned());
  ASSERT_EQ(new_block->BytesRemaining(), 128);
  ASSERT_TRUE(new_block->buffer() != NULL);
  blocks.push_back(new_block);

  // Allocate a normal block
  ASSERT_OK(block_mgr->GetNewBlock(client, NULL, &new_block));
  ASSERT_TRUE(new_block != NULL);
  ASSERT_EQ(block_mgr->bytes_allocated(), block_mgr->max_block_size());
  ASSERT_EQ(block_mgr->mem_tracker()->consumption(), block_mgr->max_block_size());
  ASSERT_EQ(client_tracker->consumption(), 128 + block_mgr->max_block_size());
  ASSERT_TRUE(new_block->is_pinned());
  ASSERT_EQ(new_block->BytesRemaining(), block_mgr->max_block_size());
  ASSERT_TRUE(new_block->buffer() != NULL);
  blocks.push_back(new_block);

  // Allocate another small block.
  ASSERT_OK(block_mgr->GetNewBlock(client, NULL, &new_block, 512));
  ASSERT_TRUE(new_block != NULL);
  ASSERT_EQ(block_mgr->bytes_allocated(), block_mgr->max_block_size());
  ASSERT_EQ(block_mgr->mem_tracker()->consumption(), block_mgr->max_block_size());
  ASSERT_EQ(client_tracker->consumption(), 128 + 512 + block_mgr->max_block_size());
  ASSERT_TRUE(new_block->is_pinned());
  ASSERT_EQ(new_block->BytesRemaining(), 512);
  ASSERT_TRUE(new_block->buffer() != NULL);
  blocks.push_back(new_block);

  // Should be able to unpin and pin the middle block
  ASSERT_OK(blocks[1]->Unpin());

  bool pinned;
  ASSERT_OK(blocks[1]->Pin(&pinned));
  ASSERT_TRUE(pinned);

  DeleteBlocks(blocks);
  TearDownMgrs();
}

// Test that pinning more blocks than the max available buffers.
TEST_F(BufferedBlockMgrTest, Pin) {
  int max_num_blocks = 5;
  const int block_size = 1024;
  BufferedBlockMgr* block_mgr;
  BufferedBlockMgr::Client* client;
  block_mgr = CreateMgrAndClient(0, max_num_blocks, block_size, 0, false, &client);

  vector<BufferedBlockMgr::Block*> blocks;
  AllocateBlocks(block_mgr, client, max_num_blocks, &blocks);

  // Unpin them all.
  for (int i = 0; i < blocks.size(); ++i) {
    ASSERT_OK(blocks[i]->Unpin());
  }

  // Allocate more, this should work since we just unpinned some blocks.
  AllocateBlocks(block_mgr, client, max_num_blocks, &blocks);

  // Try to pin a unpinned block, this should not be possible.
  bool pinned;
  ASSERT_OK(blocks[0]->Pin(&pinned));
  ASSERT_FALSE(pinned);

  // Unpin all blocks.
  for (int i = 0; i < blocks.size(); ++i) {
    ASSERT_OK(blocks[i]->Unpin());
  }

  // Should be able to pin max_num_blocks blocks.
  for (int i = 0; i < max_num_blocks; ++i) {
    ASSERT_OK(blocks[i]->Pin(&pinned));
    ASSERT_TRUE(pinned);
  }

  // Can't pin any more though.
  ASSERT_OK(blocks[max_num_blocks]->Pin(&pinned));
  ASSERT_FALSE(pinned);

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
  block_mgr = CreateMgrAndClient(0, max_num_buffers, block_size, 0, false, &client);

  // Check counters.
  RuntimeProfile* profile = block_mgr->profile();
  RuntimeProfile::Counter* recycled_cnt = profile->GetCounter("BlocksRecycled");
  RuntimeProfile::Counter* created_cnt = profile->GetCounter("BlocksCreated");

  vector<BufferedBlockMgr::Block*> blocks;
  AllocateBlocks(block_mgr, client, max_num_buffers, &blocks);
  ASSERT_EQ(created_cnt->value(), max_num_buffers);

  DeleteBlocks(blocks);
  blocks.clear();
  AllocateBlocks(block_mgr, client, max_num_buffers, &blocks);
  ASSERT_EQ(created_cnt->value(), max_num_buffers);
  ASSERT_EQ(recycled_cnt->value(), max_num_buffers);

  DeleteBlocks(blocks);
  TearDownMgrs();
}

// Delete blocks of various sizes and statuses to exercise the different code paths.
// This relies on internal validation in block manager to detect many errors.
TEST_F(BufferedBlockMgrTest, DeleteSingleBlocks) {
  int max_num_buffers = 16;
  BufferedBlockMgr::Client* client;
  BufferedBlockMgr* block_mgr =
      CreateMgrAndClient(0, max_num_buffers, block_size_, 0, false, &client);
  MemTracker* client_tracker = block_mgr->get_tracker(client);

  // Pinned I/O block.
  BufferedBlockMgr::Block* new_block;
  ASSERT_OK(block_mgr->GetNewBlock(client, NULL, &new_block));
  ASSERT_TRUE(new_block != NULL);
  ASSERT_TRUE(new_block->is_pinned());
  ASSERT_TRUE(new_block->is_max_size());
  new_block->Delete();
  ASSERT_EQ(0, client_tracker->consumption());

  // Pinned non-I/O block.
  int small_block_size = 128;
  ASSERT_OK(block_mgr->GetNewBlock(client, NULL, &new_block, small_block_size));
  ASSERT_TRUE(new_block != NULL);
  ASSERT_TRUE(new_block->is_pinned());
  ASSERT_EQ(small_block_size, client_tracker->consumption());
  new_block->Delete();
  ASSERT_EQ(0, client_tracker->consumption());

  // Unpinned I/O block - delete after written to disk.
  ASSERT_OK(block_mgr->GetNewBlock(client, NULL, &new_block));
  ASSERT_TRUE(new_block != NULL);
  ASSERT_TRUE(new_block->is_pinned());
  ASSERT_TRUE(new_block->is_max_size());
  new_block->Unpin();
  ASSERT_FALSE(new_block->is_pinned());
  WaitForWrites(block_mgr);
  new_block->Delete();
  ASSERT_EQ(client_tracker->consumption(), 0);

  // Unpinned I/O block - delete before written to disk.
  ASSERT_OK(block_mgr->GetNewBlock(client, NULL, &new_block));
  ASSERT_TRUE(new_block != NULL);
  ASSERT_TRUE(new_block->is_pinned());
  ASSERT_TRUE(new_block->is_max_size());
  new_block->Unpin();
  ASSERT_FALSE(new_block->is_pinned());
  new_block->Delete();
  WaitForWrites(block_mgr);
  ASSERT_EQ(client_tracker->consumption(), 0);

  TearDownMgrs();
}

// This exercises a code path where:
// 1. A block A is unpinned.
// 2. A block B is unpinned.
// 3. A write for block A is initiated.
// 4. Block A is pinned.
// 5. Block B is pinned, with block A passed in to be deleted.
//    Block A's buffer will be transferred to block B.
// 6. The write for block A completes.
// Previously there was a bug (IMPALA-3936) where the buffer transfer happened before the
// write completed. There were also various hangs related to missing condition variable
// notifications.
TEST_F(BufferedBlockMgrTest, TransferBufferDuringWrite) {
  const int trials = 5;
  const int max_num_buffers = 2;
  BufferedBlockMgr::Client* client;
  RuntimeState* query_state;
  BufferedBlockMgr* block_mgr = CreateMgrAndClient(
      0, max_num_buffers, block_size_, 1, false, &client, &query_state);

  for (int trial = 0; trial < trials; ++trial) {
    for (int delay_ms = 0; delay_ms <= 10; delay_ms += 5) {
      // Force writes to be delayed to enlarge window of opportunity for bug.
      block_mgr->set_debug_write_delay_ms(delay_ms);
      vector<BufferedBlockMgr::Block*> blocks;
      AllocateBlocks(block_mgr, client, 2, &blocks);

      // Force the second block to be written and have its buffer freed.
      // We only have one buffer to share between the first and second blocks now.
      ASSERT_OK(blocks[1]->Unpin());

      // Create another client. Reserving different numbers of buffers can send it
      // down different code paths because the original client is entitled to different
      // number of buffers.
      int reserved_buffers = trial % max_num_buffers;
      BufferedBlockMgr::Client* tmp_client;
      ASSERT_OK(block_mgr->RegisterClient("tmp_client", reserved_buffers, false,
          NewClientTracker(query_state), query_state, &tmp_client));
      BufferedBlockMgr::Block* tmp_block;
      ASSERT_OK(block_mgr->GetNewBlock(tmp_client, NULL, &tmp_block));

      // Initiate the write, repin the block, then immediately try to swap the buffer to
      // the second block while the write is still in flight.
      ASSERT_OK(blocks[0]->Unpin());
      bool pinned;
      ASSERT_OK(blocks[0]->Pin(&pinned));
      ASSERT_TRUE(pinned);
      ASSERT_OK(blocks[1]->Pin(&pinned, blocks[0], false));
      ASSERT_TRUE(pinned);

      blocks[1]->Delete();
      tmp_block->Delete();
      block_mgr->ClearReservations(tmp_client);
    }
  }
}

// Test that all APIs return cancelled after close.
TEST_F(BufferedBlockMgrTest, Close) {
  int max_num_buffers = 5;
  const int block_size = 1024;
  BufferedBlockMgr* block_mgr;
  BufferedBlockMgr::Client* client;
  block_mgr = CreateMgrAndClient(0, max_num_buffers, block_size, 0, false, &client);

  vector<BufferedBlockMgr::Block*> blocks;
  AllocateBlocks(block_mgr, client, max_num_buffers, &blocks);

  block_mgr->Cancel();

  BufferedBlockMgr::Block* new_block;
  Status status = block_mgr->GetNewBlock(client, NULL, &new_block);
  ASSERT_TRUE(status.IsCancelled());
  ASSERT_TRUE(new_block == NULL);
  status = blocks[0]->Unpin();
  ASSERT_TRUE(status.IsCancelled());
  bool pinned;
  status = blocks[0]->Pin(&pinned);
  ASSERT_TRUE(status.IsCancelled());

  DeleteBlocks(blocks);
  TearDownMgrs();
}

TEST_F(BufferedBlockMgrTest, DestructDuringWrite) {
  const int trials = 20;
  const int max_num_buffers = 5;

  for (int trial = 0; trial < trials; ++trial) {
    BufferedBlockMgr::Client* client;
    BufferedBlockMgr* block_mgr =
        CreateMgrAndClient(0, max_num_buffers, block_size_, 0, false, &client);

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

void BufferedBlockMgrTest::TestRuntimeStateTeardown(
    bool write_error, bool wait_for_writes) {
  const int max_num_buffers = 10;
  RuntimeState* state;
  BufferedBlockMgr::Client* client;
  CreateMgrAndClient(0, max_num_buffers, block_size_, 0, false, &client, &state);

  // Hold extra references to block mgr and query state so they outlive RuntimeState.
  shared_ptr<BufferedBlockMgr> block_mgr;
  QueryState::ScopedRef qs(state->query_id());
  Status status = BufferedBlockMgr::Create(state, state->query_mem_tracker(),
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

    const string& tmp_file_path = blocks[0]->TmpFilePath();
    DeleteBlocks(more_blocks);
    PinBlocks(blocks);
    DisableBackingFile(tmp_file_path);
  }

  // Unpin will initiate writes. If the write error propagates fast enough, some Unpin()
  // calls may see a cancelled block mgr.
  vector<TErrorCode::type> cancelled_code = {TErrorCode::CANCELLED};
  UnpinBlocks(blocks, write_error ? &cancelled_code : nullptr);

  // Tear down while writes are in flight. The block mgr may outlive the runtime state
  // because it may be referenced by other runtime states. This test simulates this
  // scenario by holding onto a reference to the block mgr. This should be safe so
  // long as blocks are properly deleted before the runtime state is torn down.
  DeleteBlocks(blocks);
  test_env_->TearDownQueries();

  // Optionally wait for writes to complete after cancellation.
  if (wait_for_writes) WaitForWrites(block_mgr.get());
  block_mgr.reset();

  ASSERT_EQ(test_env_->TotalQueryMemoryConsumption(), 0);
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

// Regression test for IMPALA-2927 write complete with cancelled runtime state
TEST_F(BufferedBlockMgrTest, WriteCompleteWithCancelledRuntimeState) {
  const int max_num_buffers = 10;
  RuntimeState* state;
  BufferedBlockMgr::Client* client;
  BufferedBlockMgr* block_mgr =
      CreateMgrAndClient(0, max_num_buffers, block_size_, 0, false, &client, &state);

  vector<BufferedBlockMgr::Block*> blocks;
  AllocateBlocks(block_mgr, client, max_num_buffers, &blocks);

  // Force flushing blocks to disk so that more writes are in flight.
  UnpinBlocks(blocks);

  // Cancel the runtime state and re-pin the blocks while writes are in flight to check
  // that WriteComplete() handles the case ok.
  state->set_is_cancelled();
  PinBlocks(blocks);

  WaitForWrites(block_mgr);
  DeleteBlocks(blocks);
}

// Remove write permissions on scratch files. Return # of scratch files.
static int remove_scratch_perms() {
  int num_files = 0;
  directory_iterator dir_it(SCRATCH_DIR);
  for (; dir_it != directory_iterator(); ++dir_it) {
    ++num_files;
    chmod(dir_it->path().c_str(), 0);
  }

  return num_files;
}

// Test that the block manager behaves correctly after a write error.  Delete the scratch
// directory before an operation that would cause a write and test that subsequent API
// calls return 'CANCELLED' correctly.
void BufferedBlockMgrTest::TestWriteError(int write_delay_ms) {
  int max_num_buffers = 2;
  const int block_size = 1024;
  BufferedBlockMgr* block_mgr;
  BufferedBlockMgr::Client* client;
  block_mgr = CreateMgrAndClient(0, max_num_buffers, block_size, 0, false, &client);
  block_mgr->set_debug_write_delay_ms(write_delay_ms);

  vector<BufferedBlockMgr::Block*> blocks;
  AllocateBlocks(block_mgr, client, max_num_buffers, &blocks);
  // Unpin two blocks here, to ensure that backing storage is allocated in tmp file.
  UnpinBlocks(blocks);
  WaitForWrites(block_mgr);
  // Repin the blocks
  PinBlocks(blocks);
  // Remove the backing storage so that future writes will fail
  int num_files = remove_scratch_perms();
  ASSERT_GT(num_files, 0);
  vector<TErrorCode::type> expected_error_codes = {TErrorCode::CANCELLED,
      TErrorCode::SCRATCH_ALLOCATION_FAILED};
  // Give the first write a chance to fail before the second write starts.
  int interval_ms = 10;
  UnpinBlocks(blocks, &expected_error_codes, interval_ms);
  WaitForWrites(block_mgr);
  // Subsequent calls should fail.
  DeleteBlocks(blocks);
  BufferedBlockMgr::Block* new_block;
  ASSERT_TRUE(block_mgr->GetNewBlock(client, NULL, &new_block).IsCancelled());
  ASSERT_TRUE(new_block == NULL);

  TearDownMgrs();
}

TEST_F(BufferedBlockMgrTest, WriteError) {
  TestWriteError(0);
}

// Regression test for IMPALA-4842 - inject a delay in the write to
// reproduce the issue.
TEST_F(BufferedBlockMgrTest, WriteErrorWriteDelay) {
  TestWriteError(100);
}

// Test block manager error handling when temporary file space cannot be allocated to
// back an unpinned buffer.
TEST_F(BufferedBlockMgrTest, TmpFileAllocateError) {
  int max_num_buffers = 2;
  BufferedBlockMgr::Client* client;
  BufferedBlockMgr* block_mgr =
      CreateMgrAndClient(0, max_num_buffers, block_size_, 0, false, &client);

  vector<BufferedBlockMgr::Block*> blocks;
  AllocateBlocks(block_mgr, client, max_num_buffers, &blocks);
  // Unpin a block, forcing a write.
  ASSERT_OK(blocks[0]->Unpin());
  WaitForWrites(block_mgr);
  // Remove temporary files - subsequent operations will fail.
  int num_files = remove_scratch_perms();
  ASSERT_TRUE(num_files > 0);
  // Current implementation will not fail here until it attempts to write the file.
  // This behavior is not contractual but we want to know if it changes accidentally.
  ASSERT_OK(blocks[1]->Unpin());

  // Write failure should cancel query
  WaitForWrites(block_mgr);
  ASSERT_TRUE(block_mgr->IsCancelled());

  DeleteBlocks(blocks);
  TearDownMgrs();
}

// Test that the block manager is able to blacklist a temporary device correctly after a
// write error. The query that encountered the write error should not allocate more
// blocks on that device, but existing blocks on the device will remain in use and future
// queries will use the device.
TEST_F(BufferedBlockMgrTest, WriteErrorBlacklist) {
  // Set up two buffered block managers with two temporary dirs.
  vector<string> tmp_dirs = InitMultipleTmpDirs(2);
  // Simulate two concurrent queries.
  const int NUM_BLOCK_MGRS = 2;
  const int MAX_NUM_BLOCKS = 4;
  int blocks_per_mgr = MAX_NUM_BLOCKS / NUM_BLOCK_MGRS;
  vector<BufferedBlockMgr*> block_mgrs;
  vector<BufferedBlockMgr::Client*> clients;
  CreateMgrsAndClients(
      0, NUM_BLOCK_MGRS, blocks_per_mgr, block_size_, 0, false, &block_mgrs, &clients);

  // Allocate files for all 2x2 combinations by unpinning blocks.
  vector<vector<BufferedBlockMgr::Block*>> blocks;
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
  const string& error_file_path = error_block->TmpFilePath();
  PinBlocks(all_blocks);
  DisableBackingFile(error_file_path);
  UnpinBlocks(all_blocks); // Should succeed since writes occur asynchronously
  WaitForWrites(block_mgrs);
  // Both block managers have a usable tmp directory so should still be usable.
  ASSERT_FALSE(block_mgrs[error_mgr]->IsCancelled());
  ASSERT_FALSE(block_mgrs[no_error_mgr]->IsCancelled());
  // Temporary device with error should still be active.
  vector<TmpFileMgr::DeviceId> active_tmp_devices =
      test_env_->tmp_file_mgr()->ActiveTmpDevices();
  ASSERT_EQ(tmp_dirs.size(), active_tmp_devices.size());
  for (int i = 0; i < active_tmp_devices.size(); ++i) {
    const string& device_path =
        test_env_->tmp_file_mgr()->GetTmpDirPath(active_tmp_devices[i]);
    ASSERT_EQ(string::npos, error_dir.find(device_path));
  }

  // The error block manager should only allocate from the device that had no error.
  // The non-error block manager should continue using both devices, since it didn't
  // encounter a write error itself.
  vector<BufferedBlockMgr::Block*> error_new_blocks;
  AllocateBlocks(
      block_mgrs[error_mgr], clients[error_mgr], blocks_per_mgr, &error_new_blocks);
  UnpinBlocks(error_new_blocks);
  WaitForWrites(block_mgrs);
  EXPECT_TRUE(FindBlockForDir(error_new_blocks, good_dir) != NULL);
  EXPECT_TRUE(FindBlockForDir(error_new_blocks, error_dir) == NULL);
  for (int i = 0; i < error_new_blocks.size(); ++i) {
    LOG(INFO) << "Newly created block backed by file "
              << error_new_blocks[i]->TmpFilePath();
    EXPECT_TRUE(BlockInDir(error_new_blocks[i], good_dir));
  }
  DeleteBlocks(error_new_blocks);

  PinBlocks(blocks[no_error_mgr]);
  UnpinBlocks(blocks[no_error_mgr]);
  WaitForWrites(block_mgrs);
  EXPECT_TRUE(FindBlockForDir(blocks[no_error_mgr], good_dir) != NULL);
  EXPECT_TRUE(FindBlockForDir(blocks[no_error_mgr], error_dir) != NULL);

  // The second block manager should use the bad directory for new blocks since
  // blacklisting is per-manager, not global.
  vector<BufferedBlockMgr::Block*> no_error_new_blocks;
  AllocateBlocks(block_mgrs[no_error_mgr], clients[no_error_mgr], blocks_per_mgr,
      &no_error_new_blocks);
  UnpinBlocks(no_error_new_blocks);
  WaitForWrites(block_mgrs);
  EXPECT_TRUE(FindBlockForDir(no_error_new_blocks, good_dir) != NULL);
  EXPECT_TRUE(FindBlockForDir(no_error_new_blocks, error_dir) != NULL);
  DeleteBlocks(no_error_new_blocks);

  // A new block manager should use the both dirs for backing storage.
  BufferedBlockMgr::Client* new_client;
  BufferedBlockMgr* new_block_mgr =
      CreateMgrAndClient(9999, blocks_per_mgr, block_size_, 0, false, &new_client);
  vector<BufferedBlockMgr::Block*> new_mgr_blocks;
  AllocateBlocks(new_block_mgr, new_client, blocks_per_mgr, &new_mgr_blocks);
  UnpinBlocks(new_mgr_blocks);
  WaitForWrites(block_mgrs);
  EXPECT_TRUE(FindBlockForDir(new_mgr_blocks, good_dir) != NULL);
  EXPECT_TRUE(FindBlockForDir(new_mgr_blocks, error_dir) != NULL);
  DeleteBlocks(new_mgr_blocks);

  DeleteBlocks(all_blocks);
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
  CreateMgrsAndClients(
      0, num_block_mgrs, blocks_per_mgr, block_size_, 0, false, &block_mgrs, &clients);

  // Allocate files for all 2x2 combinations by unpinning blocks.
  vector<vector<BufferedBlockMgr::Block*>> blocks;
  for (int i = 0; i < num_block_mgrs; ++i) {
    vector<BufferedBlockMgr::Block*> mgr_blocks;
    LOG(INFO) << "Iter " << i;
    AllocateBlocks(block_mgrs[i], clients[i], blocks_per_mgr, &mgr_blocks);
    blocks.push_back(mgr_blocks);
  }
  const string& bad_dir = tmp_dirs[0];
  const string& bad_scratch_subdir = bad_dir + SCRATCH_SUFFIX;
  chmod(bad_scratch_subdir.c_str(), 0);
  // The block mgr should attempt to allocate space in bad dir for one block, which will
  // cause an error when it tries to create/expand the file. It should recover and just
  // use the good dir.
  UnpinBlocks(blocks[0]);
  // Directories remain on active list even when they experience errors.
  ASSERT_EQ(2, test_env_->tmp_file_mgr()->NumActiveTmpDevices());
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
  RuntimeState* runtime_state;
  BufferedBlockMgr::Client* client;
  BufferedBlockMgr* block_mgr = CreateMgrAndClient(
      0, max_num_buffers, block_size_, 0, false, &client, &runtime_state);
  vector<BufferedBlockMgr::Block*> blocks;
  AllocateBlocks(block_mgr, client, max_num_buffers, &blocks);
  for (int i = 0; i < tmp_dirs.size(); ++i) {
    const string& tmp_scratch_subdir = tmp_dirs[i] + SCRATCH_SUFFIX;
    chmod(tmp_scratch_subdir.c_str(), 0);
  }
  ErrorLogMap error_log;
  runtime_state->GetErrors(&error_log);
  ASSERT_TRUE(error_log.empty());
  // Unpin the blocks. Unpinning may fail if it hits a write error before this thread is
  // done unpinning.
  vector<TErrorCode::type> cancelled_code = {TErrorCode::CANCELLED};
  UnpinBlocks(blocks, &cancelled_code);

  LOG(INFO) << "Waiting for writes.";
  // Write failure should cancel query.
  WaitForWrites(block_mgr);
  LOG(INFO) << "writes done.";
  ASSERT_TRUE(block_mgr->IsCancelled());
  runtime_state->GetErrors(&error_log);
  ASSERT_FALSE(error_log.empty());
  stringstream error_string;
  PrintErrorMap(&error_string, error_log);
  LOG(INFO) << "Errors: " << error_string.str();
  // SCRATCH_ALLOCATION_FAILED error should exist in the error log.
  ErrorLogMap::const_iterator it = error_log.find(TErrorCode::SCRATCH_ALLOCATION_FAILED);
  ASSERT_NE(it, error_log.end());
  ASSERT_GT(it->second.count, 0);
  DeleteBlocks(blocks);
}

// Test that block manager can still allocate buffers when spilling is disabled.
TEST_F(BufferedBlockMgrTest, NoTmpDirs) {
  InitMultipleTmpDirs(0);
  int max_num_buffers = 3;
  BufferedBlockMgr::Client* client;
  BufferedBlockMgr* block_mgr =
      CreateMgrAndClient(0, max_num_buffers, block_size_, 0, false, &client);
  vector<BufferedBlockMgr::Block*> blocks;
  AllocateBlocks(block_mgr, client, max_num_buffers, &blocks);
  DeleteBlocks(blocks);
}

// Test that block manager can still allocate buffers when spilling is disabled by
// setting scratch_limit = 0.
TEST_F(BufferedBlockMgrTest, ScratchLimitZero) {
  int max_num_buffers = 3;
  BufferedBlockMgr::Client* client;
  TQueryOptions query_options;
  query_options.scratch_limit = 0;
  BufferedBlockMgr* block_mgr = CreateMgrAndClient(
      0, max_num_buffers, block_size_, 0, false, &client, NULL, &query_options);
  vector<BufferedBlockMgr::Block*> blocks;
  AllocateBlocks(block_mgr, client, max_num_buffers, &blocks);
  DeleteBlocks(blocks);
}

// Create two clients with different number of reserved buffers.
TEST_F(BufferedBlockMgrTest, MultipleClients) {
  int client1_buffers = 3;
  int client2_buffers = 5;
  int max_num_buffers = client1_buffers + client2_buffers;
  const int block_size = 1024;
  RuntimeState* runtime_state;
  BufferedBlockMgr* block_mgr = CreateMgr(0, max_num_buffers, block_size, &runtime_state);

  BufferedBlockMgr::Client* client1 = NULL;
  BufferedBlockMgr::Client* client2 = NULL;
  ASSERT_OK(block_mgr->RegisterClient("", client1_buffers, false,
      NewClientTracker(runtime_state), runtime_state, &client1));
  ASSERT_TRUE(client1 != NULL);
  ASSERT_OK(block_mgr->RegisterClient("", client2_buffers, false,
      NewClientTracker(runtime_state), runtime_state, &client2));
  ASSERT_TRUE(client2 != NULL);

  // Reserve client 1's and 2's buffers. They should succeed.
  bool reserved = block_mgr->TryAcquireTmpReservation(client1, 1);
  ASSERT_TRUE(reserved);
  reserved = block_mgr->TryAcquireTmpReservation(client2, 1);
  ASSERT_TRUE(reserved);

  vector<BufferedBlockMgr::Block*> client1_blocks;
  // Allocate all of client1's reserved blocks, they should all succeed.
  AllocateBlocks(block_mgr, client1, client1_buffers, &client1_blocks);

  // Try allocating one more, that should fail.
  BufferedBlockMgr::Block* block;
  ASSERT_OK(block_mgr->GetNewBlock(client1, NULL, &block));
  ASSERT_TRUE(block == NULL);

  // Trying to reserve should also fail.
  reserved = block_mgr->TryAcquireTmpReservation(client1, 1);
  ASSERT_FALSE(reserved);

  // Allocate all of client2's reserved blocks, these should succeed.
  vector<BufferedBlockMgr::Block*> client2_blocks;
  AllocateBlocks(block_mgr, client2, client2_buffers, &client2_blocks);

  // Try allocating one more from client 2, that should fail.
  ASSERT_OK(block_mgr->GetNewBlock(client2, NULL, &block));
  ASSERT_TRUE(block == NULL);

  // Unpin one block from client 1.
  ASSERT_OK(client1_blocks[0]->Unpin());

  // Client 2 should still not be able to allocate.
  ASSERT_OK(block_mgr->GetNewBlock(client2, NULL, &block));
  ASSERT_TRUE(block == NULL);

  // Client 2 should still not be able to reserve.
  reserved = block_mgr->TryAcquireTmpReservation(client2, 1);
  ASSERT_FALSE(reserved);

  // Client 1 should be able to though.
  ASSERT_OK(block_mgr->GetNewBlock(client1, NULL, &block));
  ASSERT_TRUE(block != NULL);
  client1_blocks.push_back(block);

  // Unpin two of client 1's blocks (client 1 should have 3 unpinned blocks now).
  ASSERT_OK(client1_blocks[1]->Unpin());
  ASSERT_OK(client1_blocks[2]->Unpin());

  // Clear client 1's reservation
  block_mgr->ClearReservations(client1);

  // Client 2 should be able to reserve 1 buffers now (there are 2 left);
  reserved = block_mgr->TryAcquireTmpReservation(client2, 1);
  ASSERT_TRUE(reserved);

  // Client one can only pin 1.
  bool pinned;
  ASSERT_OK(client1_blocks[0]->Pin(&pinned));
  ASSERT_TRUE(pinned);
  // Can't get this one.
  ASSERT_OK(client1_blocks[1]->Pin(&pinned));
  ASSERT_FALSE(pinned);

  // Client 2 can pick up the one reserved buffer
  ASSERT_OK(block_mgr->GetNewBlock(client2, NULL, &block));
  ASSERT_TRUE(block != NULL);
  client2_blocks.push_back(block);

  // But not a second
  BufferedBlockMgr::Block* block2;
  ASSERT_OK(block_mgr->GetNewBlock(client2, NULL, &block2));
  ASSERT_TRUE(block2 == NULL);

  // Unpin client 2's block it got from the reservation. Sine this is a tmp
  // reservation, client 1 can pick it up again (it is not longer reserved).
  ASSERT_OK(block->Unpin());
  ASSERT_OK(client1_blocks[1]->Pin(&pinned));
  ASSERT_TRUE(pinned);

  DeleteBlocks(client1_blocks);
  DeleteBlocks(client2_blocks);
  TearDownMgrs();
}

// Create two clients with different number of reserved buffers and some additional.
TEST_F(BufferedBlockMgrTest, MultipleClientsExtraBuffers) {
  int client1_buffers = 1;
  int client2_buffers = 1;
  int max_num_buffers = client1_buffers + client2_buffers + 2;
  const int block_size = 1024;
  RuntimeState* runtime_state;
  BufferedBlockMgr* block_mgr = CreateMgr(0, max_num_buffers, block_size, &runtime_state);

  BufferedBlockMgr::Client* client1 = NULL;
  BufferedBlockMgr::Client* client2 = NULL;
  BufferedBlockMgr::Block* block = NULL;
  ASSERT_OK(block_mgr->RegisterClient("", client1_buffers, false,
      NewClientTracker(runtime_state), runtime_state, &client1));
  ASSERT_TRUE(client1 != NULL);
  ASSERT_OK(block_mgr->RegisterClient("", client2_buffers, false,
      NewClientTracker(runtime_state), runtime_state, &client2));
  ASSERT_TRUE(client2 != NULL);

  vector<BufferedBlockMgr::Block*> client1_blocks;
  // Allocate all of client1's reserved blocks, they should all succeed.
  AllocateBlocks(block_mgr, client1, client1_buffers, &client1_blocks);

  // Allocate all of client2's reserved blocks, these should succeed.
  vector<BufferedBlockMgr::Block*> client2_blocks;
  AllocateBlocks(block_mgr, client2, client2_buffers, &client2_blocks);

  // We have two spare buffers now. Each client should be able to allocate it.
  ASSERT_OK(block_mgr->GetNewBlock(client1, NULL, &block));
  ASSERT_TRUE(block != NULL);
  client1_blocks.push_back(block);
  ASSERT_OK(block_mgr->GetNewBlock(client2, NULL, &block));
  ASSERT_TRUE(block != NULL);
  client2_blocks.push_back(block);

  // Now we are completely full, no one should be able to allocate a new block.
  ASSERT_OK(block_mgr->GetNewBlock(client1, NULL, &block));
  ASSERT_TRUE(block == NULL);
  ASSERT_OK(block_mgr->GetNewBlock(client2, NULL, &block));
  ASSERT_TRUE(block == NULL);

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
  ASSERT_OK(block_mgr->RegisterClient("", client1_buffers, false,
      NewClientTracker(runtime_state), runtime_state, &client1));
  ASSERT_TRUE(client1 != NULL);
  ASSERT_OK(block_mgr->RegisterClient("", client2_buffers, false,
      NewClientTracker(runtime_state), runtime_state, &client2));
  ASSERT_TRUE(client2 != NULL);
  ASSERT_OK(block_mgr->RegisterClient("", client3_buffers, true,
      NewClientTracker(runtime_state), runtime_state, &client3));
  ASSERT_TRUE(client3 != NULL);

  // Client one allocates first block, should work.
  ASSERT_OK(block_mgr->GetNewBlock(client1, NULL, &block));
  ASSERT_TRUE(block != NULL);
  blocks.push_back(block);

  // Client two allocates first block, should work.
  ASSERT_OK(block_mgr->GetNewBlock(client2, NULL, &block));
  ASSERT_TRUE(block != NULL);
  blocks.push_back(block);

  // At this point we've used both buffers. Client one reserved one so subsequent
  // calls should fail with no error (but returns no block).
  ASSERT_OK(block_mgr->GetNewBlock(client1, NULL, &block));
  ASSERT_TRUE(block == NULL);

  // Allocate with client two. Since client two reserved 2 buffers, this should fail
  // with MEM_LIMIT_EXCEEDED.
  ASSERT_TRUE(block_mgr->GetNewBlock(client2, NULL, &block).IsMemLimitExceeded());

  // Allocate with client three. Since client three can tolerate oversubscription,
  // this should fail with no error even though it was a reserved request.
  ASSERT_OK(block_mgr->GetNewBlock(client3, NULL, &block));
  ASSERT_TRUE(block == NULL);

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
