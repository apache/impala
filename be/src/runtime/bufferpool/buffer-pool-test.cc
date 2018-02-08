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

#include <cstdlib>
#include <limits>
#include <string>
#include <vector>
#include <boost/bind.hpp>
#include <boost/filesystem.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/thread.hpp>
#include <boost/unordered_map.hpp>

#include "codegen/llvm-codegen.h"
#include "common/atomic.h"
#include "common/init.h"
#include "common/object-pool.h"
#include "runtime/bufferpool/buffer-allocator.h"
#include "runtime/bufferpool/buffer-pool-internal.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/bufferpool/reservation-tracker.h"
#include "runtime/test-env.h"
#include "runtime/query-state.h"
#include "service/fe-support.h"
#include "testutil/cpu-util.h"
#include "testutil/death-test-util.h"
#include "testutil/gtest-util.h"
#include "testutil/rand-util.h"
#include "util/blocking-queue.h"
#include "util/filesystem-util.h"
#include "util/spinlock.h"
#include "util/metrics.h"

#include "common/names.h"

using boost::filesystem::directory_iterator;
using std::mt19937;
using std::uniform_int_distribution;
using std::uniform_real_distribution;

DECLARE_bool(disk_spill_encryption);

// Note: This is the default scratch dir created by impala.
// FLAGS_scratch_dirs + TmpFileMgr::TMP_SUB_DIR_NAME.
const string SCRATCH_DIR = "/tmp/impala-scratch";

// This suffix is appended to a tmp dir
const string SCRATCH_SUFFIX = "/impala-scratch";

namespace impala {

using BufferHandle = BufferPool::BufferHandle;
using ClientHandle = BufferPool::ClientHandle;
using FileGroup = TmpFileMgr::FileGroup;
using PageHandle = BufferPool::PageHandle;

class BufferPoolTest : public ::testing::Test {
 public:
  virtual void SetUp() {
    test_env_.reset(new TestEnv);
    ASSERT_OK(test_env_->Init());
    RandTestUtil::SeedRng("BUFFER_POOL_TEST_SEED", &rng_);
  }

  virtual void TearDown() {
    for (auto entry : query_reservations_) {
      ReservationTracker* tracker = entry.second;
      tracker->Close();
    }
    for (TmpFileMgr::FileGroup* file_group : file_groups_) {
      file_group->Close();
    }
    global_reservations_.Close();
    obj_pool_.Clear();

    // Tests modify permissions, so make sure we can delete if they didn't clean up.
    for (string created_tmp_dir : created_tmp_dirs_) {
      chmod((created_tmp_dir + SCRATCH_SUFFIX).c_str(), S_IRWXU);
    }
    ASSERT_OK(FileSystemUtil::RemovePaths(created_tmp_dirs_));
    created_tmp_dirs_.clear();
    CpuTestUtil::ResetAffinity(); // Some tests modify affinity.
  }

  /// The minimum buffer size used in most tests.
  const static int64_t TEST_BUFFER_LEN = 1024;

  /// Test helper to simulate registering then deregistering a number of queries with
  /// the given initial reservation and reservation limit. 'rng' is used to generate
  /// any random numbers needed.
  void RegisterQueriesAndClients(BufferPool* pool, int query_id_hi, int num_queries,
      int64_t initial_query_reservation, int64_t query_reservation_limit, mt19937* rng);

  /// Create and destroy a page multiple times.
  void CreatePageLoop(BufferPool* pool, TmpFileMgr::FileGroup* file_group,
      ReservationTracker* parent_tracker, int num_ops);

 protected:
  /// Reinitialize test_env_ to have multiple temporary directories.
  vector<string> InitMultipleTmpDirs(int num_dirs) {
    vector<string> tmp_dirs;
    for (int i = 0; i < num_dirs; ++i) {
      const string& dir = Substitute("/tmp/buffer-pool-test.$0", i);
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

  static int64_t QueryId(int hi, int lo) { return static_cast<int64_t>(hi) << 32 | lo; }

  /// Helper function to create one reservation tracker per query.
  ReservationTracker* GetQueryReservationTracker(int64_t query_id) {
    lock_guard<SpinLock> l(query_reservations_lock_);
    ReservationTracker* tracker = query_reservations_[query_id];
    if (tracker != NULL) return tracker;
    tracker = obj_pool_.Add(new ReservationTracker());
    query_reservations_[query_id] = tracker;
    return tracker;
  }

  RuntimeProfile* NewProfile() {
    return RuntimeProfile::Create(&obj_pool_, "test profile");
  }

  /// Create a new file group with the default configs.
  TmpFileMgr::FileGroup* NewFileGroup() {
    TmpFileMgr::FileGroup* file_group =
        obj_pool_.Add(new TmpFileMgr::FileGroup(test_env_->tmp_file_mgr(),
            test_env_->exec_env()->disk_io_mgr(), NewProfile(), TUniqueId()));
    file_groups_.push_back(file_group);
    return file_group;
  }

  // Helper to check if the page is evicted.
  bool IsEvicted(BufferPool::PageHandle* page) {
    lock_guard<SpinLock> pl(page->page_->buffer_lock);
    return !page->page_->buffer.is_open();
  }

  int NumEvicted(vector<BufferPool::PageHandle>& pages) {
    int num_evicted = 0;
    for (PageHandle& page : pages) {
      if (IsEvicted(&page)) ++num_evicted;
    }
    return num_evicted;
  }

  /// Allocate buffers of varying sizes at most 'max_buffer_size' that add up to
  /// 'total_bytes'. Both numbers must be a multiple of the minimum buffer size.
  /// If 'randomize_core' is true, will switch thread between cores randomly before
  /// each allocation.
  void AllocateBuffers(BufferPool* pool, BufferPool::ClientHandle* client,
      int64_t max_buffer_size, int64_t total_bytes,
      vector<BufferPool::BufferHandle>* buffers, bool randomize_core = false) {
    int64_t curr_buffer_size = max_buffer_size;
    int64_t bytes_remaining = total_bytes;
    while (bytes_remaining > 0) {
      while (curr_buffer_size > client->GetUnusedReservation()) curr_buffer_size /= 2;
      if (randomize_core) CpuTestUtil::PinToRandomCore(&rng_);
      buffers->emplace_back();
      ASSERT_OK(pool->AllocateBuffer(client, curr_buffer_size, &buffers->back()));
      bytes_remaining -= curr_buffer_size;
    }
  }

  /// Do a temporary test allocation. Return the status of AllocateBuffer().
  Status AllocateAndFree(BufferPool* pool, ClientHandle* client, int64_t len) {
    BufferHandle tmp;
    RETURN_IF_ERROR(pool->AllocateBuffer(client, len, &tmp));
    pool->FreeBuffer(client, &tmp);
    return Status::OK();
  }

  /// Create pages of varying sizes at most 'max_page_size' that add up to
  /// 'total_bytes'. Both numbers must be a multiple of the minimum buffer size.
  /// If 'randomize_core' is true, will switch thread between cores randomly before
  /// each allocation.
  void CreatePages(BufferPool* pool, BufferPool::ClientHandle* client,
      int64_t max_page_size, int64_t total_bytes, vector<BufferPool::PageHandle>* pages,
      bool randomize_core = false) {
    ASSERT_GE(client->GetUnusedReservation(), total_bytes);
    int64_t curr_page_size = max_page_size;
    int64_t bytes_remaining = total_bytes;
    while (bytes_remaining > 0) {
      while (curr_page_size > client->GetUnusedReservation()) curr_page_size /= 2;
      pages->emplace_back();
      if (randomize_core) CpuTestUtil::PinToRandomCore(&rng_);
      ASSERT_OK(pool->CreatePage(client, curr_page_size, &pages->back()));
      bytes_remaining -= curr_page_size;
    }
  }

  /// Free all the 'buffers' and clear the vector.
  /// If 'randomize_core' is true, will switch thread between cores randomly before
  /// each free.
  void FreeBuffers(BufferPool* pool, BufferPool::ClientHandle* client,
      vector<BufferPool::BufferHandle>* buffers, bool randomize_core = false) {
    for (auto& buffer : *buffers) {
      if (randomize_core) CpuTestUtil::PinToRandomCore(&rng_);
      pool->FreeBuffer(client, &buffer);
    }
    buffers->clear();
  }

  Status PinAll(BufferPool* pool, ClientHandle* client, vector<PageHandle>* pages) {
    for (auto& page : *pages) RETURN_IF_ERROR(pool->Pin(client, &page));
    return Status::OK();
  }

  /// Unpin all of 'pages'. If 'delay_between_unpins_ms' > 0, sleep between unpins.
  void UnpinAll(BufferPool* pool, ClientHandle* client, vector<PageHandle>* pages,
      int delay_between_unpins_ms = 0) {
    for (auto& page : *pages) {
      pool->Unpin(client, &page);
      if (delay_between_unpins_ms > 0) SleepForMs(delay_between_unpins_ms);
    }
  }

  void DestroyAll(BufferPool* pool, ClientHandle* client, vector<PageHandle>* pages) {
    for (auto& page : *pages) pool->DestroyPage(client, &page);
  }

  /// Write some deterministically-generated sentinel values to pages or buffers. The same
  /// data is written each time for objects[i], based on start_num + i.
  template <typename T>
  void WriteData(const vector<T>& objects, int start_num) {
    WriteOrVerifyData(objects, start_num, true);
  }

  template <typename T>
  void WriteData(const T& object, int val) {
    return WriteOrVerifyData(object, val, true);
  }

  /// Verify data written by WriteData().
  template <typename T>
  void VerifyData(const vector<T>& objects, int start_num) {
    WriteOrVerifyData(objects, start_num, false);
  }

  template <typename T>
  void VerifyData(const T& object, int val) {
    return WriteOrVerifyData(object, val, false);
  }

  /// Implemention of WriteData() and VerifyData().
  template <typename T>
  void WriteOrVerifyData(const vector<T>& objects, int start_num, bool write) {
    for (int i = 0; i < objects.size(); ++i) {
      WriteOrVerifyData(objects[i], i + start_num, write);
    }
  }

  template <typename T>
  void WriteOrVerifyData(const T& object, int val, bool write) {
    // Only write sentinel values to start and end of buffer to make writing and
    // verification cheap.
    MemRange mem = GetMemRange(object);
    uint64_t* start_word = reinterpret_cast<uint64_t*>(mem.data());
    uint64_t* end_word =
        reinterpret_cast<uint64_t*>(&mem.data()[mem.len() - sizeof(uint64_t)]);
    if (write) {
      *start_word = val;
      *end_word = ~val;
    } else {
      EXPECT_EQ(*start_word, val);
      EXPECT_EQ(*end_word, ~val);
    }
  }

  MemRange GetMemRange(const BufferHandle& buffer) { return buffer.mem_range(); }

  MemRange GetMemRange(const PageHandle& page) {
    const BufferHandle* buffer;
    EXPECT_OK(page.GetBuffer(&buffer));
    return buffer->mem_range();
  }

  /// Set the maximum number of scavenge attempts that the pool's allocator wil do.
  void SetMaxScavengeAttempts(BufferPool* pool, int max_attempts) {
    pool->allocator()->set_max_scavenge_attempts(max_attempts);
  }

  void WaitForAllWrites(ClientHandle* client) { client->impl_->WaitForAllWrites(); }

  // Remove write permissions on scratch files. Return # of scratch files.
  static int RemoveScratchPerms() {
    int num_files = 0;
    directory_iterator dir_it(SCRATCH_DIR);
    for (; dir_it != directory_iterator(); ++dir_it) {
      ++num_files;
      EXPECT_EQ(0, chmod(dir_it->path().c_str(), 0));
    }
    return num_files;
  }

  // Remove permissions for the temporary file at 'path' - all subsequent writes
  // to the file should fail. Expects backing file has already been allocated.
  static void DisableBackingFile(const string& path) {
    EXPECT_GT(path.size(), 0);
    EXPECT_EQ(0, chmod(path.c_str(), 0));
    LOG(INFO) << "Injected fault by removing file permissions " << path;
  }

  /// Write out a bunch of nonsense to replace the file's current data.
  static void CorruptBackingFile(const string& path) {
    EXPECT_GT(path.size(), 0);
    FILE* file = fopen(path.c_str(), "rb+");
    EXPECT_EQ(0, fseek(file, 0, SEEK_END));
    int64_t size = ftell(file);
    EXPECT_EQ(0, fseek(file, 0, SEEK_SET));
    for (int64_t i = 0; i < size; ++i) fputc(123, file);
    fclose(file);
    LOG(INFO) << "Injected fault by corrupting file " << path;
  }

  /// Truncate the file to 0 bytes.
  static void TruncateBackingFile(const string& path) {
    EXPECT_GT(path.size(), 0);
    EXPECT_EQ(0, truncate(path.c_str(), 0));
    LOG(INFO) << "Injected fault by truncating file " << path;
  }

  // Return whether a pin is in flight for the page.
  static bool PinInFlight(PageHandle* page) {
    return page->page_->pin_in_flight;
  }

  // Return the path of the temporary file backing the page.
  static string TmpFilePath(PageHandle* page) {
    return page->page_->write_handle->TmpFilePath();
  }
  // Check that the file backing the page has dir as a prefix of its path.
  static bool PageInDir(PageHandle* page, const string& dir) {
    return TmpFilePath(page).find(dir) == 0;
  }

  // Find a page in the list that is backed by a file with the given directory as prefix
  // of its path.
  static PageHandle* FindPageInDir(vector<PageHandle>& pages, const string& dir) {
    for (PageHandle& page : pages) {
      if (PageInDir(&page, dir)) return &page;
    }
    return NULL;
  }

  /// Parameterised test implementations.
  void TestBufferAllocation(bool reserved);
  void TestMemoryReclamation(BufferPool* pool, int src_core, int dst_core);
  void TestEvictionPolicy(int64_t page_size);
  void TestCleanPageLimit(int max_clean_pages, bool randomize_core);
  void TestQueryTeardown(bool write_error);
  void TestWriteError(int write_delay_ms);
  void TestRandomInternalSingle(int64_t buffer_len, bool multiple_pins);
  void TestRandomInternalMulti(int num_threads, int64_t buffer_len, bool multiple_pins);
  static const int SINGLE_THREADED_TID = -1;
  void TestRandomInternalImpl(BufferPool* pool, FileGroup* file_group,
      MemTracker* parent_mem_tracker, mt19937* rng, int tid, bool multiple_pins);

  ObjectPool obj_pool_;
  ReservationTracker global_reservations_;

  boost::scoped_ptr<TestEnv> test_env_;

  /// Per-test random number generator. Seeded before every test.
  mt19937 rng_;

  /// The file groups created - closed at end of each test.
  vector<TmpFileMgr::FileGroup*> file_groups_;

  /// Paths of temporary directories created during tests - deleted at end of test.
  vector<string> created_tmp_dirs_;

  /// Map from query_id to the reservation tracker for that query. Reads and modifications
  /// of the map are protected by query_reservations_lock_.
  unordered_map<int64_t, ReservationTracker*> query_reservations_;
  SpinLock query_reservations_lock_;
};

const int64_t BufferPoolTest::TEST_BUFFER_LEN;

void BufferPoolTest::RegisterQueriesAndClients(BufferPool* pool, int query_id_hi,
    int num_queries, int64_t initial_query_reservation, int64_t query_reservation_limit,
    mt19937* rng) {
  Status status;

  int clients_per_query = 32;
  BufferPool::ClientHandle* clients[num_queries];

  for (int i = 0; i < num_queries; ++i) {
    int64_t query_id = QueryId(query_id_hi, i);

    // Initialize a tracker for a new query.
    ReservationTracker* query_reservation = GetQueryReservationTracker(query_id);
    query_reservation->InitChildTracker(
        NULL, &global_reservations_, NULL, query_reservation_limit);

    // Test that closing then reopening child tracker works.
    query_reservation->Close();
    query_reservation->InitChildTracker(
        NULL, &global_reservations_, NULL, query_reservation_limit);
    EXPECT_TRUE(query_reservation->IncreaseReservationToFit(initial_query_reservation));

    clients[i] = new BufferPool::ClientHandle[clients_per_query];

    for (int j = 0; j < clients_per_query; ++j) {
      int64_t initial_client_reservation =
          initial_query_reservation / clients_per_query + j
          < initial_query_reservation % clients_per_query;
      // Reservation limit can be anything greater or equal to the initial reservation.
      int64_t client_reservation_limit = initial_client_reservation + (*rng)() % 100000;
      string name = Substitute("Client $0 for query $1", j, query_id);
      EXPECT_OK(pool->RegisterClient(name, NULL, query_reservation, NULL,
          client_reservation_limit, NewProfile(), &clients[i][j]));
      EXPECT_TRUE(clients[i][j].IncreaseReservationToFit(initial_client_reservation));
    }

    for (int j = 0; j < clients_per_query; ++j) {
      ASSERT_TRUE(clients[i][j].is_registered());
    }
  }

  // Deregister clients then query.
  for (int i = 0; i < num_queries; ++i) {
    for (int j = 0; j < clients_per_query; ++j) {
      pool->DeregisterClient(&clients[i][j]);
      ASSERT_FALSE(clients[i][j].is_registered());
    }

    delete[] clients[i];

    GetQueryReservationTracker(QueryId(query_id_hi, i))->Close();
  }
}

/// Test that queries and clients can be registered and deregistered with the reservation
/// trackers and the buffer pool.
TEST_F(BufferPoolTest, BasicRegistration) {
  int num_concurrent_queries = 1024;
  int64_t sum_initial_reservations = 4;
  int64_t reservation_limit = 1024;
  // Need enough buffers for all initial reservations.
  int64_t total_mem = sum_initial_reservations * num_concurrent_queries;
  global_reservations_.InitRootTracker(NewProfile(), total_mem);

  BufferPool pool(TEST_BUFFER_LEN, total_mem, total_mem);

  RegisterQueriesAndClients(&pool, 0, num_concurrent_queries, sum_initial_reservations,
      reservation_limit, &rng_);

  ASSERT_EQ(global_reservations_.GetUsedReservation(), 0);
  ASSERT_EQ(global_reservations_.GetChildReservations(), 0);
  ASSERT_EQ(global_reservations_.GetReservation(), 0);
  global_reservations_.Close();
}

/// Test that queries and clients can be registered and deregistered by concurrent
/// threads.
TEST_F(BufferPoolTest, ConcurrentRegistration) {
  int queries_per_thread = 64;
  int num_threads = 64;
  int num_concurrent_queries = queries_per_thread * num_threads;
  int64_t sum_initial_reservations = 4;
  int64_t reservation_limit = 1024;
  // Need enough buffers for all initial reservations.
  int64_t total_mem = num_concurrent_queries * sum_initial_reservations;
  global_reservations_.InitRootTracker(NewProfile(), total_mem);

  BufferPool pool(TEST_BUFFER_LEN, total_mem, total_mem);
  vector<mt19937> thread_rngs = RandTestUtil::CreateThreadLocalRngs(num_threads, &rng_);
  // Launch threads, each with a different set of query IDs.
  thread_group workers;
  for (int i = 0; i < num_threads; ++i) {
    workers.add_thread(new thread(bind(&BufferPoolTest::RegisterQueriesAndClients, this,
        &pool, i, queries_per_thread, sum_initial_reservations, reservation_limit,
        &thread_rngs[i])));
  }
  workers.join_all();

  // All the reservations should be released at this point.
  ASSERT_EQ(global_reservations_.GetUsedReservation(), 0);
  ASSERT_EQ(global_reservations_.GetReservation(), 0);
  global_reservations_.Close();
}

/// Test basic page handle creation.
TEST_F(BufferPoolTest, PageCreation) {
  // Allocate many pages, each a power-of-two multiple of the minimum page length.
  int num_pages = 16;
  int64_t max_page_len = TEST_BUFFER_LEN << (num_pages - 1);
  int64_t total_mem = 2 * 2 * max_page_len;
  global_reservations_.InitRootTracker(NULL, total_mem);
  BufferPool pool(TEST_BUFFER_LEN, total_mem, total_mem);
  BufferPool::ClientHandle client;
  ASSERT_OK(pool.RegisterClient("test client", NULL, &global_reservations_, NULL,
      total_mem, NewProfile(), &client));
  ASSERT_TRUE(client.IncreaseReservation(total_mem));

  vector<BufferPool::PageHandle> handles(num_pages);

  // Create pages of various valid sizes.
  for (int i = 0; i < num_pages; ++i) {
    int size_multiple = 1 << i;
    int64_t page_len = TEST_BUFFER_LEN * size_multiple;
    int64_t used_before = client.GetUsedReservation();
    ASSERT_OK(pool.CreatePage(&client, page_len, &handles[i]));
    ASSERT_TRUE(handles[i].is_open());
    ASSERT_TRUE(handles[i].is_pinned());
    const BufferHandle* buffer;
    ASSERT_OK(handles[i].GetBuffer(&buffer));
    ASSERT_TRUE(buffer->data() != NULL);
    ASSERT_EQ(handles[i].len(), page_len);
    ASSERT_EQ(buffer->len(), page_len);
    ASSERT_EQ(client.GetUsedReservation(), used_before + page_len);
  }

  // Close the handles and check memory consumption.
  for (int i = 0; i < num_pages; ++i) {
    int64_t used_before = client.GetUsedReservation();
    int page_len = handles[i].len();
    pool.DestroyPage(&client, &handles[i]);
    ASSERT_EQ(client.GetUsedReservation(), used_before - page_len);
  }

  pool.DeregisterClient(&client);

  // All the reservations should be released at this point.
  ASSERT_EQ(global_reservations_.GetReservation(), 0);
  global_reservations_.Close();
}

TEST_F(BufferPoolTest, ReservedBufferAllocation) {
  TestBufferAllocation(true);
}

TEST_F(BufferPoolTest, UnreservedBufferAllocation) {
  TestBufferAllocation(false);
}

void BufferPoolTest::TestBufferAllocation(bool reserved) {
  // Allocate many buffers, each a power-of-two multiple of the minimum buffer length.
  const int NUM_BUFFERS = 16;
  const int64_t MAX_BUFFER_LEN = TEST_BUFFER_LEN << (NUM_BUFFERS - 1);

  // Total memory required to allocate TEST_BUFFER_LEN, 2*TEST_BUFFER_LEN, ...,
  // MAX_BUFFER_LEN.
  const int64_t TOTAL_MEM = 2 * MAX_BUFFER_LEN - TEST_BUFFER_LEN;
  global_reservations_.InitRootTracker(NULL, TOTAL_MEM);
  BufferPool pool(TEST_BUFFER_LEN, TOTAL_MEM, TOTAL_MEM);
  BufferPool::ClientHandle client;
  ASSERT_OK(pool.RegisterClient("test client", NULL, &global_reservations_, NULL,
      TOTAL_MEM, NewProfile(), &client));
  if (reserved) ASSERT_TRUE(client.IncreaseReservationToFit(TOTAL_MEM));

  vector<BufferPool::BufferHandle> handles(NUM_BUFFERS);

  // Create buffers of various valid sizes.
  int64_t total_allocated = 0;
  for (int i = 0; i < NUM_BUFFERS; ++i) {
    int size_multiple = 1 << i;
    int64_t buffer_len = TEST_BUFFER_LEN * size_multiple;
    int64_t used_before = client.GetUsedReservation();
    if (reserved) {
      ASSERT_OK(pool.AllocateBuffer(&client, buffer_len, &handles[i]));
    } else {
      // Reservation should be automatically increased.
      ASSERT_OK(pool.AllocateUnreservedBuffer(&client, buffer_len, &handles[i]));
    }
    total_allocated += buffer_len;
    ASSERT_TRUE(handles[i].is_open());
    ASSERT_TRUE(handles[i].data() != NULL);
    ASSERT_EQ(handles[i].len(), buffer_len);
    ASSERT_EQ(client.GetUsedReservation(), used_before + buffer_len);

    // Check that pool-wide values are updated correctly.
    EXPECT_EQ(total_allocated, pool.GetSystemBytesAllocated());
    EXPECT_EQ(0, pool.GetNumFreeBuffers());
    EXPECT_EQ(0, pool.GetFreeBufferBytes());
  }

  if (!reserved) {
    // Allocate all of the memory and test the failure path for unreserved allocations.
    BufferPool::BufferHandle tmp_handle;
    ASSERT_OK(pool.AllocateUnreservedBuffer(&client, TEST_BUFFER_LEN, &tmp_handle));
    ASSERT_FALSE(tmp_handle.is_open()) << "No reservation for buffer";
  }

  // Close the handles and check memory consumption.
  for (int i = 0; i < NUM_BUFFERS; ++i) {
    int64_t used_before = client.GetUsedReservation();
    int buffer_len = handles[i].len();
    pool.FreeBuffer(&client, &handles[i]);
    ASSERT_EQ(client.GetUsedReservation(), used_before - buffer_len);
  }

  pool.DeregisterClient(&client);

  // All the reservations should be released at this point.
  ASSERT_EQ(global_reservations_.GetReservation(), 0);
  // But freed memory is not released to the system immediately.
  EXPECT_EQ(total_allocated, pool.GetSystemBytesAllocated());
  EXPECT_EQ(NUM_BUFFERS, pool.GetNumFreeBuffers());
  EXPECT_EQ(total_allocated, pool.GetFreeBufferBytes());
  global_reservations_.Close();
}

// Test that the buffer pool correctly reports the number of clean pages.
TEST_F(BufferPoolTest, CleanPageStats) {
  const int MAX_NUM_BUFFERS = 4;
  const int64_t TOTAL_MEM = MAX_NUM_BUFFERS * TEST_BUFFER_LEN;
  global_reservations_.InitRootTracker(NewProfile(), TOTAL_MEM);
  BufferPool pool(TEST_BUFFER_LEN, TOTAL_MEM, TOTAL_MEM);

  ClientHandle client;
  ASSERT_OK(pool.RegisterClient("test client", NewFileGroup(), &global_reservations_,
      nullptr, TOTAL_MEM, NewProfile(), &client));
  ASSERT_TRUE(client.IncreaseReservation(TOTAL_MEM));

  vector<PageHandle> pages;
  CreatePages(&pool, &client, TEST_BUFFER_LEN, TOTAL_MEM, &pages);
  WriteData(pages, 0);
  EXPECT_FALSE(client.has_unpinned_pages());

  // Pages don't start off clean.
  EXPECT_EQ(0, pool.GetNumCleanPages());
  EXPECT_EQ(0, pool.GetCleanPageBytes());

  // Unpin pages and wait until they're written out and therefore clean.
  UnpinAll(&pool, &client, &pages);
  EXPECT_TRUE(client.has_unpinned_pages());
  WaitForAllWrites(&client);
  EXPECT_EQ(MAX_NUM_BUFFERS, pool.GetNumCleanPages());
  EXPECT_EQ(TOTAL_MEM, pool.GetCleanPageBytes());
  EXPECT_TRUE(client.has_unpinned_pages());

  // Do an allocation to force eviction of one page.
  ASSERT_OK(AllocateAndFree(&pool, &client, TEST_BUFFER_LEN));
  EXPECT_EQ(MAX_NUM_BUFFERS - 1, pool.GetNumCleanPages());
  EXPECT_EQ(TOTAL_MEM - TEST_BUFFER_LEN, pool.GetCleanPageBytes());
  EXPECT_TRUE(client.has_unpinned_pages());

  // Re-pin all the pages - none will be clean afterwards.
  ASSERT_OK(PinAll(&pool, &client, &pages));
  VerifyData(pages, 0);
  EXPECT_EQ(0, pool.GetNumCleanPages());
  EXPECT_EQ(0, pool.GetCleanPageBytes());
  EXPECT_FALSE(client.has_unpinned_pages());

  DestroyAll(&pool, &client, &pages);
  EXPECT_FALSE(client.has_unpinned_pages());
  pool.DeregisterClient(&client);
  global_reservations_.Close();
}

/// Test that the buffer pool respects the clean page limit with all pages in
/// the same arena.
TEST_F(BufferPoolTest, CleanPageLimitOneArena) {
  TestCleanPageLimit(0, false);
  TestCleanPageLimit(2, false);
  TestCleanPageLimit(4, false);
}

/// Test that the buffer pool respects the clean page limit with pages spread across
/// different arenas.
TEST_F(BufferPoolTest, CleanPageLimitRandomArenas) {
  TestCleanPageLimit(0, true);
  TestCleanPageLimit(2, true);
  TestCleanPageLimit(4, true);
}

void BufferPoolTest::TestCleanPageLimit(int max_clean_pages, bool randomize_core) {
  const int MAX_NUM_BUFFERS = 4;
  const int64_t TOTAL_MEM = MAX_NUM_BUFFERS * TEST_BUFFER_LEN;
  const int max_clean_page_bytes = max_clean_pages * TEST_BUFFER_LEN;
  global_reservations_.InitRootTracker(NewProfile(), TOTAL_MEM);
  BufferPool pool(TEST_BUFFER_LEN, TOTAL_MEM, max_clean_page_bytes);

  ClientHandle client;
  ASSERT_OK(pool.RegisterClient("test client", NewFileGroup(), &global_reservations_,
      nullptr, TOTAL_MEM, NewProfile(), &client));
  ASSERT_TRUE(client.IncreaseReservation(TOTAL_MEM));
  if (!randomize_core) CpuTestUtil::PinToCore(0);
  vector<PageHandle> pages;
  CreatePages(&pool, &client, TEST_BUFFER_LEN, TOTAL_MEM, &pages, randomize_core);
  WriteData(pages, 0);

  // Unpin pages and wait until they're written out and therefore clean.
  UnpinAll(&pool, &client, &pages);
  WaitForAllWrites(&client);
  EXPECT_EQ(max_clean_pages, pool.GetNumCleanPages());
  EXPECT_EQ(max_clean_page_bytes, pool.GetCleanPageBytes());

  // Do an allocation to force a buffer to be reclaimed from somewhere.
  ASSERT_OK(AllocateAndFree(&pool, &client, TEST_BUFFER_LEN));
  if (randomize_core) {
    // We will either evict a clean page or reclaim a free buffer, depending on the
    // arena that we pick.
    EXPECT_LE(pool.GetNumCleanPages(), max_clean_pages);
    EXPECT_LE(pool.GetCleanPageBytes(), max_clean_page_bytes);
  } else {
    // We will reclaim one of the free buffers in arena 0.
    EXPECT_EQ(min(MAX_NUM_BUFFERS - 1, max_clean_pages), pool.GetNumCleanPages());
    const int64_t expected_clean_page_bytes =
        min<int64_t>((MAX_NUM_BUFFERS - 1) * TEST_BUFFER_LEN, max_clean_page_bytes);
    EXPECT_EQ(expected_clean_page_bytes, pool.GetCleanPageBytes());
  }

  // Re-pin all the pages - none will be clean afterwards.
  ASSERT_OK(PinAll(&pool, &client, &pages));
  VerifyData(pages, 0);
  EXPECT_EQ(0, pool.GetNumCleanPages());
  EXPECT_EQ(0, pool.GetCleanPageBytes());

  DestroyAll(&pool, &client, &pages);
  pool.DeregisterClient(&client);
  global_reservations_.Close();
}

/// Test transfer of buffer handles between clients.
TEST_F(BufferPoolTest, BufferTransfer) {
  // Each client needs to have enough reservation for a buffer.
  const int num_clients = 5;
  int64_t total_mem = num_clients * TEST_BUFFER_LEN;
  global_reservations_.InitRootTracker(NULL, total_mem);
  BufferPool pool(TEST_BUFFER_LEN, total_mem, total_mem);
  BufferPool::ClientHandle clients[num_clients];
  BufferPool::BufferHandle handles[num_clients];
  for (int i = 0; i < num_clients; ++i) {
    ASSERT_OK(pool.RegisterClient("test client", NULL, &global_reservations_, NULL,
        TEST_BUFFER_LEN, NewProfile(), &clients[i]));
    ASSERT_TRUE(clients[i].IncreaseReservationToFit(TEST_BUFFER_LEN));
  }

  // Transfer the page around between the clients repeatedly in a circle.
  ASSERT_OK(pool.AllocateBuffer(&clients[0], TEST_BUFFER_LEN, &handles[0]));
  uint8_t* data = handles[0].data();
  for (int iter = 0; iter < 10; ++iter) {
    for (int client = 0; client < num_clients; ++client) {
      int next_client = (client + 1) % num_clients;
      ASSERT_OK(pool.TransferBuffer(&clients[client], &handles[client],
          &clients[next_client], &handles[next_client]));
      // Check that the transfer left things in a consistent state.
      ASSERT_FALSE(handles[client].is_open());
      ASSERT_EQ(0, clients[client].GetUsedReservation());
      ASSERT_TRUE(handles[next_client].is_open());
      ASSERT_EQ(TEST_BUFFER_LEN, clients[next_client].GetUsedReservation());
      // The same underlying buffer should be used.
      ASSERT_EQ(data, handles[next_client].data());
    }
  }

  pool.FreeBuffer(&clients[0], &handles[0]);
  for (BufferPool::ClientHandle& client : clients) pool.DeregisterClient(&client);
  ASSERT_EQ(global_reservations_.GetReservation(), 0);
  global_reservations_.Close();
}

TEST_F(BufferPoolTest, BufferTransferConcurrent) {
  // Transfer buffers between threads in a circular fashion. Each client needs to have
  // enough reservation for two buffers, since it may receive a buffer before handing
  // off the next one.
  const int NUM_CLIENTS = 5;
  const int64_t TOTAL_MEM = NUM_CLIENTS * TEST_BUFFER_LEN * 2;
  global_reservations_.InitRootTracker(NULL, TOTAL_MEM);
  BufferPool pool(TEST_BUFFER_LEN, TOTAL_MEM, TOTAL_MEM);

  BufferPool::ClientHandle clients[NUM_CLIENTS];
  BufferPool::BufferHandle handles[NUM_CLIENTS];
  SpinLock locks[NUM_CLIENTS]; // Each lock protects the corresponding BufferHandle.
  for (int i = 0; i < NUM_CLIENTS; ++i) {
    ASSERT_OK(pool.RegisterClient("test client", NULL, &global_reservations_, NULL,
        TOTAL_MEM, NewProfile(), &clients[i]));
    ASSERT_TRUE(clients[i].IncreaseReservationToFit(2 * TEST_BUFFER_LEN));
  }

  thread_group workers;

  for (int thread_idx = 0; thread_idx < NUM_CLIENTS; ++thread_idx) {
    workers.add_thread(new thread([&pool, &clients, &handles, &locks, thread_idx] {
      // Transfer buffers around between the clients repeatedly in a circle.
      BufferHandle handle;
      {
        lock_guard<SpinLock> l(locks[thread_idx]);
        LOG(INFO) << "Allocate from " << (void*)&clients[thread_idx];
        ASSERT_OK(pool.AllocateBuffer(
              &clients[thread_idx], TEST_BUFFER_LEN, &handle));
      }
      for (int iter = 0; iter < 100; ++iter) {
        int next_thread_idx = (thread_idx + 1) % NUM_CLIENTS;
        // Transfer our buffer to the next thread.
        {
          unique_lock<SpinLock> l(locks[next_thread_idx]);
          // Spin until we can add the handle.
          while (true) {
            if (!handles[next_thread_idx].is_open()) break;
            l.unlock();
            sched_yield();
            l.lock();
          }
          ASSERT_TRUE(handle.is_open());
          ASSERT_OK(pool.TransferBuffer(&clients[thread_idx], &handle,
              &clients[next_thread_idx], &handles[next_thread_idx]));
          // Check that the transfer left things in a consistent state.
          ASSERT_TRUE(handles[next_thread_idx].is_open());
          ASSERT_FALSE(handle.is_open());
          ASSERT_GE(clients[next_thread_idx].GetUsedReservation(), TEST_BUFFER_LEN);
        }
        // Get a new buffer from the previous thread.
        {
          unique_lock<SpinLock> l(locks[thread_idx]);
          // Spin until we receive a handle from the previous thread.
          while (true) {
            if (handles[thread_idx].is_open()) break;
            l.unlock();
            sched_yield();
            l.lock();
          }
          handle = move(handles[thread_idx]);
        }
      }
      pool.FreeBuffer(&clients[thread_idx], &handle);
      }));
  }
  workers.join_all();
  for (BufferPool::ClientHandle& client : clients) pool.DeregisterClient(&client);
  ASSERT_EQ(global_reservations_.GetReservation(), 0);
  global_reservations_.Close();
}

/// Test basic pinning and unpinning.
TEST_F(BufferPoolTest, Pin) {
  int64_t total_mem = TEST_BUFFER_LEN * 1024;
  // Set up client with enough reservation to pin twice.
  int64_t child_reservation = TEST_BUFFER_LEN * 2;
  BufferPool pool(TEST_BUFFER_LEN, total_mem, total_mem);
  global_reservations_.InitRootTracker(NULL, total_mem);
  BufferPool::ClientHandle client;
  ASSERT_OK(pool.RegisterClient("test client", NewFileGroup(), &global_reservations_,
      NULL, child_reservation, NewProfile(), &client));
  ASSERT_TRUE(client.IncreaseReservationToFit(child_reservation));

  BufferPool::PageHandle handle1, handle2;

  // Can pin two minimum sized pages.
  const BufferHandle* page_buffer;
  ASSERT_OK(pool.CreatePage(&client, TEST_BUFFER_LEN, &handle1, &page_buffer));
  ASSERT_TRUE(handle1.is_open());
  ASSERT_TRUE(handle1.is_pinned());
  ASSERT_TRUE(page_buffer->data() != NULL);
  ASSERT_OK(pool.CreatePage(&client, TEST_BUFFER_LEN, &handle2, &page_buffer));
  ASSERT_TRUE(handle2.is_open());
  ASSERT_TRUE(handle2.is_pinned());
  ASSERT_TRUE(page_buffer->data() != NULL);

  pool.Unpin(&client, &handle2);
  ASSERT_FALSE(handle2.is_pinned());

  // Can pin minimum-sized page twice.
  ASSERT_OK(pool.Pin(&client, &handle1));
  ASSERT_TRUE(handle1.is_pinned());
  // Have to unpin twice.
  pool.Unpin(&client, &handle1);
  ASSERT_TRUE(handle1.is_pinned());
  pool.Unpin(&client, &handle1);
  ASSERT_FALSE(handle1.is_pinned());

  // Can pin double-sized page only once.
  BufferPool::PageHandle double_handle;
  ASSERT_OK(pool.CreatePage(&client, TEST_BUFFER_LEN * 2, &double_handle, &page_buffer));
  ASSERT_TRUE(double_handle.is_open());
  ASSERT_TRUE(double_handle.is_pinned());
  ASSERT_TRUE(page_buffer->data() != NULL);

  // Destroy the pages - test destroying both pinned and unpinned.
  pool.DestroyPage(&client, &handle1);
  pool.DestroyPage(&client, &handle2);
  pool.DestroyPage(&client, &double_handle);

  pool.DeregisterClient(&client);
}

// Test the various state transitions possible with async Pin() calls.
TEST_F(BufferPoolTest, AsyncPin) {
  const int DATA_SEED = 1234;
  // Set up pool with enough reservation to keep two buffers in memory.
  const int64_t TOTAL_MEM = 2 * TEST_BUFFER_LEN;
  BufferPool pool(TEST_BUFFER_LEN, TOTAL_MEM, TOTAL_MEM);
  global_reservations_.InitRootTracker(NULL, TOTAL_MEM);
  BufferPool::ClientHandle client;
  ASSERT_OK(pool.RegisterClient("test client", NewFileGroup(), &global_reservations_,
      NULL, TOTAL_MEM, NewProfile(), &client));
  ASSERT_TRUE(client.IncreaseReservationToFit(TOTAL_MEM));

  PageHandle handle;
  const BufferHandle* buffer;
  ASSERT_OK(pool.CreatePage(&client, TEST_BUFFER_LEN, &handle, &buffer));
  WriteData(*buffer, DATA_SEED);
  // Pin() on a pinned page just increments the pin count.
  ASSERT_OK(pool.Pin(&client, &handle));
  EXPECT_EQ(2, handle.pin_count());
  EXPECT_FALSE(PinInFlight(&handle));

  pool.Unpin(&client, &handle);
  pool.Unpin(&client, &handle);
  ASSERT_FALSE(handle.is_pinned());

  // Calling Pin() then Pin() results in double-pinning.
  ASSERT_OK(pool.Pin(&client, &handle));
  ASSERT_OK(pool.Pin(&client, &handle));
  EXPECT_EQ(2, handle.pin_count());
  EXPECT_FALSE(PinInFlight(&handle));

  pool.Unpin(&client, &handle);
  pool.Unpin(&client, &handle);
  ASSERT_FALSE(handle.is_pinned());

  // Pin() on a page that isn't evicted pins it immediately.
  ASSERT_OK(pool.Pin(&client, &handle));
  EXPECT_EQ(1, handle.pin_count());
  EXPECT_FALSE(PinInFlight(&handle));
  VerifyData(handle, 1234);
  pool.Unpin(&client, &handle);
  ASSERT_FALSE(handle.is_pinned());

  // Force eviction. Pin() on an evicted page starts the write asynchronously.
  ASSERT_OK(AllocateAndFree(&pool, &client, TOTAL_MEM));
  ASSERT_OK(pool.Pin(&client, &handle));
  EXPECT_EQ(1, handle.pin_count());
  EXPECT_TRUE(PinInFlight(&handle));
  // Block on the pin and verify the buffer.
  ASSERT_OK(handle.GetBuffer(&buffer));
  EXPECT_FALSE(PinInFlight(&handle));
  VerifyData(*buffer, 1234);

  // Test that we can unpin while in flight and the data remains valid.
  pool.Unpin(&client, &handle);
  ASSERT_OK(AllocateAndFree(&pool, &client, TOTAL_MEM));
  ASSERT_OK(pool.Pin(&client, &handle));
  EXPECT_TRUE(PinInFlight(&handle));
  pool.Unpin(&client, &handle);
  ASSERT_OK(pool.Pin(&client, &handle));
  ASSERT_OK(handle.GetBuffer(&buffer));
  VerifyData(*buffer, 1234);

  // Evict the page, then destroy while we're pinning it asynchronously.
  pool.Unpin(&client, &handle);
  ASSERT_OK(AllocateAndFree(&pool, &client, TOTAL_MEM));
  ASSERT_OK(pool.Pin(&client, &handle));
  pool.DestroyPage(&client, &handle);

  pool.DeregisterClient(&client);
}

/// Creating a page or pinning without sufficient reservation should DCHECK.
TEST_F(BufferPoolTest, PinWithoutReservation) {
  int64_t total_mem = TEST_BUFFER_LEN * 1024;
  BufferPool pool(TEST_BUFFER_LEN, total_mem, total_mem);
  global_reservations_.InitRootTracker(NULL, total_mem);
  BufferPool::ClientHandle client;
  ASSERT_OK(pool.RegisterClient("test client", NULL, &global_reservations_, NULL,
      TEST_BUFFER_LEN, NewProfile(), &client));

  BufferPool::PageHandle handle;
  IMPALA_ASSERT_DEBUG_DEATH(
      discard_result(pool.CreatePage(&client, TEST_BUFFER_LEN, &handle)), "");

  // Should succeed after increasing reservation.
  ASSERT_TRUE(client.IncreaseReservationToFit(TEST_BUFFER_LEN));
  ASSERT_OK(pool.CreatePage(&client, TEST_BUFFER_LEN, &handle));

  // But we can't pin again.
  IMPALA_ASSERT_DEBUG_DEATH(discard_result(pool.Pin(&client, &handle)), "");

  pool.DestroyPage(&client, &handle);
  pool.DeregisterClient(&client);
}

TEST_F(BufferPoolTest, ExtractBuffer) {
  int64_t total_mem = TEST_BUFFER_LEN * 1024;
  // Set up client with enough reservation for two buffers/pins.
  int64_t child_reservation = TEST_BUFFER_LEN * 2;
  BufferPool pool(TEST_BUFFER_LEN, total_mem, total_mem);
  global_reservations_.InitRootTracker(NULL, total_mem);
  BufferPool::ClientHandle client;
  ASSERT_OK(pool.RegisterClient("test client", NewFileGroup(), &global_reservations_,
      NULL, child_reservation, NewProfile(), &client));
  ASSERT_TRUE(client.IncreaseReservationToFit(child_reservation));

  BufferPool::PageHandle page;
  BufferPool::BufferHandle buffer;

  // Test basic buffer extraction.
  for (int len = TEST_BUFFER_LEN; len <= 2 * TEST_BUFFER_LEN; len *= 2) {
    const BufferHandle* page_buffer;
    ASSERT_OK(pool.CreatePage(&client, len, &page, &page_buffer));
    uint8_t* page_data = page_buffer->data();
    ASSERT_OK(pool.ExtractBuffer(&client, &page, &buffer));
    ASSERT_FALSE(page.is_open());
    ASSERT_TRUE(buffer.is_open());
    ASSERT_EQ(len, buffer.len());
    ASSERT_EQ(page_data, buffer.data());
    ASSERT_EQ(len, client.GetUsedReservation());
    pool.FreeBuffer(&client, &buffer);
    ASSERT_EQ(0, client.GetUsedReservation());
  }

  // Test that ExtractBuffer() accounts correctly for pin count > 1.
  const BufferHandle* page_buffer;
  ASSERT_OK(pool.CreatePage(&client, TEST_BUFFER_LEN, &page, &page_buffer));
  uint8_t* page_data = page_buffer->data();
  ASSERT_OK(pool.Pin(&client, &page));
  ASSERT_EQ(TEST_BUFFER_LEN * 2, client.GetUsedReservation());
  ASSERT_OK(pool.ExtractBuffer(&client, &page, &buffer));
  ASSERT_EQ(TEST_BUFFER_LEN, client.GetUsedReservation());
  ASSERT_FALSE(page.is_open());
  ASSERT_TRUE(buffer.is_open());
  ASSERT_EQ(TEST_BUFFER_LEN, buffer.len());
  ASSERT_EQ(page_data, buffer.data());
  pool.FreeBuffer(&client, &buffer);
  ASSERT_EQ(0, client.GetUsedReservation());

  // Test that ExtractBuffer() DCHECKs for unpinned pages.
  ASSERT_OK(pool.CreatePage(&client, TEST_BUFFER_LEN, &page));
  pool.Unpin(&client, &page);
  IMPALA_ASSERT_DEBUG_DEATH(
      discard_result(pool.ExtractBuffer(&client, &page, &buffer)), "");
  pool.DestroyPage(&client, &page);

  pool.DeregisterClient(&client);
}

// Test concurrent creation and destruction of pages.
TEST_F(BufferPoolTest, ConcurrentPageCreation) {
  int ops_per_thread = 1024;
  // int num_threads = 64;
  int num_threads = 1;
  // Need enough buffers for all initial reservations.
  int total_mem = num_threads * TEST_BUFFER_LEN;
  global_reservations_.InitRootTracker(NULL, total_mem);

  BufferPool pool(TEST_BUFFER_LEN, total_mem, total_mem);
  // Share a file group between the threads.
  TmpFileMgr::FileGroup* file_group = NewFileGroup();

  // Launch threads, each with a different set of query IDs.
  thread_group workers;
  for (int i = 0; i < num_threads; ++i) {
    workers.add_thread(new thread(bind(&BufferPoolTest::CreatePageLoop, this, &pool,
        file_group, &global_reservations_, ops_per_thread)));
  }

  // Build debug string to test concurrent iteration over pages_ list.
  for (int i = 0; i < 64; ++i) {
    LOG(INFO) << pool.DebugString();
  }
  workers.join_all();

  // All the reservations should be released at this point.
  ASSERT_EQ(global_reservations_.GetChildReservations(), 0);
  global_reservations_.Close();
}

void BufferPoolTest::CreatePageLoop(BufferPool* pool, TmpFileMgr::FileGroup* file_group,
    ReservationTracker* parent_tracker, int num_ops) {
  BufferPool::ClientHandle client;
  ASSERT_OK(pool->RegisterClient("test client", file_group, parent_tracker, NULL,
      TEST_BUFFER_LEN, NewProfile(), &client));
  ASSERT_TRUE(client.IncreaseReservation(TEST_BUFFER_LEN));
  for (int i = 0; i < num_ops; ++i) {
    BufferPool::PageHandle handle;
    ASSERT_OK(pool->CreatePage(&client, TEST_BUFFER_LEN, &handle));
    pool->Unpin(&client, &handle);
    ASSERT_OK(pool->Pin(&client, &handle));
    pool->DestroyPage(&client, &handle);
  }
  pool->DeregisterClient(&client);
}

/// Test that DCHECK fires when trying to unpin a page with spilling disabled.
TEST_F(BufferPoolTest, SpillingDisabledDcheck) {
  global_reservations_.InitRootTracker(NULL, 2 * TEST_BUFFER_LEN);
  BufferPool pool(TEST_BUFFER_LEN, 2 * TEST_BUFFER_LEN, 2 * TEST_BUFFER_LEN);
  BufferPool::PageHandle handle;

  BufferPool::ClientHandle client;
  ASSERT_OK(pool.RegisterClient("test client", NULL, &global_reservations_, NULL,
      numeric_limits<int64_t>::max(), NewProfile(), &client));
  ASSERT_TRUE(client.IncreaseReservation(2 * TEST_BUFFER_LEN));
  ASSERT_OK(pool.CreatePage(&client, TEST_BUFFER_LEN, &handle));

  ASSERT_OK(pool.Pin(&client, &handle));
  // It's ok to Unpin() if the pin count remains positive.
  pool.Unpin(&client, &handle);
  // We didn't pass in a FileGroup, so spilling is disabled and we can't bring the
  // pin count to 0.
  IMPALA_ASSERT_DEBUG_DEATH(pool.Unpin(&client, &handle), "");

  pool.DestroyPage(&client, &handle);
  pool.DeregisterClient(&client);
}

/// Test simple case where pool must evict a page from the same client to fit another.
TEST_F(BufferPoolTest, EvictPageSameClient) {
  global_reservations_.InitRootTracker(NULL, TEST_BUFFER_LEN);
  BufferPool pool(TEST_BUFFER_LEN, TEST_BUFFER_LEN, TEST_BUFFER_LEN);
  BufferPool::PageHandle handle1, handle2;

  BufferPool::ClientHandle client;
  ASSERT_OK(pool.RegisterClient("test client", NewFileGroup(), &global_reservations_,
      NULL, TEST_BUFFER_LEN, NewProfile(), &client));
  ASSERT_TRUE(client.IncreaseReservation(TEST_BUFFER_LEN));
  ASSERT_OK(pool.CreatePage(&client, TEST_BUFFER_LEN, &handle1));

  // Do not have enough reservations because we pinned the page.
  IMPALA_ASSERT_DEBUG_DEATH(
      discard_result(pool.CreatePage(&client, TEST_BUFFER_LEN, &handle2)), "");

  // We should be able to create a new page after unpinned and evicting the first one.
  pool.Unpin(&client, &handle1);
  ASSERT_OK(pool.CreatePage(&client, TEST_BUFFER_LEN, &handle2));

  pool.DestroyPage(&client, &handle1);
  pool.DestroyPage(&client, &handle2);
  pool.DeregisterClient(&client);
}

/// Test simple case where pool must evict pages of different sizes.
TEST_F(BufferPoolTest, EvictPageDifferentSizes) {
  const int64_t TOTAL_BYTES = 2 * TEST_BUFFER_LEN;
  global_reservations_.InitRootTracker(NULL, TOTAL_BYTES);
  BufferPool pool(TEST_BUFFER_LEN, TOTAL_BYTES, TOTAL_BYTES);
  BufferPool::PageHandle handle1, handle2;

  BufferPool::ClientHandle client;
  ASSERT_OK(pool.RegisterClient("test client", NewFileGroup(), &global_reservations_,
      NULL, TOTAL_BYTES, NewProfile(), &client));
  ASSERT_TRUE(client.IncreaseReservation(2 * TEST_BUFFER_LEN));
  ASSERT_OK(pool.CreatePage(&client, TEST_BUFFER_LEN, &handle1));
  pool.Unpin(&client, &handle1);

  // We must evict the small page to fit the large one.
  ASSERT_OK(pool.CreatePage(&client, 2 * TEST_BUFFER_LEN, &handle2));
  ASSERT_TRUE(IsEvicted(&handle1));

  // We must evict the large page to fit the small one.
  pool.Unpin(&client, &handle2);
  ASSERT_OK(pool.Pin(&client, &handle1));
  ASSERT_TRUE(IsEvicted(&handle2));

  pool.DestroyPage(&client, &handle1);
  pool.DestroyPage(&client, &handle2);
  pool.DeregisterClient(&client);
}

/// Test simple case where pool must evict a page from a one client to fit another one in
/// memory.
TEST_F(BufferPoolTest, EvictPageDifferentClient) {
  const int NUM_CLIENTS = 2;
  const int64_t TOTAL_BYTES = NUM_CLIENTS * TEST_BUFFER_LEN;
  global_reservations_.InitRootTracker(NULL, TOTAL_BYTES);
  BufferPool pool(TEST_BUFFER_LEN, TOTAL_BYTES, TOTAL_BYTES);

  BufferPool::ClientHandle clients[NUM_CLIENTS];
  for (int i = 0; i < NUM_CLIENTS; ++i) {
    ASSERT_OK(pool.RegisterClient(Substitute("test client $0", i), NewFileGroup(),
        &global_reservations_, NULL, TEST_BUFFER_LEN, NewProfile(), &clients[i]));
    ASSERT_TRUE(clients[i].IncreaseReservation(TEST_BUFFER_LEN));
  }

  // Create a pinned and unpinned page for the first client.
  PageHandle handle1, handle2;
  const BufferHandle* page_buffer;
  ASSERT_OK(pool.CreatePage(&clients[0], TEST_BUFFER_LEN, &handle1, &page_buffer));
  const uint8_t TEST_VAL = 123;
  memset(
      page_buffer->data(), TEST_VAL, handle1.len()); // Fill page with an arbitrary value.
  pool.Unpin(&clients[0], &handle1);
  ASSERT_OK(pool.CreatePage(&clients[0], TEST_BUFFER_LEN, &handle2));

  // Allocating a buffer for the second client requires evicting the unpinned page.
  BufferHandle buffer;
  ASSERT_OK(pool.AllocateBuffer(&clients[1], TEST_BUFFER_LEN, &buffer));
  ASSERT_TRUE(IsEvicted(&handle1));

  // Test reading back the first page, which requires swapping buffers again.
  pool.Unpin(&clients[0], &handle2);
  ASSERT_OK(pool.Pin(&clients[0], &handle1));
  ASSERT_TRUE(IsEvicted(&handle2));
  ASSERT_OK(handle1.GetBuffer(&page_buffer));
  for (int i = 0; i < handle1.len(); ++i) {
    EXPECT_EQ(TEST_VAL, page_buffer->data()[i]) << i;
  }

  // Clean up everything.
  pool.DestroyPage(&clients[0], &handle1);
  pool.DestroyPage(&clients[0], &handle2);
  pool.FreeBuffer(&clients[1], &buffer);
  for (BufferPool::ClientHandle& client : clients) pool.DeregisterClient(&client);
}

/// Regression test for IMPALA-5113 where the page flushing invariant didn't correctly
/// take multiply pinned pages into account.
TEST_F(BufferPoolTest, MultiplyPinnedPageAccounting) {
  const int NUM_BUFFERS = 3;
  const int64_t TOTAL_BYTES = NUM_BUFFERS * TEST_BUFFER_LEN;
  global_reservations_.InitRootTracker(NULL, TOTAL_BYTES);
  BufferPool pool(TEST_BUFFER_LEN, TOTAL_BYTES, TOTAL_BYTES);

  BufferPool::ClientHandle client;
  RuntimeProfile* profile = NewProfile();
  ASSERT_OK(pool.RegisterClient("test client", NewFileGroup(), &global_reservations_,
      NULL, TOTAL_BYTES, profile, &client));
  ASSERT_TRUE(client.IncreaseReservation(TOTAL_BYTES));

  BufferPool::PageHandle handle1, handle2;
  BufferPool::BufferHandle buffer;
  ASSERT_OK(pool.CreatePage(&client, TEST_BUFFER_LEN, &handle1));
  ASSERT_OK(pool.CreatePage(&client, TEST_BUFFER_LEN, &handle2));
  pool.Unpin(&client, &handle1);
  ASSERT_OK(pool.Pin(&client, &handle2));
  ASSERT_OK(pool.AllocateBuffer(&client, TEST_BUFFER_LEN, &buffer));

  // We shouldn't need to flush anything to disk since we have only three pages/buffers in
  // memory. Rely on DCHECKs to check invariants and check we didn't evict the page.
  EXPECT_FALSE(IsEvicted(&handle1)) << handle1.DebugString();

  pool.DestroyPage(&client, &handle1);
  pool.DestroyPage(&client, &handle2);
  pool.FreeBuffer(&client, &buffer);
  pool.DeregisterClient(&client);
}

// Constants for TestMemoryReclamation().
const int MEM_RECLAMATION_NUM_CLIENTS = 2;
// Choose a non-power-of two so that AllocateBuffers() will allocate a mix of sizes:
// 32 + 32 + 32 + 8 + 4 + 2 + 1
const int64_t MEM_RECLAMATION_BUFFERS_PER_CLIENT = 127;
const int64_t MEM_RECLAMATION_CLIENT_RESERVATION =
    BufferPoolTest::TEST_BUFFER_LEN * MEM_RECLAMATION_BUFFERS_PER_CLIENT;
const int64_t MEM_RECLAMATION_TOTAL_BYTES =
    MEM_RECLAMATION_NUM_CLIENTS * MEM_RECLAMATION_CLIENT_RESERVATION;

// Test that we can reclaim buffers and pages from the same arena and from other arenas.
TEST_F(BufferPoolTest, MemoryReclamation) {
  global_reservations_.InitRootTracker(NULL, MEM_RECLAMATION_TOTAL_BYTES);
  BufferPool pool(TEST_BUFFER_LEN, MEM_RECLAMATION_TOTAL_BYTES,
      MEM_RECLAMATION_TOTAL_BYTES);
  // Assume that all cores are online. Test various combinations of cores to validate
  // that it can reclaim from any other other core.
  for (int src = 0; src < CpuInfo::num_cores(); ++src) {
    // Limit the max scavenge attempts to force use of the "locked" scavenging sometimes,
    // which would otherwise only be triggered by racing threads.
    SetMaxScavengeAttempts(&pool, 1 + src % 3);
    for (int j = 0; j < 4; ++j) {
      int dst = (src + j) % CpuInfo::num_cores();
      TestMemoryReclamation(&pool, src, dst);
    }
    // Test with one fixed and the other randomly changing
    TestMemoryReclamation(&pool, src, -1);
    TestMemoryReclamation(&pool, -1, src);
  }
  // Test with both src and dst randomly changing.
  TestMemoryReclamation(&pool, -1, -1);
  global_reservations_.Close();
}

// Test that we can reclaim buffers and pages from the same arena or a different arena.
// Allocates then frees memory on 'src_core' then allocates on 'dst_core' to force
// reclamation of memory from src_core's free buffer lists and clean page lists.
// If 'src_core' or 'dst_core' is -1, randomly switch between cores instead of sticking
// to a fixed core.
void BufferPoolTest::TestMemoryReclamation(BufferPool* pool, int src_core, int dst_core) {
  LOG(INFO) << "TestMemoryReclamation " << src_core << " -> " << dst_core;
  const bool rand_src_core = src_core == -1;
  const bool rand_dst_core = dst_core == -1;

  BufferPool::ClientHandle clients[MEM_RECLAMATION_NUM_CLIENTS];
  for (int i = 0; i < MEM_RECLAMATION_NUM_CLIENTS; ++i) {
    ASSERT_OK(pool->RegisterClient(Substitute("test client $0", i), NewFileGroup(),
        &global_reservations_, NULL, MEM_RECLAMATION_CLIENT_RESERVATION, NewProfile(),
        &clients[i]));
    ASSERT_TRUE(clients[i].IncreaseReservation(MEM_RECLAMATION_CLIENT_RESERVATION));
  }

  // Allocate and free the whole pool's buffers on src_core to populate its free lists.
  if (!rand_src_core) CpuTestUtil::PinToCore(src_core);
  vector<BufferPool::BufferHandle> client_buffers[MEM_RECLAMATION_NUM_CLIENTS];
  AllocateBuffers(pool, &clients[0], 32 * TEST_BUFFER_LEN,
      MEM_RECLAMATION_CLIENT_RESERVATION, &client_buffers[0], rand_src_core);
  AllocateBuffers(pool, &clients[1], 32 * TEST_BUFFER_LEN,
      MEM_RECLAMATION_CLIENT_RESERVATION, &client_buffers[1], rand_src_core);
  FreeBuffers(pool, &clients[0], &client_buffers[0], rand_src_core);
  FreeBuffers(pool, &clients[1], &client_buffers[1], rand_src_core);

  // Allocate buffers again on dst_core. Make sure the size is bigger, smaller, and the
  // same size as buffers we allocated earlier to we exercise different code paths.
  if (!rand_dst_core) CpuTestUtil::PinToCore(dst_core);
  AllocateBuffers(pool, &clients[0], 4 * TEST_BUFFER_LEN,
      MEM_RECLAMATION_CLIENT_RESERVATION, &client_buffers[0], rand_dst_core);
  FreeBuffers(pool, &clients[0], &client_buffers[0], rand_dst_core);

  // Allocate and unpin the whole pool's buffers as clean pages on src_core to populate
  // its clean page lists.
  if (!rand_src_core) CpuTestUtil::PinToCore(src_core);
  vector<BufferPool::PageHandle> client_pages[MEM_RECLAMATION_NUM_CLIENTS];
  CreatePages(pool, &clients[0], 32 * TEST_BUFFER_LEN, MEM_RECLAMATION_CLIENT_RESERVATION,
      &client_pages[0], rand_src_core);
  CreatePages(pool, &clients[1], 32 * TEST_BUFFER_LEN, MEM_RECLAMATION_CLIENT_RESERVATION,
      &client_pages[1], rand_src_core);
  for (auto& page : client_pages[0]) pool->Unpin(&clients[0], &page);
  for (auto& page : client_pages[1]) pool->Unpin(&clients[1], &page);

  // Allocate the buffers again to force reclamation of the buffers from the clean pages.
  if (!rand_dst_core) CpuTestUtil::PinToCore(dst_core);
  AllocateBuffers(pool, &clients[0], 4 * TEST_BUFFER_LEN,
      MEM_RECLAMATION_CLIENT_RESERVATION, &client_buffers[0], rand_dst_core);
  FreeBuffers(pool, &clients[0], &client_buffers[0]);

  // Just for good measure, pin the pages again then destroy them.
  for (auto& page : client_pages[0]) {
    ASSERT_OK(pool->Pin(&clients[0], &page));
    pool->DestroyPage(&clients[0], &page);
  }
  for (auto& page : client_pages[1]) {
    ASSERT_OK(pool->Pin(&clients[1], &page));
    pool->DestroyPage(&clients[1], &page);
  }
  for (BufferPool::ClientHandle& client : clients) pool->DeregisterClient(&client);
}

// Test the eviction policy of the buffer pool. Writes are issued eagerly as pages
// are unpinned, but pages are only evicted from memory when another buffer is
// allocated.
TEST_F(BufferPoolTest, EvictionPolicy) {
  TestEvictionPolicy(TEST_BUFFER_LEN);
  TestEvictionPolicy(2 * 1024 * 1024);
}

void BufferPoolTest::TestEvictionPolicy(int64_t page_size) {
  // The eviction policy changes if there are multiple NUMA nodes, because buffers from
  // clean pages on the local node are claimed in preference to free buffers on the
  // non-local node. The rest of the test assumes that it executes on a single NUMA node.
  if (CpuInfo::GetMaxNumNumaNodes() > 1) CpuTestUtil::PinToCore(0);
  const int MAX_NUM_BUFFERS = 5;
  int64_t total_mem = MAX_NUM_BUFFERS * page_size;
  global_reservations_.InitRootTracker(NewProfile(), total_mem);
  BufferPool pool(TEST_BUFFER_LEN, total_mem, total_mem);

  ClientHandle client;
  RuntimeProfile* profile = NewProfile();
  ASSERT_OK(pool.RegisterClient("test client", NewFileGroup(), &global_reservations_,
      nullptr, total_mem, profile, &client));
  ASSERT_TRUE(client.IncreaseReservation(total_mem));

  RuntimeProfile* buffer_pool_profile = nullptr;
  vector<RuntimeProfile*> profile_children;
  profile->GetChildren(&profile_children);
  for (RuntimeProfile* child : profile_children) {
    if (child->name() == "Buffer pool") {
      buffer_pool_profile = child;
      break;
    }
  }
  ASSERT_TRUE(buffer_pool_profile != nullptr);
  RuntimeProfile::Counter* cumulative_bytes_alloced =
      buffer_pool_profile->GetCounter("CumulativeAllocationBytes");
  RuntimeProfile::Counter* write_ios = buffer_pool_profile->GetCounter("WriteIoOps");
  RuntimeProfile::Counter* read_ios = buffer_pool_profile->GetCounter("ReadIoOps");

  vector<PageHandle> pages;
  CreatePages(&pool, &client, page_size, total_mem, &pages);
  WriteData(pages, 0);

  // Unpin pages. Writes should be started and memory should not be deallocated.
  EXPECT_EQ(total_mem, cumulative_bytes_alloced->value());
  EXPECT_EQ(total_mem, pool.GetSystemBytesAllocated());
  UnpinAll(&pool, &client, &pages);
  ASSERT_GT(write_ios->value(), 0);

  // Re-pin all the pages and validate their data. This should not require reading the
  // pages back from disk.
  ASSERT_OK(PinAll(&pool, &client, &pages));
  ASSERT_EQ(0, read_ios->value());
  VerifyData(pages, 0);

  // Unpin all pages. Writes should be started again.
  int64_t prev_write_ios = write_ios->value();
  UnpinAll(&pool, &client, &pages);
  ASSERT_GT(write_ios->value(), prev_write_ios);

  // Allocate two more buffers. Two unpinned pages must be evicted to make room.
  const int NUM_EXTRA_BUFFERS = 2;
  vector<BufferHandle> extra_buffers;
  AllocateBuffers(
      &pool, &client, page_size, page_size * NUM_EXTRA_BUFFERS, &extra_buffers);
  // At least two unpinned pages should have been written out.
  ASSERT_GE(write_ios->value(), prev_write_ios + NUM_EXTRA_BUFFERS);
  // No additional memory should have been allocated - it should have been recycled.
  EXPECT_EQ(total_mem, pool.GetSystemBytesAllocated());
  // Check that two pages were evicted.
  EXPECT_EQ(NUM_EXTRA_BUFFERS, NumEvicted(pages));

  // Free up memory required to pin the original pages again.
  FreeBuffers(&pool, &client, &extra_buffers);
  ASSERT_OK(PinAll(&pool, &client, &pages));
  // We only needed read to back the two evicted pages. Make sure we didn't do extra I/O.
  ASSERT_EQ(NUM_EXTRA_BUFFERS, read_ios->value());
  VerifyData(pages, 0);
  DestroyAll(&pool, &client, &pages);
  pool.DeregisterClient(&client);
  global_reservations_.Close();
}

/// Test that we can destroy pages while a disk write is in flight for those pages.
TEST_F(BufferPoolTest, DestroyDuringWrite) {
  const int TRIALS = 20;
  const int MAX_NUM_BUFFERS = 5;
  const int64_t TOTAL_MEM = TEST_BUFFER_LEN * MAX_NUM_BUFFERS;
  BufferPool pool(TEST_BUFFER_LEN, TOTAL_MEM, TOTAL_MEM);
  global_reservations_.InitRootTracker(NewProfile(), TOTAL_MEM);
  ClientHandle client;
  for (int trial = 0; trial < TRIALS; ++trial) {
    ASSERT_OK(pool.RegisterClient("test client", NewFileGroup(), &global_reservations_,
        nullptr, TOTAL_MEM, NewProfile(), &client));
    ASSERT_TRUE(client.IncreaseReservation(TOTAL_MEM));

    vector<PageHandle> pages;
    CreatePages(&pool, &client, TEST_BUFFER_LEN, TOTAL_MEM, &pages);

    // Unpin will initiate writes.
    UnpinAll(&pool, &client, &pages);

    // Writes should still be in flight when pages are deleted.
    DestroyAll(&pool, &client, &pages);
    pool.DeregisterClient(&client);
  }
}

/// Test teardown of a query while writes are in flight. This was based on a
/// BufferedBlockMgr regression test for IMPALA-2252 where tear-down of the
/// query's RuntimeStates raced with scratch writes. If write_error is true,
/// force writes to hit errors.
void BufferPoolTest::TestQueryTeardown(bool write_error) {
  const int64_t TOTAL_BUFFERS = 20;
  const int CLIENT_BUFFERS = 10;
  const int64_t TOTAL_MEM = TOTAL_BUFFERS * TEST_BUFFER_LEN;
  const int64_t CLIENT_MEM = CLIENT_BUFFERS * TEST_BUFFER_LEN;

  // Set up a BufferPool in the TestEnv.
  test_env_.reset(new TestEnv());
  test_env_->SetBufferPoolArgs(TEST_BUFFER_LEN, TOTAL_MEM);
  ASSERT_OK(test_env_->Init());

  BufferPool* pool = test_env_->exec_env()->buffer_pool();
  RuntimeState* state;
  ASSERT_OK(test_env_->CreateQueryState(0, nullptr, &state));

  ClientHandle client;
  ASSERT_OK(pool->RegisterClient("test client", state->query_state()->file_group(),
      state->instance_buffer_reservation(),
      obj_pool_.Add(new MemTracker(-1, "", state->instance_mem_tracker())), CLIENT_MEM,
      NewProfile(), &client));
  ASSERT_TRUE(client.IncreaseReservation(CLIENT_MEM));

  vector<PageHandle> pages;
  CreatePages(pool, &client, TEST_BUFFER_LEN, CLIENT_BUFFERS, &pages);

  if (write_error) {
    UnpinAll(pool, &client, &pages);
    // Allocate more buffers to create memory pressure and force eviction of all the
    // unpinned pages.
    vector<BufferHandle> tmp_buffers;
    AllocateBuffers(pool, &client, TEST_BUFFER_LEN, CLIENT_BUFFERS, &tmp_buffers);
    string tmp_file_path = TmpFilePath(pages.data());
    FreeBuffers(pool, &client, &tmp_buffers);

    ASSERT_OK(PinAll(pool, &client, &pages));
    // Remove temporary file to force future writes to that file to fail.
    DisableBackingFile(tmp_file_path);
  }

  // Unpin will initiate writes. If we triggered a write error earlier, some writes may
  // go down the error path.
  UnpinAll(pool, &client, &pages);

  // Tear down the pages, client, and query in the correct order while writes are in
  // flight.
  DestroyAll(pool, &client, &pages);
  pool->DeregisterClient(&client);
  test_env_->TearDownQueries();

  // All memory should be released from the query.
  EXPECT_EQ(0, test_env_->TotalQueryMemoryConsumption());
  EXPECT_EQ(0, test_env_->exec_env()->buffer_reservation()->GetChildReservations());
}

TEST_F(BufferPoolTest, QueryTeardown) {
  TestQueryTeardown(false);
}

TEST_F(BufferPoolTest, QueryTeardownWriteError) {
  TestQueryTeardown(true);
}

// Test that the buffer pool handles a write error correctly.  Delete the scratch
// directory before an operation that would cause a write and test that subsequent API
// calls return errors as expected.
void BufferPoolTest::TestWriteError(int write_delay_ms) {
  int MAX_NUM_BUFFERS = 2;
  int64_t TOTAL_MEM = MAX_NUM_BUFFERS * TEST_BUFFER_LEN;
  BufferPool pool(TEST_BUFFER_LEN, TOTAL_MEM, TOTAL_MEM);
  global_reservations_.InitRootTracker(NewProfile(), TOTAL_MEM);
  ClientHandle client;
  ASSERT_OK(pool.RegisterClient("test client", NewFileGroup(), &global_reservations_,
      nullptr, TOTAL_MEM, NewProfile(), &client));
  ASSERT_TRUE(client.IncreaseReservation(TOTAL_MEM));
  client.impl_->set_debug_write_delay_ms(write_delay_ms);

  vector<PageHandle> pages;
  CreatePages(&pool, &client, TEST_BUFFER_LEN, MAX_NUM_BUFFERS, &pages);
  // Unpin two pages here, to ensure that backing storage is allocated in tmp file.
  UnpinAll(&pool, &client, &pages);
  WaitForAllWrites(&client);
  // Repin the pages
  ASSERT_OK(PinAll(&pool, &client, &pages));
  // Remove permissions to the backing storage so that future writes will fail
  ASSERT_GT(RemoveScratchPerms(), 0);
  // Give the first write a chance to fail before the second write starts.
  const int INTERVAL_MS = 10;
  UnpinAll(&pool, &client, &pages, INTERVAL_MS);
  WaitForAllWrites(&client);

  // Subsequent calls to APIs that require allocating memory should fail: the write error
  // is picked up asynchronously.
  BufferHandle tmp_buffer;
  PageHandle tmp_page;
  Status error = pool.AllocateBuffer(&client, TEST_BUFFER_LEN, &tmp_buffer);
  EXPECT_EQ(TErrorCode::SCRATCH_ALLOCATION_FAILED, error.code());
  ASSERT_NE(string::npos, error.msg().msg().find(GetBackendString()));
  EXPECT_FALSE(tmp_buffer.is_open());
  error = pool.CreatePage(&client, TEST_BUFFER_LEN, &tmp_page);
  EXPECT_EQ(TErrorCode::SCRATCH_ALLOCATION_FAILED, error.code());
  EXPECT_FALSE(tmp_page.is_open());
  error = pool.Pin(&client, pages.data());
  EXPECT_EQ(TErrorCode::SCRATCH_ALLOCATION_FAILED, error.code());
  EXPECT_FALSE(pages[0].is_pinned());

  DestroyAll(&pool, &client, &pages);
  pool.DeregisterClient(&client);
  global_reservations_.Close();
}

TEST_F(BufferPoolTest, WriteError) {
  TestWriteError(0);
}

// Regression test for IMPALA-4842 - inject a delay in the write to
// reproduce the issue.
TEST_F(BufferPoolTest, WriteErrorWriteDelay) {
  TestWriteError(100);
}

// Test error handling when temporary file space cannot be allocated to back an unpinned
// page.
TEST_F(BufferPoolTest, TmpFileAllocateError) {
  const int MAX_NUM_BUFFERS = 2;
  const int64_t TOTAL_MEM = TEST_BUFFER_LEN * MAX_NUM_BUFFERS;
  BufferPool pool(TEST_BUFFER_LEN, TOTAL_MEM, TOTAL_MEM);
  global_reservations_.InitRootTracker(NewProfile(), TOTAL_MEM);
  ClientHandle client;
  ASSERT_OK(pool.RegisterClient("test client", NewFileGroup(), &global_reservations_,
      nullptr, TOTAL_MEM, NewProfile(), &client));
  ASSERT_TRUE(client.IncreaseReservation(TOTAL_MEM));

  vector<PageHandle> pages;
  CreatePages(&pool, &client, TEST_BUFFER_LEN, TOTAL_MEM, &pages);
  // Unpin a page, which will trigger a write.
  pool.Unpin(&client, pages.data());
  WaitForAllWrites(&client);
  // Remove permissions to the temporary files - subsequent operations will fail.
  ASSERT_GT(RemoveScratchPerms(), 0);
  // The write error will happen asynchronously.
  pool.Unpin(&client, &pages[1]);

  // Write failure causes future operations like Pin() to fail.
  WaitForAllWrites(&client);
  Status error = pool.Pin(&client, pages.data());
  EXPECT_EQ(TErrorCode::SCRATCH_ALLOCATION_FAILED, error.code());
  EXPECT_FALSE(pages[0].is_pinned());

  DestroyAll(&pool, &client, &pages);
  pool.DeregisterClient(&client);
}

// Test that scratch devices are blacklisted after a write error. The query that
// encountered the write error should not allocate more pages on that device, but
// existing pages on the device will remain in use and future queries will use the device.
TEST_F(BufferPoolTest, WriteErrorBlacklist) {
  // Set up two file groups with two temporary dirs.
  vector<string> tmp_dirs = InitMultipleTmpDirs(2);
  // Simulate two concurrent queries.
  const int TOTAL_QUERIES = 3;
  const int INITIAL_QUERIES = 2;
  const int MAX_NUM_PAGES = 6;
  const int PAGES_PER_QUERY = MAX_NUM_PAGES / TOTAL_QUERIES;
  const int64_t TOTAL_MEM = MAX_NUM_PAGES * TEST_BUFFER_LEN;
  const int64_t MEM_PER_QUERY = PAGES_PER_QUERY * TEST_BUFFER_LEN;
  BufferPool pool(TEST_BUFFER_LEN, TOTAL_MEM, TOTAL_MEM);
  global_reservations_.InitRootTracker(NewProfile(), TOTAL_MEM);
  vector<FileGroup*> file_groups;
  vector<ClientHandle> clients(TOTAL_QUERIES);
  for (int i = 0; i < INITIAL_QUERIES; ++i) {
    file_groups.push_back(NewFileGroup());
    ASSERT_OK(pool.RegisterClient("test client", file_groups[i], &global_reservations_,
        nullptr, MEM_PER_QUERY, NewProfile(), &clients[i]));
    ASSERT_TRUE(clients[i].IncreaseReservation(MEM_PER_QUERY));
  }

  // Allocate files for all 2x2 combinations by unpinning pages.
  vector<vector<PageHandle>> pages(TOTAL_QUERIES);
  for (int i = 0; i < INITIAL_QUERIES; ++i) {
    CreatePages(&pool, &clients[i], TEST_BUFFER_LEN, MEM_PER_QUERY, &pages[i]);
    WriteData(pages[i], 0);
    UnpinAll(&pool, &clients[i], &pages[i]);
    for (int j = 0; j < PAGES_PER_QUERY; ++j) {
      LOG(INFO) << "Manager " << i << " Block " << j << " backed by file "
                << TmpFilePath(&pages[i][j]);
    }
  }
  for (int i = 0; i < INITIAL_QUERIES; ++i) WaitForAllWrites(&clients[i]);
  const int ERROR_QUERY = 0;
  const int NO_ERROR_QUERY = 1;
  const string& error_dir = tmp_dirs[0];
  const string& good_dir = tmp_dirs[1];
  // Delete one file from first scratch dir for first query to trigger an error.
  PageHandle* error_page = FindPageInDir(pages[ERROR_QUERY], error_dir);
  ASSERT_TRUE(error_page != NULL) << "Expected a tmp file in dir " << error_dir;
  const string& error_file_path = TmpFilePath(error_page);
  for (int i = 0; i < INITIAL_QUERIES; ++i) {
    ASSERT_OK(PinAll(&pool, &clients[i], &pages[i]));
  }
  DisableBackingFile(error_file_path);
  for (int i = 0; i < INITIAL_QUERIES; ++i) UnpinAll(&pool, &clients[i], &pages[i]);

  // At least one write should hit an error, but it should be recoverable.
  for (int i = 0; i < INITIAL_QUERIES; ++i) WaitForAllWrites(&clients[i]);

  // Both clients should still be usable - test the API.
  for (int i = 0; i < INITIAL_QUERIES; ++i) {
    ASSERT_OK(PinAll(&pool, &clients[i], &pages[i]));
    VerifyData(pages[i], 0);
    UnpinAll(&pool, &clients[i], &pages[i]);
    ASSERT_OK(AllocateAndFree(&pool, &clients[i], TEST_BUFFER_LEN));
  }

  // Temporary device with error should still be active.
  vector<TmpFileMgr::DeviceId> active_tmp_devices =
      test_env_->tmp_file_mgr()->ActiveTmpDevices();
  ASSERT_EQ(tmp_dirs.size(), active_tmp_devices.size());
  for (int i = 0; i < active_tmp_devices.size(); ++i) {
    const string& device_path =
        test_env_->tmp_file_mgr()->GetTmpDirPath(active_tmp_devices[i]);
    ASSERT_EQ(string::npos, error_dir.find(device_path));
  }

  // The query that hit the error should only allocate from the device that had no error.
  // The other one should continue using both devices, since it didn't encounter a write
  // error itself.
  vector<PageHandle> error_new_pages;
  CreatePages(
      &pool, &clients[ERROR_QUERY], TEST_BUFFER_LEN, MEM_PER_QUERY, &error_new_pages);
  UnpinAll(&pool, &clients[ERROR_QUERY], &error_new_pages);
  WaitForAllWrites(&clients[ERROR_QUERY]);
  EXPECT_TRUE(FindPageInDir(error_new_pages, good_dir) != NULL);
  EXPECT_TRUE(FindPageInDir(error_new_pages, error_dir) == NULL);
  for (PageHandle& error_new_page : error_new_pages) {
    LOG(INFO) << "Newly created page backed by file " << TmpFilePath(&error_new_page);
    EXPECT_TRUE(PageInDir(&error_new_page, good_dir));
  }
  DestroyAll(&pool, &clients[ERROR_QUERY], &error_new_pages);

  ASSERT_OK(PinAll(&pool, &clients[NO_ERROR_QUERY], &pages[NO_ERROR_QUERY]));
  UnpinAll(&pool, &clients[NO_ERROR_QUERY], &pages[NO_ERROR_QUERY]);
  WaitForAllWrites(&clients[NO_ERROR_QUERY]);
  EXPECT_TRUE(FindPageInDir(pages[NO_ERROR_QUERY], good_dir) != NULL);
  EXPECT_TRUE(FindPageInDir(pages[NO_ERROR_QUERY], error_dir) != NULL);

  // The second client should use the bad directory for new pages since
  // blacklisting is per-query, not global.
  vector<PageHandle> no_error_new_pages;
  CreatePages(&pool, &clients[NO_ERROR_QUERY], TEST_BUFFER_LEN, MEM_PER_QUERY,
      &no_error_new_pages);
  UnpinAll(&pool, &clients[NO_ERROR_QUERY], &no_error_new_pages);
  WaitForAllWrites(&clients[NO_ERROR_QUERY]);
  EXPECT_TRUE(FindPageInDir(no_error_new_pages, good_dir) != NULL);
  EXPECT_TRUE(FindPageInDir(no_error_new_pages, error_dir) != NULL);
  DestroyAll(&pool, &clients[NO_ERROR_QUERY], &no_error_new_pages);

  // A new query should use the both dirs for backing storage.
  const int NEW_QUERY = 2;
  ASSERT_OK(pool.RegisterClient("new test client", NewFileGroup(), &global_reservations_,
      nullptr, MEM_PER_QUERY, NewProfile(), &clients[NEW_QUERY]));
  ASSERT_TRUE(clients[NEW_QUERY].IncreaseReservation(MEM_PER_QUERY));
  CreatePages(
      &pool, &clients[NEW_QUERY], TEST_BUFFER_LEN, MEM_PER_QUERY, &pages[NEW_QUERY]);
  UnpinAll(&pool, &clients[NEW_QUERY], &pages[NEW_QUERY]);
  WaitForAllWrites(&clients[NEW_QUERY]);
  EXPECT_TRUE(FindPageInDir(pages[NEW_QUERY], good_dir) != NULL);
  EXPECT_TRUE(FindPageInDir(pages[NEW_QUERY], error_dir) != NULL);

  for (int i = 0; i < TOTAL_QUERIES; ++i) {
    DestroyAll(&pool, &clients[i], &pages[i]);
    pool.DeregisterClient(&clients[i]);
  }
}

// Test error handling when on-disk data is corrupted and the read fails.
TEST_F(BufferPoolTest, ScratchReadError) {
  // Only allow one buffer in memory.
  const int64_t TOTAL_MEM = TEST_BUFFER_LEN;
  BufferPool pool(TEST_BUFFER_LEN, TOTAL_MEM, TOTAL_MEM);
  global_reservations_.InitRootTracker(NewProfile(), TOTAL_MEM);

  // Simulate different types of error.
  enum ErrType {
    CORRUPT_DATA, // Overwrite real spilled data with bogus data.
    NO_PERMS, // Remove permissions on the scratch file.
    TRUNCATE // Truncate the scratch file, destroying spilled data.
  };
  for (ErrType error_type : {CORRUPT_DATA, NO_PERMS, TRUNCATE}) {
    ClientHandle client;
    ASSERT_OK(pool.RegisterClient("test client", NewFileGroup(), &global_reservations_,
        nullptr, TOTAL_MEM, NewProfile(), &client));
    ASSERT_TRUE(client.IncreaseReservation(TOTAL_MEM));
    PageHandle page;
    ASSERT_OK(pool.CreatePage(&client, TEST_BUFFER_LEN, &page));
    // Unpin a page, which will trigger a write.
    pool.Unpin(&client, &page);
    WaitForAllWrites(&client);

    // Force eviction of the page.
    ASSERT_OK(AllocateAndFree(&pool, &client, TEST_BUFFER_LEN));

    string tmp_file = TmpFilePath(&page);
    if (error_type == CORRUPT_DATA) {
      CorruptBackingFile(tmp_file);
    } else if (error_type == NO_PERMS) {
      DisableBackingFile(tmp_file);
    } else {
      DCHECK_EQ(error_type, TRUNCATE);
      TruncateBackingFile(tmp_file);
    }
    ASSERT_OK(pool.Pin(&client, &page));
    // The read is async, so won't bubble up until we block on it with GetBuffer().
    const BufferHandle* page_buffer;
    Status status = page.GetBuffer(&page_buffer);
    if (error_type == CORRUPT_DATA && !FLAGS_disk_spill_encryption) {
      // Without encryption we can't detect that the data changed.
      EXPECT_OK(status);
    } else {
      // Otherwise the read should fail.
      EXPECT_FALSE(status.ok());
    }
    // Should be able to destroy the page, even though we hit an error.
    pool.DestroyPage(&client, &page);

    // If the backing file is still enabled, we should still be able to pin and unpin
    // pages as normal.
    ASSERT_OK(pool.CreatePage(&client, TEST_BUFFER_LEN, &page));
    WriteData(page, 1);
    pool.Unpin(&client, &page);
    WaitForAllWrites(&client);
    if (error_type == NO_PERMS) {
      // The error prevents read/write of scratch files - this will fail.
      EXPECT_FALSE(pool.Pin(&client, &page).ok());
    } else {
      // The error does not prevent read/write of scratch files.
      ASSERT_OK(AllocateAndFree(&pool, &client, TEST_BUFFER_LEN));
      ASSERT_OK(pool.Pin(&client, &page));
      VerifyData(page, 1);
    }
    pool.DestroyPage(&client, &page);
    pool.DeregisterClient(&client);
  }
}

/// Test that the buffer pool fails cleanly when all scratch directories are inaccessible
/// at runtime.
TEST_F(BufferPoolTest, NoDirsAllocationError) {
  vector<string> tmp_dirs = InitMultipleTmpDirs(2);
  int MAX_NUM_BUFFERS = 2;
  int64_t TOTAL_MEM = MAX_NUM_BUFFERS * TEST_BUFFER_LEN;
  BufferPool pool(TEST_BUFFER_LEN, TOTAL_MEM, TOTAL_MEM);
  global_reservations_.InitRootTracker(NewProfile(), TOTAL_MEM);
  ClientHandle client;
  ASSERT_OK(pool.RegisterClient("test client", NewFileGroup(), &global_reservations_,
      nullptr, TOTAL_MEM, NewProfile(), &client));
  ASSERT_TRUE(client.IncreaseReservation(TOTAL_MEM));

  vector<PageHandle> pages;
  CreatePages(&pool, &client, TEST_BUFFER_LEN, TOTAL_MEM, &pages);
  for (int i = 0; i < tmp_dirs.size(); ++i) {
    const string& tmp_scratch_subdir = tmp_dirs[i] + SCRATCH_SUFFIX;
    chmod(tmp_scratch_subdir.c_str(), 0);
  }

  // The error will happen asynchronously.
  UnpinAll(&pool, &client, &pages);
  WaitForAllWrites(&client);

  // Write failure should results in an error getting propagated back to Pin().
  for (PageHandle& page : pages) {
    Status status = pool.Pin(&client, &page);
    EXPECT_EQ(TErrorCode::SCRATCH_ALLOCATION_FAILED, status.code());
  }
  DestroyAll(&pool, &client, &pages);
  pool.DeregisterClient(&client);
}

// Test that the buffer pool can still create pages when no scratch is present.
TEST_F(BufferPoolTest, NoTmpDirs) {
  InitMultipleTmpDirs(0);
  const int MAX_NUM_BUFFERS = 3;
  const int64_t TOTAL_MEM = MAX_NUM_BUFFERS * TEST_BUFFER_LEN;
  BufferPool pool(TEST_BUFFER_LEN, TOTAL_MEM, TOTAL_MEM);
  global_reservations_.InitRootTracker(NewProfile(), TOTAL_MEM);
  ClientHandle client;
  ASSERT_OK(pool.RegisterClient("test client", NewFileGroup(), &global_reservations_,
      nullptr, TOTAL_MEM, NewProfile(), &client));
  ASSERT_TRUE(client.IncreaseReservation(TOTAL_MEM));

  vector<PageHandle> pages;
  CreatePages(&pool, &client, TEST_BUFFER_LEN, TOTAL_MEM, &pages);

  // Unpinning is allowed by the BufferPool interface but we won't start any writes to
  // disk because the flushing heuristic does not eagerly start writes when there are no
  // active scratch devices.
  UnpinAll(&pool, &client, &pages);
  WaitForAllWrites(&client);
  ASSERT_OK(pool.Pin(&client, pages.data()));

  // Allocating another buffer will force a write, which will fail.
  BufferHandle tmp_buffer;
  Status status = pool.AllocateBuffer(&client, TEST_BUFFER_LEN, &tmp_buffer);
  ASSERT_FALSE(status.ok());
  ASSERT_EQ(TErrorCode::SCRATCH_ALLOCATION_FAILED, status.code()) << status.msg().msg();

  DestroyAll(&pool, &client, &pages);
  pool.DeregisterClient(&client);
}

// Test that the buffer pool can still create pages when spilling is disabled by
// setting scratch_limit = 0.
TEST_F(BufferPoolTest, ScratchLimitZero) {
  const int QUERY_BUFFERS = 3;
  const int64_t TOTAL_MEM = 100 * TEST_BUFFER_LEN;
  const int64_t QUERY_MEM = QUERY_BUFFERS * TEST_BUFFER_LEN;

  // Set up a query state with the scratch_limit option in the TestEnv.
  test_env_.reset(new TestEnv());
  test_env_->SetBufferPoolArgs(TEST_BUFFER_LEN, TOTAL_MEM);
  ASSERT_OK(test_env_->Init());

  BufferPool* pool = test_env_->exec_env()->buffer_pool();
  RuntimeState* state;
  TQueryOptions query_options;
  query_options.scratch_limit = 0;
  ASSERT_OK(test_env_->CreateQueryState(0, &query_options, &state));

  ClientHandle client;
  ASSERT_OK(pool->RegisterClient("test client", state->query_state()->file_group(),
      state->instance_buffer_reservation(),
      obj_pool_.Add(new MemTracker(-1, "", state->instance_mem_tracker())), QUERY_MEM,
      NewProfile(), &client));
  ASSERT_TRUE(client.IncreaseReservation(QUERY_MEM));

  vector<PageHandle> pages;
  CreatePages(pool, &client, TEST_BUFFER_LEN, QUERY_MEM, &pages);

  // Spilling is disabled by the QueryState when scratch_limit is 0, so trying to unpin
  // will cause a DCHECK.
  IMPALA_ASSERT_DEBUG_DEATH(pool->Unpin(&client, pages.data()), "");

  DestroyAll(pool, &client, &pages);
  pool->DeregisterClient(&client);
}

TEST_F(BufferPoolTest, SingleRandom) {
  TestRandomInternalSingle(8 * 1024, true);
  TestRandomInternalSingle(8 * 1024, false);
}

TEST_F(BufferPoolTest, Multi2Random) {
  TestRandomInternalMulti(2, 8 * 1024, true);
  TestRandomInternalMulti(2, 8 * 1024, false);
}

TEST_F(BufferPoolTest, Multi4Random) {
  TestRandomInternalMulti(4, 8 * 1024, true);
  TestRandomInternalMulti(4, 8 * 1024, false);
}

TEST_F(BufferPoolTest, Multi8Random) {
  TestRandomInternalMulti(8, 8 * 1024, true);
  TestRandomInternalMulti(8, 8 * 1024, false);
}

// Single-threaded execution of the TestRandomInternalImpl.
void BufferPoolTest::TestRandomInternalSingle(
    int64_t min_buffer_len, bool multiple_pins) {
  const int MAX_NUM_BUFFERS = 200;
  const int64_t TOTAL_MEM = MAX_NUM_BUFFERS * min_buffer_len;
  BufferPool pool(min_buffer_len, TOTAL_MEM, TOTAL_MEM);
  global_reservations_.InitRootTracker(NewProfile(), TOTAL_MEM);
  MemTracker global_tracker(TOTAL_MEM);
  TestRandomInternalImpl(
      &pool, NewFileGroup(), &global_tracker, &rng_, SINGLE_THREADED_TID, multiple_pins);
  global_reservations_.Close();
}

// Multi-threaded execution of the TestRandomInternalImpl.
void BufferPoolTest::TestRandomInternalMulti(
    int num_threads, int64_t min_buffer_len, bool multiple_pins) {
  const int MAX_NUM_BUFFERS_PER_THREAD = 200;
  const int64_t TOTAL_MEM = num_threads * MAX_NUM_BUFFERS_PER_THREAD * min_buffer_len;
  BufferPool pool(min_buffer_len, TOTAL_MEM, TOTAL_MEM);
  global_reservations_.InitRootTracker(NewProfile(), TOTAL_MEM);
  MemTracker global_tracker(TOTAL_MEM);
  FileGroup* shared_file_group = NewFileGroup();
  thread_group workers;
  vector<mt19937> rngs = RandTestUtil::CreateThreadLocalRngs(num_threads, &rng_);
  for (int i = 0; i < num_threads; ++i) {
    workers.add_thread(new thread(
        [this, &pool, shared_file_group, &global_tracker, &rngs, i, multiple_pins]() {
          TestRandomInternalImpl(
              &pool, shared_file_group, &global_tracker, &rngs[i], i, multiple_pins);
        }));
  }

  AtomicInt32 stop_maintenance(0);
  thread* maintenance_thread = new thread([this, &pool, &stop_maintenance]() {
    while (stop_maintenance.Load() == 0) {
      pool.Maintenance();
      SleepForMs(50);
    }
  });
  workers.join_all();
  stop_maintenance.Add(1);
  maintenance_thread->join();
  global_reservations_.Close();
}

/// Randomly issue AllocateBuffer(), FreeBuffer(), CreatePage(), Pin(), Unpin(), and
/// DestroyPage() calls. All calls made are legal - error conditions are not expected.
/// When executed in single-threaded mode 'tid' should be SINGLE_THREADED_TID. If
/// 'multiple_pins' is true, pages can be pinned multiple times (useful to test this
/// functionality). Otherwise they are only pinned once (useful to test the case when
/// memory is more committed).
void BufferPoolTest::TestRandomInternalImpl(BufferPool* pool, FileGroup* file_group,
    MemTracker* parent_mem_tracker, mt19937* rng, int tid, bool multiple_pins) {
  // Encrypting and decrypting is expensive - reduce iterations when encryption is on.
  int num_iterations = FLAGS_disk_spill_encryption ? 5000 : 50000;
  // All the existing pages and buffers along with the sentinel values written to them.
  vector<pair<PageHandle, int>> pages;
  vector<pair<BufferHandle, int>> buffers;

  /// Pick a power-of-two buffer sizes that are up to 2^4 times the minimum buffer length.
  uniform_int_distribution<int> buffer_exponent_dist(0, 4);

  ClientHandle client;
  ASSERT_OK(pool->RegisterClient(Substitute("$0", tid), file_group, &global_reservations_,
      obj_pool_.Add(new MemTracker(-1, "", parent_mem_tracker)), 1L << 48, NewProfile(),
      &client));

  for (int i = 0; i < num_iterations; ++i) {
    if ((i % 10000) == 0) LOG(ERROR) << " Iteration " << i << endl;
    // Pick an operation.
    // New page: 15%
    // Pin a page and block waiting for the result: 20%
    // Pin a page and let it continue asynchronously: 10%
    // Unpin a pinned page: 25% (< Pin prob. so that memory consumption increases).
    // Destroy page: 10% (< New page prob. so that number of pages grows over time).
    // Allocate buffer: 10%
    // Free buffer: 9.9%
    // Switch core that the thread is executing on: 0.1%
    double p = uniform_real_distribution<double>(0.0, 1.0)(*rng);
    if (p < 0.15) {
      // Create a new page.
      int64_t page_len = pool->min_buffer_len() << (buffer_exponent_dist)(*rng);
      if (!client.IncreaseReservationToFit(page_len)) continue;
      PageHandle new_page;
      ASSERT_OK(pool->CreatePage(&client, page_len, &new_page));
      int data = (*rng)();
      WriteData(new_page, data);
      pages.emplace_back(move(new_page), data);
    } else if (p < 0.45) {
      // Pin a page asynchronously.
      if (pages.empty()) continue;
      int rand_pick = uniform_int_distribution<int>(0, pages.size() - 1)(*rng);
      PageHandle* page = &pages[rand_pick].first;
      if (!client.IncreaseReservationToFit(page->len())) continue;
      if (!page->is_pinned() || multiple_pins) ASSERT_OK(pool->Pin(&client, page));
      // Block on the pin and verify data for sync pins.
      if (p < 0.35) VerifyData(*page, pages[rand_pick].second);
    } else if (p < 0.70) {
      // Unpin a pinned page.
      if (pages.empty()) continue;
      int rand_pick = uniform_int_distribution<int>(0, pages.size() - 1)(*rng);
      PageHandle* page = &pages[rand_pick].first;
      if (page->is_pinned()) {
        VerifyData(*page, pages[rand_pick].second);
        pool->Unpin(&client, page);
      }
    } else if (p < 0.80) {
      // Destroy a page.
      if (pages.empty()) continue;
      int rand_pick = uniform_int_distribution<int>(0, pages.size() - 1)(*rng);
      auto page_data = move(pages[rand_pick]);
      if (page_data.first.is_pinned()) VerifyData(page_data.first, page_data.second);
      pages[rand_pick] = move(pages.back());
      pages.pop_back();
      pool->DestroyPage(&client, &page_data.first);
    } else if (p < 0.90) {
      // Allocate a buffer. Pick a random power-of-two size that is up to 2^4
      // times the minimum buffer length.
      int64_t buffer_len = pool->min_buffer_len() << (buffer_exponent_dist)(*rng);
      if (!client.IncreaseReservationToFit(buffer_len)) continue;
      BufferHandle new_buffer;
      ASSERT_OK(pool->AllocateBuffer(&client, buffer_len, &new_buffer));
      int data = (*rng)();
      WriteData(new_buffer, data);
      buffers.emplace_back(move(new_buffer), data);
    } else if (p < 0.999) {
      // Free a buffer.
      if (buffers.empty()) continue;
      int rand_pick = uniform_int_distribution<int>(0, buffers.size() - 1)(*rng);
      auto buffer_data = move(buffers[rand_pick]);
      buffers[rand_pick] = move(buffers.back());
      buffers.pop_back();
      pool->FreeBuffer(&client, &buffer_data.first);
    } else {
      CpuTestUtil::PinToRandomCore(rng);
    }
  }

  // The client needs to delete all its pages.
  for (auto& page : pages) pool->DestroyPage(&client, &page.first);
  for (auto& buffer : buffers) pool->FreeBuffer(&client, &buffer.first);
  pool->DeregisterClient(&client);
}

/// Test basic SubReservation functionality.
TEST_F(BufferPoolTest, SubReservation) {
  const int64_t TOTAL_MEM = TEST_BUFFER_LEN * 10;
  global_reservations_.InitRootTracker(NULL, TOTAL_MEM);
  BufferPool pool(TEST_BUFFER_LEN, TOTAL_MEM, TOTAL_MEM);
  BufferPool::ClientHandle client;
  ASSERT_OK(pool.RegisterClient("test client", NULL, &global_reservations_, NULL,
      TOTAL_MEM, NewProfile(), &client));
  ASSERT_TRUE(client.IncreaseReservationToFit(TEST_BUFFER_LEN));

  BufferPool::SubReservation subreservation(&client);
  BufferPool::BufferHandle buffer;
  // Save and check that the reservation moved as expected.
  client.SaveReservation(&subreservation, TEST_BUFFER_LEN);
  EXPECT_EQ(0, client.GetUnusedReservation());
  EXPECT_EQ(TEST_BUFFER_LEN, subreservation.GetReservation());

  // Should not be able to allocate from client since the reservation was moved.
  IMPALA_ASSERT_DEBUG_DEATH(AllocateAndFree(&pool, &client, TEST_BUFFER_LEN), "");

  // Restore and check that the reservation moved as expected.
  client.RestoreReservation(&subreservation, TEST_BUFFER_LEN);
  EXPECT_EQ(TEST_BUFFER_LEN, client.GetUnusedReservation());
  EXPECT_EQ(0, subreservation.GetReservation());

  // Should be able to allocate from the client after restoring.
  ASSERT_OK(AllocateAndFree(&pool, &client, TEST_BUFFER_LEN));
  EXPECT_EQ(TEST_BUFFER_LEN, client.GetUnusedReservation());

  subreservation.Close();
  pool.DeregisterClient(&client);
}

// Check that we can decrease reservation without violating any buffer pool invariants.
TEST_F(BufferPoolTest, DecreaseReservation) {
  const int MAX_NUM_BUFFERS = 4;
  const int64_t TOTAL_MEM = MAX_NUM_BUFFERS * TEST_BUFFER_LEN;
  global_reservations_.InitRootTracker(NewProfile(), TOTAL_MEM);
  BufferPool pool(TEST_BUFFER_LEN, TOTAL_MEM, TOTAL_MEM);

  ClientHandle client;
  ASSERT_OK(pool.RegisterClient("test client", NewFileGroup(), &global_reservations_,
      nullptr, TOTAL_MEM, NewProfile(), &client));
  ASSERT_TRUE(client.IncreaseReservation(TOTAL_MEM));

  vector<PageHandle> pages;
  CreatePages(&pool, &client, TEST_BUFFER_LEN, TOTAL_MEM, &pages);
  WriteData(pages, 0);

  // Unpin pages and decrease reservation while the writes are in flight.
  UnpinAll(&pool, &client, &pages);
  ASSERT_OK(client.DecreaseReservationTo(2 * TEST_BUFFER_LEN));
  // Two pages must be clean to stay within reservation
  EXPECT_GE(pool.GetNumCleanPages(), 2);
  EXPECT_EQ(2 * TEST_BUFFER_LEN, client.GetReservation());

  // Decrease it further after the pages are evicted.
  WaitForAllWrites(&client);
  ASSERT_OK(client.DecreaseReservationTo(TEST_BUFFER_LEN));
  EXPECT_GE(pool.GetNumCleanPages(), 3);
  EXPECT_EQ(TEST_BUFFER_LEN, client.GetReservation());

  // Check that we can still use the reservation.
  ASSERT_OK(AllocateAndFree(&pool, &client, TEST_BUFFER_LEN));
  EXPECT_EQ(1, NumEvicted(pages));

  // Check that we can decrease it to zero.
  ASSERT_OK(client.DecreaseReservationTo(0));
  EXPECT_EQ(0, client.GetReservation());

  DestroyAll(&pool, &client, &pages);
  pool.DeregisterClient(&client);
  global_reservations_.Close();
}

// Test concurrent operations using the same client and different buffers.
TEST_F(BufferPoolTest, ConcurrentBufferOperations) {
  const int DELETE_THREADS = 2;
  const int ALLOCATE_THREADS = 2;
  const int NUM_ALLOCATIONS_PER_THREAD = 128;
  const int MAX_NUM_BUFFERS = 16;
  const int64_t TOTAL_MEM = MAX_NUM_BUFFERS * TEST_BUFFER_LEN;
  global_reservations_.InitRootTracker(NewProfile(), TOTAL_MEM);
  BufferPool pool(TEST_BUFFER_LEN, TOTAL_MEM, TOTAL_MEM);
  BufferPool::ClientHandle client;
  ASSERT_OK(pool.RegisterClient("test client", nullptr, &global_reservations_, nullptr,
      TOTAL_MEM, NewProfile(), &client));
  ASSERT_TRUE(client.IncreaseReservationToFit(TOTAL_MEM));

  thread_group allocate_threads;
  thread_group delete_threads;
  AtomicInt64 available_reservation(TOTAL_MEM);

  // Queue of buffers to be deleted, along with the first byte of the data in
  // the buffer, for validation purposes.
  BlockingQueue<pair<uint8_t, BufferHandle>> delete_queue(MAX_NUM_BUFFERS);

  // Allocate threads allocate buffers whenever able and enqueue them.
  for (int i = 0; i < ALLOCATE_THREADS; ++i) {
    allocate_threads.add_thread(new thread([&] {
        for (int i = 0; i < NUM_ALLOCATIONS_PER_THREAD; ++i) {
          // Try to deduct reservation.
          while (true) {
            int64_t val = available_reservation.Load();
            if (val >= TEST_BUFFER_LEN
                && available_reservation.CompareAndSwap(val, val - TEST_BUFFER_LEN)) {
              break;
            }
          }
          BufferHandle buffer;
          ASSERT_OK(pool.AllocateBuffer(&client, TEST_BUFFER_LEN, &buffer));
          uint8_t first_byte = static_cast<uint8_t>(i % 256);
          buffer.data()[0] = first_byte;
          delete_queue.BlockingPut(pair<uint8_t, BufferHandle>(first_byte, move(buffer)));
        }
        }));
  }

  // Delete threads pull buffers off the queue and free them.
  for (int i = 0; i < DELETE_THREADS; ++i) {
    delete_threads.add_thread(new thread([&] {
          pair<uint8_t, BufferHandle> item;
          while (delete_queue.BlockingGet(&item)) {
            ASSERT_EQ(item.first, item.second.data()[0]);
            pool.FreeBuffer(&client, &item.second);
            available_reservation.Add(TEST_BUFFER_LEN);
          }
        }));

  }
  allocate_threads.join_all();
  delete_queue.Shutdown();
  delete_threads.join_all();
  pool.DeregisterClient(&client);
  global_reservations_.Close();
}
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport();
  ABORT_IF_ERROR(impala::LlvmCodeGen::InitializeLlvm());
  int result = 0;
  for (bool encryption : {false, true}) {
    for (bool numa : {false, true}) {
      if (!numa && encryption) continue; // Not an interesting combination.
      impala::CpuTestUtil::SetupFakeNuma(numa);
      FLAGS_disk_spill_encryption = encryption;
      std::cerr << "+==================================================" << std::endl
                << "| Running tests with encryption=" << encryption << " numa=" << numa
                << std::endl
                << "+==================================================" << std::endl;
      if (RUN_ALL_TESTS() != 0) result = 1;
    }
  }
  return result;
}
