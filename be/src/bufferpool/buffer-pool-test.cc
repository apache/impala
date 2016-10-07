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
#include <boost/scoped_ptr.hpp>
#include <boost/thread/thread.hpp>
#include <boost/unordered_map.hpp>
#include <cstdlib>
#include <string>
#include <vector>

#include "bufferpool/buffer-pool.h"
#include "bufferpool/reservation-tracker.h"
#include "common/init.h"
#include "common/object-pool.h"
#include "testutil/death-test-util.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

namespace impala {

class BufferPoolTest : public ::testing::Test {
 public:
  virtual void SetUp() {}

  virtual void TearDown() {
    for (auto entry : query_reservations_) {
      ReservationTracker* tracker = entry.second;
      tracker->Close();
    }

    global_reservations_.Close();
    obj_pool_.Clear();
  }

  /// The minimum buffer size used in most tests.
  const static int64_t TEST_BUFFER_LEN = 1024;

  /// Test helper to simulate registering then deregistering a number of queries with
  /// the given initial reservation and reservation limit.
  void RegisterQueriesAndClients(BufferPool* pool, int query_id_hi, int num_queries,
      int64_t initial_query_reservation, int64_t query_reservation_limit);

  /// Create and destroy a page multiple times.
  void CreatePageLoop(BufferPool* pool, ReservationTracker* parent_tracker, int num_ops);

 protected:
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
    return obj_pool_.Add(new RuntimeProfile(&obj_pool_, "test profile"));
  }

  ObjectPool obj_pool_;

  ReservationTracker global_reservations_;

  // Map from query_id to the reservation tracker for that query. Reads and modifications
  // of the map are protected by query_reservations_lock_.
  unordered_map<int64_t, ReservationTracker*> query_reservations_;
  SpinLock query_reservations_lock_;
};

const int64_t BufferPoolTest::TEST_BUFFER_LEN;

void BufferPoolTest::RegisterQueriesAndClients(BufferPool* pool, int query_id_hi,
    int num_queries, int64_t initial_query_reservation, int64_t query_reservation_limit) {
  Status status;

  int clients_per_query = 32;
  BufferPool::Client* clients[num_queries];
  ReservationTracker* client_reservations[num_queries];

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

    clients[i] = new BufferPool::Client[clients_per_query];
    client_reservations[i] = new ReservationTracker[clients_per_query];

    for (int j = 0; j < clients_per_query; ++j) {
      int64_t initial_client_reservation =
          initial_query_reservation / clients_per_query + j
          < initial_query_reservation % clients_per_query;
      // Reservation limit can be anything greater or equal to the initial reservation.
      int64_t client_reservation_limit = initial_client_reservation + rand() % 100000;
      client_reservations[i][j].InitChildTracker(
          NULL, query_reservation, NULL, client_reservation_limit);
      EXPECT_TRUE(
          client_reservations[i][j].IncreaseReservationToFit(initial_client_reservation));
      string name = Substitute("Client $0 for query $1", j, query_id);
      EXPECT_OK(pool->RegisterClient(
          name, &client_reservations[i][j], NewProfile(), &clients[i][j]));
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
      client_reservations[i][j].Close();
    }

    delete[] clients[i];
    delete[] client_reservations[i];

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

  BufferPool pool(TEST_BUFFER_LEN, total_mem);

  RegisterQueriesAndClients(
      &pool, 0, num_concurrent_queries, sum_initial_reservations, reservation_limit);

  DCHECK_EQ(global_reservations_.GetUsedReservation(), 0);
  DCHECK_EQ(global_reservations_.GetChildReservations(), 0);
  DCHECK_EQ(global_reservations_.GetReservation(), 0);
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

  BufferPool pool(TEST_BUFFER_LEN, total_mem);

  // Launch threads, each with a different set of query IDs.
  thread_group workers;
  for (int i = 0; i < num_threads; ++i) {
    workers.add_thread(new thread(bind(&BufferPoolTest::RegisterQueriesAndClients, this,
        &pool, i, queries_per_thread, sum_initial_reservations, reservation_limit)));
  }
  workers.join_all();

  // All the reservations should be released at this point.
  DCHECK_EQ(global_reservations_.GetUsedReservation(), 0);
  DCHECK_EQ(global_reservations_.GetReservation(), 0);
  global_reservations_.Close();
}

/// Test basic page handle creation.
TEST_F(BufferPoolTest, PageCreation) {
  // Allocate many pages, each a power-of-two multiple of the minimum page length.
  int num_pages = 16;
  int64_t max_page_len = TEST_BUFFER_LEN << (num_pages - 1);
  int64_t total_mem = 2 * 2 * max_page_len;
  global_reservations_.InitRootTracker(NULL, total_mem);
  BufferPool pool(TEST_BUFFER_LEN, total_mem);
  ReservationTracker* client_tracker = obj_pool_.Add(new ReservationTracker());
  client_tracker->InitChildTracker(NewProfile(), &global_reservations_, NULL, total_mem);
  ASSERT_TRUE(client_tracker->IncreaseReservation(total_mem));
  BufferPool::Client client;
  ASSERT_OK(pool.RegisterClient("test client", client_tracker, NewProfile(), &client));

  vector<BufferPool::PageHandle> handles(num_pages);

  // Create pages of various valid sizes.
  for (int i = 0; i < num_pages; ++i) {
    int size_multiple = 1 << i;
    int64_t page_len = TEST_BUFFER_LEN * size_multiple;
    int64_t used_before = client_tracker->GetUsedReservation();
    ASSERT_OK(pool.CreatePage(&client, page_len, &handles[i]));
    ASSERT_TRUE(handles[i].is_open());
    ASSERT_TRUE(handles[i].is_pinned());
    ASSERT_TRUE(handles[i].buffer_handle() != NULL);
    ASSERT_TRUE(handles[i].data() != NULL);
    ASSERT_EQ(handles[i].buffer_handle()->data(), handles[i].data());
    ASSERT_EQ(handles[i].len(), page_len);
    ASSERT_EQ(handles[i].buffer_handle()->len(), page_len);
    DCHECK_EQ(client_tracker->GetUsedReservation(), used_before + page_len);
  }

  // Close the handles and check memory consumption.
  for (int i = 0; i < num_pages; ++i) {
    int64_t used_before = client_tracker->GetUsedReservation();
    int page_len = handles[i].len();
    pool.DestroyPage(&client, &handles[i]);
    DCHECK_EQ(client_tracker->GetUsedReservation(), used_before - page_len);
  }

  pool.DeregisterClient(&client);
  client_tracker->Close();

  // All the reservations should be released at this point.
  DCHECK_EQ(global_reservations_.GetReservation(), 0);
  global_reservations_.Close();
}

TEST_F(BufferPoolTest, BufferAllocation) {
  // Allocate many buffers, each a power-of-two multiple of the minimum buffer length.
  int num_buffers = 16;
  int64_t max_buffer_len = TEST_BUFFER_LEN << (num_buffers - 1);
  int64_t total_mem = 2 * 2 * max_buffer_len;
  global_reservations_.InitRootTracker(NULL, total_mem);
  BufferPool pool(TEST_BUFFER_LEN, total_mem);
  ReservationTracker* client_tracker = obj_pool_.Add(new ReservationTracker());
  client_tracker->InitChildTracker(NewProfile(), &global_reservations_, NULL, total_mem);
  ASSERT_TRUE(client_tracker->IncreaseReservationToFit(total_mem));
  BufferPool::Client client;
  ASSERT_OK(pool.RegisterClient("test client", client_tracker, NewProfile(), &client));

  vector<BufferPool::BufferHandle> handles(num_buffers);

  // Create buffers of various valid sizes.
  for (int i = 0; i < num_buffers; ++i) {
    int size_multiple = 1 << i;
    int64_t buffer_len = TEST_BUFFER_LEN * size_multiple;
    int64_t used_before = client_tracker->GetUsedReservation();
    ASSERT_OK(pool.AllocateBuffer(&client, buffer_len, &handles[i]));
    ASSERT_TRUE(handles[i].is_open());
    ASSERT_TRUE(handles[i].data() != NULL);
    ASSERT_EQ(handles[i].len(), buffer_len);
    DCHECK_EQ(client_tracker->GetUsedReservation(), used_before + buffer_len);
  }

  // Close the handles and check memory consumption.
  for (int i = 0; i < num_buffers; ++i) {
    int64_t used_before = client_tracker->GetUsedReservation();
    int buffer_len = handles[i].len();
    pool.FreeBuffer(&client, &handles[i]);
    DCHECK_EQ(client_tracker->GetUsedReservation(), used_before - buffer_len);
  }

  pool.DeregisterClient(&client);
  client_tracker->Close();

  // All the reservations should be released at this point.
  DCHECK_EQ(global_reservations_.GetReservation(), 0);
  global_reservations_.Close();
}

/// Test transfer of buffer handles between clients.
TEST_F(BufferPoolTest, BufferTransfer) {
  // Each client needs to have enough reservation for a buffer.
  const int num_clients = 5;
  int64_t total_mem = num_clients * TEST_BUFFER_LEN;
  global_reservations_.InitRootTracker(NULL, total_mem);
  BufferPool pool(TEST_BUFFER_LEN, total_mem);
  ReservationTracker client_trackers[num_clients];
  BufferPool::Client clients[num_clients];
  BufferPool::BufferHandle handles[num_clients];
  for (int i = 0; i < num_clients; ++i) {
    client_trackers[i].InitChildTracker(
        NewProfile(), &global_reservations_, NULL, TEST_BUFFER_LEN);
    ASSERT_TRUE(client_trackers[i].IncreaseReservationToFit(TEST_BUFFER_LEN));
    ASSERT_OK(pool.RegisterClient(
        "test client", &client_trackers[i], NewProfile(), &clients[i]));
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
      ASSERT_EQ(0, client_trackers[client].GetUsedReservation());
      ASSERT_TRUE(handles[next_client].is_open());
      ASSERT_EQ(TEST_BUFFER_LEN, client_trackers[next_client].GetUsedReservation());
      // The same underlying buffer should be used.
      ASSERT_EQ(data, handles[next_client].data());
    }
  }

  pool.FreeBuffer(&clients[0], &handles[0]);
  for (int i = 0; i < num_clients; ++i) {
    pool.DeregisterClient(&clients[i]);
    client_trackers[i].Close();
  }
  DCHECK_EQ(global_reservations_.GetReservation(), 0);
  global_reservations_.Close();
}

/// Test basic pinning and unpinning.
TEST_F(BufferPoolTest, Pin) {
  int64_t total_mem = TEST_BUFFER_LEN * 1024;
  // Set up client with enough reservation to pin twice.
  int64_t child_reservation = TEST_BUFFER_LEN * 2;
  BufferPool pool(TEST_BUFFER_LEN, total_mem);
  global_reservations_.InitRootTracker(NULL, total_mem);
  ReservationTracker* client_tracker = obj_pool_.Add(new ReservationTracker());
  client_tracker->InitChildTracker(
      NewProfile(), &global_reservations_, NULL, child_reservation);
  ASSERT_TRUE(client_tracker->IncreaseReservationToFit(child_reservation));
  BufferPool::Client client;
  ASSERT_OK(pool.RegisterClient("test client", client_tracker, NewProfile(), &client));

  BufferPool::PageHandle handle1, handle2;

  // Can pin two minimum sized pages.
  ASSERT_OK(pool.CreatePage(&client, TEST_BUFFER_LEN, &handle1));
  ASSERT_TRUE(handle1.is_open());
  ASSERT_TRUE(handle1.is_pinned());
  ASSERT_TRUE(handle1.data() != NULL);
  ASSERT_OK(pool.CreatePage(&client, TEST_BUFFER_LEN, &handle2));
  ASSERT_TRUE(handle2.is_open());
  ASSERT_TRUE(handle2.is_pinned());
  ASSERT_TRUE(handle2.data() != NULL);

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
  ASSERT_OK(pool.CreatePage(&client, TEST_BUFFER_LEN * 2, &double_handle));
  ASSERT_TRUE(double_handle.is_open());
  ASSERT_TRUE(double_handle.is_pinned());
  ASSERT_TRUE(double_handle.data() != NULL);

  // Destroy the pages - test destroying both pinned and unpinned.
  pool.DestroyPage(&client, &handle1);
  pool.DestroyPage(&client, &handle2);
  pool.DestroyPage(&client, &double_handle);

  pool.DeregisterClient(&client);
  client_tracker->Close();
}

/// Creating a page or pinning without sufficient reservation should DCHECK.
TEST_F(BufferPoolTest, PinWithoutReservation) {
  int64_t total_mem = TEST_BUFFER_LEN * 1024;
  BufferPool pool(TEST_BUFFER_LEN, total_mem);
  global_reservations_.InitRootTracker(NULL, total_mem);
  ReservationTracker* client_tracker = obj_pool_.Add(new ReservationTracker());
  client_tracker->InitChildTracker(
      NewProfile(), &global_reservations_, NULL, TEST_BUFFER_LEN);
  BufferPool::Client client;
  ASSERT_OK(pool.RegisterClient("test client", client_tracker, NewProfile(), &client));

  BufferPool::PageHandle handle;
  IMPALA_ASSERT_DEBUG_DEATH(pool.CreatePage(&client, TEST_BUFFER_LEN, &handle), "");

  // Should succeed after increasing reservation.
  ASSERT_TRUE(client_tracker->IncreaseReservationToFit(TEST_BUFFER_LEN));
  ASSERT_OK(pool.CreatePage(&client, TEST_BUFFER_LEN, &handle));

  // But we can't pin again.
  IMPALA_ASSERT_DEBUG_DEATH(pool.Pin(&client, &handle), "");

  pool.DestroyPage(&client, &handle);
  pool.DeregisterClient(&client);
  client_tracker->Close();
}

TEST_F(BufferPoolTest, ExtractBuffer) {
  int64_t total_mem = TEST_BUFFER_LEN * 1024;
  // Set up client with enough reservation for two buffers/pins.
  int64_t child_reservation = TEST_BUFFER_LEN * 2;
  BufferPool pool(TEST_BUFFER_LEN, total_mem);
  global_reservations_.InitRootTracker(NULL, total_mem);
  ReservationTracker* client_tracker = obj_pool_.Add(new ReservationTracker());
  client_tracker->InitChildTracker(
      NewProfile(), &global_reservations_, NULL, child_reservation);
  ASSERT_TRUE(client_tracker->IncreaseReservationToFit(child_reservation));
  BufferPool::Client client;
  ASSERT_OK(pool.RegisterClient("test client", client_tracker, NewProfile(), &client));

  BufferPool::PageHandle page;
  BufferPool::BufferHandle buffer;

  // Test basic buffer extraction.
  for (int len = TEST_BUFFER_LEN; len <= 2 * TEST_BUFFER_LEN; len *= 2) {
    ASSERT_OK(pool.CreatePage(&client, len, &page));
    uint8_t* page_data = page.data();
    pool.ExtractBuffer(&client, &page, &buffer);
    ASSERT_FALSE(page.is_open());
    ASSERT_TRUE(buffer.is_open());
    ASSERT_EQ(len, buffer.len());
    ASSERT_EQ(page_data, buffer.data());
    ASSERT_EQ(len, client_tracker->GetUsedReservation());
    pool.FreeBuffer(&client, &buffer);
    ASSERT_EQ(0, client_tracker->GetUsedReservation());
  }

  // Test that ExtractBuffer() accounts correctly for pin count > 1.
  ASSERT_OK(pool.CreatePage(&client, TEST_BUFFER_LEN, &page));
  uint8_t* page_data = page.data();
  ASSERT_OK(pool.Pin(&client, &page));
  ASSERT_EQ(TEST_BUFFER_LEN * 2, client_tracker->GetUsedReservation());
  pool.ExtractBuffer(&client, &page, &buffer);
  ASSERT_EQ(TEST_BUFFER_LEN, client_tracker->GetUsedReservation());
  ASSERT_FALSE(page.is_open());
  ASSERT_TRUE(buffer.is_open());
  ASSERT_EQ(TEST_BUFFER_LEN, buffer.len());
  ASSERT_EQ(page_data, buffer.data());
  pool.FreeBuffer(&client, &buffer);
  ASSERT_EQ(0, client_tracker->GetUsedReservation());

  // Test that ExtractBuffer() DCHECKs for unpinned pages.
  ASSERT_OK(pool.CreatePage(&client, TEST_BUFFER_LEN, &page));
  pool.Unpin(&client, &page);
  IMPALA_ASSERT_DEBUG_DEATH(pool.ExtractBuffer(&client, &page, &buffer), "");
  pool.DestroyPage(&client, &page);

  pool.DeregisterClient(&client);
  client_tracker->Close();
}

// Test concurrent creation and destruction of pages.
TEST_F(BufferPoolTest, ConcurrentPageCreation) {
  int ops_per_thread = 1024;
  int num_threads = 64;
  // Need enough buffers for all initial reservations.
  int total_mem = num_threads * TEST_BUFFER_LEN;
  global_reservations_.InitRootTracker(NULL, total_mem);

  BufferPool pool(TEST_BUFFER_LEN, total_mem);

  // Launch threads, each with a different set of query IDs.
  thread_group workers;
  for (int i = 0; i < num_threads; ++i) {
    workers.add_thread(new thread(bind(&BufferPoolTest::CreatePageLoop, this, &pool,
        &global_reservations_, ops_per_thread)));
  }

  // Build debug string to test concurrent iteration over pages_ list.
  for (int i = 0; i < 64; ++i) {
    LOG(INFO) << pool.DebugString();
  }
  workers.join_all();

  // All the reservations should be released at this point.
  DCHECK_EQ(global_reservations_.GetChildReservations(), 0);
  global_reservations_.Close();
}

void BufferPoolTest::CreatePageLoop(
    BufferPool* pool, ReservationTracker* parent_tracker, int num_ops) {
  ReservationTracker client_tracker;
  client_tracker.InitChildTracker(NewProfile(), parent_tracker, NULL, TEST_BUFFER_LEN);
  BufferPool::Client client;
  ASSERT_OK(pool->RegisterClient("test client", &client_tracker, NewProfile(), &client));
  for (int i = 0; i < num_ops; ++i) {
    BufferPool::PageHandle handle;
    ASSERT_TRUE(client_tracker.IncreaseReservation(TEST_BUFFER_LEN));
    ASSERT_OK(pool->CreatePage(&client, TEST_BUFFER_LEN, &handle));
    pool->Unpin(&client, &handle);
    ASSERT_OK(pool->Pin(&client, &handle));
    pool->DestroyPage(&client, &handle);
    client_tracker.DecreaseReservation(TEST_BUFFER_LEN);
  }
  pool->DeregisterClient(&client);
  client_tracker.Close();
}

/// Test error path where pool is unable to fulfill a reservation because it cannot evict
/// unpinned pages.
TEST_F(BufferPoolTest, CapacityExhausted) {
  global_reservations_.InitRootTracker(NULL, TEST_BUFFER_LEN);
  // TODO: once we enable spilling, set up buffer pool so that spilling is disabled.
  // Set up pool with one more buffer than reservations (so that we hit the reservation
  // limit instead of the buffer limit).
  BufferPool pool(TEST_BUFFER_LEN, TEST_BUFFER_LEN * 2);

  BufferPool::PageHandle handle1, handle2, handle3;

  BufferPool::Client client;
  ASSERT_OK(
      pool.RegisterClient("test client", &global_reservations_, NewProfile(), &client));
  ASSERT_TRUE(global_reservations_.IncreaseReservation(TEST_BUFFER_LEN));
  ASSERT_OK(pool.CreatePage(&client, TEST_BUFFER_LEN, &handle1));

  // Do not have enough reservations because we pinned the page.
  IMPALA_ASSERT_DEBUG_DEATH(pool.CreatePage(&client, TEST_BUFFER_LEN, &handle2), "");

  // Even with reservations, we can only create one more unpinned page because we don't
  // support eviction yet.
  pool.Unpin(&client, &handle1);
  ASSERT_OK(pool.CreatePage(&client, TEST_BUFFER_LEN, &handle2));
  pool.Unpin(&client, &handle2);
  ASSERT_FALSE(pool.CreatePage(&client, TEST_BUFFER_LEN, &handle3).ok());

  // After destroying a page, we should have a free buffer that we can use.
  pool.DestroyPage(&client, &handle1);
  ASSERT_OK(pool.CreatePage(&client, TEST_BUFFER_LEN, &handle3));

  pool.DestroyPage(&client, &handle2);
  pool.DestroyPage(&client, &handle3);
  pool.DeregisterClient(&client);
}
}

IMPALA_TEST_MAIN();
