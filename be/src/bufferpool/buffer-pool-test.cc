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
#include "testutil/test-macros.h"

#include "common/names.h"

using strings::Substitute;

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
  const static int64_t TEST_PAGE_LEN = 1024;

  /// Test helper to simulate registering then deregistering a number of queries with
  /// the given initial reservation and reservation limit.
  void RegisterQueriesAndClients(BufferPool* pool, int query_id_hi, int num_queries,
      int64_t initial_query_reservation, int64_t query_reservation_limit);

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

const int64_t BufferPoolTest::TEST_PAGE_LEN;

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
      EXPECT_OK(pool->RegisterClient(name, &client_reservations[i][j], &clients[i][j]));
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

  BufferPool pool(TEST_PAGE_LEN, total_mem);

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

  BufferPool pool(TEST_PAGE_LEN, total_mem);

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

/// Test that reservation setup fails if the initial buffers cannot be fulfilled.
TEST_F(BufferPoolTest, QueryReservationsUnfulfilled) {
  Status status;
  int num_queries = 128;
  int64_t reservation_per_query = 128;
  // Won't be able to fulfill initial reservation for last query.
  int64_t total_mem = num_queries * reservation_per_query - 1;
  global_reservations_.InitRootTracker(NewProfile(), total_mem);

  for (int i = 0; i < num_queries; ++i) {
    ReservationTracker* query_tracker = GetQueryReservationTracker(i);
    query_tracker->InitChildTracker(
        NewProfile(), &global_reservations_, NULL, 2 * reservation_per_query);
    bool got_initial_reservation =
        query_tracker->IncreaseReservationToFit(reservation_per_query);
    if (i < num_queries - 1) {
      ASSERT_TRUE(got_initial_reservation);
    } else {
      ASSERT_FALSE(got_initial_reservation);

      // Getting the initial reservation should succeed after freeing up buffers from
      // other query.
      GetQueryReservationTracker(i - 1)->Close();
      ASSERT_TRUE(query_tracker->IncreaseReservationToFit(reservation_per_query));
    }
  }
}
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  return RUN_ALL_TESTS();
}
