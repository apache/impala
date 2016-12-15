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

#include <limits>
#include <string>
#include <vector>

#include "runtime/bufferpool/reservation-tracker.h"
#include "common/init.h"
#include "common/object-pool.h"
#include "runtime/mem-tracker.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

namespace impala {

class ReservationTrackerTest : public ::testing::Test {
 public:
  virtual void SetUp() {}

  virtual void TearDown() {
    root_.Close();
    obj_pool_.Clear();
  }

  /// The minimum allocation size used in most tests.
  const static int64_t MIN_BUFFER_LEN = 1024;

 protected:
  RuntimeProfile* NewProfile() {
    return obj_pool_.Add(new RuntimeProfile(&obj_pool_, "test profile"));
  }

  ObjectPool obj_pool_;

  ReservationTracker root_;

  scoped_ptr<RuntimeProfile> profile_;
};

const int64_t ReservationTrackerTest::MIN_BUFFER_LEN;

TEST_F(ReservationTrackerTest, BasicSingleTracker) {
  const int64_t limit = 16;
  root_.InitRootTracker(NULL, limit);
  ASSERT_EQ(0, root_.GetReservation());
  ASSERT_EQ(0, root_.GetUsedReservation());

  // Fail to increase reservation.
  ASSERT_FALSE(root_.IncreaseReservation(limit + 1));
  ASSERT_EQ(0, root_.GetReservation());
  ASSERT_EQ(0, root_.GetUsedReservation());
  ASSERT_EQ(0, root_.GetUnusedReservation());

  // Successfully increase reservation.
  ASSERT_TRUE(root_.IncreaseReservation(limit - 1));
  ASSERT_EQ(limit - 1, root_.GetReservation());
  ASSERT_EQ(0, root_.GetUsedReservation());
  ASSERT_EQ(limit - 1, root_.GetUnusedReservation());

  // Adjust usage.
  root_.AllocateFrom(2);
  ASSERT_EQ(limit - 1, root_.GetReservation());
  ASSERT_EQ(2, root_.GetUsedReservation());
  ASSERT_EQ(limit - 3, root_.GetUnusedReservation());
  root_.ReleaseTo(1);
  ASSERT_EQ(1, root_.GetUsedReservation());
  root_.ReleaseTo(1);
  ASSERT_EQ(0, root_.GetUsedReservation());
  ASSERT_EQ(limit - 1, root_.GetReservation());
  ASSERT_EQ(limit - 1, root_.GetUnusedReservation());
}

TEST_F(ReservationTrackerTest, BasicTwoLevel) {
  const int64_t limit = 16;
  root_.InitRootTracker(NULL, limit);

  const int64_t root_reservation = limit / 2;
  // Get half of the limit as an initial reservation.
  ASSERT_TRUE(root_.IncreaseReservation(root_reservation));
  ASSERT_EQ(root_reservation, root_.GetReservation());
  ASSERT_EQ(root_reservation, root_.GetUnusedReservation());
  ASSERT_EQ(0, root_.GetUsedReservation());
  ASSERT_EQ(0, root_.GetChildReservations());

  ReservationTracker child;
  child.InitChildTracker(NULL, &root_, NULL, numeric_limits<int64_t>::max());

  const int64_t child_reservation = root_reservation + 1;
  // Get parent's reservation plus a bit more.
  ASSERT_TRUE(child.IncreaseReservation(child_reservation));

  ASSERT_EQ(child_reservation, root_.GetReservation());
  ASSERT_EQ(0, root_.GetUnusedReservation());
  ASSERT_EQ(0, root_.GetUsedReservation());
  ASSERT_EQ(child_reservation, root_.GetChildReservations());

  ASSERT_EQ(child_reservation, child.GetReservation());
  ASSERT_EQ(child_reservation, child.GetUnusedReservation());
  ASSERT_EQ(0, child.GetUsedReservation());
  ASSERT_EQ(0, child.GetChildReservations());

  // Check that child allocation is reflected correctly.
  child.AllocateFrom(1);
  ASSERT_EQ(child_reservation, child.GetReservation());
  ASSERT_EQ(1, child.GetUsedReservation());
  ASSERT_EQ(0, root_.GetUsedReservation());

  // Check that parent reservation increase is reflected correctly.
  ASSERT_TRUE(root_.IncreaseReservation(1));
  ASSERT_EQ(child_reservation + 1, root_.GetReservation());
  ASSERT_EQ(1, root_.GetUnusedReservation());
  ASSERT_EQ(0, root_.GetUsedReservation());
  ASSERT_EQ(child_reservation, root_.GetChildReservations());
  ASSERT_EQ(child_reservation, child.GetReservation());

  // Check that parent allocation is reflected correctly.
  root_.AllocateFrom(1);
  ASSERT_EQ(child_reservation + 1, root_.GetReservation());
  ASSERT_EQ(0, root_.GetUnusedReservation());
  ASSERT_EQ(1, root_.GetUsedReservation());

  // Release allocations.
  root_.ReleaseTo(1);
  ASSERT_EQ(0, root_.GetUsedReservation());
  child.ReleaseTo(1);
  ASSERT_EQ(0, child.GetUsedReservation());

  // Child reservation should be returned all the way up the tree.
  child.DecreaseReservation(1);
  ASSERT_EQ(child_reservation, root_.GetReservation());
  ASSERT_EQ(child_reservation - 1, child.GetReservation());
  ASSERT_EQ(child_reservation - 1, root_.GetChildReservations());

  // Closing the child should release its reservation.
  child.Close();
  ASSERT_EQ(1, root_.GetReservation());
  ASSERT_EQ(0, root_.GetChildReservations());
}

TEST_F(ReservationTrackerTest, CloseIdempotency) {
  // Check we can close before opening.
  root_.Close();

  const int64_t limit = 16;
  root_.InitRootTracker(NULL, limit);

  // Check we can close twice
  root_.Close();
  root_.Close();
}

// Test that the tracker's reservation limit is enforced.
TEST_F(ReservationTrackerTest, ReservationLimit) {
  Status status;
  // Setup trackers so that there is a spare buffer that the client isn't entitled to.
  int64_t total_mem = MIN_BUFFER_LEN * 3;
  int64_t client_limit = MIN_BUFFER_LEN * 2;
  root_.InitRootTracker(NULL, total_mem);

  ReservationTracker* client_tracker = obj_pool_.Add(new ReservationTracker());
  client_tracker->InitChildTracker(NULL, &root_, NULL, client_limit);

  // We can only increase reservation up to the client's limit.
  ASSERT_FALSE(client_tracker->IncreaseReservation(client_limit + 1));
  ASSERT_TRUE(client_tracker->IncreaseReservation(client_limit));
  ASSERT_FALSE(client_tracker->IncreaseReservation(1));

  client_tracker->Close();
}

// Test that parent's reservation limit is enforced.
TEST_F(ReservationTrackerTest, ParentReservationLimit) {
  Status status;
  // Setup reservations so that there is a spare buffer.
  int64_t total_mem = MIN_BUFFER_LEN * 4;
  int64_t parent_limit = MIN_BUFFER_LEN * 3;
  int64_t other_client_reservation = MIN_BUFFER_LEN;
  root_.InitRootTracker(NULL, total_mem);

  // The child reservation limit is higher than the parent reservation limit, so the
  // parent limit is the effective limit.
  ReservationTracker* query_tracker = obj_pool_.Add(new ReservationTracker());
  ReservationTracker* client_tracker = obj_pool_.Add(new ReservationTracker());
  query_tracker->InitChildTracker(NULL, &root_, NULL, parent_limit);
  client_tracker->InitChildTracker(NULL, query_tracker, NULL, total_mem * 10);

  ReservationTracker* other_client_tracker = obj_pool_.Add(new ReservationTracker());
  other_client_tracker->InitChildTracker(NULL, query_tracker, NULL, total_mem);
  ASSERT_TRUE(other_client_tracker->IncreaseReservationToFit(other_client_reservation));
  ASSERT_EQ(root_.GetUsedReservation(), 0);
  ASSERT_EQ(root_.GetChildReservations(), other_client_reservation);
  ASSERT_EQ(query_tracker->GetUsedReservation(), 0);
  ASSERT_EQ(query_tracker->GetUnusedReservation(), 0);

  // Can only increase reservation up to parent limit, excluding other reservations.
  int64_t effective_limit = parent_limit - other_client_reservation;
  ASSERT_FALSE(client_tracker->IncreaseReservation(effective_limit + MIN_BUFFER_LEN));
  ASSERT_TRUE(client_tracker->IncreaseReservation(effective_limit));
  ASSERT_FALSE(client_tracker->IncreaseReservation(MIN_BUFFER_LEN));

  // Check that tracker hierarchy reports correct usage.
  ASSERT_EQ(root_.GetUsedReservation(), 0);
  ASSERT_EQ(root_.GetChildReservations(), parent_limit);
  ASSERT_EQ(query_tracker->GetUsedReservation(), 0);
  ASSERT_EQ(query_tracker->GetUnusedReservation(), 0);

  client_tracker->Close();
  other_client_tracker->Close();
  query_tracker->Close();
}

/// Test integration of ReservationTracker with MemTracker.
TEST_F(ReservationTrackerTest, MemTrackerIntegrationTwoLevel) {
  // Setup a 2-level hierarchy of trackers. The child ReservationTracker is linked to
  // the child MemTracker. We add various limits at different places to enable testing
  // of different code paths.
  root_.InitRootTracker(NewProfile(), MIN_BUFFER_LEN * 100);
  MemTracker root_mem_tracker;
  MemTracker child_mem_tracker1(-1, "Child 1", &root_mem_tracker);
  MemTracker child_mem_tracker2(MIN_BUFFER_LEN * 50, "Child 2", &root_mem_tracker);
  ReservationTracker child_reservations1, child_reservations2;
  child_reservations1.InitChildTracker(
      NewProfile(), &root_, &child_mem_tracker1, 500 * MIN_BUFFER_LEN);
  child_reservations2.InitChildTracker(
      NewProfile(), &root_, &child_mem_tracker2, 75 * MIN_BUFFER_LEN);

  // Check that a single buffer reservation is accounted correctly.
  ASSERT_TRUE(child_reservations1.IncreaseReservation(MIN_BUFFER_LEN));
  ASSERT_EQ(MIN_BUFFER_LEN, child_reservations1.GetReservation());
  ASSERT_EQ(MIN_BUFFER_LEN, child_mem_tracker1.consumption());
  ASSERT_EQ(MIN_BUFFER_LEN, root_mem_tracker.consumption());
  ASSERT_EQ(MIN_BUFFER_LEN, root_.GetChildReservations());

  // Check that a buffer reservation from the other child is accounted correctly.
  ASSERT_TRUE(child_reservations2.IncreaseReservation(MIN_BUFFER_LEN));
  ASSERT_EQ(MIN_BUFFER_LEN, child_reservations2.GetReservation());
  ASSERT_EQ(MIN_BUFFER_LEN, child_mem_tracker2.consumption());
  ASSERT_EQ(2 * MIN_BUFFER_LEN, root_mem_tracker.consumption());
  ASSERT_EQ(2 * MIN_BUFFER_LEN, root_.GetChildReservations());

  // Check that hitting the MemTracker limit leaves things in a consistent state.
  ASSERT_FALSE(child_reservations2.IncreaseReservation(MIN_BUFFER_LEN * 50));
  ASSERT_EQ(MIN_BUFFER_LEN, child_reservations1.GetReservation());
  ASSERT_EQ(MIN_BUFFER_LEN, child_mem_tracker1.consumption());
  ASSERT_EQ(MIN_BUFFER_LEN, child_reservations2.GetReservation());
  ASSERT_EQ(MIN_BUFFER_LEN, child_mem_tracker2.consumption());
  ASSERT_EQ(2 * MIN_BUFFER_LEN, root_mem_tracker.consumption());
  ASSERT_EQ(2 * MIN_BUFFER_LEN, root_.GetChildReservations());

  // Check that hitting the ReservationTracker's local limit leaves things in a
  // consistent state.
  ASSERT_FALSE(child_reservations2.IncreaseReservation(MIN_BUFFER_LEN * 75));
  ASSERT_EQ(MIN_BUFFER_LEN, child_reservations1.GetReservation());
  ASSERT_EQ(MIN_BUFFER_LEN, child_mem_tracker1.consumption());
  ASSERT_EQ(MIN_BUFFER_LEN, child_reservations2.GetReservation());
  ASSERT_EQ(MIN_BUFFER_LEN, child_mem_tracker2.consumption());
  ASSERT_EQ(2 * MIN_BUFFER_LEN, root_mem_tracker.consumption());
  ASSERT_EQ(2 * MIN_BUFFER_LEN, root_.GetChildReservations());

  // Check that hitting the ReservationTracker's parent's limit after the
  // MemTracker consumption is incremented leaves things in a consistent state.
  ASSERT_FALSE(child_reservations1.IncreaseReservation(MIN_BUFFER_LEN * 100));
  ASSERT_EQ(MIN_BUFFER_LEN, child_reservations1.GetReservation());
  ASSERT_EQ(MIN_BUFFER_LEN, child_mem_tracker1.consumption());
  ASSERT_EQ(MIN_BUFFER_LEN, child_reservations2.GetReservation());
  ASSERT_EQ(MIN_BUFFER_LEN, child_mem_tracker2.consumption());
  ASSERT_EQ(2 * MIN_BUFFER_LEN, root_mem_tracker.consumption());
  ASSERT_EQ(2 * MIN_BUFFER_LEN, root_.GetChildReservations());

  // Check that released memory is decremented from all trackers correctly.
  child_reservations1.DecreaseReservation(MIN_BUFFER_LEN);
  child_reservations2.DecreaseReservation(MIN_BUFFER_LEN);
  ASSERT_EQ(0, child_reservations2.GetReservation());
  ASSERT_EQ(0, child_mem_tracker2.consumption());
  ASSERT_EQ(0, root_mem_tracker.consumption());
  ASSERT_EQ(0, root_.GetUsedReservation());

  child_reservations1.Close();
  child_reservations2.Close();
  child_mem_tracker1.UnregisterFromParent();
  child_mem_tracker2.UnregisterFromParent();
}

TEST_F(ReservationTrackerTest, MemTrackerIntegrationMultiLevel) {
  const int HIERARCHY_DEPTH = 5;
  // Setup a multi-level hierarchy of trackers and ensure that consumption is reported
  // correctly.
  ReservationTracker reservations[HIERARCHY_DEPTH];
  scoped_ptr<MemTracker> mem_trackers[HIERARCHY_DEPTH];

  // We can only handle MemTracker limits at the topmost linked ReservationTracker,
  // so avoid adding limits at lower level.
  const int LIMIT = HIERARCHY_DEPTH;
  vector<int> mem_limits({LIMIT * 10, LIMIT, -1, -1, -1});

  // Root trackers aren't linked.
  mem_trackers[0].reset(new MemTracker(mem_limits[0]));
  reservations[0].InitRootTracker(NewProfile(), 500);
  for (int i = 1; i < HIERARCHY_DEPTH; ++i) {
    mem_trackers[i].reset(new MemTracker(
        mem_limits[i], Substitute("Child $0", i), mem_trackers[i - 1].get()));
    reservations[i].InitChildTracker(
        NewProfile(), &reservations[i - 1], mem_trackers[i].get(), 500);
  }

  vector<int> interesting_amounts({LIMIT - 1, LIMIT, LIMIT + 1});

  // Test that all limits and consumption correctly reported when consuming
  // from a non-root ReservationTracker that is connected to a MemTracker.
  for (int level = 1; level < HIERARCHY_DEPTH; ++level) {
    int64_t lowest_limit = mem_trackers[level]->lowest_limit();
    for (int amount : interesting_amounts) {
      bool increased = reservations[level].IncreaseReservation(amount);
      if (lowest_limit == -1 || amount <= lowest_limit) {
        // The increase should go through.
        ASSERT_TRUE(increased) << reservations[level].DebugString();
        ASSERT_EQ(amount, reservations[level].GetReservation());
        ASSERT_EQ(amount, mem_trackers[level]->consumption());
        for (int ancestor = 0; ancestor < level; ++ancestor) {
          ASSERT_EQ(amount, reservations[ancestor].GetChildReservations());
          ASSERT_EQ(amount, mem_trackers[ancestor]->consumption());
        }

        LOG(INFO) << "\n" << mem_trackers[0]->LogUsage();
        reservations[level].DecreaseReservation(amount);
      } else {
        ASSERT_FALSE(increased);
      }
      // We should be back in the original state.
      for (int i = 0; i < HIERARCHY_DEPTH; ++i) {
        ASSERT_EQ(0, reservations[i].GetReservation()) << i << ": "
                                                       << reservations[i].DebugString();
        ASSERT_EQ(0, reservations[i].GetChildReservations());
        ASSERT_EQ(0, mem_trackers[i]->consumption());
      }
    }
  }

  // "Pull down" a reservation from the top of the hierarchy level-by-level to the
  // leaves, checking that everything is consistent at each step.
  for (int level = 0; level < HIERARCHY_DEPTH; ++level) {
    const int amount = LIMIT;
    ASSERT_TRUE(reservations[level].IncreaseReservation(amount));
    ASSERT_EQ(amount, reservations[level].GetReservation());
    ASSERT_EQ(0, reservations[level].GetUsedReservation());
    if (level != 0) ASSERT_EQ(amount, mem_trackers[level]->consumption());
    for (int ancestor = 0; ancestor < level; ++ancestor) {
      ASSERT_EQ(0, reservations[ancestor].GetUsedReservation());
      ASSERT_EQ(amount, reservations[ancestor].GetChildReservations());
      ASSERT_EQ(amount, mem_trackers[ancestor]->consumption());
    }
    reservations[level].DecreaseReservation(amount);
  }

  for (int i = HIERARCHY_DEPTH - 1; i >= 0; --i) {
    reservations[i].Close();
    if (i != 0) mem_trackers[i]->UnregisterFromParent();
  }
}
}

IMPALA_TEST_MAIN();
