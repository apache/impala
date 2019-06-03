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
#include "runtime/bufferpool/reservation-util.h"
#include "runtime/test-env.h"
#include "service/fe-support.h"
#include "common/init.h"
#include "common/object-pool.h"
#include "runtime/mem-tracker.h"
#include "util/memory-metrics.h"
#include "util/pretty-printer.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

namespace impala {

class ReservationTrackerTest : public ::testing::Test {
 public:
  virtual void SetUp() {
    test_env_.reset(new TestEnv());
    ASSERT_OK(test_env_->Init());
    ASSERT_OK(test_env_->CreateQueryState(0, nullptr, &runtime_state_));
  }

  virtual void TearDown() {
    runtime_state_ = nullptr;
    test_env_.reset();
    root_.Close();
    obj_pool_.Clear();
  }

  /// The minimum allocation size used in most tests.
  const static int64_t MIN_BUFFER_LEN = 1024;

 protected:
  RuntimeProfile* NewProfile() {
    return RuntimeProfile::Create(&obj_pool_, "test profile");
  }

  BufferPoolMetric* CreateReservationMetric(ReservationTracker* tracker) {
    return obj_pool_.Add(new BufferPoolMetric(MetricDefs::Get("buffer-pool.reserved"),
          BufferPoolMetric::BufferPoolMetricType::RESERVED, tracker,
          nullptr));
  }

  ObjectPool obj_pool_;

  ReservationTracker root_;

  scoped_ptr<TestEnv> test_env_;
  RuntimeState* runtime_state_;
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

  // Closing the child should release its reservation all the way up the tree.
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
  MemTracker* root_mem_tracker = ExecEnv::GetInstance()->process_mem_tracker();
  MemTracker child_mem_tracker1(-1, "Child 1", root_mem_tracker);
  MemTracker child_mem_tracker2(MIN_BUFFER_LEN * 50, "Child 2", root_mem_tracker);
  ReservationTracker child_reservations1, child_reservations2;
  child_reservations1.InitChildTracker(
      NewProfile(), &root_, &child_mem_tracker1, 500 * MIN_BUFFER_LEN);
  child_reservations2.InitChildTracker(
      NewProfile(), &root_, &child_mem_tracker2, 75 * MIN_BUFFER_LEN);

  // Check that a single buffer reservation is accounted correctly.
  ASSERT_TRUE(child_reservations1.IncreaseReservation(MIN_BUFFER_LEN));
  ASSERT_EQ(MIN_BUFFER_LEN, child_reservations1.GetReservation());
  ASSERT_EQ(MIN_BUFFER_LEN, child_mem_tracker1.consumption());
  ASSERT_EQ(MIN_BUFFER_LEN, root_mem_tracker->consumption());
  ASSERT_EQ(MIN_BUFFER_LEN, root_.GetChildReservations());

  // Check that a buffer reservation from the other child is accounted correctly.
  ASSERT_TRUE(child_reservations2.IncreaseReservation(MIN_BUFFER_LEN));
  ASSERT_EQ(MIN_BUFFER_LEN, child_reservations2.GetReservation());
  ASSERT_EQ(MIN_BUFFER_LEN, child_mem_tracker2.consumption());
  ASSERT_EQ(2 * MIN_BUFFER_LEN, root_mem_tracker->consumption());
  ASSERT_EQ(2 * MIN_BUFFER_LEN, root_.GetChildReservations());

  // Check that hitting the MemTracker limit leaves things in a consistent state.
  Status error_status;
  string expected_error_str =
      "Could not allocate memory while trying to increase reservation.";
  ASSERT_FALSE(
      child_reservations2.IncreaseReservation(MIN_BUFFER_LEN * 50, &error_status));
  ASSERT_EQ(MIN_BUFFER_LEN, child_reservations1.GetReservation());
  ASSERT_EQ(MIN_BUFFER_LEN, child_mem_tracker1.consumption());
  ASSERT_EQ(MIN_BUFFER_LEN, child_reservations2.GetReservation());
  ASSERT_EQ(MIN_BUFFER_LEN, child_mem_tracker2.consumption());
  ASSERT_EQ(2 * MIN_BUFFER_LEN, root_mem_tracker->consumption());
  ASSERT_EQ(2 * MIN_BUFFER_LEN, root_.GetChildReservations());
  ASSERT_TRUE(error_status.msg().msg().find(expected_error_str) != string::npos);

  // Check that hitting the ReservationTracker's local limit leaves things in a
  // consistent state.
  string top_5_query_msg =
      "The top 5 queries that allocated memory under this tracker are";
  expected_error_str = Substitute(
      "Failed to increase reservation by $0 because it would "
      "exceed the applicable reservation limit for the \"$1\" ReservationTracker",
      PrettyPrinter::Print(MIN_BUFFER_LEN * 75, TUnit::BYTES),
      child_mem_tracker2.label());
  ASSERT_FALSE(
      child_reservations2.IncreaseReservation(MIN_BUFFER_LEN * 75, &error_status));
  ASSERT_EQ(MIN_BUFFER_LEN, child_reservations1.GetReservation());
  ASSERT_EQ(MIN_BUFFER_LEN, child_mem_tracker1.consumption());
  ASSERT_EQ(MIN_BUFFER_LEN, child_reservations2.GetReservation());
  ASSERT_EQ(MIN_BUFFER_LEN, child_mem_tracker2.consumption());
  ASSERT_EQ(2 * MIN_BUFFER_LEN, root_mem_tracker->consumption());
  ASSERT_EQ(2 * MIN_BUFFER_LEN, root_.GetChildReservations());
  ASSERT_TRUE(error_status.msg().msg().find(expected_error_str) != string::npos);
  // No queries registered under this tracker.
  ASSERT_TRUE(error_status.msg().msg().find(top_5_query_msg) == string::npos);

  // Check that hitting the ReservationTracker's parent's limit after the
  // MemTracker consumption is incremented leaves things in a consistent state.
  expected_error_str = Substitute(
      "Failed to increase reservation by $0 because it would "
      "exceed the applicable reservation limit for the \"$1\" ReservationTracker",
      PrettyPrinter::Print(MIN_BUFFER_LEN * 100, TUnit::BYTES),
      root_mem_tracker->label());
  ASSERT_FALSE(
      child_reservations1.IncreaseReservation(MIN_BUFFER_LEN * 100, &error_status));
  ASSERT_EQ(MIN_BUFFER_LEN, child_reservations1.GetReservation());
  ASSERT_EQ(MIN_BUFFER_LEN, child_mem_tracker1.consumption());
  ASSERT_EQ(MIN_BUFFER_LEN, child_reservations2.GetReservation());
  ASSERT_EQ(MIN_BUFFER_LEN, child_mem_tracker2.consumption());
  ASSERT_EQ(2 * MIN_BUFFER_LEN, root_mem_tracker->consumption());
  ASSERT_EQ(2 * MIN_BUFFER_LEN, root_.GetChildReservations());
  ASSERT_TRUE(error_status.msg().msg().find(expected_error_str) != string::npos);
  // A dummy query is registered under the Process tracker by the test env.
  ASSERT_TRUE(error_status.msg().msg().find(top_5_query_msg) != string::npos);

  // Check that released memory is decremented from all trackers correctly.
  child_reservations1.Close();
  child_reservations2.Close();
  ASSERT_EQ(0, child_mem_tracker1.consumption());
  ASSERT_EQ(0, child_mem_tracker2.consumption());
  ASSERT_EQ(0, root_mem_tracker->consumption());
  ASSERT_EQ(0, root_.GetUsedReservation());
  child_mem_tracker1.Close();
  child_mem_tracker2.Close();
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
  const int SOFT_LIMIT = static_cast<int>(LIMIT * 0.9);
  vector<int> mem_limits({LIMIT * 10, LIMIT, -1, -1, -1});

  // Root trackers aren't linked.
  mem_trackers[0].reset(new MemTracker(mem_limits[0]));
  reservations[0].InitRootTracker(NewProfile(), 500);
  for (int i = 1; i < HIERARCHY_DEPTH; ++i) {
    mem_trackers[i].reset(new MemTracker(
        mem_limits[i], Substitute("Child $0", i), mem_trackers[i - 1].get()));
  }

  vector<int> interesting_amounts(
      {LIMIT - 1, LIMIT, LIMIT + 1, SOFT_LIMIT, SOFT_LIMIT - 1});

  // Test that all limits and consumption correctly reported when consuming
  // from a non-root ReservationTracker that is connected to a MemTracker.
  // Test both soft and hard limits.
  for (MemLimit limit_mode : {MemLimit::SOFT, MemLimit::HARD}) {
    for (int level = 1; level < HIERARCHY_DEPTH; ++level) {
      int64_t lowest_limit = mem_trackers[level]->GetLowestLimit(limit_mode);
      for (int amount : interesting_amounts) {
        LOG(INFO) << "level=" << level << " limit_mode=" << static_cast<int>(limit_mode)
                  << " amount=" << amount << " lowest_limit=" << lowest_limit;
        // Initialize the tracker, increase reservation, then release reservation by
        // closing the tracker.
        reservations[level].InitChildTracker(NewProfile(), &reservations[level - 1],
            mem_trackers[level].get(), 500, limit_mode);
        bool increased = reservations[level].IncreaseReservation(amount);
        if (lowest_limit == -1 || amount <= lowest_limit) {
          // The increase should go through.
          ASSERT_TRUE(increased)
              << reservations[level].DebugString() << "\n"
              << mem_trackers[0]->LogUsage(MemTracker::UNLIMITED_DEPTH);
          ASSERT_EQ(amount, reservations[level].GetReservation());
          ASSERT_EQ(amount, mem_trackers[level]->consumption());
          for (int ancestor = 0; ancestor < level; ++ancestor) {
            ASSERT_EQ(amount, reservations[ancestor].GetChildReservations());
            ASSERT_EQ(amount, mem_trackers[ancestor]->consumption());
          }

          LOG(INFO) << "\n" << mem_trackers[0]->LogUsage(MemTracker::UNLIMITED_DEPTH);
        } else {
          ASSERT_FALSE(increased);
        }
        reservations[level].Close();
        // Reservations should be released on all ancestors.
        for (int i = 0; i < level; ++i) {
          ASSERT_EQ(0, reservations[i].GetReservation()) << i << ": "
                                                         << reservations[i].DebugString();
          ASSERT_EQ(0, reservations[i].GetChildReservations());
          ASSERT_EQ(0, mem_trackers[i]->consumption());
        }
      }
      // Set up tracker to be parent for next iteration.
      reservations[level].InitChildTracker(NewProfile(), &reservations[level - 1],
          mem_trackers[level].get(), 500, limit_mode);
    }
    for (int level = 1; level < HIERARCHY_DEPTH; ++level) reservations[level].Close();
  }

  // "Pull down" a reservation from the top of the hierarchy level-by-level to the
  // leaves, checking that everything is consistent at each step.
  for (int level = 0; level < HIERARCHY_DEPTH; ++level) {
    const int amount = LIMIT;
    if (level > 0) {
      reservations[level].InitChildTracker(
          NewProfile(), &reservations[level - 1], mem_trackers[level].get(), 500,
          MemLimit::HARD);
    }
    ASSERT_TRUE(reservations[level].IncreaseReservation(amount));
    ASSERT_EQ(amount, reservations[level].GetReservation());
    ASSERT_EQ(0, reservations[level].GetUsedReservation());
    if (level != 0) {
      ASSERT_EQ(amount, mem_trackers[level]->consumption());
    }
    for (int ancestor = 0; ancestor < level; ++ancestor) {
      ASSERT_EQ(0, reservations[ancestor].GetUsedReservation());
      ASSERT_EQ(amount, reservations[ancestor].GetChildReservations());
      ASSERT_EQ(amount, mem_trackers[ancestor]->consumption());
    }
    // Return the reservation to the root before the next iteration.
    ASSERT_TRUE(reservations[level].TransferReservationTo(&reservations[0], amount));
  }

  for (int i = HIERARCHY_DEPTH - 1; i >= 0; --i) {
    reservations[i].Close();
    if (i != 0) mem_trackers[i]->Close();
  }
}

// Test TransferReservation().
TEST_F(ReservationTrackerTest, TransferReservation) {
  Status status;
  // Set up this hierarchy, to test transfers between different levels and
  // different cases:
  //    (root) limit = 4
  //      ^
  //      |
  //  (grandparent) limit = 3
  //   ^         ^
  //   |         |
  //  (parent) (aunt) limit =2
  //   ^
  //   |
  //  (child)
  const int64_t TOTAL_MEM = MIN_BUFFER_LEN * 4;
  const int64_t GRANDPARENT_LIMIT = MIN_BUFFER_LEN * 3;
  const int64_t AUNT_LIMIT = MIN_BUFFER_LEN * 2;

  root_.InitRootTracker(nullptr, TOTAL_MEM);
  MemTracker* root_mem_tracker = obj_pool_.Add(new MemTracker);
  ReservationTracker* grandparent = obj_pool_.Add(new ReservationTracker());
  MemTracker* grandparent_mem_tracker =
      obj_pool_.Add(new MemTracker(TOTAL_MEM, "grandparent", root_mem_tracker));
  ReservationTracker* parent = obj_pool_.Add(new ReservationTracker());
  MemTracker* parent_mem_tracker =
      obj_pool_.Add(new MemTracker(-1, "parent", grandparent_mem_tracker));
  ReservationTracker* aunt = obj_pool_.Add(new ReservationTracker());
  ReservationTracker* child = obj_pool_.Add(new ReservationTracker());
  MemTracker* child_mem_tracker =
      obj_pool_.Add(new MemTracker(-1, "child", parent_mem_tracker));
  grandparent->InitChildTracker(nullptr, &root_, grandparent_mem_tracker, TOTAL_MEM);
  parent->InitChildTracker(
      nullptr, grandparent, parent_mem_tracker, numeric_limits<int64_t>::max());
  aunt->InitChildTracker(nullptr, grandparent, nullptr, AUNT_LIMIT);
  child->InitChildTracker(
      nullptr, parent, child_mem_tracker, numeric_limits<int64_t>::max());

  ASSERT_TRUE(child->IncreaseReservation(GRANDPARENT_LIMIT));
  // Transfer from child to self (no-op).
  ASSERT_TRUE(child->TransferReservationTo(child, GRANDPARENT_LIMIT));
  EXPECT_EQ(GRANDPARENT_LIMIT, child->GetReservation());

  // Transfer from child to parent.
  ASSERT_TRUE(child->TransferReservationTo(parent, GRANDPARENT_LIMIT));
  EXPECT_EQ(0, child->GetReservation());
  EXPECT_EQ(0, child_mem_tracker->consumption());
  EXPECT_EQ(0, parent->GetChildReservations());
  EXPECT_EQ(GRANDPARENT_LIMIT, parent->GetReservation());
  EXPECT_EQ(GRANDPARENT_LIMIT, parent_mem_tracker->consumption());
  EXPECT_EQ(GRANDPARENT_LIMIT, grandparent->GetChildReservations());
  EXPECT_EQ(GRANDPARENT_LIMIT, grandparent->GetReservation());
  EXPECT_EQ(GRANDPARENT_LIMIT, grandparent_mem_tracker->consumption());
  EXPECT_EQ(GRANDPARENT_LIMIT, root_.GetReservation());

  // Transfer from parent to aunt, up to aunt's limit.
  ASSERT_TRUE(parent->TransferReservationTo(aunt, AUNT_LIMIT));
  EXPECT_EQ(GRANDPARENT_LIMIT - AUNT_LIMIT, parent->GetReservation());
  EXPECT_EQ(GRANDPARENT_LIMIT - AUNT_LIMIT, parent_mem_tracker->consumption());
  EXPECT_EQ(AUNT_LIMIT, aunt->GetReservation());
  EXPECT_EQ(GRANDPARENT_LIMIT, grandparent->GetChildReservations());
  EXPECT_EQ(GRANDPARENT_LIMIT, grandparent->GetReservation());
  EXPECT_EQ(GRANDPARENT_LIMIT, grandparent_mem_tracker->consumption());
  EXPECT_EQ(GRANDPARENT_LIMIT, root_.GetReservation());
  // Cannot exceed aunt's limit by transferring.
  ASSERT_FALSE(parent->TransferReservationTo(aunt, parent->GetReservation()));

  // Transfer from parent to child.
  ASSERT_TRUE(parent->TransferReservationTo(child, parent->GetReservation()));
  EXPECT_EQ(GRANDPARENT_LIMIT - AUNT_LIMIT, child->GetReservation());
  EXPECT_EQ(GRANDPARENT_LIMIT - AUNT_LIMIT, child_mem_tracker->consumption());
  EXPECT_EQ(GRANDPARENT_LIMIT - AUNT_LIMIT, parent->GetReservation());
  EXPECT_EQ(GRANDPARENT_LIMIT - AUNT_LIMIT, parent_mem_tracker->consumption());
  EXPECT_EQ(GRANDPARENT_LIMIT, grandparent->GetChildReservations());
  EXPECT_EQ(GRANDPARENT_LIMIT, grandparent->GetReservation());
  EXPECT_EQ(GRANDPARENT_LIMIT, grandparent_mem_tracker->consumption());

  // Transfer from aunt to child.
  ASSERT_TRUE(aunt->TransferReservationTo(child, AUNT_LIMIT));
  EXPECT_EQ(GRANDPARENT_LIMIT, child->GetReservation());
  EXPECT_EQ(GRANDPARENT_LIMIT, child_mem_tracker->consumption());
  EXPECT_EQ(GRANDPARENT_LIMIT, parent->GetReservation());
  EXPECT_EQ(GRANDPARENT_LIMIT, parent_mem_tracker->consumption());
  EXPECT_EQ(0, aunt->GetReservation());
  EXPECT_EQ(GRANDPARENT_LIMIT, grandparent->GetChildReservations());
  EXPECT_EQ(GRANDPARENT_LIMIT, grandparent->GetReservation());
  EXPECT_EQ(GRANDPARENT_LIMIT, grandparent_mem_tracker->consumption());

  // Transfer from child to grandparent.
  ASSERT_TRUE(child->TransferReservationTo(grandparent, GRANDPARENT_LIMIT));
  EXPECT_EQ(0, child->GetReservation());
  EXPECT_EQ(0, child_mem_tracker->consumption());
  EXPECT_EQ(0, parent->GetReservation());
  EXPECT_EQ(0, parent_mem_tracker->consumption());
  EXPECT_EQ(0, aunt->GetReservation());
  EXPECT_EQ(0, grandparent->GetChildReservations());
  EXPECT_EQ(GRANDPARENT_LIMIT, grandparent->GetReservation());
  EXPECT_EQ(GRANDPARENT_LIMIT, grandparent_mem_tracker->consumption());
  EXPECT_EQ(GRANDPARENT_LIMIT, root_.GetReservation());

  child->Close();
  child_mem_tracker->Close();
  aunt->Close();
  parent->Close();
  parent_mem_tracker->Close();
  grandparent->Close();
  grandparent_mem_tracker->Close();
}

TEST_F(ReservationTrackerTest, ReservationUtil) {
  const int64_t MEG = 1024 * 1024;
  const int64_t GIG = 1024 * 1024 * 1024;
  EXPECT_EQ(32 * MEG, ReservationUtil::RESERVATION_MEM_MIN_REMAINING);

  EXPECT_EQ(0, ReservationUtil::GetReservationLimitFromMemLimit(0));
  EXPECT_EQ(0, ReservationUtil::GetReservationLimitFromMemLimit(-1));
  EXPECT_EQ(0, ReservationUtil::GetReservationLimitFromMemLimit(32 * MEG));
  EXPECT_EQ(8 * GIG, ReservationUtil::GetReservationLimitFromMemLimit(10 * GIG));

  EXPECT_EQ(32 * MEG, ReservationUtil::GetMinMemLimitFromReservation(0));
  EXPECT_EQ(32 * MEG, ReservationUtil::GetMinMemLimitFromReservation(-1));
  EXPECT_EQ(500 * MEG, ReservationUtil::GetMinMemLimitFromReservation(400 * MEG));
  EXPECT_EQ(5 * GIG, ReservationUtil::GetMinMemLimitFromReservation(4 * GIG));

  EXPECT_EQ(0, ReservationUtil::GetReservationLimitFromMemLimit(
      ReservationUtil::GetMinMemLimitFromReservation(0)));
  EXPECT_EQ(4 * GIG, ReservationUtil::GetReservationLimitFromMemLimit(
      ReservationUtil::GetMinMemLimitFromReservation(4 * GIG)));
}

static void LogUsageThread(MemTracker* mem_tracker, AtomicInt32* done) {
  while (done->Load() == 0) {
    int64_t logged_consumption;
    mem_tracker->LogUsage(10, "  ", &logged_consumption);
  }
}

// IMPALA-6362: regression test for deadlock between ReservationTracker and MemTracker.
TEST_F(ReservationTrackerTest, MemTrackerDeadlock) {
  const int64_t RESERVATION_LIMIT = 1024;
  root_.InitRootTracker(nullptr, numeric_limits<int64_t>::max());
  MemTracker* mem_tracker = obj_pool_.Add(new MemTracker);
  ReservationTracker* reservation = obj_pool_.Add(new ReservationTracker());
  reservation->InitChildTracker(nullptr, &root_, mem_tracker, RESERVATION_LIMIT);

  // Create a child MemTracker with a buffer pool consumption metric, that calls
  // reservation->GetReservation() when its usage is logged.
  obj_pool_.Add(new MemTracker(CreateReservationMetric(reservation),
        -1, "Reservation", mem_tracker));
  // Start background thread that repeatededly logs the 'mem_tracker' tree.
  AtomicInt32 done(0);
  thread log_usage_thread(&LogUsageThread, mem_tracker, &done);

  // Retry enough times to reproduce the deadlock with LogUsageThread().
  for (int i = 0; i < 100; ++i) {
    // Fail to increase reservation, hitting limit of 'reservation'. This will try
    // to log the 'mem_tracker' tree while holding reservation->lock_.
    Status err;
    ASSERT_FALSE(reservation->IncreaseReservation(RESERVATION_LIMIT + 1, &err));
    ASSERT_FALSE(err.ok());
  }

  done.Store(1);
  log_usage_thread.join();
  reservation->Close();
  mem_tracker->Close();
}
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  impala::InitCommonRuntime(argc, argv, true, impala::TestInfo::BE_TEST);
  impala::InitFeSupport(false);
  return RUN_ALL_TESTS();
}
