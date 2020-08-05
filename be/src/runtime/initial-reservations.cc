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

#include "runtime/initial-reservations.h"

#include <limits>
#include <mutex>

#include <gflags/gflags.h>

#include "common/logging.h"
#include "common/object-pool.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "util/debug-util.h"
#include "util/pretty-printer.h"

#include "common/names.h"

using std::numeric_limits;

DECLARE_int32(krpc_port);
DECLARE_string(hostname);

namespace impala {

InitialReservations::InitialReservations(ObjectPool* obj_pool,
    ReservationTracker* query_reservation, MemTracker* query_mem_tracker,
    int64_t initial_reservation_total_claims)
  : initial_reservation_mem_tracker_(obj_pool->Add(
        new MemTracker(-1, "Unclaimed reservations", query_mem_tracker, false))),
    remaining_initial_reservation_claims_(initial_reservation_total_claims) {
  // Soft mem_limits should not apply to the initial reservation because we don't want
  // to fail the query in the case where the initial reservation exceeds the soft
  // limit.
  initial_reservations_.InitChildTracker(nullptr, query_reservation,
      initial_reservation_mem_tracker_, numeric_limits<int64_t>::max(), MemLimit::HARD);
}

Status InitialReservations::Init(
    const TUniqueId& query_id, int64_t query_min_reservation) {
  DCHECK_EQ(0, initial_reservations_.GetReservation()) << "Already inited";
  Status reservation_status;
  if (!initial_reservations_.IncreaseReservation(
          query_min_reservation, &reservation_status)) {
    return Status(TErrorCode::MINIMUM_RESERVATION_UNAVAILABLE,
        PrettyPrinter::Print(query_min_reservation, TUnit::BYTES), FLAGS_hostname,
        FLAGS_krpc_port, PrintId(query_id), reservation_status.GetDetail());
  }
  VLOG(2) << "Successfully claimed initial reservations ("
          << PrettyPrinter::Print(query_min_reservation, TUnit::BYTES) << ") for"
          << " query " << PrintId(query_id);
  return Status::OK();
}

void InitialReservations::Claim(BufferPool::ClientHandle* dst, int64_t bytes) {
  DCHECK_GE(bytes, 0);
  lock_guard<SpinLock> l(lock_);
  DCHECK_LE(bytes, remaining_initial_reservation_claims_);
  bool success = dst->TransferReservationFrom(&initial_reservations_, bytes);
  DCHECK(success) << "Planner computation should ensure enough initial reservations";
  remaining_initial_reservation_claims_ -= bytes;
}

void InitialReservations::Return(BufferPool::ClientHandle* src, int64_t bytes) {
  lock_guard<SpinLock> l(lock_);
  bool success;
  Status status = src->TransferReservationTo(&initial_reservations_, bytes, &success);
  DCHECK(status.ok()) << status.GetDetail() << " no dirty pages to flush, can't fail "
                      << src->DebugString();
  // No limits on our tracker - no way this should fail.
  DCHECK(success) << initial_reservations_.DebugString();
  // Check to see if we can release any reservation.
  int64_t excess_reservation =
    initial_reservations_.GetReservation() - remaining_initial_reservation_claims_;
  if (excess_reservation > 0) {
    initial_reservations_.DecreaseReservation(excess_reservation);
  }
}

void InitialReservations::ReleaseResources() {
  initial_reservations_.Close();
  initial_reservation_mem_tracker_->Close();
}
}
