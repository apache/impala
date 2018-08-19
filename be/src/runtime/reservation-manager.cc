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

#include "runtime/reservation-manager.h"

#include "gutil/strings/substitute.h"
#include "runtime/exec-env.h"
#include "runtime/initial-reservations.h"
#include "runtime/query-state.h"
#include "runtime/runtime-state.h"
#include "util/string-parser.h"

using strings::Substitute;

namespace impala {

void ReservationManager::Init(string name, RuntimeProfile* runtime_profile,
    ReservationTracker* parent_reservation, MemTracker* mem_tracker,
    const TBackendResourceProfile& resource_profile, const TDebugOptions& debug_options) {
  name_ = name;
  runtime_profile_ = runtime_profile;
  parent_reservation_ = parent_reservation;
  mem_tracker_ = mem_tracker;
  resource_profile_ = resource_profile;
  debug_options_ = debug_options;
}

void ReservationManager::Close(RuntimeState* state) {
  if (buffer_pool_client_.is_registered()) {
    VLOG_FILE << name_ << " returning reservation " << resource_profile_.min_reservation;
    state->query_state()->initial_reservations()->Return(
        &buffer_pool_client_, resource_profile_.min_reservation);
    ExecEnv::GetInstance()->buffer_pool()->DeregisterClient(&buffer_pool_client_);
  }
}

Status ReservationManager::ClaimBufferReservation(RuntimeState* state) {
  DCHECK(!buffer_pool_client_.is_registered());
  BufferPool* buffer_pool = ExecEnv::GetInstance()->buffer_pool();
  // Check the minimum buffer size in case the minimum buffer size used by the planner
  // doesn't match this backend's.
  if (resource_profile_.__isset.spillable_buffer_size
      && resource_profile_.spillable_buffer_size < buffer_pool->min_buffer_len()) {
    return Status(Substitute("Spillable buffer size for $0 of $1 bytes is less "
                             "than the minimum buffer pool buffer size of $2 bytes",
        name_, resource_profile_.spillable_buffer_size, buffer_pool->min_buffer_len()));
  }

  RETURN_IF_ERROR(buffer_pool->RegisterClient(name_, state->query_state()->file_group(),
      parent_reservation_, mem_tracker_, resource_profile_.max_reservation,
      runtime_profile_, &buffer_pool_client_));
  VLOG_FILE << name_ << " claiming reservation " << resource_profile_.min_reservation;
  state->query_state()->initial_reservations()->Claim(
      &buffer_pool_client_, resource_profile_.min_reservation);
  if (debug_options_.action == TDebugAction::SET_DENY_RESERVATION_PROBABILITY
      && (debug_options_.phase == TExecNodePhase::PREPARE
             || debug_options_.phase == TExecNodePhase::OPEN)) {
    // We may not have been able to enable the debug action at the start of Prepare() or
    // Open() because the client is not registered then. Do it now to be sure that it is
    // effective.
    RETURN_IF_ERROR(EnableDenyReservationDebugAction());
  }
  return Status::OK();
}

Status ReservationManager::ReleaseUnusedReservation() {
  return buffer_pool_client_.DecreaseReservationTo(
      buffer_pool_client_.GetUnusedReservation(), resource_profile_.min_reservation);
}

Status ReservationManager::EnableDenyReservationDebugAction() {
  DCHECK_EQ(debug_options_.action, TDebugAction::SET_DENY_RESERVATION_PROBABILITY);
  // Parse [0.0, 1.0] probability.
  StringParser::ParseResult parse_result;
  double probability =
      StringParser::StringToFloat<double>(debug_options_.action_param.c_str(),
          debug_options_.action_param.size(), &parse_result);
  if (parse_result != StringParser::PARSE_SUCCESS || probability < 0.0
      || probability > 1.0) {
    return Status(Substitute("Invalid SET_DENY_RESERVATION_PROBABILITY param: '$0'",
        debug_options_.action_param));
  }
  buffer_pool_client_.SetDebugDenyIncreaseReservation(probability);
  return Status::OK();
}

} // namespace impala
