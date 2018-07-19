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

#ifndef IMPALA_RUNTIME_RESERVATION_MANAGER_H
#define IMPALA_RUNTIME_RESERVATION_MANAGER_H

#include "common/status.h"
#include "runtime/bufferpool/buffer-pool.h"

namespace impala {

class MemTracker;
class RuntimeProfile;
class RuntimeState;
class TBackendResourceProfile;
class TDebugOptions;

/// A ReservationManager provides a wrapper around a TBackendResourceProfile and
/// a BufferPool::ClientHandle, with functionality for claiming and releasing the
/// memory from the resource profile.
class ReservationManager {
 public:
  ReservationManager() {}

  /// Initialize this ReservationManager with the given values. 'name' is used in logging
  /// and error messages, 'debug_options' is used for the SET_DENY_RESERVATION_PROBABILITY
  /// action. Does not take ownership of 'runtime_profile' and 'mem_tracker'. Must be
  /// called before ClaimBufferReservation().
  void Init(std::string name, RuntimeProfile* runtime_profile,
      ReservationTracker* parent_reservation, MemTracker* mem_tracker,
      const TBackendResourceProfile& resource_profile,
      const TDebugOptions& debug_options);
  void Close(RuntimeState* state);

  BufferPool::ClientHandle* buffer_pool_client() { return &buffer_pool_client_; }

  /// Initialize 'buffer_pool_client_' and claim the initial reservation. The client is
  /// cleaned up in Close(). Should not be called if the client is already open.
  ///
  /// The initial reservation must be returned to
  /// QueryState::initial_reservations(), which is done automatically in Close() as long
  /// as the initial reservation is not released before Close().
  Status ClaimBufferReservation(RuntimeState* state) WARN_UNUSED_RESULT;

  /// Release any unused reservation in excess of the initial reservation. Returns an
  /// error if releasing the reservation requires flushing pages to disk, and that fails.
  /// Not thread-safe if other threads are accessing 'buffer_pool_client_'.
  Status ReleaseUnusedReservation() WARN_UNUSED_RESULT;

  /// Enable the increase reservation denial probability on 'buffer_pool_client_' based
  /// on 'debug_options_'. Returns an error if 'debug_options_.debug_action_param_'
  /// is invalid.
  Status EnableDenyReservationDebugAction();

 private:
  std::string name_;

  RuntimeProfile* runtime_profile_;

  MemTracker* mem_tracker_;

  ReservationTracker* parent_reservation_;

  /// Buffer pool client for this node. Initialized with the node's minimum reservation
  /// in ClaimBufferReservation(). After initialization, the client must hold onto at
  /// least the minimum reservation so that it can be returned to the initial
  /// reservations pool in Close().
  BufferPool::ClientHandle buffer_pool_client_;

  /// Resource information sent from the frontend.
  TBackendResourceProfile resource_profile_;

  TDebugOptions debug_options_;
};

} // namespace impala

#endif // IMPALA_RUNTIME_RESERVATION_MANAGER_H
