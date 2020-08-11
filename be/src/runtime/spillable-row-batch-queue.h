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

#pragma once

#include <queue>

#include "runtime/buffered-tuple-stream.h"
#include "runtime/reservation-manager.h"
#include "runtime/row-batch.h"

namespace impala {

class RowBatch;

/// A RowBatchQueue that provides non-blocking queue semantics. RowBatches are stored
/// inside a BufferedTupleStream. None of the methods block, this class is not thread
/// safe. The amount of unpinned memory used by the queue can be limited by the
/// parameter 'max_unpinned_bytes'. Calls to AddBatch after the capacity has been reached
/// will return an error Status. Calls to GetBatch on an empty queue will return an error
/// Status.
///
/// In order to manage the reservation used by the BufferedTupleStream, this class uses
/// a ReservationManager that creates the BufferPool::ClientHandle used by the
/// BufferedTupleStream. The ReservationManager uses a ResourceProfile created by the fe/
/// to limit the amount of reserved memory used by the stream.
///
/// 'name' is a unique name which is purely used for error reporting by the
/// BufferedTupleStream.
///
/// 'max_unpinned_bytes' limits the maximum number of bytes that can be unpinned in the
/// underlying BufferedTupleStream. The limit is only a soft limit, it
/// might be exceeded during AddBatch when unpinning the stream.
///
/// 'resource_profile' is created in the fe/ by PlanRootSink and specifies the min and max
/// amount of reserved memory the BufferedTupleStream can use as well as the size of the
/// default and max page length used by the stream.
///
/// The remaining parameters are used to initialize the ReservationManager and
/// BufferedTupleStream.
class SpillableRowBatchQueue {
 public:
  SpillableRowBatchQueue(const std::string& name, int64_t max_unpinned_bytes,
      RuntimeState* state, MemTracker* mem_tracker, RuntimeProfile* profile,
      const RowDescriptor* row_desc, const TBackendResourceProfile& resource_profile,
      const TDebugOptions& debug_options);
  ~SpillableRowBatchQueue();

  /// Creates and initializes the ReservationManager and BufferedTupleStream. Returns an
  /// error Status if either could not be initialized. The ReservationManager may fail
  /// to initialize if it cannot claim the initial buffer reservation. The
  /// BufferedTupleStream may fail to initialize if it could not create the read and
  /// write buffers.
  Status Open();

  /// Adds the given RowBatch to the queue. Returns Status::OK() if the batch was
  /// successfully added, returns an error if there was an issue when adding the batch to
  /// the BufferedTupleStream. If adding the batch to the BufferedTupleStream cannot be
  /// achieved because there is no more available reserved memory, this method will unpin
  /// the stream and then add the RowBatch. If the batch still cannot be added, this
  /// method returns an error Status. It is not valid to call this method if the queue is
  /// full or closed. After this returns, 'batch' can be safely destroyed (i.e. the
  /// queue makes copies of all the data from 'batch' that it needs).
  Status AddBatch(RowBatch* batch);

  /// Returns and removes the RowBatch at the head of the queue. Returns Status::OK() if
  /// the batch was successfully read from the queue. It is not valid to call this method
  /// if the queue is empty or has already been closed.
  Status GetBatch(RowBatch* batch);

  /// Returns true if the queue limit has been reached, false otherwise. It is not valid
  /// to call this method if the queue is already closed.
  bool IsFull() const;

  /// Returns true if the queue is empty, false otherwise. It is not valid to call this
  /// method if the queue is already closed.
  bool IsEmpty() const;

  /// Returns false if Close() has been called, true otherwise.
  bool IsOpen() const;

  /// Resets the remaining RowBatches in the queue and releases the queue memory.
  void Close();

 private:
  /// BufferedTupleStream that stores all RowBatches.
  std::unique_ptr<BufferedTupleStream> batch_queue_;

  /// ReservationManager that manages the reserved memory and BufferPool::ClientHandle
  /// used by the BufferedTupleStream.
  ReservationManager reservation_manager_;

  /// A unique name used by the BufferedTupleStream, used purely for debugging purposes.
  const std::string& name_;

  /// Used to in initialize and manage the BufferedTupleStream and ReservationManager.
  RuntimeState* state_;

  /// The MemTracker to use in the BufferedTupleStream.
  MemTracker* mem_tracker_;

  /// Used by the BufferPool::Client created for the BufferedTupleStream.
  RuntimeProfile* profile_;

  /// Used by the BufferedTupleStream, must match the RowDescriptor of the RowBatches
  /// stored in the queue.
  const RowDescriptor* row_desc_;

  /// Used by the ReservationManager to set the min and max reservation that can be
  /// used by the BufferedTupleStream. Used by the BufferedTupleStream to set the default
  /// and max page lengths.
  const TBackendResourceProfile& resource_profile_;

  /// Used by the ReservationManager for the SET_DENY_RESERVATION_PROBABILITY debug
  /// action.
  const TDebugOptions& debug_options_;

  /// The max number of bytes that can be unpinned in the BufferedTupleStream. Set by the
  /// query option MAX_SPILLED_RESULT_SPOOLING_MEM.
  const int64_t max_unpinned_bytes_;

  /// True if the queue has been closed, false otherwise.
  bool closed_ = false;
};
} // namespace impala
