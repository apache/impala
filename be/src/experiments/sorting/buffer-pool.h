// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef IMPALA_RUNTIME_BUFFER_POOL_H
#define IMPALA_RUNTIME_BUFFER_POOL_H

#include <boost/function.hpp>
#include <boost/thread.hpp>
#include <boost/thread/mutex.hpp>

#include <vector>
#include <gtest/gtest.h>
#include <glog/logging.h>
#include <common/atomic.h>

namespace impala {

// A BufferPool stores a set number of fixed-size buffers, which it hands out in the
// blocking GetBuffer() and are returned via non-blocking ReturnBuffer() calls.
//
// The pool avoids actually allocating buffers until they're needed, but does not
// deallocate returned buffers until the whole pool is deleted.
//
// A BufferPool can be shared by multiple ReservationContexts (i.e. Allocators).
// Using reservation contexts guarantees that each allocator can get at least N buffers.
// If there are extra buffers, they will be handed out first come first serve.
// This prevents any of them from starvation.
class BufferPool {
 public:
  // Callback to be called to try to free up a buffer.
  // Returns true if a buffer could be freed, false if none are available.
  typedef boost::function<bool ()> TryFreeBufferCallback;

  class ReservationContext;

  // Describes a chunk of memory allocated from a BufferPool.
  struct BufferDescriptor {
    BufferPool* pool;
    ReservationContext* reservation_context;
    int64_t size;
    uint8_t* buffer;

    BufferDescriptor(BufferPool* pool, int64_t size)
      : pool(pool), size(size), buffer(new uint8_t[size]) {
    }

    void Return() {
      DCHECK(pool != NULL);
      pool->ReturnBuffer(this, reservation_context);
    }
  };

  BufferPool(int64_t num_buffers, int64_t buffer_size = 8 * 1024 * 1024)
      : num_buffers_(num_buffers), buffer_size_(buffer_size),
        num_reserved_buffers_(0), num_waiting_threads_(0) {
  }

  ~BufferPool();

  // Returns a free Buffer from this pool.
  // If no buffers are available, this will call the supplied TryFreeBufferCallback
  // and block if no buffers are free-able.
  // If a ReservationContext is supplied, it is used to ensure the caller can get
  // a minimum number of buffers.
  BufferDescriptor* GetBuffer(ReservationContext* context = NULL);

  // Sets a callback to be called *before* a buffer is retrieved from GetBuffer.
  // The callback is called with lock held, so it should be fast.
  void SetTryFreeBufferCallback(TryFreeBufferCallback try_free_buffer_callback) {
    try_free_buffer_callback_ = try_free_buffer_callback;
  }

  // Reserves a certain number of buffers for a ReservationContext, which is returned
  // by the parameter.
  // This guarantees that the ReservationContext will always be able to hold at least
  // the given number of buffers.
  void Reserve(int num_buffers, ReservationContext** context);

  // Canceling a reservation immediately un-reserves all of its buffers in the Pool
  // and will cause the ReservationContext to be deallocated once all buffers with
  // references to it have been returned to the BufferPool.
  void CancelReservation(ReservationContext* context);

  int64_t num_buffers() const { return num_buffers_; }
  int64_t buffer_size() const { return buffer_size_; }
  boost::recursive_mutex& lock() { return lock_; }

  // Returns the number of unused buffers in this pool, which can be allocated
  // by anyone.
  int64_t num_free_buffers() const;

  int64_t free_reserved_buffers() const { return num_reserved_buffers_; }

 private:
  BufferDescriptor* FindFreeBuffer(ReservationContext* context);

  inline bool HasFreeBuffer(ReservationContext* context);

  // Returns the given buffer to this pool.
  void ReturnBuffer(BufferDescriptor* buffer_desc, ReservationContext* context);

  // Allocates a new buffer and puts it on the buffers_ list.
  BufferDescriptor* AllocateBuffer();

  boost::condition_variable_any free_cv_;

  // Lock guards the buffers_ and freelist_ variables.
  // Recurive mutex because we run the callback with the lock held, which may
  // subsequently call free_buffers() or ReturnBuffer().
  mutable boost::recursive_mutex lock_;

  // Number of buffers this pool holds.
  // This excludes overflow buffers which were allocated to avoid deadlock.
  int64_t num_buffers_;

  // Size in bytes of each buffer.
  int64_t buffer_size_;

  // The number of buffers that are within the BufferPool, but reserved by someone.
  AtomicInt<int> num_reserved_buffers_;

  // Number of threads waiting for a freed buffer.
  int num_waiting_threads_;

  // All buffers in this pool.
  std::vector<BufferDescriptor*> buffers_;

  // All free buffers in this pool.
  std::vector<BufferDescriptor*> freelist_;

  TryFreeBufferCallback try_free_buffer_callback_;
};

}

#endif
