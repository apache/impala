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

#include "buffer-pool.h"

namespace impala {

// Keeps track of a reservation and a current number of used buffers.
// This object is used by some user of the BufferPool to maintain a certain reservation,
// which is a minimum number of buffers needed to make progress.
class BufferPool::ReservationContext {
 private:
  friend class BufferPool;

  ReservationContext(BufferPool* pool, int reservation)
      : pool_(pool), reservation_(reservation), canceled_(false) {
  }

  void BufferUsed() {
    DCHECK(!canceled_);
    if (RemainingReservations() > 0) --pool_->num_reserved_buffers_;
    DCHECK_GE(pool_->num_reserved_buffers_, 0);
    ++buffers_used_;
  }

  void BufferReleased() {
    --buffers_used_;
    DCHECK_GE(buffers_used_, 0);
    if (RemainingReservations() > 0 && !canceled_) ++pool_->num_reserved_buffers_;
    if (canceled_ && buffers_used_ == 0) delete this;
  }

  int RemainingReservations() { return std::max(0, reservation_ - buffers_used_); }

  void Cancel() {
    canceled_ = true;
    if (buffers_used_ == 0) delete this;
  }

  BufferPool* pool_;
  const int reservation_;
  AtomicInt<int> buffers_used_;
  bool canceled_;
};

void BufferPool::Reserve(int num_buffers, ReservationContext** context) {
  *context = new ReservationContext(this, num_buffers);
  for (int i = 0; i < num_buffers; ++i) {
    BufferDescriptor* buffer = FindFreeBuffer(NULL);
    ++num_reserved_buffers_;
    ReturnBuffer(buffer, NULL);
  }
}

void BufferPool::CancelReservation(ReservationContext* context) {
  boost::unique_lock<boost::recursive_mutex> lock(lock_);
  num_reserved_buffers_ -= context->RemainingReservations();
  DCHECK_GE(num_reserved_buffers_, 0);
  context->Cancel();
  free_cv_.notify_all();
}

BufferPool::BufferDescriptor* BufferPool::GetBuffer(ReservationContext* context) {
  BufferDescriptor* buffer = FindFreeBuffer(context);
  if (context != NULL) context->BufferUsed();
  buffer->reservation_context = context;
  return buffer;
}

int64_t BufferPool::num_free_buffers() const {
  boost::lock_guard<boost::recursive_mutex> lock(lock_);
  int num_unallocated_buffers = num_buffers_ - buffers_.size();
  int num_unused_buffers = freelist_.size()  - num_reserved_buffers_;
  return num_unallocated_buffers + num_unused_buffers;
}

BufferPool::BufferDescriptor* BufferPool::FindFreeBuffer(ReservationContext* context) {
  boost::unique_lock<boost::recursive_mutex> lock(lock_);
  if (!HasFreeBuffer(context)) {
    if (buffers_.size() < num_buffers_) return AllocateBuffer();

    // Last chance: get a buffer freed for us.
    if (!try_free_buffer_callback_.empty()) {
      while (try_free_buffer_callback_() && !HasFreeBuffer(context)) ;
    }

    while (!HasFreeBuffer(context)) {
      ++num_waiting_threads_;
      free_cv_.wait(lock);
      --num_waiting_threads_;
    }
  }

  // Free another buffer if we just took a buffer that another thread was waiting for.
  if (freelist_.size() == 1 && num_waiting_threads_ > 0 ) {
    if (!try_free_buffer_callback_.empty()) try_free_buffer_callback_();
  }

  BufferDescriptor* buffer_desc = freelist_.back();
  freelist_.pop_back();
  return buffer_desc;
}

inline bool BufferPool::HasFreeBuffer(ReservationContext* context) {
  return freelist_.size() > num_reserved_buffers_
      || (context != NULL && context->RemainingReservations() > 0);
}

void BufferPool::ReturnBuffer(BufferDescriptor* buffer_desc,
    ReservationContext* context) {
  DCHECK(buffer_desc->pool == this);
  {
    boost::lock_guard<boost::recursive_mutex> lock(lock_);
    freelist_.push_back(buffer_desc);
    free_cv_.notify_all();

    if (context != NULL) context->BufferReleased();
  }
}

BufferPool::BufferDescriptor* BufferPool::AllocateBuffer() {
  BufferDescriptor* buffer_desc = new BufferDescriptor(this, buffer_size_);
  buffers_.push_back(buffer_desc);
  return buffer_desc;
}

BufferPool::~BufferPool() {
  for (int i = 0; i < buffers_.size(); ++i) {
   delete [] buffers_[i]->buffer;
   delete buffers_[i];
  }
}

}
