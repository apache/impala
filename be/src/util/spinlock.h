// Copyright 2013 Cloudera Inc.
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


#ifndef IMPALA_UTIL_SPINLOCK_H
#define IMPALA_UTIL_SPINLOCK_H

#include "common/atomic.h"
#include "common/logging.h"

namespace impala {

// Lightweight spinlock.
class SpinLock {
 public:
  SpinLock() : locked_(false) {}

  void Lock() {
    while (true) {
      if(__sync_bool_compare_and_swap(&locked_, false, true)) return;
      for (int i = 0; i < NUM_SPIN_CYCLES + 1; ++i) {
        AtomicUtil::CpuWait();
      }
      if(__sync_bool_compare_and_swap(&locked_, false, true)) return;
      sched_yield();
    }
  }

  void Unlock() {
    // Memory barrier here. All updates before the unlock need to be made
    // visible.
    __sync_synchronize();
    DCHECK(locked_);
    locked_ = false;
  }

 private:
  // In typical spin lock implements, we want to spin (and keep the core fully
  // busy), for some number of cycles before yielding. Consider these three
  // cases:
  //  1) lock is un-contended - spinning doesn't kick in and has no effect.
  //  2) lock is taken by another thread and that thread finishes quickly
  //  3) lock is taken by another thread and that thread is slow (e.g. scheduled
  //     away).
  // In case 3), we'd want to yield so another thread can do work. This thread
  // won't be able to do anything useful until the thread with the lock runs again.
  // In case 2), we don't want to yield (and give up our scheduling time slice)
  // since we will get to run soon after.
  // To try to get the best of everything, we will busy spin for a while before
  // yielding to another thread.
  // TODO: how do we set this.
  static const int NUM_SPIN_CYCLES = 70;
  // TODO: pad this to be a cache line?
  bool locked_;
};

class ScopedSpinLock {
 public:
  ScopedSpinLock(SpinLock* lock = NULL) {
    lock_ = lock;
    if (lock_ != NULL) lock_->Lock();
  }

  ~ScopedSpinLock() {
    if (lock_ != NULL) lock_->Unlock();
  }

  void AcquireLock(SpinLock* lock) {
    DCHECK(lock_ == NULL);
    lock_ = lock;
    lock_->Lock();
  }

 private:
  // Disable copy constructor and assignment
  ScopedSpinLock(const ScopedSpinLock& counter);
  ScopedSpinLock& operator=(const ScopedSpinLock& counter);

  SpinLock* lock_;
};

}

#endif
