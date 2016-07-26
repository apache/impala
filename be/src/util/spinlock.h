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

#ifndef IMPALA_UTIL_SPINLOCK_H
#define IMPALA_UTIL_SPINLOCK_H

#include "gutil/spinlock.h"
#include "common/logging.h"

namespace impala {

// Wrapper around the Google SpinLock class to adapt it to the method names
// expected by Boost.
class SpinLock {
 public:
  SpinLock() {}

  /// Acquires the lock, spins and then blocks until the lock becomes available.
  void lock() {
    l_.Lock();
  }

  /// Releases the lock.
  void unlock() {
    l_.Unlock();
  }

  /// Tries to get the lock but does not spin or block.  Returns true if the lock was
  /// acquired, false otherwise.
  bool try_lock() {
    return l_.TryLock();
  }

  /// Verify that the lock is held.
  void DCheckLocked() { DCHECK(l_.IsHeld()); }

 private:
  /// The underlying SpinLock from gutil.
  base::SpinLock l_;

  DISALLOW_COPY_AND_ASSIGN(SpinLock);
};

}
#endif
