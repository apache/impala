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

#include "util/cyclic-barrier.h"

#include <mutex>

#include "common/names.h"

namespace impala {

CyclicBarrier::CyclicBarrier(int num_threads) : num_threads_(num_threads) {}

void CyclicBarrier::Cancel(const Status& err) {
  DCHECK(!err.ok());
  {
    lock_guard<mutex> l(lock_);
    if (!cancel_status_.ok()) return; // Already cancelled.
    cancel_status_ = err;
  }
  barrier_cv_.NotifyAll();
}

void CyclicBarrier::Unregister() {
  bool notify = false;
  {
    unique_lock<mutex> l(lock_);
    if (!cancel_status_.ok()) return; // Already cancelled.
    --num_threads_;
    DCHECK_GE(num_threads_, 0);
    if (num_waiting_threads_ == num_threads_) notify = true;
  }
  if (notify) barrier_cv_.NotifyOne();
}
} // namespace impala
