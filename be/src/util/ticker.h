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

#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>

#include <boost/bind.hpp>

#include "common/atomic.h"
#include "common/status.h"
#include "util/thread.h"

namespace impala {

// Manages a thread that periodically notifies a condition variable. This thread never
// returns. An indicator variable must be specified to guard against spurious wakeups.
//
// Immediately before this class notfies the condition variable, it sets the indicator
// variable to the `wakeup_value` specified in the constructor. It is the responsibility
// of the thread consuming this class to reset the indicator variable to a value other
// than `wakeup_value` before the consuming thread goes to sleep.
//
// If the periodic code takes longer to run than the specified duration, then the code
// will immediately execute the next time around.
//
// Internally, this class uses std::this_thread:sleep_for which may sleep for longer than
// the specified duration due to scheduling or resource contention delays.
// For details, see https://en.cppreference.com/w/cpp/thread/sleep_for.
//
// Example usage:
//
//   #include <chrono>
//   #include <condition_variable>
//   #include <memory>
//   #include <mutex>
//
//   #include "common/status.h"
//
//   std::condition_variable cv;
//   std::mutex mu;
//   std::shared_ptr<bool> wakeup_guard = make_shared<bool>();
//   Ticker<std::chrono::seconds, bool> ticker(std::chrono::seconds(30), cv, mu,
//       wakeup_guard, true);
//
//   ABORT_IF_ERROR(ticker.Start());
//
//   while(true) {
//     unique_lock<mutex> l(mu);
//     cv.wait(l, ticker.WakeupGuard());
//     *wakeup_guard = false;
//
//     run_my_code();
//   }

template <typename DurationType, typename IndicatorType>
class Ticker {
  public:
    Ticker(DurationType interval, std::condition_variable& cv,
        std::mutex& lock, std::shared_ptr<IndicatorType> indicator,
        IndicatorType wakeup_value) : interval_(interval), cv_(cv), lock_(lock),
        indicator_(indicator), wakeup_value_(wakeup_value) {}

    Status Start(const std::string& category, const std::string& name) {
      return Thread::Create(category, name, &Ticker::run, this, &my_thread_);
    }

    // Specify that the next iteration of this ticker be the last. This function does not
    // block nor does it cause the ticker to wake up earlier than scheduled.
    void RequestStop() {
      stop_requested_.Store(true);
    }

    // Wait for the ticker to exit after it's final iteration.
    void Join() {
      my_thread_->Join();
    }

    // Provides a default implementation for the condition variable predicate lambda.
    std::function<bool()> WakeupGuard() {
      return [this]{ return *indicator_ == wakeup_value_; };
    }

  protected:
    const DurationType interval_;
    std::condition_variable& cv_;
    std::mutex& lock_;
    std::shared_ptr<IndicatorType> indicator_;
    const IndicatorType wakeup_value_;

  private:
    std::unique_ptr<Thread> my_thread_;
    AtomicBool stop_requested_;

    void run() {
      while (!stop_requested_.Load()) {
        std::this_thread::sleep_for(interval_);

        {
          std::lock_guard<std::mutex> l(lock_);
          *indicator_ = wakeup_value_;
        }

        cv_.notify_all();
      }
    }
}; // class Ticker

// Specialization of the Ticker class that uses seconds for the duration and bool as the
// wakeup indicator. The boolean shared_ptr indicator is internally managed. Use the
// ResetWakeupGuard() function in your code immediately after the condition variable wait
// to set the internally managed wakeup guard for the next iteration.
class TickerSecondsBool : public Ticker<std::chrono::seconds, bool> {
  public:
    TickerSecondsBool(uint32_t interval, std::condition_variable& cv,
      std::mutex& lock) :
      Ticker(std::chrono::seconds(interval), cv, lock, std::make_shared<bool>(), true) {}

    void ResetWakeupGuard() {
      *indicator_ = false;
    }
}; // class TickerSecondsBool

} // namespace impala
