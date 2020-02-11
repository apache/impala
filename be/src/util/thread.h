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

#ifndef IMPALA_UTIL_THREAD_H
#define IMPALA_UTIL_THREAD_H

#include <memory>
#include <vector>

#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/thread_only.hpp>

#include "common/status.h"
#include "util/promise.h"

namespace impala {

class MetricGroup;
class ThreadDebugInfo;
class Webserver;

/// Thin wrapper around boost::thread that can register itself with the singleton
/// ThreadMgr (a private class implemented in thread.cc entirely, which tracks all live
/// threads so that they may be monitored via the debug webpages). This class has a
/// limited subset of boost::thread's API. Construction is almost the same, but clients
/// must supply a category and a name for each thread so that they can be identified in
/// the debug web UI. Otherwise, Join() is the only supported method from boost::thread.
//
/// Each Thread object knows its operating system thread ID (tid), which can be used to
/// attach debuggers to specific threads, to retrieve resource-usage statistics from the
/// operating system, and to assign threads to resource control groups.
//
/// TODO: Consider allowing fragment IDs as category parameters.
class Thread {
 public:
  /// This static Create method pattern mimics that in the boost::thread constructors.
  /// There is one static Create method for each number of arguments that the thread
  /// function accepts. To extend the set of acceptable signatures, add
  /// another static Create method with <class F, class A1.... class An>.
  //
  /// In general:
  ///  - category: string identifying the thread category to which this thread belongs,
  ///    used for organising threads together on the debug UI.
  ///  - name: name of this thread. Will be appended with "-<thread-id>" to ensure
  ///    uniqueness.
  ///  - F - a method type that supports operator(), and the instance passed to the
  ///    constructor is executed immediately in a separate thread.
  ///  - A1...An - argument types whose instances are passed to f(...)
  ///  - thread - unique_ptr<Thread>* to reset with the created Thread.
  ///  - fault_injection_eligible - If set to true, allow fault injection at this
  ///    callsite (see thread_creation_fault_injection). If set to false, fault
  ///    injection is diabled at this callsite. Thread creation sites that crash
  ///    Impala or abort startup must have this set to false.
  template <class F>
  static Status Create(const std::string& category, const std::string& name,
      const F& f, std::unique_ptr<Thread>* thread,
      bool fault_injection_eligible = false) {
    return StartThread(category, name, f, thread, fault_injection_eligible);
  }

  template <class F, class A1>
  static Status Create(const std::string& category, const std::string& name,
      const F& f, const A1& a1, std::unique_ptr<Thread>* thread,
      bool fault_injection_eligible = false) {
    return StartThread(category, name, boost::bind(f, a1), thread,
        fault_injection_eligible);
  }

  template <class F, class A1, class A2>
  static Status Create(const std::string& category, const std::string& name,
      const F& f, const A1& a1, const A2& a2, std::unique_ptr<Thread>* thread,
      bool fault_injection_eligible = false) {
    return StartThread(category, name, boost::bind(f, a1, a2), thread,
        fault_injection_eligible);
  }

  template <class F, class A1, class A2, class A3>
  static Status Create(const std::string& category, const std::string& name,
      const F& f, const A1& a1, const A2& a2, const A3& a3,
      std::unique_ptr<Thread>* thread, bool fault_injection_eligible = false) {
    return StartThread(category, name, boost::bind(f, a1, a2, a3), thread,
        fault_injection_eligible);
  }

  template <class F, class A1, class A2, class A3, class A4>
  static Status Create(const std::string& category, const std::string& name,
      const F& f, const A1& a1, const A2& a2, const A3& a3, const A4& a4,
      std::unique_ptr<Thread>* thread, bool fault_injection_eligible = false) {
    return StartThread(category, name, boost::bind(f, a1, a2, a3, a4), thread,
        fault_injection_eligible);
  }

  template <class F, class A1, class A2, class A3, class A4, class A5>
  static Status Create(const std::string& category, const std::string& name,
      const F& f, const A1& a1, const A2& a2, const A3& a3, const A4& a4, const A5& a5,
      std::unique_ptr<Thread>* thread, bool fault_injection_eligible = false) {
    return StartThread(category, name, boost::bind(f, a1, a2, a3, a4, a5), thread,
        fault_injection_eligible);
  }

  /// Blocks until this thread finishes execution. Once this method returns, the thread
  /// will be unregistered with the ThreadMgr and will not appear in the debug UI.
  void Join() const { thread_->join(); }

  /// Detaches the underlying thread from this Thread object. It's illegal to call
  /// Join() after calling Detach(). When the underlying thread finishes execution,
  /// it unregisters itself from the ThreadMgr.
  void Detach() const { thread_->detach(); }

  /// The thread ID assigned to this thread by the operating system. If the OS does not
  /// support retrieving the tid, returns Thread::INVALID_THREAD_ID.
  int64_t tid() const { return tid_; }

  const std::string& name() const { return name_; }

  static const int64_t INVALID_THREAD_ID = -1;

 private:
  Thread(const std::string& category, const std::string& name)
    : category_(category), name_(name), tid_(UNINITIALISED_THREAD_ID) {}

  /// To distinguish between a thread ID that can't be determined, and one that hasn't
  /// been assigned. Since tid_ is set in the constructor, this value will never be seen
  /// by clients of this class.
  static const int64_t UNINITIALISED_THREAD_ID = -2;

  /// Function object that wraps the user-supplied function to run in a separate thread.
  typedef boost::function<void ()> ThreadFunctor;

  /// The actual thread object that runs the user's method via SuperviseThread().
  boost::scoped_ptr<boost::thread> thread_;

  /// Name and category for this thread
  const std::string category_;
  const std::string name_;

  /// OS-specific thread ID. Set to UNINITIALISED_THREAD_ID initially, but once the
  /// constructor returns from StartThread() the tid_ is guaranteed to be set either to a
  /// non-negative integer, or INVALID_THREAD_ID.
  int64_t tid_;

  /// Creates a new thread and starts the thread running SuperviseThread(). It waits
  /// for notification from the started thread that initialisation is complete and
  /// the TID read before returning. This will return an error if thread create fails.
  /// In the event of success, 'thread' will be set to the created Thread.
  static Status StartThread(const std::string& category, const std::string& name,
      const ThreadFunctor& functor, std::unique_ptr<Thread>* thread,
      bool fault_injection_eligible) WARN_UNUSED_RESULT;

  /// Wrapper for the user-supplied function. Always invoked from thread_. Executes the
  /// method in functor_, but before doing so registers with the global ThreadMgr and
  /// reads the thread's system TID. After the method terminates, it is unregistered.
  //
  /// SuperviseThread() notifies StartThread() when thread initialisation is completed via
  /// the promise parameter, which is set to the new thread's system ID. After this point,
  /// it is no longer safe for SuperviseThread() to refer to parameters passed by
  /// reference or pointer to this method, because of a wrinkle in the lifecycle of boost
  /// threads: if the thread object representing a thread should be destroyed, the actual
  /// operating-system thread continues to run (the thread is detached, not
  /// terminated). Therefore it's not safe to make reference to the Thread object or any
  /// of its members in SuperviseThread() after it notifies the caller via thread_started
  /// that initialisation is completed.  An alternative is to join() in the destructor of
  /// Thread, but that's not the same semantics as boost::thread, which we are trying to
  /// emulate here.
  //
  /// As a result, the 'functor' parameter is deliberately copied into this method, since
  /// it is used after the notification completes.h The tid parameter is written to
  /// exactly once before SuperviseThread() notifies the caller.
  ///
  /// parent_thread_info points to the parent thread's ThreadDebugInfo object if the
  /// parent has one, otherwise it's a nullptr. As part of the initialisation
  /// SuperviseThread() copies the useful information from the parent's ThreadDebugInfo
  /// info object to its own TDI object. This way the TDI objects can preserve the thread
  /// creation graph.
  static void SuperviseThread(const std::string& name, const std::string& category,
      Thread::ThreadFunctor functor, const ThreadDebugInfo* parent_thread_info,
      Promise<int64_t>* thread_started);
};

/// Utility class to group together a set of threads. A replacement for
/// boost::thread_group. Not thread safe.
class ThreadGroup {
 public:
  ThreadGroup() {}

  /// Adds a new Thread to this group. The ThreadGroup takes ownership of the Thread, and
  /// will destroy it when the ThreadGroup is destroyed.  Threads will linger until that
  /// point (even if terminated), however, so callers should be mindful of the cost of
  /// placing very many threads in this set.
  void AddThread(std::unique_ptr<Thread>&& thread);

  /// Waits for all threads to finish. DO NOT call this from a thread inside this set;
  /// deadlock will predictably ensue.
  void JoinAll();

  /// Returns the number of threads in the group
  int Size() const;

 private:
  /// All the threads grouped by this set.
  std::vector<std::unique_ptr<Thread>> threads_;
};

/// Initialises the threading subsystem. Must be called before a Thread is created.
void InitThreading();

/// Registers /threadz with the debug webserver, and creates thread-tracking metrics under
/// the "thread-manager." If 'include_jvm_threads' is true, shows information about
/// live JVM threads in the web UI.
Status StartThreadInstrumentation(MetricGroup* metrics, Webserver* webserver,
    bool include_jvm_threads) WARN_UNUSED_RESULT;
}

#endif
