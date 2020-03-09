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

#ifndef IMPALA_COMMON_THREAD_DEBUG_INFO_H
#define IMPALA_COMMON_THREAD_DEBUG_INFO_H

#include <cstdint>
#include <string>
#include <syscall.h>
#include <unistd.h>

#include "gen-cpp/Types_types.h"
#include "gutil/macros.h"
#include "gutil/strings/util.h"

namespace impala {

/// Stores information about the current thread that can be useful in a debug session.
/// An object of this class needs to be allocated on the stack in order to include
/// it in minidumps. While this object is alive, it is available through the global
/// function 'GetThreadDebugInfo()'.
/// During a debug session, locate this object in the core file or in the minidump,
/// then inspect its members.
class ThreadDebugInfo {
public:
  /// Only one ThreadDebugInfo object can be alive per thread at a time.
  /// This object is not copyable, nor movable
  ThreadDebugInfo() {
    system_thread_id_ = syscall(SYS_gettid);

    // This call makes the global (thread local) pointer point to this object.
    InitializeThreadDebugInfo(this);
  }

  ~ThreadDebugInfo() {
    // Resets the global (thread local) pointer to null.
    CloseThreadDebugInfo();
  }

  const TUniqueId& GetQueryId() const { return query_id_; }
  const TUniqueId& GetInstanceId() const { return instance_id_; }
  const char* GetThreadName() const { return thread_name_; }
  int64_t GetSystemThreadId() const { return system_thread_id_; }
  int64_t GetParentSystemThreadId() const { return parent_.system_thread_id_; }
  const char* GetParentThreadName() const { return parent_.thread_name_; }

  /// Saves 'query_id' to member 'query_id_'
  void SetQueryId(const TUniqueId& query_id) {
    query_id_ = query_id;
  }
  /// Saves 'instance_id' to member 'instance_id_'
  void SetInstanceId(const TUniqueId& instance_id) {
    instance_id_ = instance_id;
  }

  /// Saves param 'thread_name' to member 'thread_name_'.
  /// If the length of param 'thread_name' is larger than THREAD_NAME_SIZE,
  /// we store the front of 'thread_name' + '...' + the last few bytes
  /// of thread name, e.g.: "Long Threadname with more te...001afec4)"
  void SetThreadName(const std::string& thread_name) {
    const int64_t length = thread_name.length();

    if (length < THREAD_NAME_SIZE) {
      thread_name.copy(thread_name_, length);
    } else {
      const int64_t tail_length = THREAD_NAME_TAIL_LENGTH;
      // 4 is the length of "..." and '\0'
      const int64_t front_length = THREAD_NAME_SIZE - tail_length - 4;
      // copy 'front_length' sized front of 'thread_name' to 'thread_name_'
      thread_name.copy(thread_name_, front_length);
      // append "..."
      for (int i = 0; i < 3; ++i) thread_name_[front_length + i] = '.';
      // append 'tail_length' sized tail of 'thread_name' to 'thread_name_'
      thread_name.copy(thread_name_ + front_length + 3, tail_length,
          length - tail_length);
    }
  }

  void SetParentInfo(const ThreadDebugInfo* parent) {
    if (parent == nullptr) return;
    parent_.system_thread_id_ = parent->system_thread_id_;
    strings::strlcpy(parent_.thread_name_, parent->thread_name_, THREAD_NAME_SIZE);
    query_id_ = parent->query_id_;
    instance_id_ = parent->instance_id_;
  }

private:
  /// Initializes a thread local pointer with thread_debug_info.
  static void InitializeThreadDebugInfo(ThreadDebugInfo* thread_debug_info);
  /// Resets the thread local pointer to nullptr.
  static void CloseThreadDebugInfo();

  static constexpr int64_t THREAD_NAME_SIZE = 256;
  static constexpr int64_t THREAD_NAME_TAIL_LENGTH = 8;
  static const TUniqueId ZERO_THREAD_ID;

  /// This struct contains information we want to store about the parent.
  struct ParentInfo {
    int64_t system_thread_id_ = 0;
    char thread_name_[THREAD_NAME_SIZE] = {};
  };

  ParentInfo parent_;
  int64_t system_thread_id_ = 0;
  char thread_name_[THREAD_NAME_SIZE] = {};
  // Will be "0" (the default TUniqueId with hi=lo=0) when thread isn't
  // part of query or instance execution.
  TUniqueId query_id_;
  TUniqueId instance_id_;

  friend class ScopedThreadContext;
  friend class ThreadDebugInfo_Scoping_Test;

  DISALLOW_COPY_AND_ASSIGN(ThreadDebugInfo);
};

/// An RAII-style wrapper to clear thread debug info query and instance
/// context at destruction.
class ScopedThreadContext {
  public:
    ~ScopedThreadContext() {
      thread_debug_info_->SetQueryId(ThreadDebugInfo::ZERO_THREAD_ID);
      thread_debug_info_->SetInstanceId(ThreadDebugInfo::ZERO_THREAD_ID);
    }
    ScopedThreadContext(ThreadDebugInfo* thread_debug_info, const TUniqueId& query_id,
        const TUniqueId& instance_id = ThreadDebugInfo::ZERO_THREAD_ID) {
      thread_debug_info_ = thread_debug_info;
      thread_debug_info->SetQueryId(query_id);
      thread_debug_info->SetInstanceId(instance_id);
    }
  private:
    ThreadDebugInfo* thread_debug_info_;
    DISALLOW_COPY_AND_ASSIGN(ScopedThreadContext);
};

/// Returns a pointer to the ThreadDebugInfo object for this thread.
/// Returns nullptr if there is no ThreadDebugInfo object for the current thread.
ThreadDebugInfo* GetThreadDebugInfo();

}

#endif

