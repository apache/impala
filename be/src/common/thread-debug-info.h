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

#include <string>

#include "glog/logging.h"
#include "gutil/macros.h"
#include "util/debug-util.h"

#include "common/names.h"

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
    // This call makes the global (thread local) pointer point to this object.
    InitializeThreadDebugInfo(this);
  }

  ~ThreadDebugInfo() {
    // Resets the global (thread local) pointer to null.
    CloseThreadDebugInfo();
  }

  const char* GetInstanceId() const { return instance_id_; }
  const char* GetThreadName() const { return thread_name_; }

  /// Saves the string representation of param 'instance_id' to member 'instance_id_'
  void SetInstanceId(const TUniqueId& instance_id) {
    string id_str = PrintId(instance_id);
    DCHECK_LT(id_str.length(), TUNIQUE_ID_STRING_SIZE);
    id_str.copy(instance_id_, id_str.length());
  }

  /// Saves param 'thread_name' to member 'thread_name_'.
  /// If the length of param 'thread_name' is larger than THREAD_NAME_SIZE,
  /// we store the front of 'thread_name' + '...' + the last few bytes
  /// of thread name, e.g.: "Long Threadname with more te...001afec4)"
  void SetThreadName(const string& thread_name) {
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

private:
  /// Initializes a thread local pointer with thread_debug_info.
  static void InitializeThreadDebugInfo(ThreadDebugInfo* thread_debug_info);
  /// Resets the thread local pointer to nullptr.
  static void CloseThreadDebugInfo();

  static constexpr int64_t TUNIQUE_ID_STRING_SIZE = 34;
  static constexpr int64_t THREAD_NAME_SIZE = 256;
  static constexpr int64_t THREAD_NAME_TAIL_LENGTH = 8;

  char instance_id_[TUNIQUE_ID_STRING_SIZE] = {};
  char thread_name_[THREAD_NAME_SIZE] = {};

  DISALLOW_COPY_AND_ASSIGN(ThreadDebugInfo);
};

/// Returns a pointer to the ThreadDebugInfo object for this thread.
/// The ThreadDebugInfo object needs to be created before this function is called.
ThreadDebugInfo* GetThreadDebugInfo();

}

#endif

