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

#include <algorithm>
#include <cstdint>

#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"

namespace impala {

inline int64_t ComputeDefaultProfileSizeLimitBytes(
    int64_t default_max_bytes, int64_t default_percentage) {
  ExecEnv* exec_env = ExecEnv::GetInstance();
  if (exec_env == nullptr || exec_env->process_mem_tracker() == nullptr) {
    return default_max_bytes;
  }
  const int64_t process_mem_limit = exec_env->process_mem_tracker()->limit();
  if (process_mem_limit <= 0) return default_max_bytes;

  int64_t percentage_limit_bytes =
      static_cast<int64_t>(default_percentage / 100.0 * process_mem_limit);
  percentage_limit_bytes = std::max<int64_t>(1L, percentage_limit_bytes);
  return std::min(default_max_bytes, percentage_limit_bytes);
}

} // namespace impala
