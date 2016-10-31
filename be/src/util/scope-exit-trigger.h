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

#ifndef IMPALA_UTIL_SCOPE_EXIT_TRIGGER_H
#define IMPALA_UTIL_SCOPE_EXIT_TRIGGER_H

#include <functional>

namespace impala {

/// Utility class that calls a client-supplied function when it is destroyed.
///
/// Use judiciously - scope exits can be hard to reason about, and this class should not
/// act as proxy for work-performing d'tors, which we try to avoid.
class ScopeExitTrigger {
 public:
  ScopeExitTrigger(const auto& trigger) : trigger_(trigger) {}

  ~ScopeExitTrigger() { trigger_(); }

 private:
  std::function<void()> trigger_;
};
}

#endif
