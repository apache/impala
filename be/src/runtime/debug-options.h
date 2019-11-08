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

#ifndef IMPALA_RUNTIME_DEBUG_OPTIONS_H
#define IMPALA_RUNTIME_DEBUG_OPTIONS_H

#include <vector>
#include <string>

#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/ImpalaInternalService_types.h"

namespace impala {

// Container for debug options in TPlanFragmentInstanceCtx (debug_node, debug_action,
// debug_phase).
// TODO: move to subdirectory /coordinator
class DebugOptions {
 public:
  DebugOptions(const TQueryOptions& query_options);
  TDebugOptions ToThrift() const;

  /// query-wide fragment instance index; -1 if not set
  int instance_idx() const { return instance_idx_; }

  /// The node the debug option should be applied to. -1 if it should be applied to all
  /// nodes.
  int node_id() const { return node_id_; }

  /// True if a debug action is enabled.
  bool enabled() const { return phase_ != TExecNodePhase::INVALID; }

  TDebugAction::type action() const { return action_; }
  TExecNodePhase::type phase() const { return phase_; }
  std::string action_param() const { return action_param_; }

 private:
  int instance_idx_;
  int node_id_;
  TDebugAction::type action_;
  TExecNodePhase::type phase_; // INVALID: debug options invalid
  // A string parameter that goes along with 'action_'. The semantics depend on the
  // specific action.
  std::string action_param_;

  static TExecNodePhase::type GetExecNodePhase(const std::string& key);
  // Parse a debug action from a string. Either of format "ACTION_TYPE" or
  // "ACTION_TYPE@<param value>". Returns true when 'action' is a valid debug action
  // string.
  static bool GetDebugAction(
      const std::string& action, TDebugAction::type* type, std::string* action_param);
};
}

#endif
