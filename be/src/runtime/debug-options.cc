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

#include <boost/algorithm/string.hpp>
#include <gutil/strings/substitute.h>

#include "runtime/debug-options.h"
#include "util/debug-util.h"
#include "common/logging.h"

#include "common/names.h"

using namespace impala;
using namespace std;
using namespace boost;

DebugOptions::DebugOptions(const TQueryOptions& query_options)
  : instance_idx_(-1),
    node_id_(-1),
    action_(TDebugAction::WAIT),
    phase_(TExecNodePhase::INVALID) {
  if (!query_options.__isset.debug_action || query_options.debug_action.empty()) {
    // signal not set
    return;
  }
  const DebugActionTokens& actions = TokenizeDebugActions(query_options.debug_action);
  for (const vector<string>& components : actions) {
    // This will filter out global debug actions which only have two components.
    if (components.size() < 3 || components.size() > 4) continue;

    const string* phase_str;
    const string* action_str;
    if (components.size() == 3) {
      instance_idx_ = -1;
      node_id_ = atoi(components[0].c_str());
      phase_str = &components[1];
      action_str = &components[2];
    } else {
      instance_idx_ = atoi(components[0].c_str());
      node_id_ = atoi(components[1].c_str());
      phase_str = &components[2];
      action_str = &components[3];
    }
    phase_ = GetExecNodePhase(*phase_str);
    if (phase_ == TExecNodePhase::INVALID) {
      LOG(WARNING) << Substitute("Invalid debug_action phase $0", *phase_str);
      return;
    }
    if (!GetDebugAction(*action_str, &action_, &action_param_)) {
      LOG(WARNING) << Substitute("Invalid debug_action command $0", *action_str);
      phase_ = TExecNodePhase::INVALID;
      return;
    }
    if ((phase_ == TExecNodePhase::PREPARE || phase_ == TExecNodePhase::CLOSE) &&
        action_ == TDebugAction::WAIT) {
      // Cancellation is not allowed before PREPARE phase completes (to keep FIS
      // startup simpler - it will be checked immediately after when execution begins).
      // Close() must always run to completion to free resources, so cancellation does
      // not make sense for the CLOSE phase and is not checked. So we disallow these.
      LOG(WARNING) << Substitute("Do not use $0:$1 debug actions because nodes cannot "
          "be cancelled in that phase.", *phase_str, *action_str);
      phase_ = TExecNodePhase::INVALID;
      return;
    }
    return; // Found ExecNode debug action
  }
}

TExecNodePhase::type DebugOptions::GetExecNodePhase(const string& key) {
  map<int, const char*>::const_iterator entry =
      _TExecNodePhase_VALUES_TO_NAMES.begin();
  for (; entry != _TExecNodePhase_VALUES_TO_NAMES.end(); ++entry) {
    if (iequals(key, (*entry).second)) {
      return static_cast<TExecNodePhase::type>(entry->first);
    }
  }
  return TExecNodePhase::INVALID;
}

bool DebugOptions::GetDebugAction(
    const string& action, TDebugAction::type* type, string* action_param) {
  // Either "ACTION_TYPE" or "ACTION_TYPE@<integer value>".
  vector<string> tokens = TokenizeDebugActionParams(action);
  if (tokens.size() < 1 || tokens.size() > 2) return false;
  if (tokens.size() == 2) *action_param = tokens[1];
  for (auto& entry : _TDebugAction_VALUES_TO_NAMES) {
    if (iequals(tokens[0], entry.second)) {
      *type = static_cast<TDebugAction::type>(entry.first);
      return true;
    }
  }
  return false;
}


TDebugOptions DebugOptions::ToThrift() const {
  TDebugOptions result;
  result.__set_node_id(node_id_);
  result.__set_phase(phase_);
  result.__set_action(action_);
  result.__set_action_param(action_param_);
  return result;
}
