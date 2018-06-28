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

// Utilities for ExecNode implementations.

#pragma once

#include "exec/exec-node.h"
#include "util/runtime-profile-counters.h"
#include "util/time.h"

namespace impala {

/// Scoped object that is intended to be used in the top-level scope of an Open()
/// implementation to update node lifecycle events in 'node->events_'.
/// Does not add or update timers if 'node->events_' is NULL (i.e. in subplans).
class ScopedOpenEventAdder {
 public:
  ScopedOpenEventAdder(ExecNode* node) : node_(node) {
    if (node_->events_ == nullptr) return;
    node->events_->MarkEvent("Open Started");
  }

  ~ScopedOpenEventAdder() {
    if (node_->events_ == nullptr) return;
    node_->events_->MarkEvent("Open Finished");
  }

 private:
  ExecNode* node_;
};

/// Scoped object that is intended to be used in the top-level scope of a GetNext()
/// implementation to update node lifecycle events in 'node->events_'.
/// Does not add or update timers if 'node->events_' is NULL (i.e. in subplans).
class ScopedGetNextEventAdder {
 public:
  /// 'eos' is the 'eos' argument to GetNext(), used to check if this GetNext()
  /// call set *eos = true.
  ScopedGetNextEventAdder(ExecNode* node, bool* eos) : node_(node), eos_(eos) {
    // Don't set if this was already initialized.
    if (node->events_ == nullptr || node->first_getnext_added_) return;
    node->events_->MarkEvent("First Batch Requested");
  }

  ~ScopedGetNextEventAdder() {
    if (node_->events_ == nullptr) return;
    if (!node_->first_getnext_added_) {
      node_->events_->MarkEvent("First Batch Returned");
      node_->first_getnext_added_ = true;
    }
    // Note that we won't mark the last batch if the node is closed before eos,
    // e.g. if an ancestor node hits a limit.
    if (*eos_) node_->events_->MarkEvent("Last Batch Returned");
  }

 private:
  ExecNode* const node_;
  bool* const eos_;
};
} // namespace impala
