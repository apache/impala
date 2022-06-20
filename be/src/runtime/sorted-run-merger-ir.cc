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

#include "runtime/exec-env.h"
#include "runtime/runtime-state.h"
#include "util/runtime-profile-counters.h"
#include "runtime/sorted-run-merger.h"

namespace impala {

void SortedRunMerger::HeapifyHelper(int parent_index) {
  int left_index = 2 * parent_index + 1;
  int right_index = left_index + 1;
  int least_child;
  while (left_index < min_heap_.size()) {
    // Find the least child of parent.
    if (right_index >= min_heap_.size() ||
        comparator_.Less(min_heap_[left_index]->current_row(),
                        min_heap_[right_index]->current_row())) {
      least_child = left_index;
    } else {
      least_child = right_index;
    }

    // If the parent is out of place, swap it with the least child.
    if (comparator_.Less(min_heap_[least_child]->current_row(),
            min_heap_[parent_index]->current_row())) {
      iter_swap(min_heap_.begin() + least_child, min_heap_.begin() + parent_index);
      parent_index = least_child;
    } else {
      break;
    }
    left_index = 2 * parent_index + 1;
    right_index = left_index + 1;
  }
}

}