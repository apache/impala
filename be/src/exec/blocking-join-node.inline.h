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

#include <cstring>

#include "codegen/impala-ir.h"
#include "exec/blocking-join-node.h"

namespace impala {

// This function is replaced by codegen.
// It is an inline function to reduce function call overhead for interpreted code.
inline void IR_NO_INLINE BlockingJoinNode::CreateOutputRow(
    TupleRow* out_row, TupleRow* probe_row, TupleRow* build_row) {
  uint8_t* out_ptr = reinterpret_cast<uint8_t*>(out_row);
  if (probe_row == nullptr) {
    memset(out_ptr, 0, probe_tuple_row_size_);
  } else {
    memcpy(out_ptr, probe_row, probe_tuple_row_size_);
  }
  if (build_row == nullptr) {
    memset(out_ptr + probe_tuple_row_size_, 0, build_tuple_row_size_);
  } else {
    memcpy(out_ptr + probe_tuple_row_size_, build_row, build_tuple_row_size_);
  }
}
}
