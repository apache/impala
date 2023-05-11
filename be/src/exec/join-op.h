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

// Helpers for working with the different join operations.
#pragma once

#include "gen-cpp/PlanNodes_types.h" // for TJoinOp

namespace impala {

/// Returns true if this is a semi-join that does not return tuple data from the
/// build rows.
inline bool IsLeftSemiJoin(TJoinOp::type join_op) {
  return join_op == TJoinOp::LEFT_ANTI_JOIN || join_op == TJoinOp::LEFT_SEMI_JOIN
      || join_op == TJoinOp::NULL_AWARE_LEFT_ANTI_JOIN
      || join_op == TJoinOp::ICEBERG_DELETE_JOIN;
}

/// Returns true if this is a semi-join that does not return tuple data from the
/// probe rows.
inline bool IsRightSemiJoin(TJoinOp::type join_op) {
  return join_op == TJoinOp::RIGHT_ANTI_JOIN || join_op == TJoinOp::RIGHT_SEMI_JOIN;
}

inline bool IsSemiJoin(TJoinOp::type join_op) {
  return IsLeftSemiJoin(join_op) || IsRightSemiJoin(join_op);
}

/// Returns true if the join needs to process unmatched build rows, false
/// otherwise.
inline bool NeedToProcessUnmatchedBuildRows(TJoinOp::type join_op) {
  return join_op == TJoinOp::RIGHT_ANTI_JOIN || join_op == TJoinOp::RIGHT_OUTER_JOIN
      || join_op == TJoinOp::FULL_OUTER_JOIN;
}

/// Returns true if the join returns references to the build-side data.
inline bool ReturnsBuildData(TJoinOp::type join_op) {
  return join_op == TJoinOp::INNER_JOIN || join_op == TJoinOp::LEFT_OUTER_JOIN
      || join_op == TJoinOp::RIGHT_OUTER_JOIN || join_op == TJoinOp::RIGHT_ANTI_JOIN
      || join_op == TJoinOp::RIGHT_SEMI_JOIN || join_op == TJoinOp::FULL_OUTER_JOIN
      || join_op == TJoinOp::CROSS_JOIN;
}
} // namespace impala
