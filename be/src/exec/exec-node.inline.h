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

#include "codegen/impala-ir.h"
#include "exec/exec-node.h"
#include "exprs/scalar-expr-evaluator.h"

namespace impala {

// This function is replaced by codegen.
// It is an inline function to reduce function call overhead for interpreted code.
inline bool IR_NO_INLINE ExecNode::EvalConjuncts(
    ScalarExprEvaluator* const* evals, int num_conjuncts, TupleRow* row) {
  for (int i = 0; i < num_conjuncts; ++i) {
    if (!evals[i]->EvalPredicate(row)) return false;
  }
  return true;
}
}
