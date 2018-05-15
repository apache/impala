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

#include "exprs/valid-tuple-id.h"

#include <sstream>

#include "gen-cpp/Exprs_types.h"

#include "common/names.h"
#include "runtime/descriptors.h"
#include "runtime/tuple-row.h"

namespace impala {

ValidTupleIdExpr::ValidTupleIdExpr(const TExprNode& node) : ScalarExpr(node) {}

Status ValidTupleIdExpr::Init(const RowDescriptor& row_desc, RuntimeState* state) {
  RETURN_IF_ERROR(ScalarExpr::Init(row_desc, state));
  DCHECK_EQ(0, children_.size());
  tuple_ids_.reserve(row_desc.tuple_descriptors().size());
  for (TupleDescriptor* tuple_desc : row_desc.tuple_descriptors()) {
    tuple_ids_.push_back(tuple_desc->id());
  }
  return Status::OK();
}

int ValidTupleIdExpr::ComputeNonNullCount(const TupleRow* row) const {
  int num_tuples = tuple_ids_.size();
  int non_null_count = 0;
  for (int i = 0; i < num_tuples; ++i) non_null_count += (row->GetTuple(i) != nullptr);
  return non_null_count;
}

IntVal ValidTupleIdExpr::GetIntVal(ScalarExprEvaluator* eval, const TupleRow* row) const {
  // Validate that exactly one tuple is non-NULL.
  DCHECK_EQ(1, ComputeNonNullCount(row));
  int num_tuples = tuple_ids_.size();
  for (int i = 0; i < num_tuples; ++i) {
    if (row->GetTuple(i) != nullptr) return IntVal(tuple_ids_[i]);
  }
  return IntVal::null();
}

Status ValidTupleIdExpr::GetCodegendComputeFn(LlvmCodeGen* codegen, llvm::Function** fn) {
  return GetCodegendComputeFnWrapper(codegen, fn);
}

string ValidTupleIdExpr::DebugString() const {
  return "ValidTupleId()";
}

} // namespace impala
