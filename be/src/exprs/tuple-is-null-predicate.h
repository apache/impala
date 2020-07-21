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


#ifndef IMPALA_EXPRS_TUPLE_IS_NULL_PREDICATE_H_
#define IMPALA_EXPRS_TUPLE_IS_NULL_PREDICATE_H_

#include "exprs/predicate.h"

namespace impala {

class TExprNode;

/// Returns true if all of the given tuples are NULL, false otherwise.
/// Returns false if any of the given tuples is non-nullable.
/// It is important that this predicate not require the given tuples to be nullable,
/// because the FE sometimes wrap expressions in this predicate that contain SlotRefs
/// on non-nullable tuples (see IMPALA-904/IMPALA-5504). This happens for exprs that
/// are evaluated before the outer join that makes the tuples given to this predicate
/// nullable, e.g., in the ON-clause of that join.
class TupleIsNullPredicate: public Predicate {
 protected:
  friend class ScalarExpr;

  TupleIsNullPredicate(const TExprNode& node);

  virtual Status Init(
      const RowDescriptor& row_desc, bool is_entry_point, FragmentState* state) override;
  virtual Status GetCodegendComputeFnImpl(
      LlvmCodeGen* codegen, llvm::Function** fn) override WARN_UNUSED_RESULT;
  virtual std::string DebugString() const override;

  virtual BooleanVal GetBooleanValInterpreted(
      ScalarExprEvaluator*, const TupleRow*) const override;

 private:
  void FillCodegendComputeFnConstantFalse(
      LlvmCodeGen* codegen, llvm::Function* function) const;
  void FillCodegendComputeFnNonConstant(
      LlvmCodeGen* codegen, llvm::Function* function, llvm::Value* args[2]) const;

  /// Tuple ids to check for NULL. May contain ids of nullable and non-nullable tuples.
  std::vector<TupleId> tuple_ids_;

  /// Tuple indexes into the RowDescriptor. Only contains indexes of nullable tuples.
  std::vector<int32_t> tuple_idxs_;
};

}

#endif
