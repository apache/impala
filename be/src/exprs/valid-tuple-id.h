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

#ifndef IMPALA_EXPRS_VALID_TUPLE_ID_H_
#define IMPALA_EXPRS_VALID_TUPLE_ID_H_

#include "exprs/scalar-expr.h"

namespace impala {

class TExprNode;

/// Returns the tuple id of the single non-NULL tuple in the input row.
/// Valid input rows must have exactly one non-NULL tuple.
class ValidTupleIdExpr : public ScalarExpr {
 public:
  static const char* LLVM_CLASS_NAME;

 protected:
  friend class ScalarExpr;

  explicit ValidTupleIdExpr(const TExprNode& node);

  virtual Status Init(
      const RowDescriptor& row_desc, bool is_entry_point, FragmentState* state) override;
  virtual Status GetCodegendComputeFnImpl(
      LlvmCodeGen* codegen, llvm::Function** fn) override WARN_UNUSED_RESULT;
  virtual std::string DebugString() const override;

  virtual IntVal GetIntValInterpreted(
      ScalarExprEvaluator*, const TupleRow*) const override;

 private:
  /// Maps from tuple index in the row to its corresponding tuple id.
  std::vector<TupleId> tuple_ids_;

  /// Returns the number of tuples in 'row' that are non-null. Used for debugging.
  static int ComputeNonNullCount(const TupleRow* row, int num_tuples);

  /// Returns true if the tuple of the row at the specified index is non-null.
  /// Called by Codegen.
  static bool IsTupleFromRowNonNull(const TupleRow* row, int index);
};

} // namespace impala

#endif
