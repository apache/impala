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


#ifndef IMPALA_EXPRS_COMPOUND_PREDICATES_H_
#define IMPALA_EXPRS_COMPOUND_PREDICATES_H_

#include <string>
#include "exprs/predicate.h"
#include "gen-cpp/Exprs_types.h"

namespace impala {

using impala_udf::FunctionContext;
using impala_udf::BooleanVal;

class CompoundPredicate: public Predicate {
 public:
  static BooleanVal Not(FunctionContext* context, const BooleanVal&);

 protected:
  CompoundPredicate(const TExprNode& node) : Predicate(node) { }

  Status CodegenComputeFn(bool and_fn, LlvmCodeGen* codegen, llvm::Function** fn);
};

/// Expr for evaluating and (&&) operators
class AndPredicate: public CompoundPredicate {
 public:
  virtual BooleanVal GetBooleanValInterpreted(
      ScalarExprEvaluator*, const TupleRow*) const;

  virtual Status GetCodegendComputeFnImpl(LlvmCodeGen* codegen, llvm::Function** fn) {
    return CompoundPredicate::CodegenComputeFn(true, codegen, fn);
  }

 protected:
  friend class ScalarExpr;
  AndPredicate(const TExprNode& node) : CompoundPredicate(node) { }

  virtual std::string DebugString() const;

 private:
  friend class OpcodeRegistry;
};

/// Expr for evaluating or (||) operators
class OrPredicate: public CompoundPredicate {
 public:
  virtual BooleanVal GetBooleanValInterpreted(
      ScalarExprEvaluator*, const TupleRow*) const;

  virtual Status GetCodegendComputeFnImpl(LlvmCodeGen* codegen, llvm::Function** fn) {
    return CompoundPredicate::CodegenComputeFn(false, codegen, fn);
  }

 protected:
  friend class ScalarExpr;
  OrPredicate(const TExprNode& node) : CompoundPredicate(node) { }

  virtual std::string DebugString() const;

 private:
  friend class OpcodeRegistry;
};

}

#endif
