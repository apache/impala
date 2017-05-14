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


#ifndef IMPALA_EXPRS_NULL_LITERAL_H_
#define IMPALA_EXPRS_NULL_LITERAL_H_

#include "exprs/scalar-expr.h"

namespace impala {

using impala_udf::FunctionContext;
using impala_udf::BooleanVal;
using impala_udf::TinyIntVal;
using impala_udf::SmallIntVal;
using impala_udf::IntVal;
using impala_udf::BigIntVal;
using impala_udf::FloatVal;
using impala_udf::DoubleVal;
using impala_udf::TimestampVal;
using impala_udf::StringVal;
using impala_udf::DecimalVal;

class TExprNode;

class NullLiteral: public ScalarExpr {
 public:
  virtual bool IsLiteral() const override { return true; }
  virtual Status GetCodegendComputeFn(LlvmCodeGen* codegen, llvm::Function** fn)
      override WARN_UNUSED_RESULT;
  virtual std::string DebugString() const override;

  /// Constructor for test.
  NullLiteral(PrimitiveType type) : ScalarExpr(type, true) { }

 protected:
  friend class ScalarExpr;
  friend class ScalarExprEvaluator;

  NullLiteral(const TExprNode& node) : ScalarExpr(node) { }

  virtual BooleanVal GetBooleanVal(ScalarExprEvaluator*, const TupleRow*) const override;
  virtual TinyIntVal GetTinyIntVal(ScalarExprEvaluator*, const TupleRow*) const override;
  virtual SmallIntVal GetSmallIntVal(
      ScalarExprEvaluator*, const TupleRow*) const override;
  virtual IntVal GetIntVal(ScalarExprEvaluator*, const TupleRow*) const override;
  virtual BigIntVal GetBigIntVal(ScalarExprEvaluator*, const TupleRow*) const override;
  virtual FloatVal GetFloatVal(ScalarExprEvaluator*, const TupleRow*) const override;
  virtual DoubleVal GetDoubleVal(ScalarExprEvaluator*, const TupleRow*) const override;
  virtual StringVal GetStringVal(ScalarExprEvaluator*, const TupleRow*) const override;
  virtual TimestampVal GetTimestampVal(
      ScalarExprEvaluator*, const TupleRow*) const override;
  virtual DecimalVal GetDecimalVal(ScalarExprEvaluator*, const TupleRow*) const override;
  virtual CollectionVal GetCollectionVal(
      ScalarExprEvaluator*, const TupleRow*) const override;
};

}

#endif
