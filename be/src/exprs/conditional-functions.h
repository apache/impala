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


#ifndef IMPALA_EXPRS_CONDITIONAL_FUNCTIONS_H
#define IMPALA_EXPRS_CONDITIONAL_FUNCTIONS_H

#include <stdint.h>

#include "exprs/scalar-expr.h"
#include "udf/udf.h"

namespace impala {

using impala_udf::FunctionContext;
using impala_udf::AnyVal;
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
using impala_udf::DateVal;

class ScalarExprEvaluator;
class TupleRow;

/// Conditional functions that can be expressed as UDFs
class ConditionalFunctions {
 public:
  static TinyIntVal NullIfZero(FunctionContext* context, const TinyIntVal& val);
  static SmallIntVal NullIfZero(FunctionContext* context, const SmallIntVal& val);
  static IntVal NullIfZero(FunctionContext* context, const IntVal& val);
  static BigIntVal NullIfZero(FunctionContext* context, const BigIntVal& val);
  static FloatVal NullIfZero(FunctionContext* context, const FloatVal& val);
  static DoubleVal NullIfZero(FunctionContext* context, const DoubleVal& val);
  static DecimalVal NullIfZero(FunctionContext* context, const DecimalVal& val);

  static TinyIntVal ZeroIfNull(FunctionContext* context, const TinyIntVal& val);
  static SmallIntVal ZeroIfNull(FunctionContext* context, const SmallIntVal& val);
  static IntVal ZeroIfNull(FunctionContext* context, const IntVal& val);
  static BigIntVal ZeroIfNull(FunctionContext* context, const BigIntVal& val);
  static FloatVal ZeroIfNull(FunctionContext* context, const FloatVal& val);
  static DoubleVal ZeroIfNull(FunctionContext* context, const DoubleVal& val);
  static DecimalVal ZeroIfNull(FunctionContext* context, const DecimalVal& val);

  /// Functions IsFalse and IsTrue return false when the input is NULL.
  /// Functions IsNotFalse and IsNotTrue return true when the input is NULL.
  static BooleanVal IsFalse(FunctionContext* ctx, const BooleanVal& val);
  static BooleanVal IsNotFalse(FunctionContext* ctx, const BooleanVal& val);
  static BooleanVal IsTrue(FunctionContext* ctx, const BooleanVal& val);
  static BooleanVal IsNotTrue(FunctionContext* ctx, const BooleanVal& val);
};

/// The following conditional functions require separate Expr classes to take advantage of
/// short circuiting

class IsNullExpr : public ScalarExpr {
 public:
  virtual Status GetCodegendComputeFnImpl(LlvmCodeGen* codegen, llvm::Function** fn)
      override WARN_UNUSED_RESULT;
  virtual std::string DebugString() const override {
    return ScalarExpr::DebugString("IsNullExpr");
  }

 protected:
  friend class ScalarExpr;
  friend class ScalarExprEvaluator;
  IsNullExpr(const TExprNode& node) : ScalarExpr(node) {}
  GENERATE_GET_VAL_INTERPRETED_OVERRIDES_FOR_ALL_SCALAR_TYPES
};

class IfExpr : public ScalarExpr {
 public:
  virtual Status GetCodegendComputeFnImpl(LlvmCodeGen* codegen, llvm::Function** fn)
      override WARN_UNUSED_RESULT;
  virtual std::string DebugString() const override {
    return ScalarExpr::DebugString("IfExpr");
  }

 protected:
  friend class ScalarExpr;
  friend class ScalarExprEvaluator;

  IfExpr(const TExprNode& node) : ScalarExpr(node) {}
  GENERATE_GET_VAL_INTERPRETED_OVERRIDES_FOR_ALL_SCALAR_TYPES
};

class CoalesceExpr : public ScalarExpr {
 public:
  virtual Status GetCodegendComputeFnImpl(LlvmCodeGen* codegen, llvm::Function** fn)
      override WARN_UNUSED_RESULT;
  virtual std::string DebugString() const override {
    return ScalarExpr::DebugString("CoalesceExpr");
  }

 protected:
  friend class ScalarExpr;
  friend class ScalarExprEvaluator;

  CoalesceExpr(const TExprNode& node) : ScalarExpr(node) { }
  GENERATE_GET_VAL_INTERPRETED_OVERRIDES_FOR_ALL_SCALAR_TYPES
};

}

#endif
