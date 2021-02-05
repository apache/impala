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


#ifndef IMPALA_EXPRS_LITERAL_H_
#define IMPALA_EXPRS_LITERAL_H_

#include <string>
#include "exprs/scalar-expr.h"
#include "exprs/expr-value.h"
#include "runtime/string-value.h"
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
class TExprNode;

class Literal: public ScalarExpr {
  friend bool operator==(const Literal& lhs, const Literal& rhs);
 public:
  virtual bool IsLiteral() const override { return true; }
  virtual Status GetCodegendComputeFnImpl(LlvmCodeGen* codegen, llvm::Function** fn)
      override WARN_UNUSED_RESULT;
  virtual std::string DebugString() const override;

 protected:
  friend class ExprTest;
  friend class ScalarExpr;
  friend class ScalarExprEvaluator;

  Literal(const TExprNode& node);

  /// Test ctors
  Literal(ColumnType type, bool v);
  Literal(ColumnType type, int8_t v);
  Literal(ColumnType type, int16_t v);
  Literal(ColumnType type, int32_t v);
  Literal(ColumnType type, int64_t v);
  Literal(ColumnType type, float v);
  Literal(ColumnType type, double v);
  Literal(ColumnType type, const std::string& v);
  Literal(ColumnType type, const StringValue& v);
  Literal(ColumnType type, const TimestampValue& v);
  Literal(ColumnType type, const DateValue& v);

  GENERATE_GET_VAL_INTERPRETED_OVERRIDES_FOR_ALL_SCALAR_TYPES

 private:
  ExprValue value_;
};

}

#endif
