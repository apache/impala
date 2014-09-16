// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef IMPALA_EXPRS_LITERAL_H_
#define IMPALA_EXPRS_LITERAL_H_

#include <string>
#include "exprs/expr.h"
#include "exprs/expr-value.h"
#include "runtime/string-value.h"

namespace impala {

class TExprNode;

class Literal: public Expr {
 public:
  // Test ctors
  Literal(ColumnType type, bool v);
  Literal(ColumnType type, int8_t v);
  Literal(ColumnType type, int16_t v);
  Literal(ColumnType type, int32_t v);
  Literal(ColumnType type, int64_t v);
  Literal(ColumnType type, float v);
  Literal(ColumnType type, double v);
  Literal(ColumnType type, const std::string& v);
  Literal(ColumnType type, const StringValue& v);

  // Test function that parses 'str' according to 'type'. The caller owns the returned
  // Literal.
  static Literal* CreateLiteral(const ColumnType& type, const std::string& str);

  virtual Status GetCodegendComputeFn(RuntimeState* state, llvm::Function** fn);

  virtual impala_udf::BooleanVal GetBooleanVal(ExprContext*, TupleRow*);
  virtual impala_udf::TinyIntVal GetTinyIntVal(ExprContext*, TupleRow*);
  virtual impala_udf::SmallIntVal GetSmallIntVal(ExprContext*, TupleRow*);
  virtual impala_udf::IntVal GetIntVal(ExprContext*, TupleRow*);
  virtual impala_udf::BigIntVal GetBigIntVal(ExprContext*, TupleRow*);
  virtual impala_udf::FloatVal GetFloatVal(ExprContext*, TupleRow*);
  virtual impala_udf::DoubleVal GetDoubleVal(ExprContext*, TupleRow*);
  virtual impala_udf::StringVal GetStringVal(ExprContext*, TupleRow*);
  virtual impala_udf::DecimalVal GetDecimalVal(ExprContext*, TupleRow*);

 protected:
  friend class Expr;

  Literal(const TExprNode& node);

  virtual std::string DebugString() const;

 private:
  ExprValue value_;
};

}

#endif
