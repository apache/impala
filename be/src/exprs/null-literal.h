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

#include "exprs/expr.h"

namespace impala {

class TExprNode;

class NullLiteral: public Expr {
 public:
  NullLiteral(PrimitiveType type) : Expr(type, true, false) { }

  virtual bool IsLiteral() const;

  virtual Status GetCodegendComputeFn(LlvmCodeGen* codegen, llvm::Function** fn);

  virtual impala_udf::BooleanVal GetBooleanVal(ExprContext*, const TupleRow*);
  virtual impala_udf::TinyIntVal GetTinyIntVal(ExprContext*, const TupleRow*);
  virtual impala_udf::SmallIntVal GetSmallIntVal(ExprContext*, const TupleRow*);
  virtual impala_udf::IntVal GetIntVal(ExprContext*, const TupleRow*);
  virtual impala_udf::BigIntVal GetBigIntVal(ExprContext*, const TupleRow*);
  virtual impala_udf::FloatVal GetFloatVal(ExprContext*, const TupleRow*);
  virtual impala_udf::DoubleVal GetDoubleVal(ExprContext*, const TupleRow*);
  virtual impala_udf::StringVal GetStringVal(ExprContext*, const TupleRow*);
  virtual impala_udf::TimestampVal GetTimestampVal(ExprContext*, const TupleRow*);
  virtual impala_udf::DecimalVal GetDecimalVal(ExprContext*, const TupleRow*);
  virtual impala_udf::CollectionVal GetCollectionVal(ExprContext*, const TupleRow*);

  virtual std::string DebugString() const;

 protected:
  friend class Expr;

  NullLiteral(const TExprNode& node) : Expr(node) { }
};

}

#endif
