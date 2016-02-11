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


#ifndef IMPALA_EXPRS_NULL_LITERAL_H_
#define IMPALA_EXPRS_NULL_LITERAL_H_

#include "exprs/expr.h"

namespace impala {

class TExprNode;

class NullLiteral: public Expr {
 public:
  NullLiteral(PrimitiveType type) : Expr(type) { }
  virtual Status GetCodegendComputeFn(RuntimeState* state, llvm::Function** fn);

  virtual impala_udf::BooleanVal GetBooleanVal(ExprContext*, TupleRow*);
  virtual impala_udf::TinyIntVal GetTinyIntVal(ExprContext*, TupleRow*);
  virtual impala_udf::SmallIntVal GetSmallIntVal(ExprContext*, TupleRow*);
  virtual impala_udf::IntVal GetIntVal(ExprContext*, TupleRow*);
  virtual impala_udf::BigIntVal GetBigIntVal(ExprContext*, TupleRow*);
  virtual impala_udf::FloatVal GetFloatVal(ExprContext*, TupleRow*);
  virtual impala_udf::DoubleVal GetDoubleVal(ExprContext*, TupleRow*);
  virtual impala_udf::StringVal GetStringVal(ExprContext*, TupleRow*);
  virtual impala_udf::TimestampVal GetTimestampVal(ExprContext*, TupleRow*);
  virtual impala_udf::DecimalVal GetDecimalVal(ExprContext*, TupleRow*);
  virtual impala_udf::CollectionVal GetCollectionVal(ExprContext*, TupleRow*);

  virtual std::string DebugString() const;

 protected:
  friend class Expr;
  
  NullLiteral(const TExprNode& node) : Expr(node) { }
};

}

#endif
