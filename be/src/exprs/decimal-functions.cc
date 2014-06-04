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

#include "exprs/decimal-functions.h"

#include "exprs/decimal-operators.h"
#include "exprs/expr.h"
#include "exprs/function-call.h"

#include <ctype.h>
#include <math.h>

using namespace std;

namespace impala {

void* DecimalFunctions::Precision(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  DCHECK_EQ(e->children()[0]->type().type, TYPE_DECIMAL);
  e->result_.int_val = e->children()[0]->type().precision;
  return &e->result_.int_val;
}

void* DecimalFunctions::Scale(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  DCHECK_EQ(e->children()[0]->type().type, TYPE_DECIMAL);
  e->result_.int_val = e->children()[0]->type().scale;
  return &e->result_.int_val;
}

void* DecimalFunctions::Abs(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  DCHECK_EQ(e->children()[0]->type().type, TYPE_DECIMAL);
  DCHECK_EQ(e->type().type, TYPE_DECIMAL);
  DCHECK_EQ(e->type().scale, e->children()[0]->type().scale);
  DCHECK_EQ(e->type().precision, e->children()[0]->type().precision);

  void* v = e->children()[0]->GetValue(row);
  if (v == NULL) return NULL;
  switch (e->type().GetByteSize()) {
    case 4:
      e->result_.decimal4_val = reinterpret_cast<Decimal4Value*>(v)->Abs();
      return &e->result_.decimal4_val;
    case 8:
      e->result_.decimal8_val = reinterpret_cast<Decimal8Value*>(v)->Abs();
      return &e->result_.decimal8_val;
    case 16:
      e->result_.decimal16_val = reinterpret_cast<Decimal16Value*>(v)->Abs();
      return &e->result_.decimal16_val;
    default:
      DCHECK(false);
      return NULL;
  }
}

void* DecimalFunctions::Ceil(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  DCHECK_EQ(e->children()[0]->type().type, TYPE_DECIMAL);
  DCHECK_EQ(e->type().type, TYPE_DECIMAL);
  DCHECK_EQ(e->type().scale, 0);
  DCHECK_LE(e->type().precision, e->children()[0]->type().precision);
  return DecimalOperators::RoundDecimal(e, row, DecimalOperators::CEIL);
}

void* DecimalFunctions::Floor(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  DCHECK_EQ(e->children()[0]->type().type, TYPE_DECIMAL);
  DCHECK_EQ(e->type().type, TYPE_DECIMAL);
  DCHECK_EQ(e->type().scale, 0);
  DCHECK_LE(e->type().precision, e->children()[0]->type().precision);
  return DecimalOperators::RoundDecimal(e, row, DecimalOperators::FLOOR);
}

void* DecimalFunctions::Round(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  DCHECK_EQ(e->children()[0]->type().type, TYPE_DECIMAL);
  DCHECK_EQ(e->type().type, TYPE_DECIMAL);
  DCHECK_EQ(e->type().scale, 0);
  DCHECK_LE(e->type().precision, e->children()[0]->type().precision);
  return DecimalOperators::RoundDecimal(e, row, DecimalOperators::ROUND);
}

void* DecimalFunctions::RoundTo(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  DCHECK_EQ(e->children()[0]->type().type, TYPE_DECIMAL);
  // We require that the second argument to Round() (the resulting scale) to be
  // a constant, otherwise the resulting values will have variable scale.
  DCHECK(e->children()[1]->IsConstant());
  DCHECK_EQ(e->type().type, TYPE_DECIMAL);
  FunctionCall* fn_call = reinterpret_cast<FunctionCall*>(e);
  if (fn_call->scale() < 0) {
    return DecimalOperators::RoundDecimalNegativeScale(
        e, row, DecimalOperators::ROUND, -fn_call->scale());
  } else {
    return DecimalOperators::RoundDecimal(e, row, DecimalOperators::ROUND);
  }
}

void* DecimalFunctions::Truncate(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 1);
  DCHECK_EQ(e->children()[0]->type().type, TYPE_DECIMAL);
  DCHECK_EQ(e->type().type, TYPE_DECIMAL);
  DCHECK_EQ(e->type().scale, 0);
  DCHECK_LE(e->type().precision, e->children()[0]->type().precision);
  return DecimalOperators::RoundDecimal(e, row, DecimalOperators::TRUNCATE);
}

void* DecimalFunctions::TruncateTo(Expr* e, TupleRow* row) {
  DCHECK_EQ(e->GetNumChildren(), 2);
  DCHECK_EQ(e->children()[0]->type().type, TYPE_DECIMAL);
  // We require that the second argument to Truncate() (the resulting scale) to be
  // a constant, otherwise the resulting values will have variable scale.
  DCHECK(e->children()[1]->IsConstant());
  DCHECK_EQ(e->type().type, TYPE_DECIMAL);
  FunctionCall* fn_call = reinterpret_cast<FunctionCall*>(e);
  if (fn_call->scale() < 0) {
    return DecimalOperators::RoundDecimalNegativeScale(
        e, row, DecimalOperators::TRUNCATE, -fn_call->scale());
  } else {
    return DecimalOperators::RoundDecimal(e, row, DecimalOperators::TRUNCATE);
  }
}

}
