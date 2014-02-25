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

#include "exprs/expr.h"

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

}


