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


#ifndef IMPALA_EXPRS_CONDITIONAL_FUNCTIONS_H
#define IMPALA_EXPRS_CONDITIONAL_FUNCTIONS_H

#include <stdint.h>

namespace impala {

class Expr;
class TupleRow;

class ConditionalFunctions {
 public:
  static void* IfBool(Expr* e, TupleRow* row);
  static void* IfSmallint(Expr* e, TupleRow* row);
  static void* IfTinyint(Expr* e, TupleRow* row);
  static void* IfInt(Expr* e, TupleRow* row);
  static void* IfBigint(Expr* e, TupleRow* row);
  static void* IfFloat(Expr* e, TupleRow* row);
  static void* IfDouble(Expr* e, TupleRow* row);
  static void* IfString(Expr* e, TupleRow* row);
  static void* IfTimestamp(Expr* e, TupleRow* row);
  static void* CoalesceBool(Expr* e, TupleRow* row);
  static void* CoalesceInt(Expr* e, TupleRow* row);
  static void* CoalesceFloat(Expr* e, TupleRow* row);
  static void* CoalesceString(Expr* e, TupleRow* row);
  static void* CoalesceTimestamp(Expr* e, TupleRow* row);
  // Compute function of case expr if its has_case_expr_ is false.
  static void* NoCaseComputeFn(Expr* e, TupleRow* row);
};

}

#endif
