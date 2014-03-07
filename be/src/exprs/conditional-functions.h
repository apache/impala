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
  static void* IsNull(Expr* e, TupleRow* row);
  template <typename T> static void* NullIf(Expr* e, TupleRow* row);

  // Return NULL if the numeric argument is zero, the argument otherwise.
  // Returns the same type as the argument.
  template <typename T> static void* NullIfZero(Expr* e, TupleRow* row);

  // Returns 0 if the argument is NULL, the argument otherwise. Returns the
  // same type as the argument.
  template <typename T> static void* ZeroIfNull(Expr* e, TupleRow* row);

  template <typename T> static void* IfFn(Expr* e, TupleRow* row);
  template <typename T> static void* Coalesce(Expr* e, TupleRow* row);

  // Compute function of case expr if its has_case_expr_ is false.
  static void* NoCaseComputeFn(Expr* e, TupleRow* row);
};

}

#endif
