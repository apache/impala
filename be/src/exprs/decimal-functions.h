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


#ifndef IMPALA_EXPRS_DECIMAL_FUNCTIONS_H
#define IMPALA_EXPRS_DECIMAL_FUNCTIONS_H

namespace impala {

class Expr;
class TupleRow;

class DecimalFunctions {
 public:
  static void* Precision(Expr* e, TupleRow* row);
  static void* Scale(Expr* e, TupleRow* row);

  static void* Abs(Expr* e, TupleRow* row);
  static void* Ceil(Expr* e, TupleRow* row);
  static void* Floor(Expr* e, TupleRow* row);

  static void* Round(Expr* e, TupleRow* row);
  static void* RoundTo(Expr* e, TupleRow* row);

  static void* Truncate(Expr* e, TupleRow* row);
  static void* TruncateTo(Expr* e, TupleRow* row);
};

}

#endif
