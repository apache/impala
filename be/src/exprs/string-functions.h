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


#ifndef IMPALA_EXPRS_STRING_FUNCTIONS_H
#define IMPALA_EXPRS_STRING_FUNCTIONS_H

#include "runtime/string-value.h"
#include "runtime/string-search.h"

namespace impala {

class Expr;
class OpcodeRegistry;
class TupleRow;

class StringFunctions {
 public:
  template <class T> static void* Substring(Expr* e, TupleRow* row);
  template <class T> static void* Left(Expr* e, TupleRow* row);
  template <class T> static void* Right(Expr* e, TupleRow* row);
  template <class T> static void* Space(Expr* e, TupleRow* row);
  template <class T> static void* Repeat(Expr* e, TupleRow* row);
  template <class T> static void* Lpad(Expr* e, TupleRow* row);
  template <class T> static void* Rpad(Expr* e, TupleRow* row);
  static void* Length(Expr* e, TupleRow* row);
  static void* Lower(Expr* e, TupleRow* row);
  static void* Upper(Expr* e, TupleRow* row);
  static void* InitCap(Expr* e, TupleRow* row);
  static void* Reverse(Expr* e, TupleRow* row);
  static void* Translate(Expr* e, TupleRow* row);
  static void* Trim(Expr* e, TupleRow* row);
  static void* Ltrim(Expr* e, TupleRow* row);
  static void* Rtrim(Expr* e, TupleRow* row);
  static void* Ascii(Expr* e, TupleRow* row);
  static void* Instr(Expr* e, TupleRow* row);
  static void* Locate(Expr* e, TupleRow* row);
  template <class T> static void* LocatePos(Expr* e, TupleRow* row);
  template <class T> static void* RegexpExtract(Expr* e, TupleRow* row);
  static void* RegexpReplace(Expr* e, TupleRow* row);
  static void* Concat(Expr* e, TupleRow* row);
  static void* ConcatWs(Expr* e, TupleRow* row);
  static void* FindInSet(Expr* e, TupleRow* row);
  static void* ParseUrl(Expr* e, TupleRow* row);
  static void* ParseUrlKey(Expr* e, TupleRow* row);
};

}

#endif
