// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_STRING_FUNCTIONS_H
#define IMPALA_EXPRS_STRING_FUNCTIONS_H

#include "runtime/string-value.h"
#include "runtime/string-search.h"

namespace impala {

class Expr;
class OpcodeRegistry;
class TupleRow;

// TODO: We use std::string::append() in a few places despite reserving
// enough space. Look into whether that is a performance issue.
class StringFunctions {
 public:
  static void* Substring(Expr* e, TupleRow* row);
  static void* Left(Expr* e, TupleRow* row);
  static void* Right(Expr* e, TupleRow* row);
  static void* Length(Expr* e, TupleRow* row);
  static void* Lower(Expr* e, TupleRow* row);
  static void* Upper(Expr* e, TupleRow* row);
  static void* Reverse(Expr* e, TupleRow* row);
  static void* Trim(Expr* e, TupleRow* row);
  static void* Ltrim(Expr* e, TupleRow* row);
  static void* Rtrim(Expr* e, TupleRow* row);
  static void* Space(Expr* e, TupleRow* row);
  static void* Repeat(Expr* e, TupleRow* row);
  static void* Ascii(Expr* e, TupleRow* row);
  static void* Lpad(Expr* e, TupleRow* row);
  static void* Rpad(Expr* e, TupleRow* row);
  static void* Instr(Expr* e, TupleRow* row);
  static void* Locate(Expr* e, TupleRow* row);
  static void* LocatePos(Expr* e, TupleRow* row);
  static void* RegexpExtract(Expr* e, TupleRow* row);
  static void* RegexpReplace(Expr* e, TupleRow* row);
  static void* Concat(Expr* e, TupleRow* row);
  static void* ConcatWs(Expr* e, TupleRow* row);
  static void* FindInSet(Expr* e, TupleRow* row);
  static void* ParseUrl(Expr* e, TupleRow* row);
  static void* ParseUrlKey(Expr* e, TupleRow* row);
};

}

#endif
