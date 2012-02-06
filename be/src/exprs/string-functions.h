// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_STRING_FUNCTIONS_H
#define IMPALA_EXPRS_STRING_FUNCTIONS_H

namespace impala {

class Expr;
class OpcodeRegistry;
class TupleRow;

class StringFunctions {
 public:
  static void* Substring(Expr* e, TupleRow* row);
  static void* Left(Expr* e, TupleRow* row);
  static void* Right(Expr* e, TupleRow* row);
  static void* Length(Expr* e, TupleRow* row);
  static void* Lower(Expr* e, TupleRow* row);
  static void* Upper(Expr* e, TupleRow* row);
  static void* Reverse(Expr* e, TupleRow* row);
};

}

#endif
