// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_EXPRS_UTILITY_FUNCTIONS_H
#define IMPALA_EXPRS_UTILITY_FUNCTIONS_H

namespace impala {

class Expr;
class OpcodeRegistry;
class TupleRow;

class UtilityFunctions {
 public:
  // Implementation of the version() function. Returns the version string.  
  static void* Version(Expr* e, TupleRow* row);
};

}

#endif
