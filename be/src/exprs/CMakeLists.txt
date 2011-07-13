# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/exprs")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${CMAKE_SOURCE_DIR}/build/exprs")

add_library(Exprs
  agg-expr.cc
  arithmetic-expr.cc
  binary-predicate.cc
  bool-literal.cc
  case-expr.cc
  cast-expr.cc
  compound-predicate.cc
  date-literal.cc
  expr.cc
  float-literal.cc
  function-call.cc
  functions.cc
  int-literal.cc
  is-null-predicate.cc
  like-predicate.cc
  literal-predicate.cc
  slot-ref.cc
  string-literal.cc
)

add_custom_command(
  OUTPUT functions.cc
  COMMAND "./gen-functions.py"
)
