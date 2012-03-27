# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/exprs")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/exprs")

add_library(Exprs
  agg-expr.cc
  arithmetic-expr.cc
  binary-predicate.cc
  bool-literal.cc
  case-expr.cc
  cast-expr.cc
  compound-predicate.cc
  conditional-functions.cc
  date-literal.cc
  expr.cc
  float-literal.cc
  function-call.cc
  int-literal.cc
  is-null-predicate.cc
  like-predicate.cc
  literal-predicate.cc
  math-functions.cc
  null-literal.cc
  opcode-registry.cc
  slot-ref.cc
  string-literal.cc
  string-functions.cc
  timestamp-functions.cc  
)

target_link_libraries(Exprs
  Util
  boost_regex-mt
)

add_executable(expr-test
  expr-test.cc
)

# The order of link libaries matters.  Specifically:
#  - Util must come before ImpalaThrift
#  - Exprs & Opcode have a circular dependency (Exprs is included twice)
target_link_libraries(expr-test
  TestUtil
  Service
  Exec
  Exprs
  Opcode
  Exprs
  Runtime
  Common
  Util
  thriftstatic
  ImpalaThrift
  Exec
  Exprs
  Opcode
  Exprs
  gtest
  ${Boost_LIBRARIES}
)

add_test(expr-test ${BUILD_OUTPUT_ROOT_DIRECTORY}/exprs/expr-test)
