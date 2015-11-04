# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/exprs")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/exprs")

add_library(Exprs
  agg-fn-evaluator.cc
  aggregate-functions.cc
  anyval-util.cc
  bit-byte-functions.cc
  case-expr.cc
  cast-functions.cc
  compound-predicates.cc
  compound-predicates-ir.cc
  conditional-functions.cc
  conditional-functions-ir.cc
  decimal-functions.cc
  decimal-operators.cc
  expr.cc
  expr-context.cc
  expr-ir.cc
  hive-udf-call.cc
  in-predicate-ir.cc
  is-null-predicate.cc
  like-predicate.cc
  literal.cc
  math-functions.cc
  null-literal.cc
  operators.cc
  slot-ref.cc
  string-functions.cc
  timestamp-functions.cc
  timezone_db.cc
  tuple-is-null-predicate.cc
  scalar-fn-call.cc
  udf-builtins.cc
  utility-functions.cc
)
add_dependencies(Exprs thrift-deps gen_ir_descriptions)

add_executable(expr-benchmark expr-benchmark.cc)
target_link_libraries(expr-benchmark ${IMPALA_TEST_LINK_LIBS})

ADD_BE_TEST(expr-test)
ADD_BE_TEST(expr-codegen-test)

# expr-codegen-test includes test IR functions
COMPILE_TO_IR(expr-codegen-test.cc)
add_dependencies(expr-codegen-test-ir thrift-deps)

ADD_EXECUTABLE(aggregate-functions-test aggregate-functions-test.cc)
TARGET_LINK_LIBRARIES(aggregate-functions-test ${UDF_TEST_LINK_LIBS})
ADD_TEST(uda-test "${EXECUTABLE_OUTPUT_PATH}/aggregate-functions-test")

ADD_BE_BENCHMARK(in-predicate-benchmark)
