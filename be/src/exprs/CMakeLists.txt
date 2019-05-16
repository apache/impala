# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/exprs")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/exprs")

add_library(Exprs
  agg-fn.cc
  agg-fn-evaluator.cc
  agg-fn-evaluator-ir.cc
  aggregate-functions-ir.cc
  anyval-util.cc
  bit-byte-functions-ir.cc
  case-expr.cc
  cast-functions-ir.cc
  compound-predicates.cc
  compound-predicates-ir.cc
  conditional-functions.cc
  conditional-functions-ir.cc
  date-functions-ir.cc
  decimal-functions-ir.cc
  decimal-operators-ir.cc
  expr.cc
  hive-udf-call.cc
  in-predicate-ir.cc
  is-not-empty-predicate.cc
  is-null-predicate-ir.cc
  kudu-partition-expr.cc
  like-predicate.cc
  like-predicate-ir.cc
  literal.cc
  math-functions-ir.cc
  null-literal.cc
  operators-ir.cc
  scalar-expr.cc
  scalar-expr-evaluator.cc
  scalar-expr-ir.cc
  slot-ref.cc
  string-functions.cc
  string-functions-ir.cc
  timestamp-functions.cc
  timestamp-functions-ir.cc
  timezone_db.cc
  tuple-is-null-predicate.cc
  scalar-fn-call.cc
  udf-builtins.cc
  udf-builtins-ir.cc
  utility-functions.cc
  utility-functions-ir.cc
  valid-tuple-id.cc
)
add_dependencies(Exprs gen-deps gen_ir_descriptions)

ADD_BE_LSAN_TEST(expr-test)
ADD_BE_LSAN_TEST(expr-codegen-test)
ADD_BE_LSAN_TEST(timezone_db-test)

# expr-codegen-test includes test IR functions
COMPILE_TO_IR(expr-codegen-test.cc)
add_dependencies(expr-codegen-test-ir gen-deps)
add_dependencies(expr-codegen-test expr-codegen-test-ir)

ADD_UDF_TEST(aggregate-functions-test)
