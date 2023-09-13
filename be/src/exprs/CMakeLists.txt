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

set(MURMURHASH_SRC_DIR "${CMAKE_SOURCE_DIR}/be/src/thirdparty/murmurhash")

add_library(ExprsIr
  ai-functions-ir.cc
  agg-fn-evaluator-ir.cc
  aggregate-functions-ir.cc
  bit-byte-functions-ir.cc
  cast-functions-ir.cc
  compound-predicates-ir.cc
  conditional-functions-ir.cc
  datasketches-functions-ir.cc
  date-functions-ir.cc
  decimal-functions-ir.cc
  decimal-operators-ir.cc
  hive-udf-call-ir.cc
  iceberg-functions-ir.cc
  in-predicate-ir.cc
  is-null-predicate-ir.cc
  kudu-partition-expr-ir.cc
  like-predicate-ir.cc
  mask-functions-ir.cc
  math-functions-ir.cc
  operators-ir.cc
  scalar-expr-ir.cc
  scalar-expr-evaluator-ir.cc
  string-functions-ir.cc
  timestamp-functions-ir.cc
  tuple-is-null-predicate-ir.cc
  udf-builtins-ir.cc
  utility-functions-ir.cc
)
add_dependencies(ExprsIr gen-deps)

add_library(Exprs
  agg-fn.cc
  agg-fn-evaluator.cc
  anyval-util.cc
  case-expr.cc
  cast-format-expr.cc
  compound-predicates.cc
  conditional-functions.cc
  datasketches-common.cc
  expr.cc
  hive-udf-call.cc
  is-not-empty-predicate.cc
  kudu-partition-expr.cc
  like-predicate.cc
  literal.cc
  ${MURMURHASH_SRC_DIR}/MurmurHash3.cpp
  null-literal.cc
  scalar-expr-evaluator.cc
  scalar-expr.cc
  slot-ref.cc
  string-functions.cc
  timestamp-functions.cc
  timezone_db.cc
  tuple-is-null-predicate.cc
  scalar-fn-call.cc
  udf-builtins.cc
  utility-functions.cc
  valid-tuple-id.cc
)
add_dependencies(Exprs gen-deps)

if (BUILD_WITH_NO_TESTS)
  return()
endif()

add_library(ExprsTests STATIC
  datasketches-test.cc
  expr-test.cc
  iceberg-functions-test.cc
  timezone_db-test.cc
)
add_dependencies(ExprsTests gen-deps)

ADD_UNIFIED_BE_LSAN_TEST(datasketches-test
 "TestDataSketchesHll.*:TestDataSketchesKll.*:TestDataSketchesCpc.*:TestDataSketchesTheta.*")
ADD_UNIFIED_BE_LSAN_TEST(iceberg-functions-test "TestIcebergFunctions.*")
ADD_UNIFIED_BE_LSAN_TEST(expr-test "Instantiations/ExprTest.*")
# Exception to unified be tests: custom main initiailizes LLVM
ADD_BE_LSAN_TEST(expr-codegen-test)
ADD_UNIFIED_BE_LSAN_TEST(timezone_db-test
 "TimezoneDbNamesTest.*:TimezoneDbLoadAliasTest.*:TimezoneDbLoadZoneInfoTest.*")

# expr-codegen-test includes test IR functions
COMPILE_TO_IR(expr-codegen-test.cc)
add_dependencies(expr-codegen-test-ir gen-deps)
add_dependencies(expr-codegen-test expr-codegen-test-ir)

ADD_UDF_TEST(aggregate-functions-test)
