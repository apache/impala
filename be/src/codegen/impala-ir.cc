// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Includes source files for cross compiling to IR.  By having all of the code in
// one .cc file, clang will be able to compile all of code into one module.
// All cross compiled code needs to be added to this file.
// All files here must be added explicitly to the codegen/CMakeLists.txt dependency list

#ifdef IR_COMPILE

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wheader-hygiene"

#include "codegen/codegen-anyval-ir.cc"
#include "exec/grouping-aggregator-ir.cc"
#include "exec/hash-table-ir.cc"
#include "exec/avro/hdfs-avro-scanner-ir.cc"
#include "exec/hdfs-columnar-scanner-ir.cc"
#include "exec/hdfs-scanner-ir.cc"
#include "exec/kudu/kudu-util-ir.cc"
#include "exec/non-grouping-aggregator-ir.cc"
#include "exec/partitioned-hash-join-builder-ir.cc"
#include "exec/partitioned-hash-join-node-ir.cc"
#include "exec/select-node-ir.cc"
#include "exec/topn-node-ir.cc"
#include "exec/union-node-ir.cc"
#include "exprs/agg-fn-evaluator-ir.cc"
#include "exprs/aggregate-functions-ir.cc"
#include "exprs/bit-byte-functions-ir.cc"
#include "exprs/cast-functions-ir.cc"
#include "exprs/compound-predicates-ir.cc"
#include "exprs/conditional-functions-ir.cc"
#include "exprs/datasketches-functions-ir.cc"
#include "exprs/date-functions-ir.cc"
#include "exprs/decimal-functions-ir.cc"
#include "exprs/decimal-operators-ir.cc"
#include "exprs/hive-udf-call-ir.cc"
#include "exprs/iceberg-functions-ir.cc"
#include "exprs/in-predicate-ir.cc"
#include "exprs/is-null-predicate-ir.cc"
#include "exprs/kudu-partition-expr-ir.cc"
#include "exprs/like-predicate-ir.cc"
#include "exprs/mask-functions-ir.cc"
#include "exprs/math-functions-ir.cc"
#include "exprs/operators-ir.cc"
#include "exprs/scalar-expr-ir.cc"
#include "exprs/scalar-expr-evaluator-ir.cc"
#include "exprs/string-functions-ir.cc"
#include "exprs/timestamp-functions-ir.cc"
#include "exprs/tuple-is-null-predicate-ir.cc"
#include "exprs/udf-builtins-ir.cc"
#include "exprs/utility-functions-ir.cc"
#include "runtime/krpc-data-stream-sender-ir.cc"
#include "runtime/mem-pool.h"
#include "runtime/raw-value-ir.cc"
#include "runtime/runtime-filter-ir.cc"
#include "runtime/sorter-ir.cc"
#include "runtime/string-value-ir.cc"
#include "runtime/tuple-ir.cc"
#include "runtime/sorted-run-merger-ir.cc"
#include "udf/udf-ir.cc"
#include "util/bloom-filter-ir.cc"
#include "util/hash-util-ir.cc"
#include "util/in-list-filter-ir.cc"
#include "util/min-max-filter-ir.cc"

#pragma clang diagnostic pop

// Unused function to make sure printf declaration is included in IR module. Used by
// LlvmCodegen::CodegenDebugTrace().
void printf_dummy_fn() {
  printf("dummy");
}

#else
#error "This file should only be used for cross compiling to IR."
#endif
