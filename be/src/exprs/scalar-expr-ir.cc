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

#include "exprs/scalar-expr.inline.h"
#include "udf/udf-internal.h"

#ifdef IR_COMPILE

/// Compile ScalarExprEvaluator declaration to IR so we can use it in codegen'd functions
#include "exprs/scalar-expr-evaluator.h"

/// Dummy function to force compilation of UDF types.
/// The arguments are pointers to prevent Clang from lowering the struct types
/// (e.g. IntVal={bool, i32} can be coerced to i64).
void dummy(impala_udf::FunctionContext*, impala_udf::BooleanVal*, impala_udf::TinyIntVal*,
    impala_udf::SmallIntVal*, impala_udf::IntVal*, impala_udf::BigIntVal*,
    impala_udf::FloatVal*, impala_udf::DoubleVal*, impala_udf::StringVal*,
    impala_udf::TimestampVal*, impala_udf::DecimalVal*, impala_udf::DateVal*,
    impala_udf::CollectionVal*, impala::ScalarExprEvaluator*) { }
#endif
