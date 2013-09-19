// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exprs/expr.h"
#include "udf/udf.h"

#ifdef IR_COMPILE

// Generate a llvm loadable function for calling GetValue on an Expr.  This is
// used as an adapter for Expr's that do not have an IR implementation.
extern "C"
void* IrExprGetValue(Expr* expr, TupleRow* row) {
  return expr->GetValue(row);
}

// Dummy function to force compilation of UdfContext type
void dummy(impala_udf::UdfContext) { }

#else
#error "This file should only be compiled by clang."
#endif

