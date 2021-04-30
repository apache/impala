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

#include "cast-format-expr.h"

#include "exprs/scalar-expr-evaluator.h"
#include "exprs/timestamp-functions.h"
#include "runtime/datetime-iso-sql-format-tokenizer.h"
#include "runtime/runtime-state.h"
#include "udf/udf.h"

using namespace impala;
using namespace datetime_parse_util;

Status CastFormatExpr::OpenEvaluator(FunctionContext::FunctionStateScope scope,
    RuntimeState* state, ScalarExprEvaluator* eval) const {
  DCHECK(eval != nullptr);
  RETURN_IF_ERROR(ScalarFnCall::OpenEvaluator(scope, state, eval));
  if (scope != FunctionContext::FRAGMENT_LOCAL) return Status::OK();
  DCHECK_GE(fn_ctx_idx_, 0);
  FunctionContext* fn_ctx = eval->fn_context(fn_ctx_idx_);

  std::unique_ptr<DateTimeFormatContext> dt_ctx =
      std::make_unique<DateTimeFormatContext>(format_.c_str(), format_.length());
  bool accept_time_toks = (fn_ctx->GetReturnType().type != FunctionContext::TYPE_DATE &&
                           fn_ctx->GetArgType(0)->type != FunctionContext::TYPE_DATE);
  IsoSqlFormatTokenizer format_tokenizer(dt_ctx.get(), GetCastMode(fn_ctx),
      accept_time_toks);
  FormatTokenizationResult parse_result = format_tokenizer.Tokenize();
  if (parse_result != FormatTokenizationResult::SUCCESS) {
    ReportBadFormat(fn_ctx, parse_result, StringVal(format_.c_str()), true);
    return Status(TErrorCode::INTERNAL_ERROR, fn_ctx->error_msg());
  }
  dt_ctx->SetCenturyBreakAndCurrentTime(*fn_ctx->impl()->state()->now());
  fn_ctx->SetFunctionState(scope, dt_ctx.release());

  return Status::OK();
}

void CastFormatExpr::CloseEvaluator(FunctionContext::FunctionStateScope scope,
    RuntimeState* state, ScalarExprEvaluator* eval) const {
  DCHECK(eval != nullptr);
  if (scope == FunctionContext::FRAGMENT_LOCAL) {
    DCHECK_GE(fn_ctx_idx_, 0);
    FunctionContext* fn_ctx = eval->fn_context(fn_ctx_idx_);
    DateTimeFormatContext* dt_ctx =
        (DateTimeFormatContext*) fn_ctx->GetFunctionState(scope);
    if (dt_ctx != nullptr) {
      delete dt_ctx;
      fn_ctx->SetFunctionState(scope, nullptr);
    }
  }
  ScalarFnCall::CloseEvaluator(scope, state, eval);
}

CastDirection CastFormatExpr::GetCastMode(const FunctionContext* ctx) const {
  DCHECK(ctx != nullptr);
  FunctionContext::TypeDesc type_desc = ctx->GetReturnType();
  if (type_desc.type == FunctionContext::TYPE_STRING ||
      type_desc.type == FunctionContext::TYPE_VARCHAR) {
    return FORMAT;
  }
  return PARSE;
}

