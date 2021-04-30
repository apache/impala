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

#pragma once

#include "exprs/scalar-fn-call.h"
#include "runtime/datetime-parser-common.h"
#include <string>

namespace impala {

/// This is a wrapper class around ScalarFnCall to store additional information specific
/// to the cast() operator.
class CastFormatExpr : public ScalarFnCall {
public:
  /// Note, that it should be verified at the callsite that TExprNode.cast_expr is set.
  CastFormatExpr(const TExprNode& node)
      : ScalarFnCall(node), format_(node.cast_expr.cast_format) {}

  /// Saves 'format_' into the FunctionContext object within the ScalarExprEvaluator.
  Status OpenEvaluator(FunctionContext::FunctionStateScope scope, RuntimeState* state,
      ScalarExprEvaluator* eval) const override WARN_UNUSED_RESULT;
  void CloseEvaluator(FunctionContext::FunctionStateScope scope, RuntimeState* state,
      ScalarExprEvaluator* eval) const override;
private:
  std::string format_;

  datetime_parse_util::CastDirection GetCastMode(const FunctionContext* ctx) const;
};

}

