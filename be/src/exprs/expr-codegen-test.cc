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

// The following is cross-compiled to native code and IR, and used in the test below
#include "exprs/decimal-operators.h"
#include "exprs/scalar-expr.h"
#include "runtime/fragment-state.h"
#include "runtime/query-state.h"
#include "udf/udf.h"

#ifdef IR_COMPILE
#include "exprs/decimal-operators-ir.cc"
#endif

using namespace impala;
using namespace impala_udf;

// TestGetTypeAttrs() fills in the following constants
struct FnAttr {
  int return_type_size;
  int arg0_type_size;
  int arg1_type_size;
  int arg2_type_size;
};

DecimalVal TestGetFnAttrs(
    FunctionContext* ctx, const DecimalVal& arg0, BooleanVal& arg1, StringVal& arg2) {
  FnAttr* state = reinterpret_cast<FnAttr*>(
      ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));
  state->return_type_size =
      ctx->impl()->GetConstFnAttr(FunctionContextImpl::RETURN_TYPE_SIZE);
  state->arg0_type_size =
      ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SIZE, 0);
  state->arg1_type_size =
      ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SIZE, 1);
  state->arg2_type_size =
      ctx->impl()->GetConstFnAttr(FunctionContextImpl::ARG_TYPE_SIZE, 2);
  // This function and its callees call FunctionContextImpl::GetConstFnAttr();
  return DecimalOperators::CastToDecimalVal(ctx, arg0);
}

// Don't compile the actual test to IR
#ifndef IR_COMPILE

#include "testutil/gtest-util.h"
#include "codegen/codegen-util.h"
#include "codegen/llvm-codegen.h"
#include "common/init.h"
#include "exprs/anyval-util.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "runtime/runtime-state.h"
#include "runtime/test-env.h"
#include "service/fe-support.h"
#include "udf/udf-internal.h"
#include "udf/udf-test-harness.h"

#include "gen-cpp/Exprs_types.h"

#include "common/names.h"

namespace impala {

const char* TEST_GET_FN_ATTR_SYMBOL =
    "_Z14TestGetFnAttrsPN10impala_udf15FunctionContextERKNS_10DecimalValERNS_10BooleanValERNS_9StringValE";

const int ARG0_PRECISION = 10;
const int ARG0_SCALE = 2;
const int ARG1_LEN = 1;
const int RET_PRECISION = 10;
const int RET_SCALE = 1;

class ExprCodegenTest : public ::testing::Test {
 protected:
  scoped_ptr<TestEnv> test_env_;
  RuntimeState* runtime_state_;
  FragmentState* fragment_state_;
  FunctionContext* fn_ctx_;
  FnAttr fn_type_attr_;

  int InlineConstFnAttrs(const Expr* expr, LlvmCodeGen* codegen, llvm::Function* fn) {
    FunctionContext::TypeDesc ret_type = AnyValUtil::ColumnTypeToTypeDesc(expr->type());
    vector<FunctionContext::TypeDesc> arg_types;
    for (const Expr* child : expr->children()) {
      arg_types.push_back(AnyValUtil::ColumnTypeToTypeDesc(child->type()));
    }
    return codegen->InlineConstFnAttrs(ret_type, arg_types, fn);
  }

  Status CreateFromFile(const string& filename, scoped_ptr<LlvmCodeGen>* codegen) {
    RETURN_IF_ERROR(LlvmCodeGen::CreateFromFile(fragment_state_,
        fragment_state_->obj_pool(), NULL, filename, "test", codegen));
    return (*codegen)->MaterializeModule();
  }

  virtual void SetUp() {
    TQueryOptions query_options;
    query_options.__set_disable_codegen(false);
    query_options.__set_decimal_v2(true);
    test_env_.reset(new TestEnv());
    ASSERT_OK(test_env_->Init());
    ASSERT_OK(test_env_->CreateQueryState(0, &query_options, &runtime_state_));
    QueryState* qs = runtime_state_->query_state();
    TPlanFragment* fragment = qs->obj_pool()->Add(new TPlanFragment());
    PlanFragmentCtxPB* fragment_ctx = qs->obj_pool()->Add(new PlanFragmentCtxPB());
    fragment_state_ =
        qs->obj_pool()->Add(new FragmentState(qs, *fragment, *fragment_ctx));

    FunctionContext::TypeDesc return_type;
    return_type.type = FunctionContext::TYPE_DECIMAL;
    return_type.precision = RET_PRECISION;
    return_type.scale = RET_SCALE;

    FunctionContext::TypeDesc arg0_type;
    arg0_type.type = FunctionContext::TYPE_DECIMAL;
    arg0_type.precision = ARG0_PRECISION;
    arg0_type.scale = ARG0_SCALE;

    FunctionContext::TypeDesc arg1_type;
    arg1_type.type = FunctionContext::TYPE_BOOLEAN;

    FunctionContext::TypeDesc arg2_type;
    arg2_type.type = FunctionContext::TYPE_STRING;

    vector<FunctionContext::TypeDesc> arg_types;
    arg_types.push_back(arg0_type);
    arg_types.push_back(arg1_type);
    arg_types.push_back(arg2_type);

    fn_ctx_ = UdfTestHarness::CreateTestContext(return_type, arg_types, runtime_state_);

    // Initialize fn_ctx_ with constants
    memset(&fn_type_attr_, -1, sizeof(FnAttr));
    fn_ctx_->SetFunctionState(FunctionContext::THREAD_LOCAL, &fn_type_attr_);
  }

  virtual void TearDown() {
    fn_ctx_->impl()->Close();
    delete fn_ctx_;
    fragment_state_->ReleaseResources();
    fragment_state_ = nullptr;
    runtime_state_ = nullptr;
    test_env_.reset();
  }

  void CheckFnAttr() {
    EXPECT_EQ(fn_type_attr_.return_type_size, 8);
    EXPECT_EQ(fn_type_attr_.arg0_type_size, 8);
    EXPECT_EQ(fn_type_attr_.arg1_type_size, ARG1_LEN);
    EXPECT_EQ(fn_type_attr_.arg2_type_size, 0); // varlen
  }

  static bool VerifyFunction(LlvmCodeGen* codegen, llvm::Function* fn) {
    return codegen->VerifyFunction(fn);
  }

  static void ResetVerification(LlvmCodeGen* codegen) {
    codegen->ResetVerification();
  }
};

TExprNode CreateBooleanLiteral() {
  TScalarType scalar_type;
  scalar_type.type = TPrimitiveType::BOOLEAN;

  TTypeNode type;
  type.type = TTypeNodeType::SCALAR;
  type.__set_scalar_type(scalar_type);

  TColumnType col_type;
  col_type.__set_types(vector<TTypeNode>(1, type));

  TBoolLiteral bool_literal;
  bool_literal.__set_value(true);

  TExprNode expr;
  expr.node_type = TExprNodeType::BOOL_LITERAL;
  expr.type = col_type;
  expr.num_children = 0;
  expr.__set_bool_literal(bool_literal);
  return expr;
}

TExprNode CreateDecimalLiteral(int precision, int scale) {
  TScalarType scalar_type;
  scalar_type.type = TPrimitiveType::DECIMAL;
  scalar_type.__set_precision(precision);
  scalar_type.__set_scale(scale);

  TTypeNode type;
  type.type = TTypeNodeType::SCALAR;
  type.__set_scalar_type(scalar_type);

  TColumnType col_type;
  col_type.__set_types(vector<TTypeNode>(1, type));

  TDecimalLiteral decimal_literal;
  decimal_literal.value = "\1";

  TExprNode expr;
  expr.node_type = TExprNodeType::DECIMAL_LITERAL;
  expr.type = col_type;
  expr.num_children = 0;
  expr.__set_decimal_literal(decimal_literal);
  return expr;
}

// len > 0 => char
TExprNode CreateStringLiteral(int len = -1) {
  TScalarType scalar_type;
  scalar_type.type = len > 0 ? TPrimitiveType::VARCHAR : TPrimitiveType::STRING;
  if (len > 0) scalar_type.__set_len(len);

  TTypeNode type;
  type.type = TTypeNodeType::SCALAR;
  type.__set_scalar_type(scalar_type);

  TColumnType col_type;
  col_type.__set_types(vector<TTypeNode>(1, type));

  TStringLiteral string_literal;
  string_literal.value = "\1";

  TExprNode expr;
  expr.node_type = TExprNodeType::STRING_LITERAL;
  expr.type = col_type;
  expr.num_children = 0;
  expr.__set_string_literal(string_literal);
  return expr;
}

// Creates a function call to TestGetFnAttrs() in test-udfs.h
TExprNode CreateFunctionCall(vector<TExprNode> children, int precision, int scale) {
  TScalarType scalar_type;
  scalar_type.type = TPrimitiveType::DECIMAL;
  scalar_type.__set_precision(precision);
  scalar_type.__set_scale(scale);

  TTypeNode type;
  type.type = TTypeNodeType::SCALAR;
  type.__set_scalar_type(scalar_type);

  TColumnType col_type;
  col_type.__set_types(vector<TTypeNode>(1, type));

  TFunctionName fn_name;
  fn_name.function_name = "test_get_type_attr";

  TScalarFunction scalar_fn;
  scalar_fn.symbol = TEST_GET_FN_ATTR_SYMBOL;

  TFunction fn;
  fn.name = fn_name;
  fn.binary_type = TFunctionBinaryType::IR;
  for (const TExprNode& child: children) {
    fn.arg_types.push_back(child.type);
  }
  fn.ret_type = col_type;
  fn.has_var_args = false;
  fn.__set_scalar_fn(scalar_fn);

  TExprNode expr;
  expr.node_type = TExprNodeType::FUNCTION_CALL;
  expr.type = col_type;
  expr.num_children = children.size();
  expr.__set_fn(fn);
  return expr;
}

TEST_F(ExprCodegenTest, TestGetConstFnAttrsInterpreted) {
  // Call fn and check results'. The input is of type Decimal(10,2) (i.e. 10000.25) and
  // the output type is Decimal(10,1) (i.e. 10000.3). The precision and scale of arguments
  // and return types are encoded above (ARG0_*, RET_*);
  int64_t v = 1000025;
  DecimalVal arg0_val(v);
  BooleanVal arg1_val;
  StringVal arg2_val;
  DecimalVal result = TestGetFnAttrs(fn_ctx_, arg0_val, arg1_val, arg2_val);
  // sanity check result
  EXPECT_EQ(result.is_null, false);
  EXPECT_EQ(result.val8, 100003);
  CheckFnAttr();
}

TEST_F(ExprCodegenTest, TestInlineConstFnAttrs) {
  // Setup thrift descriptors
  TExprNode arg0 = CreateDecimalLiteral(ARG0_PRECISION, ARG0_SCALE);
  TExprNode arg1 = CreateBooleanLiteral();
  TExprNode arg2 = CreateStringLiteral();

  vector<TExprNode> exprs;
  exprs.push_back(arg0);
  exprs.push_back(arg1);
  exprs.push_back(arg2);

  TExprNode fn_call = CreateFunctionCall(exprs, RET_PRECISION, RET_SCALE);
  exprs.insert(exprs.begin(), fn_call);

  TExpr texpr;
  texpr.__set_nodes(exprs);

  // Create Expr
  MemTracker tracker;
  ScalarExpr* expr;
  ASSERT_OK(ScalarExpr::Create(texpr, RowDescriptor(), fragment_state_, &expr));

  // Get TestGetFnAttrs() IR function
  stringstream test_udf_file;
  test_udf_file << getenv("IMPALA_HOME") << "/be/build/latest/exprs/expr-codegen-test.ll";
  scoped_ptr<LlvmCodeGen> codegen;
  ASSERT_OK(CreateFromFile(test_udf_file.str(), &codegen));
  llvm::Function* fn = codegen->GetFunction(TEST_GET_FN_ATTR_SYMBOL, false);
  ASSERT_TRUE(fn != NULL);

  // Function verification should fail because we haven't inlined GetTypeAttr() calls
  bool verification_succeeded = VerifyFunction(codegen.get(), fn);
  EXPECT_FALSE(verification_succeeded);

  // Call InlineConstFnAttrs() and rerun verification
  int replaced = InlineConstFnAttrs(expr, codegen.get(), fn);
  EXPECT_EQ(replaced, 19);
  ResetVerification(codegen.get());
  verification_succeeded = VerifyFunction(codegen.get(), fn);
  EXPECT_TRUE(verification_succeeded) << CodeGenUtil::Print(fn);

  // Compile module
  fn = codegen->FinalizeFunction(fn);
  ASSERT_TRUE(fn != NULL);
  typedef DecimalVal (*TestGetFnAttrs)(FunctionContext*, const DecimalVal&);
  CodegenFnPtr<TestGetFnAttrs> fn_ptr;
  codegen->AddFunctionToJit(fn, &fn_ptr);
  EXPECT_TRUE(codegen->FinalizeModule().ok()) << LlvmCodeGen::Print(fn);

  // Call fn and check results'. The input is of type Decimal(10,2) (i.e. 10000.25) and
  // the output type is Decimal(10,1) (i.e. 10000.3). The precision and scale of arguments
  // and return types are encoded above (ARG0_*, RET_*);
  int64_t v = 1000025;
  DecimalVal arg0_val(v);
  DecimalVal result = fn_ptr.load()(fn_ctx_, arg0_val);
  // sanity check result
  EXPECT_EQ(result.is_null, false);
  EXPECT_EQ(result.val8, 100003);
  CheckFnAttr();
  codegen->Close();
}

}

using namespace impala;

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  InitCommonRuntime(argc, argv, true, TestInfo::BE_TEST);
  InitFeSupport();
  ABORT_IF_ERROR(LlvmCodeGen::InitializeLlvm());

  return RUN_ALL_TESTS();
}

#endif
