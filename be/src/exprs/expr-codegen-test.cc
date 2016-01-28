// Copyright 2015 Cloudera Inc.
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

// The following is cross-compiled to native code and IR, and used in the test below

#include "exprs/expr.h"
#include "udf/udf.h"

using namespace impala;
using namespace impala_udf;

// TestGetConstant() fills in the following constants
struct Constants {
  int return_type_size;
  int arg0_type_size;
  int arg1_type_size;
  int arg2_type_size;
};

IntVal TestGetConstant(
    FunctionContext* ctx, const DecimalVal& arg0, StringVal arg1, StringVal arg2) {
  Constants* state = reinterpret_cast<Constants*>(
      ctx->GetFunctionState(FunctionContext::THREAD_LOCAL));
  state->return_type_size = Expr::GetConstant<int>(*ctx, Expr::RETURN_TYPE_SIZE);
  state->arg0_type_size = Expr::GetConstant<int>(*ctx, Expr::ARG_TYPE_SIZE, 0);
  state->arg1_type_size = Expr::GetConstant<int>(*ctx, Expr::ARG_TYPE_SIZE, 1);
  state->arg2_type_size = Expr::GetConstant<int>(*ctx, Expr::ARG_TYPE_SIZE, 2);
  return IntVal(10);
}

// Don't compile the actual test to IR
#ifndef IR_COMPILE

#include "testutil/gtest-util.h"
#include "codegen/llvm-codegen.h"
#include "common/init.h"
#include "exprs/expr-context.h"
#include "runtime/exec-env.h"
#include "runtime/runtime-state.h"
#include "service/fe-support.h"
#include "udf/udf-internal.h"
#include "udf/udf-test-harness.h"

#include "gen-cpp/Exprs_types.h"

#include "common/names.h"

using namespace llvm;

namespace impala {

const char* TEST_GET_CONSTANT_SYMBOL =
    "_Z15TestGetConstantPN10impala_udf15FunctionContextERKNS_10DecimalValENS_9StringValES5_";

const int ARG0_PRECISION = 10;
const int ARG0_SCALE = 2;
const int ARG1_LEN = 1;

class ExprCodegenTest : public ::testing::Test {
 protected:
  int InlineConstants(Expr* expr, LlvmCodeGen* codegen, llvm::Function* fn) {
    return expr->InlineConstants(codegen, fn);
  }

  virtual void SetUp() {
    FunctionContext::TypeDesc return_type;
    return_type.type = FunctionContext::TYPE_INT;

    FunctionContext::TypeDesc arg0_type;
    arg0_type.type = FunctionContext::TYPE_DECIMAL;
    arg0_type.precision = ARG0_PRECISION;
    arg0_type.scale = ARG0_SCALE;

    FunctionContext::TypeDesc arg1_type;
    arg1_type.type = FunctionContext::TYPE_FIXED_BUFFER;
    arg1_type.len = ARG1_LEN;

    FunctionContext::TypeDesc arg2_type;
    arg2_type.type = FunctionContext::TYPE_STRING;

    vector<FunctionContext::TypeDesc> arg_types;
    arg_types.push_back(arg0_type);
    arg_types.push_back(arg1_type);
    arg_types.push_back(arg2_type);

    fn_ctx_ = UdfTestHarness::CreateTestContext(return_type, arg_types);

    // Initialize fn_ctx_ with constants
    memset(&constants_, -1, sizeof(Constants));
    fn_ctx_->SetFunctionState(FunctionContext::THREAD_LOCAL, &constants_);
  }

  virtual void TearDown() {
    fn_ctx_->impl()->Close();
    delete fn_ctx_;
  }

  void CheckConstants() {
    EXPECT_EQ(constants_.return_type_size, 4);
    EXPECT_EQ(constants_.arg0_type_size, 8);
    EXPECT_EQ(constants_.arg1_type_size, ARG1_LEN);
    EXPECT_EQ(constants_.arg2_type_size, 0); // varlen
  }

  FunctionContext* fn_ctx_;
  Constants constants_;
};

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
  scalar_type.type = len > 0 ? TPrimitiveType::CHAR : TPrimitiveType::STRING;
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

// Creates a function call to TestGetConstant() in test-udfs.h
TExprNode CreateFunctionCall(vector<TExprNode> children) {
  TScalarType scalar_type;
  scalar_type.type = TPrimitiveType::INT;

  TTypeNode type;
  type.type = TTypeNodeType::SCALAR;
  type.__set_scalar_type(scalar_type);

  TColumnType col_type;
  col_type.__set_types(vector<TTypeNode>(1, type));

  TFunctionName fn_name;
  fn_name.function_name = "test_get_constant";

  TScalarFunction scalar_fn;
  scalar_fn.symbol = TEST_GET_CONSTANT_SYMBOL;

  TFunction fn;
  fn.name = fn_name;
  fn.binary_type = TFunctionBinaryType::IR;
  BOOST_FOREACH(const TExprNode& child, children) {
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

TEST_F(ExprCodegenTest, TestGetConstantInterpreted) {
  DecimalVal arg0_val;
  StringVal arg1_val;
  StringVal arg2_val;
  IntVal result = TestGetConstant(fn_ctx_, arg0_val, arg1_val, arg2_val);
  // sanity check result
  EXPECT_EQ(result.is_null, false);
  EXPECT_EQ(result.val, 10);
  CheckConstants();
}

TEST_F(ExprCodegenTest, TestInlineConstants) {
  // Setup thrift descriptors
  TExprNode arg0 = CreateDecimalLiteral(ARG0_PRECISION, ARG0_SCALE);
  TExprNode arg1 = CreateStringLiteral(ARG1_LEN);
  TExprNode arg2 = CreateStringLiteral();

  vector<TExprNode> exprs;
  exprs.push_back(arg0);
  exprs.push_back(arg1);
  exprs.push_back(arg2);

  TExprNode fn_call = CreateFunctionCall(exprs);
  exprs.insert(exprs.begin(), fn_call);

  TExpr texpr;
  texpr.__set_nodes(exprs);

  // Create Expr
  ObjectPool pool;
  MemTracker tracker;
  ExprContext* ctx;
  ASSERT_OK(Expr::CreateExprTree(&pool, texpr, &ctx));

  // Get TestGetConstant() IR function
  stringstream test_udf_file;
  test_udf_file << getenv("IMPALA_HOME") << "/be/build/latest/exprs/expr-codegen-test.ll";
  scoped_ptr<LlvmCodeGen> codegen;
  ASSERT_OK(LlvmCodeGen::LoadFromFile(&pool, test_udf_file.str(), "test", &codegen));
  Function* fn = codegen->module()->getFunction(TEST_GET_CONSTANT_SYMBOL);
  ASSERT_TRUE(fn != NULL);

  // Function verification should fail because we haven't inlined GetConstant() calls
  bool verification_succeeded = codegen->VerifyFunction(fn);
  EXPECT_FALSE(verification_succeeded);

  // Call InlineConstants() and rerun verification
  int replaced = InlineConstants(ctx->root(), codegen.get(), fn);
  EXPECT_EQ(replaced, 4);
  codegen->ResetVerification();
  verification_succeeded = codegen->VerifyFunction(fn);
  EXPECT_TRUE(verification_succeeded) << LlvmCodeGen::Print(fn);

  // Compile module
  fn = codegen->FinalizeFunction(fn);
  ASSERT_TRUE(fn != NULL);
  void* fn_ptr;
  codegen->AddFunctionToJit(fn, &fn_ptr);
  ASSERT_OK(codegen->FinalizeModule());
  LOG(ERROR) << "Optimized fn: " << LlvmCodeGen::Print(fn);

  // Call fn and check results
  DecimalVal arg0_val;
  typedef IntVal (*TestGetConstantType)(FunctionContext*, const DecimalVal&);
  IntVal result = reinterpret_cast<TestGetConstantType>(fn_ptr)(fn_ctx_, arg0_val);
  // sanity check result
  EXPECT_EQ(result.is_null, false);
  EXPECT_EQ(result.val, 10);
  CheckConstants();
}

}

using namespace impala;

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  InitCommonRuntime(argc, argv, true, TestInfo::BE_TEST);
  InitFeSupport();
  LlvmCodeGen::InitializeLlvm();

  return RUN_ALL_TESTS();
}

#endif
