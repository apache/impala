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

#include <string>

#include "llvm/IR/Module.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/CallingConv.h"
#include "llvm/IR/IRBuilder.h"

#include "codegen/llvm-codegen.h"
#include "codegen/instruction-counter.h"
#include "testutil/gtest-util.h"

#include "common/names.h"

namespace impala {

class InstructionCounterTest : public testing:: Test {
 protected:
  llvm::LLVMContext context_;
};

// IR output from CodegenMullAdd
// define i32 @mul_add(i32 %x, i32 %y, i32 %z) {
// entry:
//   %tmp = mul i32 %x, %y
//   %tmp2 = add i32 %tmp, %z
//   ret i32 %tmp2
// }
// Create a module with one function, which multiplies two arguments, add a third and
// then returns the result.
llvm::Module* CodegenMulAdd(llvm::LLVMContext* context) {
  llvm::Module* mod = new llvm::Module("test", *context);
  llvm::Constant* c = mod->getOrInsertFunction("mul_add",
      llvm::IntegerType::get(*context, 32), llvm::IntegerType::get(*context, 32),
      llvm::IntegerType::get(*context, 32), llvm::IntegerType::get(*context, 32));
  llvm::Function* mul_add = llvm::cast<llvm::Function>(c);
  mul_add->setCallingConv(llvm::CallingConv::C);
  llvm::Function::arg_iterator args = mul_add->arg_begin();
  llvm::Value* x = &*args;
  ++args;
  x->setName("x");
  llvm::Value* y = &*args;
  ++args;
  y->setName("y");
  llvm::Value* z = &*args;
  ++args;
  z->setName("z");
  llvm::BasicBlock* block = llvm::BasicBlock::Create(*context, "entry", mul_add);
  llvm::IRBuilder<> builder(block);
  llvm::Value* tmp = builder.CreateBinOp(llvm::Instruction::Mul, x, y, "tmp");
  llvm::Value* tmp2 = builder.CreateBinOp(llvm::Instruction::Add, tmp, z, "tmp2");
  builder.CreateRet(tmp2);
  return mod;
}

TEST_F(InstructionCounterTest, Count) {
  llvm::Module* MulAddModule = CodegenMulAdd(&context_);
  InstructionCounter instruction_counter;
  instruction_counter.visit(*MulAddModule);
  instruction_counter.PrintCounters();
  EXPECT_EQ(instruction_counter.GetCount(InstructionCounter::TOTAL_FUNCTIONS), 1);
  EXPECT_EQ(instruction_counter.GetCount(InstructionCounter::TOTAL_INSTS), 3);
  EXPECT_EQ(instruction_counter.GetCount(InstructionCounter::TERMINATOR_INSTS), 1);
  EXPECT_EQ(instruction_counter.GetCount(InstructionCounter::MEMORY_INSTS), 0);

  // Test Reset
  instruction_counter.ResetCount();
  EXPECT_EQ(instruction_counter.GetCount(InstructionCounter::TOTAL_FUNCTIONS), 0);
  EXPECT_EQ(instruction_counter.GetCount(InstructionCounter::TOTAL_INSTS), 0);
  EXPECT_EQ(instruction_counter.GetCount(InstructionCounter::MEMORY_INSTS), 0);
}

// IR output from CodegenGcd
// define i32 @gcd(i32 %x, i32 %y) {
// entry:
//   %tmp = icmp eq i32 %x, %y
//   br i1 %tmp, label %return, label %cond_false
//
// return:                                           ; preds = %entry
//   ret i32 %x
//
// cond_false:                                       ; preds = %entry
//   %tmp2 = icmp ult i32 %x, %y
//   br i1 %tmp2, label %cond_true, label %cond_false1
//
// cond_true:                                        ; preds = %cond_false
//   %tmp3 = sub i32 %y, %x
//   %tmp4 = call i32 @gcd(i32 %x, i32 %tmp3)
//   ret i32 %tmp4
//
// cond_false1:                                      ; preds = %cond_false
//   %tmp5 = sub i32 %x, %y
//   %tmp6 = call i32 @gcd(i32 %tmp5, i32 %y)
//   ret i32 %tmp6
// LLVM IR module which contains one function to return the GCD of two numbers
// }
llvm::Module* CodegenGcd(llvm::LLVMContext* context) {
  llvm::Module* mod = new llvm::Module("gcd", *context);
  llvm::Constant* c = mod->getOrInsertFunction("gcd",
      llvm::IntegerType::get(*context, 32), llvm::IntegerType::get(*context, 32),
      llvm::IntegerType::get(*context, 32));
  llvm::Function* gcd = llvm::cast<llvm::Function>(c);
  llvm::Function::arg_iterator args = gcd->arg_begin();
  llvm::Value* x = &*args;
  ++args;
  x->setName("x");
  llvm::Value* y = &*args;
  ++args;
  y->setName("y");
  llvm::BasicBlock* entry = llvm::BasicBlock::Create(*context, "entry", gcd);
  llvm::BasicBlock* ret = llvm::BasicBlock::Create(*context, "return", gcd);
  llvm::BasicBlock* cond_false = llvm::BasicBlock::Create(*context, "cond_false", gcd);
  llvm::BasicBlock* cond_true = llvm::BasicBlock::Create(*context, "cond_true", gcd);
  llvm::BasicBlock* cond_false_2 = llvm::BasicBlock::Create(*context, "cond_false", gcd);
  llvm::IRBuilder<> builder(entry);
  llvm::Value* xEqualsY = builder.CreateICmpEQ(x, y, "tmp");
  builder.CreateCondBr(xEqualsY, ret, cond_false);  builder.SetInsertPoint(ret);
  builder.CreateRet(x);
  builder.SetInsertPoint(cond_false);
  llvm::Value* xLessThanY = builder.CreateICmpULT(x, y, "tmp");
  builder.CreateCondBr(xLessThanY, cond_true, cond_false_2);
  builder.SetInsertPoint(cond_true);
  llvm::Value* yMinusX = builder.CreateSub(y, x, "tmp");
  llvm::Value* args1[2] = {x, yMinusX};
  llvm::Value* recur_1 = builder.CreateCall(gcd, args1, "tmp");
  builder.CreateRet(recur_1);
  builder.SetInsertPoint(cond_false_2);
  llvm::Value* xMinusY = builder.CreateSub(x, y, "tmp");
  llvm::Value* args2[2] = {xMinusY, y};
  llvm::Value* recur_2 = builder.CreateCall(gcd, args2, "tmp");
  builder.CreateRet(recur_2);
  return mod;
}

TEST_F(InstructionCounterTest, TestMemInstrCount) {
  llvm::Module* GcdModule = CodegenGcd(&context_);
  InstructionCounter instruction_counter;
  instruction_counter.visit(*GcdModule);
  std::cout << instruction_counter.PrintCounters();
  EXPECT_EQ(instruction_counter.GetCount(InstructionCounter::TOTAL_FUNCTIONS), 1);
  EXPECT_EQ(instruction_counter.GetCount(InstructionCounter::TOTAL_BLOCKS), 5);
  EXPECT_EQ(instruction_counter.GetCount(InstructionCounter::TOTAL_INSTS), 11);
  // Test Category Totals
  EXPECT_EQ(instruction_counter.GetCount(InstructionCounter::TERMINATOR_INSTS), 5);
  EXPECT_EQ(instruction_counter.GetCount(InstructionCounter::MEMORY_INSTS), 0);
  EXPECT_EQ(instruction_counter.GetCount(InstructionCounter::OTHER_INSTS), 4);

  // Test Reset
  instruction_counter.ResetCount();
  EXPECT_EQ(instruction_counter.GetCount(InstructionCounter::TOTAL_FUNCTIONS), 0);
  EXPECT_EQ(instruction_counter.GetCount(InstructionCounter::TOTAL_BLOCKS), 0);
  EXPECT_EQ(instruction_counter.GetCount(InstructionCounter::TOTAL_INSTS), 0);
  EXPECT_EQ(instruction_counter.GetCount(InstructionCounter::TERMINATOR_INSTS), 0);
  EXPECT_EQ(instruction_counter.GetCount(InstructionCounter::MEMORY_INSTS), 0);
  EXPECT_EQ(instruction_counter.GetCount(InstructionCounter::OTHER_INSTS), 0);
}

}  // namespace impala
