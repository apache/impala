// Copyright (c) 2012 Cloudera, Inc.  All right reserved.

#include <iostream>
#include <sstream>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include <llvm/Analysis/Passes.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/JIT.h>
#include <llvm/PassManager.h>
#include <llvm/Support/NoFolder.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/Target/TargetData.h>
#include "llvm/Transforms/IPO.h"
#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/Utils/BasicInliner.h>

#include "codegen/llvm-codegen.h"

using namespace llvm;
using namespace std;

namespace impala {

// Not thread safe, used for debugging
static bool llvm_intialized = false;

void LlvmCodeGen::InitializeLlvm() {
  DCHECK(!llvm_intialized);
  // This allocates a global llvm struct and enables multithreading.
  // There is no real good time to clean this up but we only make it once.
  bool result = llvm::llvm_start_multithreaded();
  DCHECK(result);
  // This can *only* be called once per process and is used to setup 
  // dynamically linking jitted code.
  llvm::InitializeNativeTarget();
  llvm_intialized = true;
}

LlvmCodeGen::LlvmCodeGen(const string& name) :
  name_(name),
  optimizations_enabled_(false),
  verifier_enabled_(true),
  execution_engine_(NULL),
  scratch_buffer_offset_(0),
  debug_trace_fn_(NULL) {
}


Status LlvmCodeGen::Init() {
  DCHECK(llvm_intialized) << "Must call LlvmCodeGen::InitializeLlvm first.";
  context_.reset(new LLVMContext());
  builder_.reset(new IRBuilder<>(context()));

  module_ = new Module(name_, context());

  execution_engine_.reset(ExecutionEngine::createJIT(module_, &error_string_));
  if (execution_engine_ == NULL) {
    stringstream ss;
    ss << "Could not create ExecutionEngine: " << error_string_;
    LOG(ERROR) << ss.str();
    // execution_engine_ will take ownership of the module if it is created
    delete module_;
    return Status(ss.str());
  }
  
  function_pass_mgr_.reset(new FunctionPassManager(module_));

  // The creates below are just wrappers for calling new on the optimization pass object.
  // The function_pass_mgr_ will take ownership of the object for add, which is
  // why we don't delete them
  TargetData* target_data_pass = new TargetData(*execution_engine_->getTargetData());
  function_pass_mgr_->add(target_data_pass);

  inliner_.reset(new BasicInliner(target_data_pass));
  // TODO: what optimization passes do we want to use? 
  if (optimizations_enabled_) {
    // Provide basic AliasAnalysis support for GVN.
    function_pass_mgr_->add(createBasicAliasAnalysisPass());
    // Promote allocas to registers.
    function_pass_mgr_->add(createPromoteMemoryToRegisterPass());
    // Do simple "peephole" optimizations and bit-twiddling optzns.
    function_pass_mgr_->add(createInstructionCombiningPass());
    // Reassociate expressions.
    function_pass_mgr_->add(createReassociatePass());
    // Eliminate Common SubExpressions.
    function_pass_mgr_->add(createGVNPass());
    // Simplify the control flow graph (deleting unreachable blocks, etc).
    function_pass_mgr_->add(createCFGSimplificationPass());
  }
  function_pass_mgr_->doInitialization();

  void_type_ = Type::getVoidTy(context());
  ptr_type_ = PointerType::get(GetType(TYPE_TINYINT), 0);
  bool_ptr_type_ = PointerType::get(GetType(TYPE_BOOLEAN), 0);
  true_value_ = ConstantInt::get(context(), APInt(1, true, true));
  false_value_ = ConstantInt::get(context(), APInt(1, false, true));

  return Status::OK;
}

LlvmCodeGen::~LlvmCodeGen() {
  ClearModule();
}

void LlvmCodeGen::EnableOptimizations(bool enable) {
  optimizations_enabled_ = enable;
}

void LlvmCodeGen::EnableVerifier(bool enable) {
  verifier_enabled_ = enable;
}

string LlvmCodeGen::GetLlvmIR() const {
  string str;
  raw_string_ostream stream(str);
  module_->print(stream, NULL);
  return str;
}

Type* LlvmCodeGen::GetType(PrimitiveType type) {
  switch (type) {
    case TYPE_BOOLEAN:
      return Type::getInt1Ty(context());
    case TYPE_TINYINT:
      return Type::getInt8Ty(context());
    case TYPE_SMALLINT:
      return Type::getInt16Ty(context());
    case TYPE_INT:
      return Type::getInt32Ty(context());
    case TYPE_BIGINT:
      return Type::getInt64Ty(context());
    case TYPE_FLOAT:
      return Type::getFloatTy(context());
    case TYPE_DOUBLE:
      return Type::getDoubleTy(context());
    default:
      // TODO: add StringValue* and Timestamp* when they are implemented
      return NULL;
  }
}

AllocaInst* LlvmCodeGen::CreateEntryBlockAlloca(Function* f, const NamedVariable& var) {
  IRBuilder<> tmp(&f->getEntryBlock(), f->getEntryBlock().begin());
  return tmp.CreateAlloca(var.type, 0, var.name.c_str());
}

Function* LlvmCodeGen::GetLibCFunction(FunctionPrototype* prototype) {
  if (external_functions_.find(prototype->name()) != external_functions_.end()) {
    return external_functions_[prototype->name()];
  }
  Function* func = prototype->GeneratePrototype();
  external_functions_[prototype->name()] = func;
  return func;
}

void LlvmCodeGen::AddInlineFunction(Function* function) {
  inliner_->addFunction(function);
}

bool LlvmCodeGen::VerifyFunction(Function* function) {
  if (verifier_enabled()) {
    // Verify the function is valid;  
    bool corrupt = llvm::verifyFunction(*function, ReturnStatusAction);
    if (corrupt) {
      LOG(ERROR) << "Function corrupt.";
      function->dump();
      return false;
    }
  }
  return true;
}

LlvmCodeGen::FunctionPrototype::FunctionPrototype(
    LlvmCodeGen* gen, const string& name, Type* ret_type) :
  codegen_(gen), name_(name), ret_type_(ret_type) {
}

Function* LlvmCodeGen::FunctionPrototype::GeneratePrototype() {
  vector<Type*> arguments;
  for (int i = 0; i < args_.size(); ++i) {
    arguments.push_back(args_[i].type);
  }
  FunctionType* prototype = FunctionType::get(ret_type_, arguments, false);
  
  Function* func = Function::Create(
        prototype, Function::ExternalLinkage, name_, codegen_->module_);
  DCHECK(func != NULL);

  // Name the arguments
  int idx = 0;
  for (Function::arg_iterator iter = func->arg_begin(); 
      iter != func->arg_end(); ++iter, ++idx) {
    iter->setName(args_[idx].name);
  }
  return func;
}

void LlvmCodeGen::ClearModule() {
  if (module_ != NULL) module_->getFunctionList().clear();
  external_functions_.clear();
  debug_trace_fn_ = NULL;
  debug_strings_.clear();
}

void* LlvmCodeGen::JitFunction(Function* function, int* scratch_size) {
  if (optimizations_enabled_) {
    inliner_->inlineFunctions();
    function_pass_mgr_->run(*function);
  }

  if (verifier_enabled_) {
    // Verify the module is valid;  
    bool corrupt = verifyModule(*module_, ReturnStatusAction);
    if (corrupt) {
      LOG(ERROR) << "Module corrupt.";
      module_->dump();
      DCHECK(false);
      return NULL;
    }
  }
  *scratch_size = scratch_buffer_offset_;
  return execution_engine_->getPointerToFunction(function);
}

int LlvmCodeGen::GetScratchBuffer(int byte_size) {
  // TODO: this is not yet implemented/tested
  DCHECK(false);
  int result = scratch_buffer_offset_;
  // TODO: alignment? 
  result += byte_size;
  return result;
}

// Wrapper around printf to make it easier to call from IR
extern "C" void DebugTrace(const char* str) {
  printf("LLVM Trace: %s\n", str);
}

void LlvmCodeGen::CodegenDebugTrace(const char* str) {
  LOG(ERROR) << "Remove IR codegen debug traces before checking in.";

  // Lazily link in debug function to the module
  if (debug_trace_fn_ == NULL) {
    vector<Type*> args;
    args.push_back(ptr_type_);
    FunctionType* fn_type = FunctionType::get(void_type_, args, false);
    debug_trace_fn_ = Function::Create(fn_type, GlobalValue::ExternalLinkage,
        "DebugTrace", module_);
    
    DCHECK(debug_trace_fn_ != NULL);
    // DebugTrace shouldn't already exist (llvm mangles function names if there
    // are duplicates)
    DCHECK(debug_trace_fn_->getName() ==  "DebugTrace");
    
    debug_trace_fn_->setCallingConv(CallingConv::C);

    // Add a mapping to the execution engine so it can link the DebugTrace function
    execution_engine_->addGlobalMapping(debug_trace_fn_, 
        reinterpret_cast<void*>(&DebugTrace));
  }

  // Make a copy of str into memory owned by this object.  This is no guarantee that str is 
  // still around when the debug printf is executed.
  debug_strings_.push_back(str);
  str = debug_strings_[debug_strings_.size() - 1].c_str();

  // Call the function by turning 'str' into a constant ptr value
  Type* ptr_int_type = IntegerType::get(context(), 64);
  Constant* const_int = ConstantInt::get(ptr_int_type, reinterpret_cast<int64_t>(str));
  Value* const_ptr = ConstantExpr::getIntToPtr(const_int, ptr_type_);
  vector<Value*> calling_args;
  calling_args.push_back(const_ptr);
  builder_->CreateCall(debug_trace_fn_, calling_args);
}

}

