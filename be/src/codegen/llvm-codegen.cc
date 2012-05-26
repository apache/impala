// Copyright (c) 2012 Cloudera, Inc.  All right reserved.

#include <fstream>
#include <iostream>
#include <sstream>
#include <glog/logging.h>
#include <gflags/gflags.h>

#include <llvm/Analysis/Passes.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/JIT.h>
#include <llvm/PassManager.h>
#include <llvm/Support/DynamicLibrary.h>
#include <llvm/Support/IRReader.h>
#include <llvm/Support/MemoryBuffer.h>
#include <llvm/Support/NoFolder.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/Support/system_error.h>
#include <llvm/Target/TargetData.h>
#include "llvm/Transforms/IPO.h"
#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/Utils/BasicInliner.h>
#include <llvm/Transforms/Utils/Cloning.h>

#include "codegen/llvm-codegen.h"
#include "impala-ir/impala-ir-names.h"
#include "util/path-builder.h"

using namespace boost;
using namespace llvm;
using namespace std;

DEFINE_bool(dump_ir, false, "if true, output IR after optimization passes");
DEFINE_string(module_output, "", "if set, saves the generated IR to the output file.");

namespace impala {

// Not thread safe, used for debugging
static bool llvm_intialized = false;

void LlvmCodeGen::InitializeLlvm(bool load_backend) {
  DCHECK(!llvm_intialized);
  // This allocates a global llvm struct and enables multithreading.
  // There is no real good time to clean this up but we only make it once.
  bool result = llvm::llvm_start_multithreaded();
  DCHECK(result);
  // This can *only* be called once per process and is used to setup 
  // dynamically linking jitted code.
  llvm::InitializeNativeTarget();
  llvm_intialized = true;

  if (load_backend) {
    string path;
    // TODO: this is not robust but we should be able to remove all this
    // once impalad is in the final shape.
    PathBuilder::GetFullBuildPath("service/libbackend.so", &path);
    bool failed = llvm::sys::DynamicLibrary::LoadLibraryPermanently(path.c_str());
    DCHECK_EQ(failed, 0);
  }
}

LlvmCodeGen::LlvmCodeGen(ObjectPool* pool, const string& name) :
  name_(name),
  profile_(pool, "CodeGen"),
  optimizations_enabled_(false),
  verifier_enabled_(true),
  context_(new llvm::LLVMContext()),
  module_(NULL),
  execution_engine_(NULL),
  scratch_buffer_offset_(0),
  debug_trace_fn_(NULL) {
  
  DCHECK(llvm_intialized) << "Must call LlvmCodeGen::InitializeLlvm first.";
  
  load_module_timer_ = ADD_COUNTER(&profile_, "LoadTime", TCounterType::CPU_TICKS);
  module_file_size_ = ADD_COUNTER(&profile_, "ModuleFileSize", TCounterType::BYTES);
  compile_timer_ = ADD_COUNTER(&profile_, "CompileTime", TCounterType::CPU_TICKS);
  codegen_timer_ = ADD_COUNTER(&profile_, "CodegenTime", TCounterType::CPU_TICKS);

  loaded_functions_.resize(IRFunction::FN_END);
}

Status LlvmCodeGen::LoadFromFile(ObjectPool* pool, 
    const string& file, scoped_ptr<LlvmCodeGen>* codegen) {
  codegen->reset(new LlvmCodeGen(pool, ""));
  
  COUNTER_SCOPED_TIMER((*codegen)->load_module_timer_);
  OwningPtr<MemoryBuffer> file_buffer;
  llvm::error_code err = MemoryBuffer::getFile(file, file_buffer);
  if (err.value() != 0) {
    stringstream ss;
    ss << "Could not load module " << file << ": " << err.message();
    return Status(ss.str());
  }
  
  COUNTER_UPDATE((*codegen)->module_file_size_, file_buffer->getBufferSize());
  string error;
  Module* loaded_module = ParseBitcodeFile(file_buffer.get(), 
      (*codegen)->context(), &error);
  
  if (loaded_module == NULL) {
    stringstream ss;
    ss << "Could not parse module " << file << ": " << error;
    return Status(ss.str());
  }
  (*codegen)->module_ = loaded_module;

  return (*codegen)->Init();
}

Status LlvmCodeGen::LoadImpalaIR(ObjectPool* pool, scoped_ptr<LlvmCodeGen>* codegen_ret) {
  // TODO: is this how we want to pick up external files?  Do we need better configuration
  // support?
  string module_file;
  PathBuilder::GetFullPath("be/build/llvm-ir/impala.ll", &module_file);
  RETURN_IF_ERROR(LoadFromFile(pool, module_file, codegen_ret));
  LlvmCodeGen* codegen = codegen_ret->get();

  // Parse module for cross compiled functions and types
  COUNTER_SCOPED_TIMER(codegen->load_module_timer_);

  // Get type for StringValue
  codegen->string_val_type_ = codegen->GetType(StringValue::LLVM_CLASS_NAME);
  codegen->string_val_ptr_type_ = PointerType::get(codegen->string_val_type_, 0);

  // TODO: get type for Timestamp

  // Verify size is correct
  const TargetData* target_data = codegen->execution_engine()->getTargetData();
  const StructLayout* layout = 
      target_data->getStructLayout(static_cast<StructType*>(codegen->string_val_type_));
  if (layout->getSizeInBytes() != sizeof(StringValue)) {
    DCHECK_EQ(layout->getSizeInBytes(), sizeof(StringValue));
    return Status("Could not create llvm struct type for StringVal");
  }

  // Parse functions from module
  vector<Function*> functions;
  codegen->GetFunctions(&functions);
  int parsed_functions = 0;
  for (int i = 0; i < functions.size(); ++i) {
    string fn_name = functions[i]->getName();
    for (int j = IRFunction::FN_START; j < IRFunction::FN_END; ++j) {
      // Substring match to match precompiled functions.  The compiled function names
      // will be mangled.
      // TODO: reconsider this.  Substring match is probably not strict enough but
      // undoing the mangling is no fun either.
      if (fn_name.find(FN_MAPPINGS[j].fn_name) != string::npos) {
        if (codegen->loaded_functions_[FN_MAPPINGS[j].fn] != NULL) {
          return Status("Duplicate definition found for function: " + fn_name);
        }
        codegen->loaded_functions_[FN_MAPPINGS[j].fn] = functions[i];
        codegen->AddInlineFunction(functions[i]);
        ++parsed_functions;
      }
    }
  }

  if (parsed_functions != IRFunction::FN_END) {
    stringstream ss;
    ss << "Unable to find these precompiled functions: ";
    bool first = true;
    for (int i = IRFunction::FN_START; i != IRFunction::FN_END; ++i) {
      if (codegen->loaded_functions_[i] == NULL) {
        if (!first) ss << ", ";
        ss << FN_MAPPINGS[i].fn_name;
        first = false;
      }
    }
    return Status(ss.str());
  }

  return Status::OK;
}

Status LlvmCodeGen::Init() {
  if (module_ == NULL) {
    module_ = new Module(name_, context());
  }
  llvm::CodeGenOpt::Level opt_level = CodeGenOpt::Default;
#ifndef NDEBUG
  // For debug builds, don't generate JIT compiled optimized assembly.
  // This takes a non-neglible amount of time (~.5 ms per function) and
  // blows up the fe tests (which take ~10-20 ms each).
  opt_level = CodeGenOpt::None;
#endif
  execution_engine_.reset(
      ExecutionEngine::createJIT(module_, &error_string_, NULL, opt_level));
  if (execution_engine_ == NULL) {
    // execution_engine_ will take ownership of the module if it is created
    delete module_;
    stringstream ss;
    ss << "Could not create ExecutionEngine: " << error_string_;
    return Status(ss.str());
  }
  
  function_pass_mgr_.reset(new FunctionPassManager(module_));

  const TargetData* target_data = execution_engine_->getTargetData();
  // The creates below are just wrappers for calling new on the optimization pass object.
  // The function_pass_mgr_ will take ownership of the object for add, which is
  // why we don't delete them
  TargetData* target_data_pass = new TargetData(*target_data);
  function_pass_mgr_->add(target_data_pass);

  inliner_.reset(new BasicInliner(target_data_pass));
  // These optimizations are based on the passes that clang uses with -O3.
  // clang will set these passes up as module passes to try to optimize the entire
  // module.  This is not suitable to how we use llvm.  We have "function trees" 
  // (inlined tree of functions) and we can optimize each tree individually.
  // More details about the passes are here: http://llvm.org/docs/Passes.html
  // Some passes are repeated since after running some passes, there will be 
  // some benefit with running previous passes again. The order of the passes
  // is *very* important.  
  // TODO: Loop optimizations are turned off until we have code that uses them.
  // Provide basic AliasAnalysis support for GVN.
  function_pass_mgr_->add(createBasicAliasAnalysisPass());
  // Rearrange expressions for better constant propagation
  function_pass_mgr_->add(createReassociatePass());
  // Remove redundant instructions
  function_pass_mgr_->add(createGVNPass());
  // Simplify the control flow graph (deleting unreachable blocks, etc).
  function_pass_mgr_->add(createCFGSimplificationPass());
  // Delete dead instructions
  function_pass_mgr_->add(createAggressiveDCEPass());
  // Simplify the control flow graph (deleting unreachable blocks, etc).
  function_pass_mgr_->add(createCFGSimplificationPass());
  // Collapse dependent blocks based on condition propagation
  function_pass_mgr_->add(createJumpThreadingPass());
  function_pass_mgr_->add(createCorrelatedValuePropagationPass()); 
  // Simplify the control flow graph (deleting unreachable blocks, etc).
  function_pass_mgr_->add(createCFGSimplificationPass());

#if 0 // Loop optimizations
    // Loop rotation
    function_pass_mgr_->add(createLoopUnrollPass());
    // Hoist loop invariants
    function_pass_mgr_->add(createLICMPass());
    // Loop unswitch optimization
    function_pass_mgr_->add(createLoopUnswitchPass());
    // Do simple "peephole" optimizations and bit-twiddling optzns.
    function_pass_mgr_->add(createInstructionCombiningPass());
    // Induction var optimization
    function_pass_mgr_->add(createIndVarSimplifyPass());
    // Rewrite loop idioms like memset.
    function_pass_mgr_->add(createLoopIdiomPass());
    // Remove dead loops
    function_pass_mgr_->add(createLoopDeletionPass());
    // Unroll loops
    function_pass_mgr_->add(createLoopUnrollPass());
    // Remove redundant instructions
    function_pass_mgr_->add(createGVNPass());
    // Remove unnecessary memcpys or collapse stores to memcpy/memset
    function_pass_mgr_->add(createMemCpyOptPass());
    // Sparse conditional constant propagation
    function_pass_mgr_->add(createSCCPPass());
    // Do simple "peephole" optimizations and bit-twiddling optzns.
    function_pass_mgr_->add(createInstructionCombiningPass());
    // Collapse dependent blocks based on condition propagation
    function_pass_mgr_->add(createJumpThreadingPass());
    function_pass_mgr_->add(createCorrelatedValuePropagationPass()); 
#endif

  // Dead store elimination
  function_pass_mgr_->add(createDeadStoreEliminationPass());
  // Simplify the control flow graph (deleting unreachable blocks, etc).
  function_pass_mgr_->add(createCFGSimplificationPass());
  // Delete dead instructions
  function_pass_mgr_->add(createAggressiveDCEPass());
  // Simplify the control flow graph (deleting unreachable blocks, etc).
  function_pass_mgr_->add(createCFGSimplificationPass());
  // Do simple "peephole" optimizations and bit-twiddling optzns.
  function_pass_mgr_->add(createInstructionCombiningPass());

  function_pass_mgr_->doInitialization();

  void_type_ = Type::getVoidTy(context());
  ptr_type_ = PointerType::get(GetType(TYPE_TINYINT), 0);
  bool_ptr_type_ = PointerType::get(GetType(TYPE_BOOLEAN), 0);
  int64_ptr_type_ = PointerType::get(GetType(TYPE_BIGINT), 0);
  true_value_ = ConstantInt::get(context(), APInt(1, true, true));
  false_value_ = ConstantInt::get(context(), APInt(1, false, true));

  return Status::OK;
}

LlvmCodeGen::~LlvmCodeGen() {
  if (FLAGS_module_output.size() != 0) {
    fstream f(FLAGS_module_output.c_str(), fstream::out | fstream::trunc);
    if (f.fail()) {
      LOG(ERROR) << "Could not save IR to: " << FLAGS_module_output;
    } else {
      f << GetIR(true); 
      f.close();
    }
  }
  for (map<Function*, bool>::iterator iter = jitted_functions_.begin();
      iter != jitted_functions_.end(); ++iter) {
    execution_engine_->freeMachineCodeForFunction(iter->first);
  }
}

void LlvmCodeGen::EnableOptimizations(bool enable) {
  optimizations_enabled_ = enable;
}

void LlvmCodeGen::EnableVerifier(bool enable) {
  verifier_enabled_ = enable;
}

string LlvmCodeGen::GetIR(bool full_module) const {
  string str;
  raw_string_ostream stream(str);
  if (full_module) {
    module_->print(stream, NULL);
  } else {
    for (int i = 0; i < codegend_functions_.size(); ++i) {
      codegend_functions_[i]->print(stream, NULL);
    }
  }
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
    case TYPE_STRING:
      return string_val_type_;
    default:
      DCHECK(false) << "Invalid type.";
      return NULL;
  }
}

Type* LlvmCodeGen::GetType(const string& name) {
  return module_->getTypeByName(name);
}

// Llvm doesn't let you create a PointerValue from a c-side ptr.  Instead
// cast it to an int and then to 'type'.
Value* LlvmCodeGen::CastPtrToLlvmPtr(Type* type, void* ptr) {
  Constant* const_int = ConstantInt::get(Type::getInt64Ty(context()), (int64_t)ptr);
  return ConstantExpr::getIntToPtr(const_int, type);
}

Value* LlvmCodeGen::GetIntConstant(PrimitiveType type, int64_t val) {
  switch (type) {
    case TYPE_TINYINT:
      return ConstantInt::get(context(), APInt(8, val));
    case TYPE_SMALLINT:
      return ConstantInt::get(context(), APInt(16, val));
    case TYPE_INT:
      return ConstantInt::get(context(), APInt(32, val));
    case TYPE_BIGINT:
      return ConstantInt::get(context(), APInt(64, val));
    default:
      DCHECK(false);
      return NULL;
  }
}

AllocaInst* LlvmCodeGen::CreateEntryBlockAlloca(Function* f, const NamedVariable& var) {
  IRBuilder<> tmp(&f->getEntryBlock(), f->getEntryBlock().begin());
  return tmp.CreateAlloca(var.type, 0, var.name.c_str());
}

void LlvmCodeGen::CreateIfElseBlocks(Function* fn, const string& if_name, 
    const string& else_name, BasicBlock** if_block, BasicBlock** else_block, 
    BasicBlock* insert_before) {
  *if_block = BasicBlock::Create(context(), if_name, fn, insert_before);
  *else_block = BasicBlock::Create(context(), else_name, fn, insert_before);
}

Function* LlvmCodeGen::GetLibCFunction(FnPrototype* prototype) {
  if (external_functions_.find(prototype->name()) != external_functions_.end()) {
    return external_functions_[prototype->name()];
  }
  Function* func = prototype->GeneratePrototype();
  external_functions_[prototype->name()] = func;
  return func;
}

Function* LlvmCodeGen::GetFunction(IRFunction::Type function) {
  DCHECK(loaded_functions_[function] != NULL);
  return loaded_functions_[function];
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

LlvmCodeGen::FnPrototype::FnPrototype(
    LlvmCodeGen* gen, const string& name, Type* ret_type) :
  codegen_(gen), name_(name), ret_type_(ret_type) {
}

Function* LlvmCodeGen::FnPrototype::GeneratePrototype(
      LlvmBuilder* builder, Value** params) {
  vector<Type*> arguments;
  for (int i = 0; i < args_.size(); ++i) {
    arguments.push_back(args_[i].type);
  }
  FunctionType* prototype = FunctionType::get(ret_type_, arguments, false);
  
  Function* fn = Function::Create(
      prototype, Function::ExternalLinkage, name_, codegen_->module_);
  DCHECK(fn != NULL);

  // Name the arguments
  int idx = 0;
  for (Function::arg_iterator iter = fn->arg_begin(); 
      iter != fn->arg_end(); ++iter, ++idx) {
    iter->setName(args_[idx].name);
    if (params != NULL) params[idx] = iter;
  }
  
  if (builder != NULL) {
    BasicBlock* entry_block = BasicBlock::Create(codegen_->context(), "entry", fn);
    builder->SetInsertPoint(entry_block);
  }

  codegen_->codegend_functions_.push_back(fn);

  return fn;
}

Function* LlvmCodeGen::ReplaceCallSites(Function* caller, bool update_in_place,
    Function* new_fn, const string& replacee_name, bool drop_first_arg, int* replaced) {
  DCHECK(caller->getParent() == module_);

  if (!update_in_place) {
    // Clone the function and add it to the module
    Function* new_caller = llvm::CloneFunction(caller);
    new_caller->copyAttributesFrom(caller);
    module_->getFunctionList().push_back(new_caller);
    caller = new_caller;
  } else if (jitted_functions_.find(caller) != jitted_functions_.end()) {
    // This function is already dynamically linked, unlink it.
    execution_engine_->freeMachineCodeForFunction(caller);
    jitted_functions_.erase(caller);
  }

  *replaced = 0;
  // loop over all blocks
  Function::iterator block_iter = caller->begin();
  while (block_iter != caller->end()) {
    BasicBlock* block = block_iter++;
    // loop over instructions in the block
    BasicBlock::iterator instr_iter = block->begin();
    while (instr_iter != block->end()) {
      Instruction* instr = instr_iter++;
      // look for call instructions
      if (CallInst::classof(instr)) {
        CallInst* call_instr = reinterpret_cast<CallInst*>(instr);
        Function* old_fn = call_instr->getCalledFunction();
        // look for call instruction that matches the name
        if (old_fn->getName().find(replacee_name) != string::npos) {
          DCHECK(!drop_first_arg || call_instr->getNumArgOperands() > 0);
          // Insert a new call instruction to the new function, using the 
          // identical arguments.
          LlvmBuilder builder(block, instr_iter);
          vector<Value*> calling_args;
          for (int i = static_cast<int>(drop_first_arg); 
              i < call_instr->getNumArgOperands(); ++i) {
            calling_args.push_back(call_instr->getArgOperand(i));
          }
          builder.CreateCall(new_fn, calling_args);
          // remove the old call instruction
          // Note: removeFromParent does not do the right thing.
          call_instr->eraseFromParent();
          ++*replaced;
        }
      }
    }
  }

  return caller;
}

void LlvmCodeGen::OptimizeFunction(Function* function) {
  if (optimizations_enabled_) {
    COUNTER_SCOPED_TIMER(compile_timer_);
    function_pass_mgr_->run(*function);
  }
}

void* LlvmCodeGen::JitFunction(Function* function, int* scratch_size) {
  COUNTER_SCOPED_TIMER(compile_timer_);
  if (optimizations_enabled_) {
    inliner_->inlineFunctions();
    function_pass_mgr_->run(*function);
  }

  if (FLAGS_dump_ir) {
    function->dump();
  }

  if (scratch_size == NULL) {
    DCHECK_EQ(scratch_buffer_offset_, 0);
  } else {
    *scratch_size = scratch_buffer_offset_;
  }
  // TODO: log a warning if the jitted function is too big (larger than I cache)
  void* jitted_function = execution_engine_->getPointerToFunction(function);
  if (jitted_function != NULL) { 
    jitted_functions_[function] = true;
  }
  return jitted_function;
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

void LlvmCodeGen::CodegenDebugTrace(LlvmBuilder* builder, const char* str) {
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
  Value* str_ptr = CastPtrToLlvmPtr(ptr_type_, const_cast<char*>(str));
  vector<Value*> calling_args;
  calling_args.push_back(str_ptr);
  builder->CreateCall(debug_trace_fn_, calling_args);
}

void LlvmCodeGen::GetFunctions(vector<Function*>* functions) {
  Module::iterator fn_iter = module_->begin();
  while (fn_iter != module_->end()) {
    Function* fn = fn_iter++;
    if (!fn->empty()) {
      functions->push_back(fn);
    }
  }
}

// TODO: cache this function (e.g. all min(int, int) are identical).
// we probably want some more global IR function cache, or, implement this
// in c and precompile it with clang.
// define i32 @Min(i32 %v1, i32 %v2) {
// entry:
//   %0 = icmp slt i32 %v1, %v2
//   br i1 %0, label %ret_v1, label %ret_v2
// 
// ret_v1:                                           ; preds = %entry
//   ret i32 %v1
// 
// ret_v2:                                           ; preds = %entry
//   ret i32 %v2
// }
Function* LlvmCodeGen::CodegenMinMax(PrimitiveType type, bool min) {
  LlvmCodeGen::FnPrototype prototype(this, min ? "Min" : "Max", GetType(type));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("v1", GetType(type)));
  prototype.AddArgument(LlvmCodeGen::NamedVariable("v2", GetType(type)));

  Value* params[2];
  LlvmBuilder builder(context());
  Function* fn = prototype.GeneratePrototype(&builder, &params[0]);
  
  Value* compare = NULL;
  switch (type) {
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
      if (min) {
        compare = builder.CreateICmpSLT(params[0], params[1]);
      } else {
        compare = builder.CreateICmpSGT(params[0], params[1]);
      }
      break;
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
      if (min) {
        compare = builder.CreateFCmpULT(params[0], params[1]);
      } else {
        compare = builder.CreateFCmpUGT(params[0], params[1]);
      }
      break;
    default:
      DCHECK(false);
  }

  BasicBlock* ret_v1, *ret_v2;
  CreateIfElseBlocks(fn, "ret_v1", "ret_v2", &ret_v1, &ret_v2);
  
  builder.CreateCondBr(compare, ret_v1, ret_v2);
  builder.SetInsertPoint(ret_v1);
  builder.CreateRet(params[0]);
  builder.SetInsertPoint(ret_v2);
  builder.CreateRet(params[1]);

  if (!VerifyFunction(fn)) return NULL;
  AddInlineFunction(fn);
  return fn;
}

}

