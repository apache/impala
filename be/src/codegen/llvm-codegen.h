// Copyright (c) 2012 Cloudera, Inc.  All right reserved.

#ifndef IMPALA_CODEGEN_LLVM_CODEGEN_H
#define IMPALA_CODEGEN_LLVM_CODEGEN_H

#include "common/status.h"

#include <map>
#include <string>
#include <vector>
#include <boost/scoped_ptr.hpp>

#include <llvm/DerivedTypes.h>
#include <llvm/LLVMContext.h>
#include <llvm/Module.h>
#include <llvm/Support/IRBuilder.h>
#include <llvm/Analysis/Verifier.h>

#include "exprs/expr.h"
#include "runtime/primitive-type.h"

// Forward declare all llvm classes to avoid namespace pollution.  
namespace llvm {
  class AllocaInst;
  class BasicBlock;
  class BasicInliner;
  class ConstantFolder;
  class ExecutionEngine;
  class Function;
  class FunctionPassManager;
  class LLVMContext;
  class Module;
  class NoFolder;
  class PointerType;
  class StructType;
  class TargetData;
  class Type;
  class Value;

  template<bool B, typename T, typename I>
  class IRBuilder;

  template<bool preserveName>
  class IRBuilderDefaultInserter;
}

namespace impala {

// LLVM code generator.  This is the top level object to generate jitted code.  
//
// LLVM provides a c++ IR builder interface so IR does not need to be written
// manually.  The interface is very low level so each line of IR that needs to
// be outputted maps 1:1 with calls to the interface.
// The llvm documentation is not fantastic and a lot of this was figured out
// by experimenting.  Thankfully, their API is pretty well designed so it's
// possible to get by without great documentation.  The llvm tutorial is very
// helpful, http://llvm.org/docs/tutorial/LangImpl1.html.  In this tutorial, they
// go over how to JIT an AST for a toy language they create.
// It is also helpful to use their online app that lets you compile c/c++ to IR.
// http://llvm.org/demo/index.cgi.  
//
// LLVM has a nontrivial memory management scheme and objects will take
// ownership of others.  The document is pretty good about being explicit with this
// but it is not very intuitive.
// TODO: look into diagnostic output and debuggability
// TODO: confirm that the multi-threaded usage is correct
class LlvmCodeGen {
 public:
  // This function must be called once per process before any llvm API calls are
  // made.  LLVM needs to allocate data structures for multi-threading support and
  // to enable dynamic linking of jitted code.
  static void InitializeLlvm();

  // Top level codegen object.  'name' is the only used for debugging when
  // outputting the IR
  LlvmCodeGen(const std::string& name);

  ~LlvmCodeGen();

  // Initializes the jitter and execution engine.
  Status Init();

  // Turns on/off optimization passes
  void EnableOptimizations(bool enable);

  // Turns on/off verifying IR
  void EnableVerifier(bool enable);

  // For debugging. Returns the IR that was generated.
  std::string GetLlvmIR() const;

  // Utility struct that wraps a variable name and llvm type.
  struct NamedVariable {
    std::string name;
    llvm::Type* type;

    NamedVariable(const std::string& name="", llvm::Type* type = NULL) {
      this->name = name;
      this->type = type;
    }
  };
  
  // Abstraction over function prototypes.  Contains helpers to build prototypes and
  // generate IR for the types.  
  class FunctionPrototype {
   public:
    // Create a function prototype object, specifying the name of the function and
    // the return type.
    FunctionPrototype(LlvmCodeGen*, const std::string& name, llvm::Type* ret_type);

    // Returns name of function
    const std::string& name() const { return name_; }

    // Add argument
    void AddArgument(const NamedVariable& var) {
      args_.push_back(var);
    }

    // Generate LLVM function prototype.
    llvm::Function* GeneratePrototype();

   private:
    friend class LlvmCodeGen;

    LlvmCodeGen* codegen_;
    std::string name_;
    llvm::Type* ret_type_;
    std::vector<NamedVariable> args_;
  };

  // Returns llvm type for the primitive type
  llvm::Type* GetType(PrimitiveType type);

  // Returns reference to llvm context object.  Each LlvmCodeGen has its own
  // context to allow multiple threads to be calling into llvm at the same time.
  llvm::LLVMContext& context() { return *context_.get(); }

  // Typedef builder in case we want to change the template arguments later
  typedef llvm::IRBuilder<> LlvmBuilder;

  // Returns reference to IR builder interface
  LlvmBuilder* builder() { return builder_.get(); }

  // Returns whether functions should be verfified
  bool verifier_enabled() { return verifier_enabled_; }

  // Jit compile the function.  This will run optimization passes and verify 
  // the function.  The result is a function pointer that is dynamically linked
  // into the process. 
  // Returns NULL if the function is invalid.
  // scratch_size will be set to the buffer size required to call the function
  // scratch_size is the total size from all LlvmCodeGen::GetScratchBuffer
  // calls (with some additional bytes for alignment)
  void* JitFunction(llvm::Function* function, int* scratch_size);

  // Verfies the function if the verfier is enabled.  Returns false if function
  // is invalid.
  bool VerifyFunction(llvm::Function* function);

  // Clear functions. This removes the functions from the module including any
  // jitted functions.  Function pointers from the module are no longer usable.
  // TODO: maintain separate lists of reusable jitted functions (i.e. ADD_LONG_LONG)
  // vs. query specific functions (i.e. a particular slotref)
  void ClearModule();

  // Add function to inliner
  void AddInlineFunction(llvm::Function* function);
  
  // This will generate a printf call instructin to output 'message' at the 
  // current builder insert point.  Only for debugging.
  void CodegenDebugTrace(const char* message);

  // Returns the libc function, adding it to the module if it has not already been.
  llvm::Function* GetLibCFunction(FunctionPrototype* prototype);

  // Allocate stack storage for local variables.  This is similar to traditional c, where
  // all the variables must be declared at the top of the function.  This helper can be
  // called from anywhere and will add a stack allocation for 'var' at the beginning of 
  // the function.  This would be used, for example, if a function needed a temporary
  // struct allocated.  The allocated variable is scoped to the function.
  // This is not related to GetScratchBuffer which is used for structs that are returned 
  // to the caller.
  llvm::AllocaInst* CreateEntryBlockAlloca(llvm::Function* f, const NamedVariable& var);

  // Returns offset into scratch buffer: offset points to area of size 'byte_size'
  // Called by expr generation to request scratch buffer.  This is used for struct
  // types (i.e. StringValue) where data cannot be returned by registers.
  // For example, to jit the expr "strlen(str_col)", we need a temporary StringValue
  // struct from the inner SlotRef expr node.  The SlotRef node would call
  // GetScratchBuffer(sizeof(StringValue)) and output the intermediate struct at
  // scratch_buffer (passed in as argument to compute function) + offset. 
  int GetScratchBuffer(int byte_size);

  // Returns true/false constants (bool type)
  llvm::Value* true_value() { return true_value_; }
  llvm::Value* false_value() { return false_value_; }

  // Simple wrappers to reduce code verbosity
  llvm::Type* boolean_type() { return GetType(TYPE_BOOLEAN); }
  llvm::Type* double_type() { return GetType(TYPE_DOUBLE); }
  llvm::Type* bigint_type() { return GetType(TYPE_BIGINT); }
  llvm::Type* ptr_type() { return ptr_type_; }
  llvm::Type* bool_ptr_type() { return bool_ptr_type_; }

 private:
  // Name of the JIT module.  Useful for debugging.
  std::string name_;

  // whether or not optimizations are enabled
  bool optimizations_enabled_;

  // whether or not generated IR should be verified. 
  bool verifier_enabled_;

  // Error string that llvm will write to
  std::string error_string_;

  // Top level llvm object.  Objects from different contexts do not share anything.
  // We can have multiple instances of the LlvmCodegen object in different threads
  boost::scoped_ptr<llvm::LLVMContext> context_;

  // Top level codegen object.  Contains everything to jit one 'unit' of code.
  // Owned by the execution_engine_.
  llvm::Module* module_;

  // Execution/Jitting engine.  
  boost::scoped_ptr<llvm::ExecutionEngine> execution_engine_;

  // Contains multiple optimization passes to optimize the IR
  boost::scoped_ptr<llvm::FunctionPassManager> function_pass_mgr_;

  // Object to build IR
  boost::scoped_ptr<LlvmBuilder> builder_;

  // Function inlining utility
  boost::scoped_ptr<llvm::BasicInliner> inliner_;

  // current offset into scratch buffer 
  int scratch_buffer_offset_;

  // Keeps track of the external functions that have been included in this module
  // e.g libc functions or non-jitted impala functions.
  // TODO: this should probably be FunctionPrototype->Functions mapping
  std::map<std::string, llvm::Function*> external_functions_;

  // Debug utility that will insert a printf-like function into the generated
  // IR.  Useful for debugging the IR.  This is lazily created.
  llvm::Function* debug_trace_fn_;

  // Debug strings that will be outputted by jitted code.  This is a copy of all
  // strings passed to CodegenDebugTrace.
  std::vector<std::string> debug_strings_;

  // llvm representation of void*.  Owned by module_
  llvm::PointerType* ptr_type_; // int8_t*
  llvm::PointerType* bool_ptr_type_;
  llvm::Type* void_type_;

  // llvm constants to help with code gen verbosity
  llvm::Value* true_value_;
  llvm::Value* false_value_;
};

}

#endif

