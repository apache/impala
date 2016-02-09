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


#ifndef IMPALA_CODEGEN_LLVM_CODEGEN_H
#define IMPALA_CODEGEN_LLVM_CODEGEN_H

#include "common/status.h"

#include <map>
#include <string>
#include <vector>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/unordered_set.hpp>

#include <llvm/Analysis/Verifier.h>
#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Intrinsics.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/Support/MemoryBuffer.h>
#include <llvm/Support/raw_ostream.h>

#include "exprs/expr.h"
#include "impala-ir/impala-ir-functions.h"
#include "runtime/types.h"
#include "util/runtime-profile.h"

/// Forward declare all llvm classes to avoid namespace pollution.
namespace llvm {
  class AllocaInst;
  class BasicBlock;
  class ConstantFolder;
  class ExecutionEngine;
  class Function;
  class FunctionPassManager;
  class LLVMContext;
  class Module;
  class NoFolder;
  class PassManager;
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

class CodegenAnyVal;
class SubExprElimination;

/// LLVM code generator.  This is the top level object to generate jitted code.
//
/// LLVM provides a c++ IR builder interface so IR does not need to be written
/// manually.  The interface is very low level so each line of IR that needs to
/// be output maps 1:1 with calls to the interface.
/// The llvm documentation is not fantastic and a lot of this was figured out
/// by experimenting.  Thankfully, their API is pretty well designed so it's
/// possible to get by without great documentation.  The llvm tutorial is very
/// helpful, http://llvm.org/docs/tutorial/LangImpl1.html.  In this tutorial, they
/// go over how to JIT an AST for a toy language they create.
/// It is also helpful to use their online app that lets you compile c/c++ to IR.
/// http://llvm.org/demo/index.cgi.
//
/// This class provides two interfaces, one for testing and one for the query
/// engine.  The interface for the query engine will load the cross-compiled
/// IR module (output during the build) and extract all of functions that will
/// be called directly.  The test interface can be used to load any precompiled
/// module or none at all (but this class will not validate the module).
//
/// This class is mostly not threadsafe.  During the Prepare() phase of the fragment
/// execution, nodes should codegen functions, and register those functions with
/// AddFunctionToJit().
/// Afterward, FinalizeModule() should be called at which point all codegened functions
/// are optimized. After FinalizeModule() returns, all function pointers registered with
/// AddFunctionToJit() will be pointing to the appropriate JIT'd function.
//
/// Currently, each query will create and initialize one of these
/// objects.  This requires loading and parsing the cross compiled modules.
/// TODO: we should be able to do this once per process and let llvm compile
/// functions from across modules.
//
/// LLVM has a nontrivial memory management scheme and objects will take
/// ownership of others.  The document is pretty good about being explicit with this
/// but it is not very intuitive.
/// TODO: look into diagnostic output and debuggability
/// TODO: confirm that the multi-threaded usage is correct
class LlvmCodeGen {
 public:
  /// This function must be called once per process before any llvm API calls are
  /// made.  LLVM needs to allocate data structures for multi-threading support and
  /// to enable dynamic linking of jitted code.
  /// if 'load_backend', load the backend static object for llvm.  This is needed
  /// when libbackend.so is loaded from java.  llvm will be default only look in
  /// the current object and not be able to find the backend symbols
  /// TODO: this can probably be removed after impalad refactor where the java
  /// side is not loading the be explicitly anymore.
  static void InitializeLlvm(bool load_backend = false);

  /// Loads and parses the precompiled impala IR module
  /// 'codegen' will contain the created object on success.
  /// 'id' is used for outputting the IR module for debugging.
  static Status LoadImpalaIR(
      ObjectPool*, const std::string& id, boost::scoped_ptr<LlvmCodeGen>* codegen);

  /// Load a pre-compiled IR module from 'file'.  This creates a top level
  /// codegen object.
  /// codegen will contain the created object on success.
  static Status LoadFromFile(ObjectPool*, const std::string& file, const std::string& id,
      boost::scoped_ptr<LlvmCodeGen>* codegen);

  /// Load a pre-compiled IR module from module_ir.  This creates a top level codegen
  /// object.  codegen will contain the created object on success.
  static Status LoadFromMemory(ObjectPool*, llvm::MemoryBuffer* module_ir,
      const std::string& module_name, const std::string& id,
      boost::scoped_ptr<LlvmCodeGen>* codegen);

  /// Removes all jit compiled dynamically linked functions from the process.
  ~LlvmCodeGen();

  RuntimeProfile* runtime_profile() { return &profile_; }
  RuntimeProfile::Counter* codegen_timer() { return codegen_timer_; }

  /// Turns on/off optimization passes
  void EnableOptimizations(bool enable);

  /// For debugging. Returns the IR that was generated.  If full_module, the
  /// entire module is dumped, including what was loaded from precompiled IR.
  /// If false, only output IR for functions which were generated.
  std::string GetIR(bool full_module) const;

  /// Typedef builder in case we want to change the template arguments later
  typedef llvm::IRBuilder<> LlvmBuilder;

  /// Utility struct that wraps a variable name and llvm type.
  struct NamedVariable {
    std::string name;
    llvm::Type* type;

    NamedVariable(const std::string& name="", llvm::Type* type = NULL) {
      this->name = name;
      this->type = type;
    }
  };

  /// Abstraction over function prototypes.  Contains helpers to build prototypes and
  /// generate IR for the types.
  class FnPrototype {
   public:
    /// Create a function prototype object, specifying the name of the function and
    /// the return type.
    FnPrototype(LlvmCodeGen*, const std::string& name, llvm::Type* ret_type);

    /// Returns name of function
    const std::string& name() const { return name_; }

    /// Add argument
    void AddArgument(const NamedVariable& var) {
      args_.push_back(var);
    }
    void AddArgument(const std::string& name, llvm::Type* type) {
      args_.push_back(NamedVariable(name, type));
    }

    /// Generate LLVM function prototype.
    /// If a non-null builder is passed, this function will also create the entry block
    /// and set the builder's insert point to there.
    /// If params is non-null, this function will also return the arguments
    /// values (params[0] is the first arg, etc).
    /// In that case, params should be preallocated to be number of arguments
    llvm::Function* GeneratePrototype(LlvmBuilder* builder = NULL,
        llvm::Value** params = NULL);

   private:
    friend class LlvmCodeGen;

    LlvmCodeGen* codegen_;
    std::string name_;
    llvm::Type* ret_type_;
    std::vector<NamedVariable> args_;
  };

  /// Return a pointer type to 'type'
  llvm::PointerType* GetPtrType(llvm::Type* type);

  /// Returns llvm type for the column type
  llvm::Type* GetType(const ColumnType& type);

  /// Return a pointer type to 'type' (e.g. int16_t*)
  llvm::PointerType* GetPtrType(const ColumnType& type);

  /// Returns the type with 'name'.  This is used to pull types from clang
  /// compiled IR.  The types we generate at runtime are unnamed.
  /// The name is generated by the clang compiler in this form:
  /// <class/struct>.<namespace>::<class name>.  For example:
  /// "class.impala::AggregationNode"
  llvm::Type* GetType(const std::string& name);

  /// Returns the pointer type of the type returned by GetType(name)
  llvm::PointerType* GetPtrType(const std::string& name);

  /// Alloca's an instance of the appropriate pointer type and sets it to point at 'v'
  llvm::Value* GetPtrTo(LlvmBuilder* builder, llvm::Value* v, const char* name = "");

  /// Returns reference to llvm context object.  Each LlvmCodeGen has its own
  /// context to allow multiple threads to be calling into llvm at the same time.
  llvm::LLVMContext& context() { return *context_.get(); }

  /// Returns execution engine interface
  llvm::ExecutionEngine* execution_engine() { return execution_engine_.get(); }

  /// Returns the underlying llvm module
  llvm::Module* module() { return module_; }

  /// Register a expr function with unique id.  It can be subsequently retrieved via
  /// GetRegisteredExprFn with that id.
  void RegisterExprFn(int64_t id, llvm::Function* function) {
    DCHECK(registered_exprs_map_.find(id) == registered_exprs_map_.end());
    registered_exprs_map_[id] = function;
    registered_exprs_.insert(function);
  }

  /// Returns a registered expr function for id or NULL if it does not exist.
  llvm::Function* GetRegisteredExprFn(int64_t id) {
    std::map<int64_t, llvm::Function*>::iterator it = registered_exprs_map_.find(id);
    if (it == registered_exprs_map_.end()) return NULL;
    return it->second;
  }

  /// Optimize and compile the module. This should be called after all functions to JIT
  /// have been added to the module via AddFunctionToJit(). If optimizations_enabled_ is
  /// false, the module will not be optimized before compilation.
  Status FinalizeModule();

  /// Replaces all instructions that call 'target_name' with a call instruction
  /// to the new_fn.  Returns the modified function.
  /// - target_name is the unmangled function name that should be replaced.
  ///   The name is assumed to be unmangled so all call sites that contain the
  ///   replace_name substring will be replaced. target_name is case-sensitive
  ///   TODO: be more strict than substring? work out the mangling rules?
  /// - If update_in_place is true, the caller function will be modified in place.
  ///   Otherwise, the caller function will be cloned and the original function
  ///   is unmodified.  If update_in_place is false and the function is already
  ///   been dynamically linked, the existing function will be unlinked. Note that
  ///   this is very unthread-safe, if there are threads in the function to be unlinked,
  ///   bad things will happen.
  /// - 'num_replaced' returns the number of call sites updated
  //
  /// Most of our use cases will likely not be in place.  We will have one 'template'
  /// version of the function loaded for each type of Node (e.g. AggregationNode).
  /// Each instance of the node will clone the function, replacing the inner loop
  /// body with the codegened version.  The codegened bodies differ from instance
  /// to instance since they are specific to the node's tuple desc.
  llvm::Function* ReplaceCallSites(llvm::Function* caller, bool update_in_place,
      llvm::Function* new_fn, const std::string& target_name, int* num_replaced);

  /// Returns a copy of fn. The copy is added to the module.
  llvm::Function* CloneFunction(llvm::Function* fn);

  /// Replace all uses of the instruction 'from' with the value 'to', and delete
  /// 'from'. This is a wrapper around llvm::ReplaceInstWithValue().
  void ReplaceInstWithValue(llvm::Instruction* from, llvm::Value* to);

  /// Returns the i-th argument of fn.
  llvm::Argument* GetArgument(llvm::Function* fn, int i);

  /// Verify function.  This should be called at the end for each codegen'd function.  If
  /// the function does not verify, it will delete the function and return NULL,
  /// otherwise, it returns the function object.
  llvm::Function* FinalizeFunction(llvm::Function* function);

  /// Inlines all function calls for 'fn' that are marked as always inline.
  /// (We can't inline all call sites since pulling in boost/other libs could have
  /// recursion.  Instead, we just inline our functions and rely on the llvm inliner to
  /// pick the rest.)
  /// 'fn' is modified in place. Returns the number of functions inlined.  This is *not*
  /// called recursively (i.e. second level function calls are not inlined). This can be
  /// called again to inline those until this returns 0.
  int InlineCallSites(llvm::Function* fn, bool skip_registered_fns);

  /// Optimizes the function in place.  This uses a combination of llvm optimization
  /// passes as well as some custom heuristics.  This should be called for all
  /// functions which call Exprs.  The exprs will be inlined as much as possible,
  /// and will do basic sub expression elimination.
  /// This should be called before FinalizeModule for functions that want to remove
  /// redundant exprs.  This should be called at the highest level possible to
  /// maximize the number of redundant exprs that can be found.
  /// TODO: we need to spend more time to output better IR.  Asking llvm to
  /// remove redundant codeblocks on its own is too difficult for it.
  /// TODO: this should implement the llvm FunctionPass interface and integrated
  /// with the llvm optimization passes.
  llvm::Function* OptimizeFunctionWithExprs(llvm::Function* fn);

  /// Adds the function to be automatically jit compiled after the module is optimized.
  /// That is, after FinalizeModule(), this will do *result_fn_ptr = JitFunction(fn);
  //
  /// This is useful since it is not valid to call JitFunction() before every part of the
  /// query has finished adding their IR and it's convenient to not have to rewalk the
  /// objects. This provides the same behavior as walking each of those objects and calling
  /// JitFunction().
  //
  /// In addition, any functions not registered with AddFunctionToJit() are marked as
  /// internal in FinalizeModule() and may be removed as part of optimization.
  //
  /// This will also wrap functions returning DecimalVals in an ABI-compliant wrapper (see
  /// the comment in the .cc file for details). This is so we don't accidentally try to
  /// call non-compliant code from native code.
  void AddFunctionToJit(llvm::Function* fn, void** fn_ptr);

  /// Verfies the function, e.g., checks that the IR is well-formed.  Returns false if
  /// function is invalid.
  bool VerifyFunction(llvm::Function* function);

  /// This will generate a printf call instruction to output 'message' at the
  /// builder's insert point.  Only for debugging.
  void CodegenDebugTrace(LlvmBuilder* builder, const char* message);

  /// Returns the string representation of a llvm::Value* or llvm::Type*
  template <typename T> static std::string Print(T* value_or_type) {
    std::string str;
    llvm::raw_string_ostream stream(str);
    value_or_type->print(stream);
    return str;
  }

  /// Returns the libc function, adding it to the module if it has not already been.
  llvm::Function* GetLibCFunction(FnPrototype* prototype);

  /// Returns the cross compiled function. IRFunction::Type is an enum which is
  /// defined in 'impala-ir/impala-ir-functions.h'
  llvm::Function* GetFunction(IRFunction::Type);

  /// Returns the hash function with signature:
  ///   int32_t Hash(int8_t* data, int len, int32_t seed);
  /// If num_bytes is non-zero, the returned function will be codegen'd to only
  /// work for that number of bytes.  It is invalid to call that function with a
  /// different 'len'.
  llvm::Function* GetHashFunction(int num_bytes = -1);
  llvm::Function* GetFnvHashFunction(int num_bytes = -1);
  llvm::Function* GetMurmurHashFunction(int num_bytes = -1);

  /// Allocate stack storage for local variables.  This is similar to traditional c, where
  /// all the variables must be declared at the top of the function.  This helper can be
  /// called from anywhere and will add a stack allocation for 'var' at the beginning of
  /// the function.  This would be used, for example, if a function needed a temporary
  /// struct allocated.  The allocated variable is scoped to the function.
  //
  /// This should always be used instead of calling LlvmBuilder::CreateAlloca directly.
  /// LLVM doesn't optimize alloca's occuring in the middle of functions very well (e.g, an
  /// alloca may end up in a loop, potentially blowing the stack).
  llvm::AllocaInst* CreateEntryBlockAlloca(llvm::Function* f, const NamedVariable& var);
  llvm::AllocaInst* CreateEntryBlockAlloca(const LlvmBuilder& builder, llvm::Type* type,
                                           const char* name = "");

  /// Utility to create two blocks in 'fn' for if/else codegen.  if_block and else_block
  /// are return parameters.  insert_before is optional and if set, the two blocks
  /// will be inserted before that block otherwise, it will be inserted at the end
  /// of 'fn'.  Being able to place blocks is useful for debugging so the IR has a
  /// better looking control flow.
  void CreateIfElseBlocks(llvm::Function* fn, const std::string& if_name,
      const std::string& else_name,
      llvm::BasicBlock** if_block, llvm::BasicBlock** else_block,
      llvm::BasicBlock* insert_before = NULL);

  /// Create a llvm pointer value from 'ptr'.  This is used to pass pointers between
  /// c-code and code-generated IR.  The resulting value will be of 'type'.
  llvm::Value* CastPtrToLlvmPtr(llvm::Type* type, const void* ptr);

  /// Returns the constant 'val' of 'type'
  llvm::Value* GetIntConstant(PrimitiveType type, int64_t val);

  /// Returns true/false constants (bool type)
  llvm::Value* true_value() { return true_value_; }
  llvm::Value* false_value() { return false_value_; }
  llvm::Value* null_ptr_value() { return llvm::ConstantPointerNull::get(ptr_type()); }

  /// Simple wrappers to reduce code verbosity
  llvm::Type* boolean_type() { return GetType(TYPE_BOOLEAN); }
  llvm::Type* tinyint_type() { return GetType(TYPE_TINYINT); }
  llvm::Type* smallint_type() { return GetType(TYPE_SMALLINT); }
  llvm::Type* int_type() { return GetType(TYPE_INT); }
  llvm::Type* bigint_type() { return GetType(TYPE_BIGINT); }
  llvm::Type* float_type() { return GetType(TYPE_FLOAT); }
  llvm::Type* double_type() { return GetType(TYPE_DOUBLE); }
  llvm::Type* string_val_type() { return string_val_type_; }
  llvm::PointerType* ptr_type() { return ptr_type_; }
  llvm::Type* void_type() { return void_type_; }
  llvm::Type* i128_type() { return llvm::Type::getIntNTy(context(), 128); }

  /// Fills 'functions' with all the functions that are defined in the module.
  /// Note: this does not include functions that are just declared
  void GetFunctions(std::vector<llvm::Function*>* functions);

  /// Fils in 'symbols' with all the symbols in the module.
  void GetSymbols(boost::unordered_set<std::string>* symbols);

  /// Generates function to return min/max(v1, v2)
  llvm::Function* CodegenMinMax(const ColumnType& type, bool min);

  /// Codegen to call llvm memcpy intrinsic at the current builder location
  /// dst & src must be pointer types. size is the number of bytes to copy.
  /// No-op if size is zero.
  void CodegenMemcpy(LlvmBuilder*, llvm::Value* dst, llvm::Value* src, int size);

  /// Loads an LLVM module. 'file' should be the local path to the LLVM bitcode
  /// file. The caller is responsible for cleaning up module.
  static Status LoadModuleFromFile(LlvmCodeGen* codegen, const string& file,
      llvm::Module** module);

  /// Codegens IR to load array[idx] and returns the loaded value. 'array' should be a
  /// C-style array (e.g. i32*) or an IR array (e.g. [10 x i32]). This function does not
  /// do bounds checking.
  llvm::Value* CodegenArrayAt(LlvmBuilder*, llvm::Value* array, int idx,
      const char* name = "");

  /// Loads an LLVM module. 'module_ir' should be a reference to a memory buffer containing
  /// LLVM bitcode. module_name is the name of the module to use when reporting errors.
  /// The caller is responsible for cleaning up module.
  static Status LoadModuleFromMemory(LlvmCodeGen* codegen, llvm::MemoryBuffer* module_ir,
      std::string module_name, llvm::Module** module);

  /// Loads a module at 'file' and links it to the module associated with
  /// this LlvmCodeGen object. The module must be on the local filesystem.
  Status LinkModule(const std::string& file);

  // Used for testing.
  void ResetVerification() { is_corrupt_ = false; }

 private:
  friend class LlvmCodeGenTest;
  friend class SubExprElimination;

  /// Top level codegen object.  'module_id' is used for debugging when outputting the IR.
  LlvmCodeGen(ObjectPool* pool, const std::string& module_id);

  /// Initializes the jitter and execution engine.
  Status Init();

  /// Load the intrinsics impala needs.  This is a one time initialization.
  /// Values are stored in 'llvm_intrinsics_'
  Status LoadIntrinsics();

  /// Get the function pointer to the JIT'd version of function.
  /// The result is a function pointer that is dynamically linked into the process.
  /// Returns NULL if the function is invalid.
  /// Note that this will compile, but not optimize, function if necessary.
  //
  /// This function shouldn't be called after calling FinalizeModule(). Instead use
  /// AddFunctionToJit() to register a function pointer. This is because FinalizeModule()
  /// may remove any functions not registered in AddFunctionToJit(). As such, this
  /// function is mostly useful for tests that do not call FinalizeModule() at all.
  void* JitFunction(llvm::Function* function);

  /// Optimizes the module. This includes pruning the module of any unused functions.
  void OptimizeModule();

  /// Clears generated hash fns.  This is only used for testing.
  void ClearHashFns();

  /// ID used for debugging (can be e.g. the fragment instance ID)
  std::string id_;

  /// Codegen counters
  RuntimeProfile profile_;

  /// Time spent reading the .ir file from the file system.
  RuntimeProfile::Counter* load_module_timer_;

  /// Time spent constructing the in-memory module from the ir.
  RuntimeProfile::Counter* prepare_module_timer_;

  /// Time spent doing codegen (adding IR to the module)
  RuntimeProfile::Counter* codegen_timer_;

  /// Time spent optimizing the module.
  RuntimeProfile::Counter* optimization_timer_;

  /// Time spent compiling the module.
  RuntimeProfile::Counter* compile_timer_;

  /// Total size of bitcode modules loaded in bytes.
  RuntimeProfile::Counter* module_bitcode_size_;

  /// whether or not optimizations are enabled
  bool optimizations_enabled_;

  /// If true, the module is corrupt and we cannot codegen this query.
  /// TODO: we could consider just removing the offending function and attempting to
  /// codegen the rest of the query.  This requires more testing though to make sure
  /// that the error is recoverable.
  bool is_corrupt_;

  /// If true, the module has been compiled.  It is not valid to add additional
  /// functions after this point.
  bool is_compiled_;

  /// Error string that llvm will write to
  std::string error_string_;

  /// Top level llvm object.  Objects from different contexts do not share anything.
  /// We can have multiple instances of the LlvmCodeGen object in different threads
  boost::scoped_ptr<llvm::LLVMContext> context_;

  /// Top level codegen object.  Contains everything to jit one 'unit' of code.
  /// Owned by the execution_engine_.
  llvm::Module* module_;

  /// Execution/Jitting engine.
  boost::scoped_ptr<llvm::ExecutionEngine> execution_engine_;

  /// Keeps track of all the functions that have been jit compiled and linked into
  /// the process. Special care needs to be taken if we need to modify these functions.
  /// bool is unused.
  std::map<llvm::Function*, bool> jitted_functions_;

  /// Lock protecting jitted_functions_
  boost::mutex jitted_functions_lock_;

  /// Keeps track of the external functions that have been included in this module
  /// e.g libc functions or non-jitted impala functions.
  /// TODO: this should probably be FnPrototype->Functions mapping
  std::map<std::string, llvm::Function*> external_functions_;

  /// Functions parsed from pre-compiled module.  Indexed by ImpalaIR::Function enum
  std::vector<llvm::Function*> loaded_functions_;

  /// Stores functions codegen'd by impala.  This does not contain cross compiled
  /// functions, only function that were generated at runtime.  Does not overlap
  /// with loaded_functions_.
  std::vector<llvm::Function*> codegend_functions_;

  /// A mapping of unique id to registered expr functions
  std::map<int64_t, llvm::Function*> registered_exprs_map_;

  /// A set of all the functions in 'registered_exprs_map_' for quick lookup.
  std::set<llvm::Function*> registered_exprs_;

  /// A cache of loaded llvm intrinsics
  std::map<llvm::Intrinsic::ID, llvm::Function*> llvm_intrinsics_;

  /// This is a cache of generated hash functions by byte size.  It is common
  /// for the caller to know the number of bytes to hash (e.g. tuple width) and
  /// we can codegen a loop unrolled hash function.
  std::map<int, llvm::Function*> hash_fns_;

  /// The locations of modules that have been linked. Used to avoid linking the same module
  /// twice, which causes symbol collision errors.
  std::set<std::string> linked_modules_;

  /// The vector of functions to automatically JIT compile after FinalizeModule().
  std::vector<std::pair<llvm::Function*, void**> > fns_to_jit_compile_;

  /// Debug utility that will insert a printf-like function into the generated
  /// IR.  Useful for debugging the IR.  This is lazily created.
  llvm::Function* debug_trace_fn_;

  /// Debug strings that will be outputted by jitted code.  This is a copy of all
  /// strings passed to CodegenDebugTrace.
  std::vector<std::string> debug_strings_;

  /// llvm representation of a few common types.  Owned by context.
  llvm::PointerType* ptr_type_;             // int8_t*
  llvm::Type* void_type_;                   // void
  llvm::Type* string_val_type_;             // StringValue
  llvm::Type* timestamp_val_type_;          // TimestampValue

  /// llvm constants to help with code gen verbosity
  llvm::Value* true_value_;
  llvm::Value* false_value_;
};

}

#endif
