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
#include <memory>
#include <string>
#include <vector>
#include <boost/scoped_ptr.hpp>

#include <boost/unordered_set.hpp>

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
  class LLVMContext;
  class Module;
  class NoFolder;
  class PointerType;
  class StructType;
  class TargetData;
  class Type;
  class Value;
  namespace legacy {
    class FunctionPassManager;
    class PassManager;
  }

  template<bool B, typename T, typename I>
  class IRBuilder;

  template<bool preserveName>
  class IRBuilderDefaultInserter;
}

namespace impala {

class CodegenAnyVal;
class CodegenSymbolEmitter;
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
/// This class is not threadsafe.  During the Prepare() phase of the fragment execution,
/// nodes should codegen functions, and register those functions with AddFunctionToJit().
/// Afterward, FinalizeModule() should be called at which point all codegened functions
/// are optimized and compiled. After FinalizeModule() returns, all function pointers
/// registered with AddFunctionToJit() will be pointing to the appropriate JIT'd function.
//
/// Currently, each fragment instance  will create and initialize one of these
/// objects.  This requires loading and parsing the cross compiled modules.
/// TODO: we should be able to do this once per process and let llvm compile
/// functions from across modules.
//
/// LLVM has a nontrivial memory management scheme and objects will take
/// ownership of others. The document is pretty good about being explicit with this
/// but it is not very intuitive.
/// TODO: look into diagnostic output and debuggability
/// TODO: confirm that the multi-threaded usage is correct
//
/// llvm::Function objects in the module are materialized lazily to save the cost of
/// parsing IR of functions which are dead code. An unmaterialized function is similar
/// to a function declaration which only contains the function signature and needs to
/// be materialized before optimization and compilation happen if it's not dead code.
/// Materializing a function means parsing the bitcode to populate the basic blocks and
/// instructions attached to the function object. Functions reachable by the function
/// are also materialized recursively.
//
class LlvmCodeGen {
 public:
  /// This function must be called once per process before any llvm API calls are
  /// made.  It is not valid to call it multiple times. LLVM needs to allocate data
  /// structures for multi-threading support and to enable dynamic linking of jitted code.
  /// if 'load_backend', load the backend static object for llvm.  This is needed
  /// when libbackend.so is loaded from java.  llvm will be default only look in
  /// the current object and not be able to find the backend symbols
  /// TODO: this can probably be removed after impalad refactor where the java
  /// side is not loading the be explicitly anymore.
  static void InitializeLlvm(bool load_backend = false);

  /// Creates a codegen instance for Impala initialized with the cross-compiled Impala IR.
  /// 'codegen' will contain the created object on success.
  /// 'id' is used for outputting the IR module for debugging.
  static Status CreateImpalaCodegen(
      ObjectPool*, const std::string& id, boost::scoped_ptr<LlvmCodeGen>* codegen);

  /// Creates a LlvmCodeGen instance initialized with the module bitcode from 'file'.
  /// 'codegen' will contain the created object on success.
  static Status CreateFromFile(ObjectPool*, const std::string& file, const std::string& id,
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
    FnPrototype(LlvmCodeGen* codegen, const std::string& name, llvm::Type* ret_type);

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
    /// If a non-null 'builder' is passed, this function will also create the entry block
    /// and set the builder's insert point to there.
    ///
    /// If 'params' is non-null, this function will also return the arguments values
    /// (params[0] is the first arg, etc). In that case, 'params' should be preallocated
    /// to be number of arguments
    ///
    /// If 'print_ir' is true, the generated llvm::Function's IR will be printed when
    /// GetIR() is called. Avoid doing so for IR function prototypes generated for
    /// externally defined native function.
    llvm::Function* GeneratePrototype(LlvmBuilder* builder = NULL,
        llvm::Value** params = NULL, bool print_ir = true);

   private:
    friend class LlvmCodeGen;

    LlvmCodeGen* codegen_;
    std::string name_;
    llvm::Type* ret_type_;
    std::vector<NamedVariable> args_;
  };

  /// Get host cpu attributes in format expected by EngineBuilder.
  static void GetHostCPUAttrs(std::vector<std::string>* attrs);

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

  /// Replaces all instructions in 'caller' that call 'target_name' with a call
  /// instruction to 'new_fn'. Returns the number of call sites updated.
  ///
  /// 'target_name' must be a substring of the mangled symbol of the function to be
  /// replaced. This usually means that the unmangled function name is sufficient.
  ///
  /// Note that this modifies 'caller' in-place, so this should only be called on cloned
  /// functions.
  int ReplaceCallSites(llvm::Function* caller, llvm::Function* new_fn,
      const std::string& target_name);

  /// Same as ReplaceCallSites(), except replaces the function call instructions with the
  /// boolean value 'constant'.
  int ReplaceCallSitesWithBoolConst(llvm::Function* caller, bool constant,
      const std::string& target_name);

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

  /// Adds the function to be automatically jit compiled when the codegen object is
  /// finalized. FinalizeModule() will set fn_ptr to point to the jitted function.
  ///
  /// Only functions registered with AddFunctionToJit() and their dependencies are
  /// compiled by FinalizeModule(): other functions are considered dead code and will
  /// be removed during optimization.
  ///
  /// This will also wrap functions returning DecimalVals in an ABI-compliant wrapper (see
  /// the comment in the .cc file for details). This is so we don't accidentally try to
  /// call non-compliant code from native code.
  void AddFunctionToJit(llvm::Function* fn, void** fn_ptr);

  /// This will generate a printf call instruction to output 'message' at the builder's
  /// insert point. If 'v1' is non-NULL, it will also be passed to the printf call. Only
  /// for debugging.
  void CodegenDebugTrace(LlvmBuilder* builder, const char* message,
      llvm::Value* v1 = NULL);

  /// Returns the string representation of a llvm::Value* or llvm::Type*
  template <typename T> static std::string Print(T* value_or_type) {
    std::string str;
    llvm::raw_string_ostream stream(str);
    value_or_type->print(stream);
    return str;
  }

  /// Returns the cross compiled function. 'ir_type' is an enum which is generated
  /// by gen_ir_descriptions.py. The returned function and its callee will be materialized
  /// recursively. Returns NULL if there is any error.
  ///
  /// If 'clone' is true, a clone of the function will be returned. Clones should be used
  /// iff the caller will modify the returned function. This avoids clobbering the
  /// function in case other users need it, but we don't clone if we can avoid it to
  /// reduce compilation time.
  ///
  /// TODO: Return Status instead.
  llvm::Function* GetFunction(IRFunction::Type ir_type, bool clone);

  /// Return the function with the symbol name 'symbol' from the module. The returned
  /// function and its callee will be recursively materialized. The returned function
  /// isn't cloned. Returns NULL if there is any error.
  /// TODO: Return Status instead.
  llvm::Function* GetFunction(const string& symbol);

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

  /// Returns the constant 'val' of 'type'.
  llvm::Value* GetIntConstant(PrimitiveType type, int64_t val);

  /// Returns the constant 'val' of the int type of size 'byte_size'.
  llvm::Value* GetIntConstant(int byte_size, int64_t val);

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

  /// Fils in 'symbols' with all the symbols in the module.
  void GetSymbols(boost::unordered_set<std::string>* symbols);

  /// Generates function to return min/max(v1, v2)
  llvm::Function* CodegenMinMax(const ColumnType& type, bool min);

  /// Codegen to call llvm memcpy intrinsic at the current builder location
  /// dst & src must be pointer types. size is the number of bytes to copy.
  /// No-op if size is zero.
  void CodegenMemcpy(LlvmBuilder* builder, llvm::Value* dst, llvm::Value* src, int size);
  void CodegenMemcpy(LlvmBuilder* builder, llvm::Value* dst, llvm::Value* src,
      llvm::Value* size);

  /// Codegen to call llvm memset intrinsic at the current builder location. 'dst' should
  /// be a pointer. No-op if size is zero.
  void CodegenMemset(LlvmBuilder* builder, llvm::Value* dst, int value, int size);

  /// Codegen to call pool->Allocate(size).
  llvm::Value* CodegenAllocate(LlvmBuilder* builder, MemPool* pool, llvm::Value* size,
      const char* name = "");

  /// Codegens IR to load array[idx] and returns the loaded value. 'array' should be a
  /// C-style array (e.g. i32*) or an IR array (e.g. [10 x i32]). This function does not
  /// do bounds checking.
  llvm::Value* CodegenArrayAt(LlvmBuilder*, llvm::Value* array, int idx,
      const char* name = "");

  /// Loads a module at 'file' and links it to the module associated with
  /// this LlvmCodeGen object. The module must be on the local filesystem.
  Status LinkModule(const std::string& file);

 private:
  friend class ExprCodegenTest;
  friend class LlvmCodeGenTest;
  friend class SubExprElimination;

  /// Returns true if the function 'fn' is defined in the Impalad native code.
  static bool IsDefinedInImpalad(const std::string& fn);

  /// Parses the given global constant recursively and adds functions referenced in it
  /// to the set 'ref_fns' if they are not defined in the Impalad native code. These
  /// functions need to be materialized to avoid linking error.
  static void ParseGlobalConstant(llvm::Value* global_const,
      boost::unordered_set<string>* ref_fns);

  /// Parses all the global variables in 'module' and adds any functions referenced by
  /// them to the set 'ref_fns' if they are not defined in the Impalad native code.
  /// These functions need to be materialized to avoid linking error.
  static void ParseGVForFunctions(llvm::Module* module,
      boost::unordered_set<string>* ref_fns);

  /// Top level codegen object.  'module_id' is used for debugging when outputting the IR.
  LlvmCodeGen(ObjectPool* pool, const std::string& module_id);

  /// Initializes the jitter and execution engine with the given module.
  Status Init(std::unique_ptr<llvm::Module> module);

  /// Creates a LlvmCodeGen instance initialized with the module bitcode in memory.
  /// 'codegen' will contain the created object on success. Note that the functions
  /// are not materialized. Getting a reference to the function via GetFunction()
  /// will materialize the function and its callees recursively.
  static Status CreateFromMemory(ObjectPool* pool, const std::string& id,
      boost::scoped_ptr<LlvmCodeGen>* codegen);

  /// Loads an LLVM module from 'file' which is the local path to the LLVM bitcode file.
  /// The functions in the module are not materialized. Getting a reference to the
  /// function via GetFunction() will materialize the function and its callees
  /// recursively. The caller is responsible for cleaning up the module.
  Status LoadModuleFromFile(const string& file, std::unique_ptr<llvm::Module>* module);

  /// Loads an LLVM module. 'module_ir_buf' is the memory buffer containing LLVM bitcode.
  /// 'module_name' is the name of the module to use when reporting errors.
  /// The caller is responsible for cleaning up 'module'. The functions in the module
  /// aren't materialized. Getting a reference to the function via GetFunction() will
  /// materialize the function and its callees recursively.
  Status LoadModuleFromMemory(std::unique_ptr<llvm::MemoryBuffer> module_ir_buf,
      std::string module_name, std::unique_ptr<llvm::Module>* module);

  /// Strip global constructors and destructors from an LLVM module. We never run them
  /// anyway (they must be explicitly invoked) so it is dead code.
  static void StripGlobalCtorsDtors(llvm::Module* module);

  // Setup any JIT listeners to process generated machine code object, e.g. to generate
  // perf symbol map or disassembly.
  void SetupJITListeners();

  /// Load the intrinsics impala needs.  This is a one time initialization.
  /// Values are stored in 'llvm_intrinsics_'
  Status LoadIntrinsics();

  /// Internal function for unit tests: skips Impala-specific wrapper generation logic.
  void AddFunctionToJitInternal(llvm::Function* fn, void** fn_ptr);

  /// Verifies the function, e.g., checks that the IR is well-formed.  Returns false if
  /// function is invalid.
  bool VerifyFunction(llvm::Function* function);

  // Used for testing.
  void ResetVerification() { is_corrupt_ = false; }

  /// Optimizes the module. This includes pruning the module of any unused functions.
  void OptimizeModule();

  /// Clears generated hash fns.  This is only used for testing.
  void ClearHashFns();

  /// Replace calls to functions in 'caller' where the callee's name has 'target_name'
  /// as a substring. Calls to functions are replaced with the value 'replacement'. The
  /// return value is the number of calls replaced.
  int ReplaceCallSitesWithValue(llvm::Function* caller, llvm::Value* replacement,
      const std::string& target_name);

  /// Finds call instructions in 'caller' where 'target_name' is a substring of the
  /// callee's name. Found instructions are appended to the 'results' vector.
  static void FindCallSites(llvm::Function* caller, const std::string& target_name,
      std::vector<llvm::CallInst*>* results);

  /// This function parses the function body of the given function 'fn' and materializes
  /// any functions called by it.
  Status MaterializeCallees(llvm::Function* fn);

  /// This is the workhorse for materializing function 'fn'. It's invoked by
  /// MaterializeFunction(). Calls LLVM to materialize 'fn' if it's materializable
  /// (i.e. the function has a definition in the module and it's not materialized yet).
  /// This function parses the bitcode of 'fn' to populate basic blocks, instructions
  /// and other data structures attached to the function object. Return error status
  /// for any error.
  Status MaterializeFunctionHelper(llvm::Function* fn);

  /// Entry point for materializing function 'fn'. Invokes MaterializeFunctionHelper()
  /// to do the actual work. Return error status for any error.
  Status MaterializeFunction(llvm::Function* fn);

  /// Materialize the given module by materializing all its unmaterialized functions
  /// and deleting the module's materializer. Returns error status for any error.
  Status MaterializeModule(llvm::Module* module);

  /// With lazy materialization, functions which haven't been materialized when the module
  /// is finalized must be dead code or referenced only by global variables (e.g. boost
  /// library functions or virtual function (e.g. IfExpr::GetBooleanVal())), in which case
  /// the function is not inlined so the native version can be used and the IR version is
  /// dead code. Mark them as not materializable, change their linkage types to external
  /// (so linking will happen to the native version) and strip their personality functions
  /// and comdats. DCE may complain if the above are not done. Return error status if
  /// there is error in materializing the module.
  Status FinalizeLazyMaterialization();

  /// Whether InitializeLlvm() has been called.
  static bool llvm_initialized_;

  /// Host CPU name and attributes, filled in by InitializeLlvm().
  static std::string cpu_name_;
  static std::vector<std::string> cpu_attrs_;

  /// This set contains names of functions referenced by global variables which aren't
  /// defined in the Impalad native code (they may have been inlined by gcc). These
  /// functions are always materialized each time the module is loaded to ensure that
  /// LLVM can resolve references to them.
  static boost::unordered_set<std::string> gv_ref_ir_fns_;

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

  /// Number of functions and instructions that are optimized and compiled after pruning
  /// unused functions from the module.
  RuntimeProfile::Counter* num_functions_;
  RuntimeProfile::Counter* num_instructions_;

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
  std::unique_ptr<llvm::LLVMContext> context_;

  /// Top level codegen object.  Contains everything to jit one 'unit' of code.
  /// module_ is set by Init(). module_ is owned by execution_engine_.
  llvm::Module* module_;

  /// Execution/Jitting engine.
  std::unique_ptr<llvm::ExecutionEngine> execution_engine_;

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
  std::vector<std::pair<llvm::Function*, void**>> fns_to_jit_compile_;

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

  /// The symbol emitted associated with 'execution_engine_'. Methods on
  /// 'symbol_emitter_' are called by 'execution_engine_' when code is emitted or freed.
  /// The lifetime of the symbol emitter must be longer than 'execution_engine_'.
  boost::scoped_ptr<CodegenSymbolEmitter> symbol_emitter_;
};

}

#endif
