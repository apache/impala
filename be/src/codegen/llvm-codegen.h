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


#ifndef IMPALA_CODEGEN_LLVM_CODEGEN_H
#define IMPALA_CODEGEN_LLVM_CODEGEN_H

#include "common/status.h"

#include <map>
#include <memory>
#include <string>
#include <vector>
#include <unordered_set>
#include <boost/scoped_ptr.hpp>

#include <boost/unordered_set.hpp>

#include <llvm/IR/DerivedTypes.h>
#include <llvm/IR/IRBuilder.h>
#include <llvm/IR/Intrinsics.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/Support/raw_ostream.h>

#include "codegen/llvm-codegen-object-cache.h"
#include "exprs/scalar-expr.h"
#include "impala-ir/impala-ir-functions.h"
#include "runtime/types.h"
#include "util/runtime-profile.h"

/// Forward declare all llvm classes to avoid namespace pollution.
namespace llvm {
  class AllocaInst;
  class BasicBlock;
  class ConstantFolder;
  class DiagnosticInfo;
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

  template<typename T, typename I>
  class IRBuilder;

  class IRBuilderDefaultInserter;
}

// The number of function calls replaced is not knowable when UBSAN is enabled, since it
// can double the number of references to a function. To fix, we replaced
// "DCHECK_EQ(replaced" with "DCHECK_REPLACE_COUNT(replaced":
//
// find be/src -type f -execdir sed -i s/DCHECK_EQ\(replaced,\ /DCHECK_REPLACE_COUNT\(replaced,\ /g {} \;
#if defined(UNDEFINED_SANITIZER)
#define DCHECK_REPLACE_COUNT(p, q) DCHECK_GE(p, q); DCHECK_LE(p, 2*(q))
#else
#define DCHECK_REPLACE_COUNT(p, q) DCHECK_EQ(p, q)
#endif

namespace impala {

class CodegenCallGraph;
class CodegenFnPtrBase;
class CodegenSymbolEmitter;
class FragmentState;
class ImpalaMCJITMemoryManager;
class SubExprElimination;
class Thread;
class TupleDescriptor;
class CodeGenCache;
class CodeGenCacheKey;

/// Define builder subclass in case we want to change the template arguments later
class LlvmBuilder : public llvm::IRBuilder<> {
  using llvm::IRBuilder<>::IRBuilder;
};

/// LLVM code generator.  This is the top level object to generate jitted code.
//
/// LLVM provides a c++ IR builder interface so IR does not need to be written
/// manually.  The interface is very low level so each line of IR that needs to
/// be output maps 1:1 with calls to the interface.
/// The llvm documentation is not fantastic and a lot of this was figured out
/// by experimenting.  Thankfully, their API is pretty well designed so it's
/// possible to get by without great documentation.  The llvm tutorial is very
/// helpful, https://llvm.org/docs/tutorial/LangImpl01.html.  In this tutorial, they
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
/// There are two classes of functions defined based on how they are generated:
/// 1. Handcrafted functions - These functions are built from scratch using the IRbuilder
/// interface.
/// 2. Cross-compiled functions - These functions are loaded directly from a
/// cross-compiled IR module and are either directly used or are cloned and modified
/// before use.
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
/// Function objects in the module are materialized lazily to save the cost of
/// parsing IR of functions which are dead code. An unmaterialized function is similar
/// to a function declaration which only contains the function signature and needs to
/// be materialized before optimization and compilation happen if it's not dead code.
/// Materializing a function means parsing the bitcode to populate the basic blocks and
/// instructions attached to the function object. Functions reachable by the function
/// are also materialized recursively.
//
/// Memory used for codegen is tracked via the MemTracker hierarchy. Codegen can use
/// significant memory for the IR module and for the optimization and compilation
/// algorithms. LLVM provides no way to directly track this transient memory - instead
/// the memory consumption is estimated based on the size of the IR module and released
/// once compilation finishes. Once compilation finishes, the size of the compiled
/// machine code is obtained from LLVM and and is tracked until the LlvmCodeGen object
/// is torn down and the compiled code is freed.
//
class LlvmCodeGen {
 public:
  /// This function must be called once per process before any llvm API calls are
  /// made.  It is not valid to call it multiple times. LLVM needs to allocate data
  /// structures for multi-threading support and to enable dynamic linking of jitted code.
  /// if 'load_backend', load the backend static object for llvm. This is needed
  /// when libfesupport.so is loaded from java. llvm will by default only look in
  /// the current object and not be able to find the backend symbols
  /// TODO: this can probably be removed after impalad refactor where the java
  /// side is not loading the be explicitly anymore.
  static Status InitializeLlvm(const char* procname = "main", bool load_backend = false);

  /// Creates a codegen instance for Impala initialized with the cross-compiled Impala IR.
  /// 'codegen' will contain the created object on success.
  /// 'parent_mem_tracker' - if non-NULL, the CodeGen MemTracker is created under this.
  /// 'id' is used for outputting the IR module for debugging.
  static Status CreateImpalaCodegen(FragmentState* state, MemTracker* parent_mem_tracker,
      const std::string& id, boost::scoped_ptr<LlvmCodeGen>* codegen);

  ~LlvmCodeGen();

  /// Releases all resources associated with the codegen object. It is invalid to call
  /// any other API methods after calling close.
  void Close();

  RuntimeProfile* runtime_profile() { return profile_; }
  RuntimeProfile::Counter* ir_generation_timer() { return ir_generation_timer_; }
  RuntimeProfile::Counter* main_thread_timer() { return main_thread_timer_; }
  RuntimeProfile::ThreadCounters* llvm_thread_counters() { return llvm_thread_counters_; }

  /// Turns on/off optimization passes
  void EnableOptimizations(bool enable);

  std::string DebugCacheEntryString(CodeGenCacheKey& key, bool is_lookup, bool debug_mode,
      bool success) const;

  /// For debugging. Returns the IR that was generated.  If full_module, the
  /// entire module is dumped, including what was loaded from precompiled IR.
  /// If false, only output IR for functions which were handcrafted.
  std::string GetIR(bool full_module) const;

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

    /// (Re-)sets name of function
    void SetName(const std::string& name) { name_ = name; }

    /// Add argument
    void AddArgument(const NamedVariable& var) {
      args_.push_back(var);
    }

    void AddArgument(const std::string& name, llvm::Type* type) {
      args_.push_back(NamedVariable(name, type));
    }

    /// Generate LLVM function prototype.
    /// This is the canonical way to start generating a handcrafted codegen'd function.
    /// If a non-null 'builder' is passed, this function will also create the entry
    /// block, add it to the llvm module via the builder by setting the builder's insert
    /// point to the entry block, and add it to the list of functions handcrafted by
    /// impala. FinalizeFunction() must be called for any function generated this way
    /// otherwise it will be deleted during FinalizeModule().
    ///
    /// If 'params' is non-null, this function will also return the arguments values
    /// (params[0] is the first arg, etc). In that case, 'params' should be preallocated
    /// to be number of arguments
    llvm::Function* GeneratePrototype(
        LlvmBuilder* builder = nullptr, llvm::Value** params = nullptr);

   private:
    friend class LlvmCodeGen;

    LlvmCodeGen* codegen_;
    std::string name_;
    llvm::Type* ret_type_;
    std::vector<NamedVariable> args_;
  };

  /// Get host cpu attributes in format expected by EngineBuilder.
  static void GetHostCPUAttrs(std::unordered_set<std::string>* attrs);

  /// Returns whether or not this cpu feature is supported.
  static bool IsCPUFeatureEnabled(int64_t flag);

  /// Return a pointer type to 'type'
  llvm::PointerType* GetPtrType(llvm::Type* type);

  /// Return a pointer to pointer type to 'type'.
  llvm::PointerType* GetPtrPtrType(llvm::Type* type);

  /// Return a pointer to pointer type for 'name' type.
  llvm::PointerType* GetNamedPtrPtrType(const std::string& name);

  /// Returns llvm type for Impala's internal representation of this column type,
  /// i.e. the way Impala represents this type in a Tuple.
  llvm::Type* GetSlotType(const ColumnType& type);

  /// Return a pointer type to 'type' (e.g. int16_t*)
  llvm::PointerType* GetSlotPtrType(const ColumnType& type);

  /// Returns the type with 'name'.  This is used to pull types from clang
  /// compiled IR.  The types we generate at runtime are unnamed.
  /// The name is generated by the clang compiler in this form:
  /// <class/struct>.<namespace>::<class name>.  For example:
  /// "class.impala::AggregationNode"
  llvm::Type* GetNamedType(const std::string& name);

  /// Returns the pointer type of the type returned by GetNamedType(name)
  llvm::PointerType* GetNamedPtrType(const std::string& name);

  /// Template versions of GetNamed*Type functions that expect the llvm name of
  /// type T to be T::LLVM_CLASS_NAME. T must be a struct/class, so GetStructType
  /// can return llvm::StructType* to avoid casting on the caller side.
  template<class T>
  llvm::StructType* GetStructType() {
    return llvm::cast<llvm::StructType>(GetNamedType(T::LLVM_CLASS_NAME));
  }

  template<class T>
  llvm::PointerType* GetStructPtrType() { return GetNamedPtrType(T::LLVM_CLASS_NAME); }

  template<class T>
  llvm::PointerType* GetStructPtrPtrType() {
    return GetNamedPtrPtrType(T::LLVM_CLASS_NAME);
  }

  /// Alloca's an instance of the appropriate pointer type and sets it to point at 'v'
  llvm::Value* GetPtrTo(LlvmBuilder* builder, llvm::Value* v, const char* name = "");

  /// Creates a global value 'name' using constant 'ir_constant' and returns
  /// a pointer to the global value. Useful for creating constant function arguments
  /// which cannot be represented with primitive types (e.g. struct).
  llvm::Constant* ConstantToGVPtr(llvm::Type* type, llvm::Constant* ir_constant,
      const std::string& name);

  /// Creates a global value 'name' that is an array with element type 'element_type'
  /// containing 'ir_constants'. Returns a pointer to the global value, i.e. a pointer
  /// to a constant array of 'element_type'.
  llvm::Constant* ConstantsToGVArrayPtr(llvm::Type* element_type,
      llvm::ArrayRef<llvm::Constant*> ir_constants, const std::string& name);

  /// Returns reference to llvm context object.  Each LlvmCodeGen has its own
  /// context to allow multiple threads to be calling into llvm at the same time.
  llvm::LLVMContext& context() { return *context_.get(); }

  /// Returns execution engine interface
  llvm::ExecutionEngine* execution_engine() { return execution_engine_.get(); }

  /// Returns the cache which is for the execution engine to write the compiled functions
  /// to.
  CodeGenObjectCache* engine_cache() { return engine_cache_.get(); }

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
  /// false, the module will not be optimized before compilation. After FinalizeModule()
  /// is called, the LLVM module is destroyed and it is invalid to call any LlvmCodegen
  /// functions.
  /// During FinalizeModule(), a new module id might be assigned for caching storage and
  /// retrieval. If module_id is not nullptr, the final module id is returned.
  Status FinalizeModule(string* module_id = nullptr);

  /// Start executing 'FinalizeModule' in a separate thread and return.
  /// 'async_compile_thread_' is set to point to the new 'Thread' object.
  ///
  /// Execution of the query starts in interpreted mode in the calling thread while
  /// compilation is done in 'async_compile_thread_'. When compilation has finished
  /// function pointers that have been added via 'AddFunctionToJit' will be set to the
  /// compiled functions and the query will automatically use the codegen'd version the
  /// next time the corresponding function is called (we always check the codegen'd
  /// function pointer and fall back to interpreted mode if it is nullptr).
  ///
  /// The function pointers are atomic so no locking is needed.
  ///
  /// 'Close' calls 'Join' on '*async_compile_thread_' if it is not a nullptr.
  Status FinalizeModuleAsync(RuntimeProfile::EventSequence* event_sequence);

  /// Loads a native or IR function 'fn' with symbol 'symbol' from the builtins or
  /// an external library and puts the result in *llvm_fn. *llvm_fn can be safely
  /// modified in place, because it is either newly generated or cloned. The caller must
  /// call FinalizeFunction() on 'llvm_fn' once it is done modifying it. The function has
  /// return type 'return_type' (void if 'return_type' is NULL) and input argument types
  /// 'arg_types'. The first 'num_fixed_args' arguments are fixed arguments, and the
  /// remaining arguments are varargs. 'has_varargs' indicates whether the function
  /// accepts varargs. If 'has_varargs' is true, there must be at least one vararg. If
  /// the function is loaded from a library, 'cache_entry' is updated to point to the
  /// library containing the function. If 'cache_entry' is set to a non-NULL value by
  /// this function, the caller must call LibCache::DecrementUseCount() on it when done
  /// using the function.
  Status LoadFunction(const TFunction& fn, const std::string& symbol,
      const ColumnType* return_type, const std::vector<ColumnType>& arg_types,
      int num_fixed_args, bool has_varargs, llvm::Function** llvm_fn,
      LibCacheEntry** cache_entry);

  /// Replaces all instructions in 'caller' that call 'target_name' with a call
  /// instruction to 'new_fn'. The argument types of 'new_fn' must exactly match
  /// the argument types of the function to be replaced. Returns the number of
  /// call sites updated.
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

  /// Replace calls to functions in 'caller' where the callee's name has 'target_name'
  /// as a substring. Calls to functions are replaced with the value 'replacement'. The
  /// return value is the number of calls replaced.
  int ReplaceCallSitesWithValue(llvm::Function* caller, llvm::Value* replacement,
      const std::string& target_name);

  /// This function replaces calls to FunctionContextImpl::GetConstFnAttr() with constants
  /// derived from 'return_type', 'arg_types' and the runtime state 'state_'. Please note
  /// that this function only replaces call instructions inside 'fn' so to replace the
  /// call to FunctionContextImpl::GetConstFnAttr() inside the callee functions, please
  /// inline the callee functions (by annotating them with IR_ALWAYS_INLINE).
  ///
  /// TODO: implement a loop unroller (or use LLVM's) so we can use
  /// FunctionContextImpl::GetConstFnAttr() in loops
  int InlineConstFnAttrs(const FunctionContext::TypeDesc& return_type,
      const std::vector<FunctionContext::TypeDesc>& arg_types, llvm::Function* fn);

  /// Returns a copy of fn. The copy is added to the module.
  llvm::Function* CloneFunction(llvm::Function* fn);

  /// Replace all uses of the instruction 'from' with the value 'to', and delete
  /// 'from'. This is a wrapper around llvm::ReplaceInstWithValue().
  void ReplaceInstWithValue(llvm::Instruction* from, llvm::Value* to);

  /// Returns the i-th argument of fn.
  llvm::Argument* GetArgument(llvm::Function* fn, int i);

  /// Verify function. All handcrafted functions need to be finalized before being
  /// passed to AddFunctionToJit() otherwise the functions will be deleted from the
  /// module when the module is finalized. Also, all loaded functions that need to be JIT
  /// compiled after modification also need to be finalized.
  /// If the function does not verify, it returns NULL and the function will eventually
  /// be deleted in FinalizeModule(), otherwise, it returns the function object.
  llvm::Function* FinalizeFunction(llvm::Function* function);

  /// Prunes any unused functions from the module.
  void PruneModule();

  /// Adds the function to be automatically jit compiled when the codegen object is
  /// finalized. FinalizeModule() will set *fn_ptr to point to the jitted function.
  ///
  /// Pre-condition: FinalizeFunction() must have been called on the function passed to
  /// this method.
  ///
  /// Only functions registered with AddFunctionToJit() and their dependencies are
  /// compiled by FinalizeModule(): other functions are considered dead code and will
  /// be removed during optimization.
  ///
  /// This will also wrap functions returning DecimalVals in an ABI-compliant wrapper (see
  /// the comment in the .cc file for details). This is so we don't accidentally try to
  /// call non-compliant code from native code.
  void AddFunctionToJit(llvm::Function* fn, CodegenFnPtrBase* fn_ptr);

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
  /// iff the caller will modify the returned function so that the original unmodified
  /// function remains available. Avoid cloning if possible to reduce compilation time.
  ///
  /// TODO: Return Status instead.
  llvm::Function* GetFunction(IRFunction::Type ir_type, bool clone);

  /// Return the function with the symbol name 'symbol' from the module. The returned
  /// function and its callee will be recursively materialized. Returns NULL if there is
  /// any error.
  ///
  /// If 'clone' is true, a clone of the function will be returned. Clones should be used
  /// iff the caller will modify the returned function so that the original unmodified
  /// function remains available. Avoid cloning if possible to reduce compilation time.
  ///
  /// TODO: Return Status instead.
  llvm::Function* GetFunction(const string& symbol, bool clone);

  /// Returns the hash function with signature:
  ///   int32_t Hash(int8_t* data, int len, int32_t seed);
  /// If num_bytes is non-zero, the returned function will be codegen'd to only
  /// work for that number of bytes.  It is invalid to call that function with a
  /// different 'len'. Functions returned by these methods have already been finalized.
  llvm::Function* GetHashFunction(int num_bytes = -1);
  llvm::Function* GetFnvHashFunction(int num_bytes = -1);
  llvm::Function* GetMurmurHashFunction(int num_bytes = -1);

  /// Set the NoInline attribute on 'function' and remove the AlwaysInline and InlineHint
  /// attributes if present.
  void SetNoInline(llvm::Function* function) const;

  /// Allocate stack storage for local variables.  This is similar to traditional c, where
  /// all the variables must be declared at the top of the function.  This helper can be
  /// called from anywhere and will add a stack allocation for 'var' at the beginning of
  /// the function.  This would be used, for example, if a function needed a temporary
  /// struct allocated.  The allocated variable is scoped to the function.
  //
  /// This should always be used instead of calling LlvmBuilder::CreateAlloca directly.
  /// LLVM doesn't optimize alloca's occurring in the middle of functions very well (e.g,
  /// an alloca may end up in a loop, potentially blowing the stack).
  llvm::AllocaInst* CreateEntryBlockAlloca(llvm::Function* f, const NamedVariable& var);
  llvm::AllocaInst* CreateEntryBlockAlloca(
      const LlvmBuilder& builder, llvm::Type* type, const char* name = "");

  /// Same as above, except allocates an array of 'type' with 'num_entries' entries
  /// and alignment 'alignment'.
  llvm::AllocaInst* CreateEntryBlockAlloca(const LlvmBuilder& builder, llvm::Type* type,
      int num_entries, int alignment, const char* name = "");

  /// Utility to create two blocks in 'fn' for if/else codegen.  if_block and else_block
  /// are return parameters.  insert_before is optional and if set, the two blocks
  /// will be inserted before that block otherwise, it will be inserted at the end
  /// of 'fn'.  Being able to place blocks is useful for debugging so the IR has a
  /// better looking control flow.
  void CreateIfElseBlocks(llvm::Function* fn, const std::string& if_name,
      const std::string& else_name,
      llvm::BasicBlock** if_block, llvm::BasicBlock** else_block,
      llvm::BasicBlock* insert_before = NULL);

  // Creates a PHI node with two incoming blocks that will have the value 'value1' for the
  // incoming block 'incoming_block1' and the value 'value2' for incoming block
  // 'incoming_block2'.
  static llvm::PHINode* CreateBinaryPhiNode(LlvmBuilder* builder, llvm::Value* value1,
      llvm::Value* value2, llvm::BasicBlock* incoming_block1,
      llvm::BasicBlock* incoming_block2, std::string name = "");

  /// Returns a constant int of 'byte_size' bytes based on 'low_bits' and 'high_bits'
  /// which stand for the lower and upper 64-bits of the constant respectively. For
  /// values less than or equal to 64-bits, 'high_bits' is not used. This function
  /// can generate constant up to 128-bit wide. 'byte_size' must be power of 2.
  llvm::Constant* GetIntConstant(int byte_size, uint64_t low_bits, uint64_t high_bits);

  /// Initialise a constant global string and returns an i8* pointer to it.
  llvm::Value* GetStringConstant(LlvmBuilder* builder, const char* data, int len);
  llvm::Value* GetStringConstant(LlvmBuilder* builder, const std::string& str) {
    return GetStringConstant(builder, str.c_str(), str.size());
  }

  /// Returns true/false constants (bool type)
  llvm::Constant* true_value() { return true_value_; }
  llvm::Constant* false_value() { return false_value_; }
  llvm::Constant* null_ptr_value() { return llvm::ConstantPointerNull::get(ptr_type()); }

  /// Simple wrappers to reduce code verbosity
  llvm::Type* bool_type() { return llvm::Type::getInt1Ty(context()); }
  llvm::Type* i8_type() { return llvm::Type::getInt8Ty(context()); }
  llvm::Type* i16_type() { return llvm::Type::getInt16Ty(context()); }
  llvm::Type* i32_type() { return llvm::Type::getInt32Ty(context()); }
  llvm::Type* i64_type() { return llvm::Type::getInt64Ty(context()); }
  llvm::Type* i128_type() { return llvm::Type::getIntNTy(context(), 128); }
  llvm::Type* float_type() { return llvm::Type::getFloatTy(context()); }
  llvm::Type* double_type() { return llvm::Type::getDoubleTy(context()); }
  llvm::PointerType* ptr_type() { return ptr_type_; }
  llvm::Type* void_type() { return void_type_; }

  llvm::PointerType* i8_ptr_type() { return GetPtrType(i8_type()); }
  llvm::PointerType* i16_ptr_type() { return GetPtrType(i16_type()); }
  llvm::PointerType* i32_ptr_type() { return GetPtrType(i32_type()); }
  llvm::PointerType* i64_ptr_type() { return GetPtrType(i64_type()); }
  llvm::PointerType* float_ptr_type() { return GetPtrType(float_type()); }
  llvm::PointerType* double_ptr_type() { return GetPtrType(double_type()); }
  llvm::PointerType* ptr_ptr_type() { return GetPtrType(ptr_type_); }

  llvm::Constant* GetBoolConstant(bool val) { return val ? true_value_ : false_value_; }
  llvm::Constant* GetI8Constant(uint64_t val) {
    return llvm::ConstantInt::get(context(), llvm::APInt(8, val));
  }
  llvm::Constant* GetI16Constant(uint64_t val) {
    return llvm::ConstantInt::get(context(), llvm::APInt(16, val));
  }
  llvm::Constant* GetI32Constant(uint64_t val) {
    return llvm::ConstantInt::get(context(), llvm::APInt(32, val));
  }
  llvm::Constant* GetI64Constant(uint64_t val) {
    return llvm::ConstantInt::get(context(), llvm::APInt(64, val));
  }

  /// Load the module temporarily and populate 'symbols' with the symbols in the module.
  static Status GetSymbols(const string& file, const string& module_id,
      boost::unordered_set<std::string>* symbols);

  /// Codegen at the current builder location in function 'fn' to store the
  /// max/min('src', value in 'dst_slot_ptr') in 'dst_slot_ptr'
  void CodegenMinMax(LlvmBuilder* builder, const ColumnType& type,
      llvm::Value* dst_slot_ptr, llvm::Value* src, bool min, llvm::Function* fn);

  /// Codegen to call llvm memcpy intrinsic at the current builder location
  /// dst & src must be pointer types. size is the number of bytes to copy.
  /// No-op if size is zero.
  void CodegenMemcpy(LlvmBuilder* builder, llvm::Value* dst, llvm::Value* src, int size);
  void CodegenMemcpy(LlvmBuilder* builder, llvm::Value* dst, llvm::Value* src,
      llvm::Value* size);

  /// Codegen to call llvm memset intrinsic at the current builder location. 'dst' should
  /// be a pointer. No-op if size is zero.
  void CodegenMemset(LlvmBuilder* builder, llvm::Value* dst, int value, int size);

  /// Codegen to set all null bytes of the given tuple to 0.
  void CodegenClearNullBits(LlvmBuilder* builder, llvm::Value* tuple_ptr,
      const TupleDescriptor& tuple_desc);

  /// Codegen to call pool_val->Allocate(size_val).
  /// 'pool_val' has to be of type MemPool*.
  llvm::Value* CodegenMemPoolAllocate(LlvmBuilder* builder, llvm::Value* pool_val,
      llvm::Value* size_val, const char* name = "");

  /// Codegens IR to load array[idx] and returns the loaded value. 'array' should be a
  /// C-style array (e.g. i32*) or an IR array (e.g. [10 x i32]). This function does not
  /// do bounds checking.
  llvm::Value* CodegenArrayAt(
      LlvmBuilder*, llvm::Value* array, int idx, const char* name = "");

  /// Codegens IR to call the function corresponding to 'ir_type' with argument 'args'
  /// and returns the value.
  llvm::Value* CodegenCallFunction(LlvmBuilder* builder, IRFunction::Type ir_type,
      llvm::ArrayRef<llvm::Value*> args, const char* name);

  /// If there are more than this number of expr trees (or functions that evaluate
  /// expressions), avoid inlining avoid inlining for the exprs exceeding this threshold.
  static const int CODEGEN_INLINE_EXPRS_THRESHOLD = 100;

  /// If there are more than this number of expr trees (or functions that evaluate
  /// expressions), avoid inlining the function that evaluates the expression batch
  /// into the calling function.
  static const int CODEGEN_INLINE_EXPR_BATCH_THRESHOLD = 25;

  /// Name prefix of the thread counters that track async codegen time.
  static const std::string ASYNC_CODEGEN_THREAD_COUNTERS_PREFIX;

 private:
  friend class ExprCodegenTest;
  friend class LlvmCodeGenTest;
  friend class LlvmCodeGenTest_CpuAttrWhitelist_Test;
  friend class LlvmCodeGenTest_HashTest_Test;
  friend class LlvmOptTest;
  friend class SubExprElimination;
  friend class CodeGenCache;
  friend class LlvmCodeGenCacheTest;

  /// Top level codegen object. 'module_id' is used for debugging when outputting the IR.
  LlvmCodeGen(FragmentState* state, ObjectPool* pool, MemTracker* parent_mem_tracker,
      const std::string& module_id);

  /// Initializes the jitter and execution engine with the given module.
  Status Init(std::unique_ptr<llvm::Module> module);

  /// Creates a LlvmCodeGen instance initialized with the module bitcode from 'file'.
  /// 'codegen' will contain the created object on success. The functions in the module
  /// are materialized lazily. Getting a reference to a function via GetFunction() will
  /// materialize the function and its callees recursively.
  static Status CreateFromFile(FragmentState* state, ObjectPool* pool,
      MemTracker* parent_mem_tracker, const std::string& file,
      const std::string& id, boost::scoped_ptr<LlvmCodeGen>* codegen);

  /// Creates a LlvmCodeGen instance initialized with the module bitcode in memory.
  /// 'codegen' will contain the created object on success. The functions in the module
  /// are materialized lazily. Getting a reference to a function via GetFunction() will
  /// materialize the function and its callees recursively.
  static Status CreateFromMemory(FragmentState* state, ObjectPool* pool,
      MemTracker* parent_mem_tracker, const std::string& id,
      boost::scoped_ptr<LlvmCodeGen>* codegen);

  /// Loads an LLVM module from 'file' which is the local path to the LLVM bitcode file.
  /// The functions in the module are materialized lazily. Getting a reference to the
  /// function via GetFunction() will materialize the function and its callees
  /// recursively. The caller is responsible for cleaning up the module.
  Status LoadModuleFromFile(const string& file, std::unique_ptr<llvm::Module>* module);

  /// Loads an LLVM module. 'module_ir_buf' is the memory buffer containing LLVM bitcode.
  /// 'module_name' is the name of the module to use when reporting errors. The caller is
  /// responsible for cleaning up 'module'. The functions in the module aren't
  /// materialized. Getting a reference to the functiom via GetFunction() will materialize
  /// the function and its callees recursively.
  Status LoadModuleFromMemory(std::unique_ptr<llvm::MemoryBuffer> module_ir_buf,
      std::string module_name, std::unique_ptr<llvm::Module>* module);

  /// Loads a module at 'file' and links it to the module associated with this
  /// LlvmCodeGen object. The 'file' must be on the local filesystem.
  Status LinkModuleFromLocalFs(const std::string& file);

  /// Same as 'LinkModuleFromLocalFs', but takes an hdfs file location instead and makes
  /// sure that the same hdfs file is not linked twice. The mtime is used ensure that the
  /// cached hdfs_file that's used is the most recent.
  Status LinkModuleFromHdfs(const std::string& hdfs_file, const time_t mtime);

  /// Strip global constructors and destructors from an LLVM module. We never run them
  /// anyway (they must be explicitly invoked) so it is dead code.
  static void StripGlobalCtorsDtors(llvm::Module* module);

  /// Set the "target-cpu" and "target-features" of 'function' to match the host's CPU's
  /// features. Having consistent attributes for all materialized functions allows
  /// generated IR to be inlined into cross-compiled functions' IR and vice versa.
  static void SetCPUAttrs(llvm::Function* function);

  /// If a symbol emitter is needed, creates one and registers it as a listener of
  /// 'execution_engine'. It is used to generate perf symbol map or disassembly.
  /// If no symbol emitter is needed, returns NULL.
  std::unique_ptr<CodegenSymbolEmitter> SetupSymbolEmitter(
      llvm::ExecutionEngine* execution_engine);

  /// Load the intrinsics impala needs.  This is a one time initialization.
  /// Values are stored in 'llvm_intrinsics_'
  Status LoadIntrinsics();

  /// Internal function for unit tests: skips Impala-specific wrapper generation logic.
  void AddFunctionToJitInternal(llvm::Function* fn, CodegenFnPtrBase* fn_ptr);

  /// Verifies the function, e.g., checks that the IR is well-formed.  Returns false if
  /// function is invalid.
  bool VerifyFunction(llvm::Function* function);

  // Used for testing.
  void ResetVerification() { is_corrupt_ = false; }

  // Lookup the codegen functions from the cache to reduce optimization time.
  bool LookupCache(CodeGenCacheKey& key);

  // Store the codegen functions to the cache if codegen cache is enabled.
  Status StoreCache(CodeGenCacheKey& key);

  /// Optimizes the module. This includes pruning the module of any unused functions.
  Status OptimizeModule();

  /// Points the function pointers in 'fns_to_jit_compile_' to the compiled functions. If
  /// 'cache' and 'cache_key' are non-NULL, retrieves the functions from the cached
  /// execution engine, otherwise from the current execution engine.
  /// Note: either both or none of 'cache' and 'cache_key' should be NULL.
  bool SetFunctionPointers(CodeGenCache* cache = nullptr,
      const CodeGenCacheKey* cache_key = nullptr);

  /// Clears generated hash fns.  This is only used for testing.
  void ClearHashFns();

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
  Status MaterializeFunction(llvm::Function* fn);

  /// Materialize the module owned by this codegen object. This will materialize all
  /// functions and delete the module's materializer. Returns error status for any error.
  Status MaterializeModule();

  /// With lazy materialization, functions which haven't been materialized when the module
  /// is finalized must be dead code or referenced only by global variables (e.g. boost
  /// library functions or virtual function (e.g. IfExpr::GetBooleanVal())), in which case
  /// the function is not inlined so the native version can be used and the IR version is
  /// dead code. Mark them as not materializable, change their linkage types to external
  /// (so linking will happen to the native version) and strip their personality functions
  /// and comdats. DCE may complain if the above are not done. Return error status if
  /// there is error in materializing the module.
  Status FinalizeLazyMaterialization();

  /// Destroy the IR module, freeing memory used by the IR. Any machine code that was
  /// generated is retained by the execution engine.
  void DestroyModule();

  /// Generate a string containing all jitted function names from fns_to_jit_compile_.
  /// The generation of the string is simply the concatenation of all the names by
  /// sequence in fns_to_jit_compile_. Would be used in the codegen cache lookup to
  /// confirm whether a cache entry matches the need of the LlvmCodeGen.
  std::string GetAllFunctionNames();

  /// Generate and store the hash code of all the function names. Will be used to
  /// codegen cache only.
  void GenerateFunctionNamesHashCode();

  /// Disable CPU attributes in 'cpu_attrs' that are not present in
  /// the '--llvm_cpu_attr_whitelist' flag. The same attributes in the input are
  /// always present in the output, except "+" is flipped to "-" for the disabled
  /// attributes. E.g. if 'cpu_attrs' is {"+x", "+y", "-z"} and the whitelist is
  /// {"x", "z"}, returns {"+x", "-y", "-z"}.
  static std::unordered_set<std::string> ApplyCpuAttrWhitelist(
      const std::unordered_set<std::string>& cpu_attrs);

  /// Whether InitializeLlvm() has been called.
  static bool llvm_initialized_;

  /// Host CPU name and attributes, filled in by InitializeLlvm().
  static std::string cpu_name_;
  /// The cpu_attrs_ should not be modified during the execution except for tests.
  static std::unordered_set<std::string> cpu_attrs_;

  /// Value of "target-features" attribute to be set on all IR functions. Derived from
  /// 'cpu_attrs_'. Using a consistent value for this attribute among
  /// hand-crafted IR and cross-compiled functions allow them to be inlined into each
  /// other.
  static std::string target_features_attr_;

  /// Mapping between CpuInfo flags and the corresponding strings.
  /// The key is mapped to the string as follows:
  /// CpuInfo flag -> enabled feature.
  /// Bitwise negation of CpuInfo flag -> disabled feature.
  const static std::map<int64_t, std::string> cpu_flag_mappings_;

  /// A global shared call graph for all IR functions in the main module.
  /// Used for determining dependencies when materializing IR functions.
  static CodegenCallGraph shared_call_graph_;

  /// Pointer to the FragmentState which owns this codegen object. Needed in
  /// InlineConstFnAttr() to access the query options.
  const FragmentState* state_;

  /// ID used for debugging (can be e.g. the fragment instance ID)
  std::string id_;

  /// Codegen counters
  RuntimeProfile* const profile_;

  /// MemTracker used for tracking memory consumed by codegen. Connected to a parent
  /// MemTracker if one was provided during initialization. Owned by the ObjectPool
  /// provided in the constructor.
  MemTracker* mem_tracker_;

  /// Time spent reading the .ir file from the file system.
  RuntimeProfile::Counter* load_module_timer_;

  /// Time spent creating the initial module with the cross-compiled Impala IR.
  RuntimeProfile::Counter* prepare_module_timer_;

  /// Time spent generating module bitcode.
  RuntimeProfile::Counter* module_bitcode_gen_timer_;

  /// Time spent for codegen cache look up and save.
  RuntimeProfile::Counter* codegen_cache_lookup_timer_;
  RuntimeProfile::Counter* codegen_cache_save_timer_;

  /// Time spent by ExecNodes while adding IR to the module. Update by
  /// FragmentInstanceState during its 'CODEGEN_START' state.
  RuntimeProfile::Counter* ir_generation_timer_;

  /// Time spent pruning unused functions.
  RuntimeProfile::Counter* function_prune_timer_;

  /// Time spent optimizing the module.
  RuntimeProfile::Counter* optimization_timer_;

  /// Time spent compiling the module.
  RuntimeProfile::Counter* compile_timer_;

  /// Total codegen time spent in the main thread.
  RuntimeProfile::Counter* main_thread_timer_;

  /// Total codegen time spent in the compiler (helper) thread.
  RuntimeProfile::ThreadCounters* compile_thread_counters_;

  /// Total size of bitcode modules loaded in bytes.
  RuntimeProfile::Counter* module_bitcode_size_;

  /// Number of functions and instructions that are optimized and compiled after pruning
  /// unused functions from the module.
  RuntimeProfile::Counter* num_functions_;
  RuntimeProfile::Counter* num_instructions_;

  /// Number of instructions after optimization.
  RuntimeProfile::Counter* num_opt_functions_;
  RuntimeProfile::Counter* num_opt_instructions_;

  /// Number of functions that are used and cached.
  RuntimeProfile::Counter* num_cached_functions_;

  /// Aggregated llvm thread counters. Also includes the phase represented by
  /// 'ir_generation_timer_' and hence is also updated by FragmentInstanceState.
  RuntimeProfile::ThreadCounters* llvm_thread_counters_;

  std::unique_ptr<Thread> async_compile_thread_;

  /// whether or not optimizations are enabled
  bool optimizations_enabled_;

  /// Whether or not codegen cache is enabled.
  bool codegen_cache_enabled_ = true;

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

  /// Top level codegen object. Contains everything to jit one 'unit' of code.  module_ is
  /// set by Init(). module_ is owned by the execution engine.
  llvm::Module* module_;

  // Execution/Jitting engine.
  std::unique_ptr<llvm::ExecutionEngine> execution_engine_;

  /// Object cache which is for the execution engine to write the compiled codegened
  /// functions to. Would be used as a part of CodeGen caching.
  std::shared_ptr<CodeGenObjectCache> engine_cache_;

  /// Object cache from the global CodeGen Cache, storing compiled codegened functions
  /// that align with the module of the current ExecutionEngine.
  /// Not null only when there is a cache hit.
  /// The purpose of it is to maintain the lifecycle of this CodeGenObjectCache in case
  /// it gets evicted from the global cache while in use.
  std::shared_ptr<CodeGenObjectCache> engine_cache_cached_;

  /// The memory manager used by 'execution_engine_'. Owned by 'execution_engine_'.
  ImpalaMCJITMemoryManager* memory_manager_;

  /// Functions parsed from pre-compiled module. Indexed by ImpalaIR::Function enum.
  std::vector<llvm::Function*> cross_compiled_functions_;

  /// Stores functions handcrafted by impala.  This does not contain cross compiled
  /// functions, only function that were generated from scratch at runtime. Does not
  /// overlap with loaded_functions_.
  std::vector<llvm::Function*> handcrafted_functions_;

  /// Stores the functions that have been finalized.
  std::unordered_set<llvm::Function*> finalized_functions_;

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

  /// The locations of modules that have been linked. Uses hdfs file location as the key.
  /// Used to avoid linking the same module twice, which causes symbol collision errors.
  std::set<std::string> linked_modules_;

  /// Stores the functions to automatically JIT compile after FinalizeModule(). The
  /// 'CodegenFnPtrBase*' function pointers will be set to the functions compiled from the
  /// corresponding 'llvm::Function' objects.
  ///
  /// The functions are stored in a sorted map where the keys are the function names.
  /// This is because we need the function names in GetAllFunctionNames() (in sorted
  /// order) and in PruneModule() (in any order).
  ///
  /// There is a one-to-one correspondence between function names and 'llvm::Function'
  /// objects but an 'llvm::Function' object may correspond to multiple
  /// 'CodegenFnPtrBase*'s, for example if multiple 'SlotRef' expressions refer to the
  /// same slot and the 'llvm::Function' is reused. In these cases the function pointers
  /// corresponding to a single 'llvm::Function' are owned by different objects but they
  /// will be set to the same value.
  using LlvmFunctionWithFnPtrTargets =
      std::pair<llvm::Function*, std::vector<CodegenFnPtrBase*>>;
  std::map<llvm::StringRef, LlvmFunctionWithFnPtrTargets> fns_to_jit_compile_;

  /// The hash code generated from all the function names in fns_to_jit_compile_.
  /// Used by the codegen cache only.
  uint64_t function_names_hashcode_;

  /// The symbol emitted associated with 'execution_engine_'. Methods on
  /// llvm representation of a few common types.  Owned by context.
  llvm::PointerType* ptr_type_;             // int8_t*
  llvm::Type* void_type_;                   // void
  llvm::Type* string_value_type_;           // StringValue
  llvm::Type* timestamp_value_type_;        // TimestampValue
  llvm::Type* collection_value_type_;       // CollectionValue

  /// llvm constants to help with code gen verbosity
  llvm::Constant* true_value_;
  llvm::Constant* false_value_;

  /// The symbol emitter associated with 'execution_engine_'. Methods on
  /// 'symbol_emitter_' are called by 'execution_engine_' when code is emitted or
  /// freed. The lifetime of the symbol emitter must be longer than
  /// 'execution_engine_'.
  std::unique_ptr<CodegenSymbolEmitter> symbol_emitter_;

  /// Provides an implementation of a LLVM diagnostic handler and maintains the error
  /// information from its callbacks.
  class DiagnosticHandler {
   public:
    /// Returns the last error that was reported via DiagnosticHandlerFn() and then
    /// clears it. Returns an empty string otherwise. This should be called after any
    /// LLVM API call that can fail but returns error info via this mechanism.
    /// TODO: IMPALA-6038: use this to check and handle errors wherever needed.
    std::string GetErrorString();

    /// Handler function that sets the state on an instance of this class which is
    /// accessible via the LlvmCodeGen object passed to it using the 'context'
    /// input parameter.
    static void DiagnosticHandlerFn(const llvm::DiagnosticInfo &info, void *context);

   private:
    /// Contains the last error that was reported via DiagnosticHandlerFn().
    /// Is cleared by a call to GetErrorString().
    std::string error_str_;
  };

  DiagnosticHandler diagnostic_handler_;

  /// Very rough estimate of memory in bytes that the IR and the intermediate data
  /// structures used by the optimizer may consume per LLVM IR instruction to be
  /// optimized (after dead code is removed). The number is chosen to avoid pathological
  /// behaviour at either extreme: failing queries unnecessarily because the memory
  /// estimate is too high versus having large amounts of untracked memory because the
  /// estimate is too low.
  ///
  /// This was chosen by looking at the behaviour of TPC-H queries. Using the heap growth
  /// profile from gperftools reveal that LLVM allocated ~9mb of memory for fragments with
  /// ~17k total instructions in TPC-H Q2. Inspection of other TPC-H queries revealed
  /// that a typical fragment from a TPC-H query is < 5,000 instructions, which translates
  /// to 2.5MB, which is almost always lower than the runtime memory requirement of the
  /// fragment - so we are unlikely to fail queries unnecessarily.
  ///
  /// This assumes optimizer memory usage scales linearly with instruction count. This is
  /// true only if the size of functions is bounded, because some optimization passes
  /// (e.g. global value numbering) use time and memory that is super-linear in relation
  /// to the # of instructions in a function. So codegen should avoid generating
  /// arbitrarily large function.
  static constexpr int64_t ESTIMATED_OPTIMIZER_BYTES_PER_INST = 512;
};
}

#endif
