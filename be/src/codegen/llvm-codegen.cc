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

#include "codegen/llvm-codegen.h"
#include "codegen/llvm-codegen-cache.h"

#include <fstream>
#include <mutex>
#include <sstream>
#include <unordered_set>
#include <boost/algorithm/string.hpp>
#include <boost/assert/source_location.hpp>
#include <gutil/strings/substitute.h>

#include <llvm/Bitcode/BitcodeReader.h>
#include <llvm/Bitcode/BitcodeWriter.h>
#include <llvm/ExecutionEngine/ExecutionEngine.h>
#include <llvm/ExecutionEngine/MCJIT.h>
#include <llvm/IR/Constants.h>
#include <llvm/IR/DataLayout.h>
#include <llvm/IR/DiagnosticInfo.h>
#include <llvm/IR/DiagnosticPrinter.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/GlobalVariable.h>
#include <llvm/IR/InstIterator.h>
#include <llvm/IR/Verifier.h>
#include <llvm/Linker/Linker.h>
#include <llvm/Passes/PassBuilder.h>
#include <llvm/Support/CommandLine.h>
#include <llvm/Support/DynamicLibrary.h>
#include <llvm/Support/ErrorHandling.h>
#include <llvm/Support/Host.h>
#include <llvm/Support/TargetSelect.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/Transforms/IPO/GlobalDCE.h>
#include <llvm/Transforms/IPO/Internalize.h>
#include <llvm/Transforms/Scalar.h>
#include <llvm/Transforms/Utils/BasicBlockUtils.h>
#include <llvm/Transforms/Utils/Cloning.h>

#include "codegen/codegen-anyval.h"
#include "codegen/codegen-callgraph.h"
#include "codegen/codegen-fn-ptr.h"
#include "codegen/codegen-symbol-emitter.h"
#include "codegen/impala-ir-data.h"
#include "codegen/instruction-counter.h"
#include "codegen/mcjit-mem-mgr.h"
#include "common/logging.h"
#include "exprs/anyval-util.h"
#include "gutil/sysinfo.h"
#include "impala-ir/impala-ir-names.h"
#include "runtime/collection-value.h"
#include "runtime/descriptors.h"
#include "runtime/exec-env.h"
#include "runtime/fragment-state.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/lib-cache.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/runtime-state.h"
#include "runtime/string-value.h"
#include "runtime/timestamp-value.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/hdfs-util.h"
#include "util/path-builder.h"
#include "util/runtime-profile-counters.h"
#include "util/symbols-util.h"
#include "util/test-info.h"
#include "util/thread.h"
#include "util/thrift-debug-util.h"

#include "common/names.h"

using namespace strings;
using std::fstream;
using std::move;

DEFINE_bool(print_llvm_ir_instruction_count, false,
    "if true, prints the instruction counts of all JIT'd functions");
DEFINE_bool(disable_optimization_passes, false,
    "if true, disables llvm optimization passes (used for testing)");
DEFINE_bool(dump_ir, false, "if true, output IR after optimization passes");
DEFINE_bool(perf_map, false,
    "if true, generate /tmp/perf-<pid>.map file for linux perf symbols. "
    "This is not recommended for production use because it may affect performance.");
DEFINE_string(unopt_module_dir, "",
    "if set, saves unoptimized generated IR modules to the specified directory.");
DEFINE_string(opt_module_dir, "",
    "if set, saves optimized generated IR modules to the specified directory.");
DEFINE_string(asm_module_dir, "",
    "if set, saves disassembly for generated IR modules to the specified directory.");
DECLARE_string(local_library_dir);
// IMPALA-6291: AVX-512 and other CPU attrs the community doesn't routinely test are
// disabled. AVX-512 is affected by known bugs in LLVM 3.9.1. The following attrs that
// exist in LLVM 3.9.1 are disabled: avx512bw,avx512cd,avx512dq,avx512er,avx512f,
// avx512ifma,avx512pf,avx512vbmi,avx512vl,clflushopt,clwb,fma4,mwaitx.1.2,pcommit,pku,
// prefetchwt1,sgx,sha,sse4a,tbm,xop,xsavec,xsaves. If new attrs are added to LLVM,
// they will be disabled until added to this whitelist.
#ifdef __aarch64__
DEFINE_string_hidden(llvm_cpu_attr_whitelist, "crc,neon,fp-armv8,crypto",
    "(Experimental) a comma-separated list of LLVM CPU attribute flags that are enabled "
    "for runtime code generation. This flag is provided to enable additional LLVM CPU "
    "attribute flags for testing.");
#else
DEFINE_string_hidden(llvm_cpu_attr_whitelist, "adx,aes,avx,avx2,bmi,bmi2,cmov,cx16,f16c,"
    "fma,fsgsbase,hle,invpcid,lzcnt,mmx,movbe,pclmul,popcnt,prfchw,rdrnd,rdseed,rtm,smap,"
    "sse,sse2,sse3,sse4.1,sse4.2,ssse3,xsave,xsaveopt",
    "(Experimental) a comma-separated list of LLVM CPU attribute flags that are enabled "
    "for runtime code generation. The default flags are a known-good set that are "
    "routinely tested. This flag is provided to enable additional LLVM CPU attribute "
    "flags for testing.");
#endif
DEFINE_string_hidden(llvm_ir_opt, "Os",
    "The IR optimization level for pre-generated code; supports O1, O2, and Os.");
DECLARE_bool(enable_legacy_avx_support);

namespace impala {
const string LlvmCodeGen::ASYNC_CODEGEN_THREAD_COUNTERS_PREFIX = "CodegenCompileThread";
bool LlvmCodeGen::llvm_initialized_ = false;
string LlvmCodeGen::cpu_name_;
std::unordered_set<string> LlvmCodeGen::cpu_attrs_;
string LlvmCodeGen::target_features_attr_;
CodegenCallGraph LlvmCodeGen::shared_call_graph_;

const map<int64_t, std::string> LlvmCodeGen::cpu_flag_mappings_{
    {CpuInfo::SSSE3, "+ssse3"}, {CpuInfo::SSE4_1, "+sse4.1"},
    {CpuInfo::SSE4_2, "+sse4.2"}, {CpuInfo::POPCNT, "+popcnt"}, {CpuInfo::AVX, "+avx"},
    {CpuInfo::AVX2, "+avx2"}, {CpuInfo::PCLMULQDQ, "+pclmul"},
    {~(CpuInfo::SSSE3), "-ssse3"}, {~(CpuInfo::SSE4_1), "-sse4.1"},
    {~(CpuInfo::SSE4_2), "-sse4.2"}, {~(CpuInfo::POPCNT), "-popcnt"},
    {~(CpuInfo::AVX), "-avx"}, {~(CpuInfo::AVX2), "-avx2"},
    {~(CpuInfo::PCLMULQDQ), "-pclmul"}};

[[noreturn]] static void LlvmCodegenHandleError(
    void* user_data, const string& reason, bool gen_crash_diag) {
  LOG(FATAL) << "LLVM hit fatal error: " << reason.c_str();
}

Status LlvmCodeGen::InitializeLlvm(const char* procname, bool load_backend) {
  DCHECK(!llvm_initialized_);
  // Treat all functions as having the inline hint
  std::array<const char*, 2> argv = { { procname, "-inline-threshold=325" } };
  CHECK(llvm::cl::ParseCommandLineOptions(argv.size(), argv.data()));
  llvm::remove_fatal_error_handler();
  llvm::install_fatal_error_handler(LlvmCodegenHandleError);
  // These functions can *only* be called once per process and are used to set up
  // LLVM subsystems for code generation targeting the machine we're running on.
  llvm::InitializeNativeTarget();
  llvm::InitializeNativeTargetAsmPrinter();
  llvm::InitializeNativeTargetAsmParser();
  llvm::InitializeNativeTargetDisassembler();
  llvm_initialized_ = true;

  if (load_backend) {
    string path;
    // For test env, we have to load libfesupport.so to provide sym for LLVM.
    PathBuilder::GetFullBuildPath("service/libfesupport.so", &path);
    bool failed = llvm::sys::DynamicLibrary::LoadLibraryPermanently(path.c_str());
    DCHECK_EQ(failed, 0);
  }

  cpu_name_ = llvm::sys::getHostCPUName().str();
  LOG(INFO) << "CPU class for runtime code generation: " << cpu_name_;
  GetHostCPUAttrs(&cpu_attrs_);
  LOG(INFO) << "Detected CPU flags: " << boost::join(cpu_attrs_, ",");
  cpu_attrs_ = ApplyCpuAttrWhitelist(cpu_attrs_);
  target_features_attr_ = boost::join(cpu_attrs_, ",");
  LOG(INFO) << "CPU flags enabled for runtime code generation: " << target_features_attr_;

  // Write an empty map file for perf to find.
  if (FLAGS_perf_map) CodegenSymbolEmitter::WritePerfMap();

  ObjectPool init_pool;
  scoped_ptr<LlvmCodeGen> init_codegen;
  RETURN_IF_ERROR(LlvmCodeGen::CreateFromMemory(
      nullptr, &init_pool, nullptr, "init", &init_codegen));
  // LLVM will construct "use" lists only when the entire module is materialized.
  RETURN_IF_ERROR(init_codegen->MaterializeModule());

  // Validate the module by verifying that functions for all IRFunction::Type
  // can be found.
  for (int i = IRFunction::FN_START; i < IRFunction::FN_END; ++i) {
    DCHECK_EQ(FN_MAPPINGS[i].fn, i);
    const string& fn_name = FN_MAPPINGS[i].fn_name;
    if (init_codegen->module_->getFunction(fn_name) == nullptr) {
      const string& err_msg = Substitute("Failed to find function $0", fn_name);
      LOG(ERROR) << err_msg;
      return Status(err_msg);
    }
  }

  // Initialize the global shared call graph.
  shared_call_graph_.Init(init_codegen->module_);
  init_codegen->Close();
  return Status::OK();
}

LlvmCodeGen::LlvmCodeGen(FragmentState* state, ObjectPool* pool,
    MemTracker* parent_mem_tracker, const string& id)
  : state_(state),
    id_(id),
    profile_(RuntimeProfile::Create(pool, "CodeGen")),
    mem_tracker_(pool->Add(new MemTracker(profile_, -1, "CodeGen", parent_mem_tracker))),
    optimizations_enabled_(false),
    is_corrupt_(false),
    is_compiled_(false),
    context_(new llvm::LLVMContext()),
    module_(nullptr),
    memory_manager_(nullptr),
    cross_compiled_functions_(IRFunction::FN_END, nullptr) {
  DCHECK(llvm_initialized_) << "Must call LlvmCodeGen::InitializeLlvm first.";

  context_->setDiagnosticHandler(&DiagnosticHandler::DiagnosticHandlerFn, this);
  load_module_timer_ = ADD_TIMER(profile_, "LoadTime");
  prepare_module_timer_ = ADD_TIMER(profile_, "PrepareTime");
  codegen_cache_lookup_timer_ = ADD_TIMER(profile_, "CodegenCacheLookupTime");
  codegen_cache_save_timer_ = ADD_TIMER(profile_, "CodegenCacheSaveTime");
  module_bitcode_gen_timer_ = ADD_TIMER(profile_, "ModuleBitcodeGenTime");
  module_bitcode_size_ = ADD_COUNTER(profile_, "ModuleBitcodeSize", TUnit::BYTES);
  ir_generation_timer_ = ADD_TIMER(profile_, "IrGenerationTime");
  function_prune_timer_ = ADD_TIMER(profile_, "FunctionPruneTime");
  optimization_timer_ = ADD_TIMER(profile_, "OptimizationTime");
  compile_timer_ = ADD_TIMER(profile_, "CompileTime");
  main_thread_timer_ = ADD_TIMER(profile_, "MainThreadCodegenTime");
  compile_thread_counters_ = ADD_THREAD_COUNTERS(profile_,
      ASYNC_CODEGEN_THREAD_COUNTERS_PREFIX);
  num_functions_ = ADD_COUNTER(profile_, "NumFunctions", TUnit::UNIT);
  num_instructions_ = ADD_COUNTER(profile_, "NumInstructions", TUnit::UNIT);
  num_opt_functions_ = ADD_COUNTER(profile_, "NumOptimizedFunctions", TUnit::UNIT);
  num_opt_instructions_ = ADD_COUNTER(profile_, "NumOptimizedInstructions", TUnit::UNIT);
  num_cached_functions_ = ADD_COUNTER(profile_, "NumCachedFunctions", TUnit::UNIT);
  llvm_thread_counters_ = ADD_THREAD_COUNTERS(profile_, "Codegen");
}

Status LlvmCodeGen::CreateFromFile(FragmentState* state, ObjectPool* pool,
    MemTracker* parent_mem_tracker, const string& file, const string& id,
    scoped_ptr<LlvmCodeGen>* codegen) {
  codegen->reset(new LlvmCodeGen(state, pool, parent_mem_tracker, id));
  SCOPED_TIMER((*codegen)->profile_->total_time_counter());
  SCOPED_THREAD_COUNTER_MEASUREMENT((*codegen)->llvm_thread_counters());

  unique_ptr<llvm::Module> loaded_module;
  Status status = (*codegen)->LoadModuleFromFile(file, &loaded_module);
  if (!status.ok()) goto error;
  status = (*codegen)->Init(move(loaded_module));
  if (!status.ok()) goto error;
  return Status::OK();
error:
  (*codegen)->Close();
  return status;
}

Status LlvmCodeGen::CreateFromMemory(FragmentState* state, ObjectPool* pool,
    MemTracker* parent_mem_tracker, const string& id, scoped_ptr<LlvmCodeGen>* codegen) {
  codegen->reset(new LlvmCodeGen(state, pool, parent_mem_tracker, id));
  SCOPED_TIMER((*codegen)->profile_->total_time_counter());
  SCOPED_TIMER((*codegen)->prepare_module_timer_);
  SCOPED_THREAD_COUNTER_MEASUREMENT((*codegen)->llvm_thread_counters());

  llvm::StringRef module_ir;
  string module_name = "Impala IR";
  if (FLAGS_llvm_ir_opt == "O1") {
    module_ir = llvm::StringRef(
        reinterpret_cast<const char*>(impala_llvm_o1_ir), impala_llvm_o1_ir_len);
  } else if (FLAGS_llvm_ir_opt == "O2") {
    module_ir = llvm::StringRef(
        reinterpret_cast<const char*>(impala_llvm_o2_ir), impala_llvm_o2_ir_len);
  } else if (FLAGS_llvm_ir_opt == "Os") {
    module_ir = llvm::StringRef(
        reinterpret_cast<const char*>(impala_llvm_os_ir), impala_llvm_os_ir_len);
  } else {
    CHECK(false) << "llvm_ir_opt flag invalid; try O1, O2, or Os.";
  }
#if __x86_64__
  // By default, Impala now requires AVX2 support, but the enable_legacy_avx_support
  // flag can allow running on AVX machines. The minimum requirement must have already
  // been enforced prior to this call, so this only needs to select the appropriate
  // LLVM IR to use.
  if (IsCPUFeatureEnabled(CpuInfo::AVX2)) {
    // Use the default IR that supports AVX2
    module_name = "Impala IR with AVX2 support";
  } else if (FLAGS_enable_legacy_avx_support && IsCPUFeatureEnabled(CpuInfo::AVX)) {
    // If there is no AVX but legacy mode is enabled, use legacy IR with AVX support
    module_ir = llvm::StringRef(
        reinterpret_cast<const char*>(impala_legacy_avx_llvm_ir),
        impala_legacy_avx_llvm_ir_len);
    module_name = "Legacy Impala IR with AVX support";
  } else {
    // This should have been enforced earlier.
    CHECK(false) << "CPU is missing AVX/AVX2 support";
  }
#endif

  unique_ptr<llvm::MemoryBuffer> module_ir_buf(
      llvm::MemoryBuffer::getMemBuffer(module_ir, "", false));
  unique_ptr<llvm::Module> loaded_module;
  Status status = (*codegen)->LoadModuleFromMemory(move(module_ir_buf),
      module_name, &loaded_module);
  if (!status.ok()) goto error;
  status = (*codegen)->Init(move(loaded_module));
  if (!status.ok()) goto error;
  return Status::OK();
error:
  (*codegen)->Close();
  return status;
}

Status LlvmCodeGen::LoadModuleFromFile(
    const string& file, unique_ptr<llvm::Module>* module) {
  unique_ptr<llvm::MemoryBuffer> file_buffer;
  {
    SCOPED_TIMER(load_module_timer_);

    llvm::ErrorOr<unique_ptr<llvm::MemoryBuffer>> tmp_file_buffer =
        llvm::MemoryBuffer::getFile(file);
    if (!tmp_file_buffer) {
      stringstream ss;
      ss << "Could not load module " << file << ": "
         << tmp_file_buffer.getError().message();
      return Status(ss.str());
    }
    file_buffer = move(tmp_file_buffer.get());
  }

  COUNTER_ADD(module_bitcode_size_, file_buffer->getBufferSize());
  return LoadModuleFromMemory(move(file_buffer), file, module);
}

Status LlvmCodeGen::LoadModuleFromMemory(unique_ptr<llvm::MemoryBuffer> module_ir_buf,
    string module_name, unique_ptr<llvm::Module>* module) {
  DCHECK(!module_name.empty());
  COUNTER_ADD(module_bitcode_size_, module_ir_buf->getMemBufferRef().getBufferSize());
  llvm::Expected<unique_ptr<llvm::Module>> tmp_module =
      getOwningLazyBitcodeModule(move(module_ir_buf), context());
  if (llvm::Error err = tmp_module.takeError()) {
    string err_string;
    llvm::handleAllErrors(
        move(err), [&](llvm::ErrorInfoBase& eib) { err_string = eib.message(); });
    return Status(err_string);
  }

  *module = move(tmp_module.get());

  // We never run global constructors or destructors so let's strip them out for all
  // modules when we load them.
  StripGlobalCtorsDtors((*module).get());

  (*module)->setModuleIdentifier(module_name);
  return Status::OK();
}

// TODO: Create separate counters/timers (file size, load time) for each module linked
Status LlvmCodeGen::LinkModuleFromLocalFs(const string& file) {
  unique_ptr<llvm::Module> new_module;
  RETURN_IF_ERROR(LoadModuleFromFile(file, &new_module));

  // The module data layout must match the one selected by the execution engine.
  new_module->setDataLayout(execution_engine()->getDataLayout());

  // Parse all functions' names from the new module and find those which also exist in
  // the main module. They are declarations in the new module or duplicated definitions
  // of the same function in both modules. For the latter case, it's unclear which one
  // the linker will choose. Materialize these functions in the main module in case they
  // are chosen by the linker or referenced by functions in the new module. Note that
  // linkModules() will materialize functions defined only in the new module.
  for (llvm::Function& fn : new_module->functions()) {
    const string& fn_name = fn.getName();
    if (shared_call_graph_.GetCallees(fn_name) != nullptr) {
      llvm::Function* local_fn = module_->getFunction(fn_name);
      RETURN_IF_ERROR(MaterializeFunction(local_fn));
    }
  }

  bool error = llvm::Linker::linkModules(*module_, move(new_module));
  string diagnostic_err = diagnostic_handler_.GetErrorString();
  if (error) {
    stringstream ss;
    ss << "Problem linking " << file << " to main module.";
    if (!diagnostic_err.empty()) ss << " " << diagnostic_err;
    return Status(ss.str());
  }
  return Status::OK();
}

Status LlvmCodeGen::LinkModuleFromHdfs(const string& hdfs_location, const time_t mtime) {
  if (linked_modules_.find(hdfs_location) != linked_modules_.end()) return Status::OK();
  LibCacheEntryHandle handle;
  string local_path;
  RETURN_IF_ERROR(LibCache::instance()->GetLocalPath(
      hdfs_location, LibCache::TYPE_IR, mtime, &handle, &local_path));
  RETURN_IF_ERROR(LinkModuleFromLocalFs(local_path));
  linked_modules_.insert(hdfs_location);
  return Status::OK();
}

void LlvmCodeGen::StripGlobalCtorsDtors(llvm::Module* module) {
  llvm::GlobalVariable* constructors = module->getGlobalVariable("llvm.global_ctors");
  if (constructors != NULL) constructors->eraseFromParent();
  llvm::GlobalVariable* destructors = module->getGlobalVariable("llvm.global_dtors");
  if (destructors != NULL) destructors->eraseFromParent();
}

Status LlvmCodeGen::CreateImpalaCodegen(FragmentState* state,
    MemTracker* parent_mem_tracker, const string& id,
    scoped_ptr<LlvmCodeGen>* codegen_ret) {
  DCHECK(state != nullptr);
  RETURN_IF_ERROR(CreateFromMemory(
      state, state->obj_pool(), parent_mem_tracker, id, codegen_ret));
  LlvmCodeGen* codegen = codegen_ret->get();

  // Parse module for cross compiled functions and types
  SCOPED_TIMER(codegen->profile_->total_time_counter());
  SCOPED_TIMER(codegen->prepare_module_timer_);
  SCOPED_THREAD_COUNTER_MEASUREMENT(codegen->llvm_thread_counters_);

  // Get type for StringValue
  codegen->string_value_type_ = codegen->GetStructType<StringValue>();

  // Get type for TimestampValue
  codegen->timestamp_value_type_ = codegen->GetStructType<TimestampValue>();

  // Get type for CollectionValue
  codegen->collection_value_type_ = codegen->GetStructType<CollectionValue>();

  // Verify size is correct
  const llvm::DataLayout& data_layout = codegen->execution_engine()->getDataLayout();
  const llvm::StructLayout* layout = data_layout.getStructLayout(
      static_cast<llvm::StructType*>(codegen->string_value_type_));
  if (layout->getSizeInBytes() != sizeof(StringValue)) {
    DCHECK_EQ(layout->getSizeInBytes(), sizeof(StringValue));
    return Status("Could not create llvm struct type for StringVal");
  }

  // Materialize functions referenced by the global variables.
  for (const string& fn_name : shared_call_graph_.fns_referenced_by_gv()) {
    llvm::Function* fn = codegen->module_->getFunction(fn_name);
    DCHECK(fn != nullptr);
    RETURN_IF_ERROR(codegen->MaterializeFunction(fn));
  }
  return Status::OK();
}

Status LlvmCodeGen::Init(unique_ptr<llvm::Module> module) {
  DCHECK(module != nullptr);

  llvm::CodeGenOpt::Level opt_level = llvm::CodeGenOpt::Aggressive;
#ifndef NDEBUG
  // For debug builds, don't generate JIT compiled optimized assembly.
  // This takes a non-neglible amount of time (~.5 ms per function) and
  // blows up the fe tests (which take ~10-20 ms each).
  opt_level = llvm::CodeGenOpt::None;
#endif
  module_ = module.get();
  llvm::EngineBuilder builder(move(module));
  builder.setEngineKind(llvm::EngineKind::JIT);
  builder.setOptLevel(opt_level);
  unique_ptr<ImpalaMCJITMemoryManager> memory_manager(new ImpalaMCJITMemoryManager);
  memory_manager_ = memory_manager.get();
  builder.setMCJITMemoryManager(move(memory_manager));
  builder.setMCPU(cpu_name_);
  builder.setMAttrs(cpu_attrs_);
  builder.setErrorStr(&error_string_);

  execution_engine_ = unique_ptr<llvm::ExecutionEngine>(builder.create());
  if (execution_engine_ == nullptr) {
    module_ = nullptr; // module_ was owned by builder.
    stringstream ss;
    ss << "Could not create ExecutionEngine: " << error_string_;
    return Status(ss.str());
  }

  // The module data layout must match the one selected by the execution engine.
  module_->setDataLayout(execution_engine_->getDataLayout());

  void_type_ = llvm::Type::getVoidTy(context());
  ptr_type_ = llvm::PointerType::get(i8_type(), 0);
  true_value_ = llvm::ConstantInt::get(context(), llvm::APInt(1, true, true));
  false_value_ = llvm::ConstantInt::get(context(), llvm::APInt(1, false, true));

  symbol_emitter_ = SetupSymbolEmitter(execution_engine_.get());
  engine_cache_ = make_shared<CodeGenObjectCache>();

  RETURN_IF_ERROR(LoadIntrinsics());

  return Status::OK();
}

unique_ptr<CodegenSymbolEmitter> LlvmCodeGen::SetupSymbolEmitter(
    llvm::ExecutionEngine* execution_engine) {
  bool need_symbol_emitter = !FLAGS_asm_module_dir.empty() || FLAGS_perf_map;
  if (!need_symbol_emitter) return nullptr;
  unique_ptr<CodegenSymbolEmitter> symbol_emitter =
      make_unique<CodegenSymbolEmitter>(id_);
  execution_engine->RegisterJITEventListener(symbol_emitter.get());
  symbol_emitter->set_emit_perf_map(FLAGS_perf_map);

  if (!FLAGS_asm_module_dir.empty()) {
    symbol_emitter->set_asm_path(Substitute("$0/$1.asm", FLAGS_asm_module_dir, id_));
  }

  return symbol_emitter;
}

LlvmCodeGen::~LlvmCodeGen() {
  DCHECK(execution_engine_ == nullptr) << "Must Close() before destruction";
}

void LlvmCodeGen::Close() {
  if (async_compile_thread_ != nullptr) async_compile_thread_->Join();

  if (memory_manager_ != nullptr) {
    mem_tracker_->Release(memory_manager_->bytes_tracked());
    memory_manager_ = nullptr;
  }
  if (mem_tracker_ != nullptr) mem_tracker_->Close();
  engine_cache_.reset();
  engine_cache_cached_.reset();
  execution_engine_.reset();
  symbol_emitter_.reset();
  module_ = nullptr;
}

void LlvmCodeGen::EnableOptimizations(bool enable) {
  optimizations_enabled_ = enable;
}

void LlvmCodeGen::GetHostCPUAttrs(std::unordered_set<string>* attrs) {
  // LLVM's ExecutionEngine expects features to be enabled or disabled with a list
  // of strings like ["+feature1", "-feature2"].
  llvm::StringMap<bool> cpu_features;
  llvm::sys::getHostCPUFeatures(cpu_features);
  for (const llvm::StringMapEntry<bool>& entry : cpu_features) {
    attrs->emplace(Substitute("$0$1", entry.second ? "+" : "-", entry.first().data()));
  }
}

bool LlvmCodeGen::IsCPUFeatureEnabled(int64_t flag) {
  DCHECK(llvm_initialized_);
  auto enable_flag_it = cpu_flag_mappings_.find(flag);
  DCHECK(enable_flag_it != cpu_flag_mappings_.end());
  const std::string& enabled_feature = enable_flag_it->second;
  return cpu_attrs_.find(enabled_feature) != cpu_attrs_.end();
}

string LlvmCodeGen::GetIR(bool full_module) const {
  string str;
  llvm::raw_string_ostream stream(str);
  if (full_module) {
    module_->print(stream, NULL);
  } else {
    for (int i = 0; i < handcrafted_functions_.size(); ++i) {
      handcrafted_functions_[i]->print(stream, nullptr, false, true);
    }
  }
  return str;
}

llvm::Type* LlvmCodeGen::GetSlotType(const ColumnType& type) {
  switch (type.type) {
    case TYPE_NULL:
      return llvm::Type::getInt1Ty(context());
    case TYPE_BOOLEAN:
      return bool_type();
    case TYPE_TINYINT:
      return i8_type();
    case TYPE_SMALLINT:
      return i16_type();
    case TYPE_INT:
      return i32_type();
    case TYPE_BIGINT:
      return i64_type();
    case TYPE_FLOAT:
      return float_type();
    case TYPE_DOUBLE:
      return double_type();
    case TYPE_STRING:
    case TYPE_VARCHAR:
      return string_value_type_;
    case TYPE_CHAR:
    case TYPE_FIXED_UDA_INTERMEDIATE:
      // Represent this as an array of bytes.
      return llvm::ArrayType::get(i8_type(), type.len);
    case TYPE_TIMESTAMP:
      return timestamp_value_type_;
    case TYPE_DECIMAL:
      return llvm::Type::getIntNTy(context(), type.GetByteSize() * 8);
    case TYPE_DATE:
      return i32_type();
    case TYPE_ARRAY:
    case TYPE_MAP:
      return collection_value_type_;
    default:
      DCHECK(false) << "Invalid type: " << type;
      return NULL;
  }
}

llvm::PointerType* LlvmCodeGen::GetSlotPtrType(const ColumnType& type) {
  return llvm::PointerType::get(GetSlotType(type), 0);
}

llvm::Type* LlvmCodeGen::GetNamedType(const string& name) {
  llvm::Type* type = module_->getTypeByName(name);
  DCHECK(type != NULL) << name;
  return type;
}

llvm::PointerType* LlvmCodeGen::GetNamedPtrType(const string& name) {
  llvm::Type* type = GetNamedType(name);
  DCHECK(type != NULL) << name;
  return llvm::PointerType::get(type, 0);
}

llvm::PointerType* LlvmCodeGen::GetPtrType(llvm::Type* type) {
  return llvm::PointerType::get(type, 0);
}

llvm::PointerType* LlvmCodeGen::GetPtrPtrType(llvm::Type* type) {
  return llvm::PointerType::get(llvm::PointerType::get(type, 0), 0);
}

llvm::PointerType* LlvmCodeGen::GetNamedPtrPtrType(const string& name) {
  return llvm::PointerType::get(GetNamedPtrType(name), 0);
}

llvm::Constant* LlvmCodeGen::GetIntConstant(
    int num_bytes, uint64_t low_bits, uint64_t high_bits) {
  DCHECK_GE(num_bytes, 1);
  DCHECK_LE(num_bytes, 16);
  DCHECK(BitUtil::IsPowerOf2(num_bytes));
  vector<uint64_t> vals({low_bits, high_bits});
  return llvm::ConstantInt::get(context(), llvm::APInt(8 * num_bytes, vals));
}

llvm::Value* LlvmCodeGen::GetStringConstant(
    LlvmBuilder* builder, const char* data, int len) {
  // Create a global string with private linkage.
  llvm::Constant* const_string =
      llvm::ConstantDataArray::getString(context(), llvm::StringRef(data, len), false);
  llvm::GlobalVariable* gv = new llvm::GlobalVariable(*module_, const_string->getType(),
      true, llvm::GlobalValue::PrivateLinkage, const_string);
  // Get a pointer to the first element of the string.
  return builder->CreateConstInBoundsGEP2_32(NULL, gv, 0, 0, "");
}

llvm::AllocaInst* LlvmCodeGen::CreateEntryBlockAlloca(
    llvm::Function* f, const NamedVariable& var) {
  llvm::IRBuilder<> tmp(&f->getEntryBlock(), f->getEntryBlock().begin());
  llvm::AllocaInst* alloca = tmp.CreateAlloca(var.type, NULL, var.name.c_str());
  if (var.type == GetNamedType(CodegenAnyVal::LLVM_DECIMALVAL_NAME)) {
    // Generated functions may manipulate DecimalVal arguments via SIMD instructions such
    // as 'movaps' that require 16-byte memory alignment. LLVM uses 8-byte alignment by
    // default, so explicitly set the alignment for DecimalVals.
    alloca->setAlignment(16);
  }
  return alloca;
}

llvm::AllocaInst* LlvmCodeGen::CreateEntryBlockAlloca(
    const LlvmBuilder& builder, llvm::Type* type, const char* name) {
  return CreateEntryBlockAlloca(
      builder.GetInsertBlock()->getParent(), NamedVariable(name, type));
}

llvm::AllocaInst* LlvmCodeGen::CreateEntryBlockAlloca(const LlvmBuilder& builder,
    llvm::Type* type, int num_entries, int alignment, const char* name) {
  llvm::Function* fn = builder.GetInsertBlock()->getParent();
  llvm::IRBuilder<> tmp(&fn->getEntryBlock(), fn->getEntryBlock().begin());
  llvm::AllocaInst* alloca =
      tmp.CreateAlloca(type, GetI32Constant(num_entries), name);
  alloca->setAlignment(alignment);
  return alloca;
}

void LlvmCodeGen::CreateIfElseBlocks(llvm::Function* fn, const string& if_name,
    const string& else_name, llvm::BasicBlock** if_block, llvm::BasicBlock** else_block,
    llvm::BasicBlock* insert_before) {
  *if_block = llvm::BasicBlock::Create(context(), if_name, fn, insert_before);
  *else_block = llvm::BasicBlock::Create(context(), else_name, fn, insert_before);
}

llvm::PHINode* LlvmCodeGen::CreateBinaryPhiNode(LlvmBuilder* builder, llvm::Value* value1,
    llvm::Value* value2, llvm::BasicBlock* incoming_block1,
    llvm::BasicBlock* incoming_block2, std::string name) {
  std::string node_name = name == "" ? (value1->getName().str() + "_phi") : name;

  llvm::PHINode* res = builder->CreatePHI(value1->getType(), 2, node_name);

  res->addIncoming(value1, incoming_block1);
  res->addIncoming(value2, incoming_block2);

  return res;
}

Status LlvmCodeGen::MaterializeFunction(llvm::Function* fn) {
  DCHECK(!is_compiled_);
  if (fn->isIntrinsic() || !fn->isMaterializable()) return Status::OK();

  llvm::Error err = module_->materialize(fn);
  if (UNLIKELY(err)) {
    string err_string;
    llvm::handleAllErrors(
        move(err), [&](llvm::ErrorInfoBase& eib) { err_string = eib.message(); });
    return Status(Substitute("Failed to materialize $0: $1",
        fn->getName().str(), err_string));
  }

  // Materialized functions are marked as not materializable by LLVM.
  DCHECK(!fn->isMaterializable());
  SetCPUAttrs(fn);
  const unordered_set<string>* callees = shared_call_graph_.GetCallees(fn->getName());
  if (callees != nullptr) {
    for (const string& callee : *callees) {
      llvm::Function* callee_fn = module_->getFunction(callee);
      DCHECK(callee_fn != nullptr);
      RETURN_IF_ERROR(MaterializeFunction(callee_fn));
    }
  }
  return Status::OK();
}

llvm::Function* LlvmCodeGen::GetFunction(const string& symbol, bool clone) {
  llvm::Function* fn = module_->getFunction(symbol.c_str());
  if (fn == NULL) {
    LOG(ERROR) << "Unable to locate function " << symbol;
    return NULL;
  }
  Status status = MaterializeFunction(fn);
  if (UNLIKELY(!status.ok())) return NULL;
  if (clone) return CloneFunction(fn);
  return fn;
}

llvm::Function* LlvmCodeGen::GetFunction(IRFunction::Type ir_type, bool clone) {
  llvm::Function* fn = cross_compiled_functions_[ir_type];
  if (fn == NULL) {
    DCHECK_EQ(FN_MAPPINGS[ir_type].fn, ir_type);
    const string& fn_name = FN_MAPPINGS[ir_type].fn_name;
    fn = module_->getFunction(fn_name);
    if (fn == NULL) {
      LOG(ERROR) << "Unable to locate function " << fn_name;
      return NULL;
    }
    cross_compiled_functions_[ir_type] = fn;
  }
  Status status = MaterializeFunction(fn);
  if (UNLIKELY(!status.ok())) return NULL;
  if (clone) return CloneFunction(fn);
  return fn;
}

// TODO: this should return a Status
bool LlvmCodeGen::VerifyFunction(llvm::Function* fn) {
  if (is_corrupt_) return false;

  // Check that there are no calls to FunctionContextImpl::GetConstFnAttr(). These should all
  // have been inlined via InlineConstFnAttrs().
  for (llvm::inst_iterator iter = inst_begin(fn); iter != inst_end(fn); ++iter) {
    llvm::Instruction* instr = &*iter;
    if (!llvm::isa<llvm::CallInst>(instr)) continue;
    llvm::CallInst* call_instr = reinterpret_cast<llvm::CallInst*>(instr);
    llvm::Function* called_fn = call_instr->getCalledFunction();

    // Look for call to FunctionContextImpl::GetConstFnAttr().
    if (called_fn != nullptr &&
        called_fn->getName() == FunctionContextImpl::GET_CONST_FN_ATTR_SYMBOL) {
      LOG(ERROR) << "Found call to FunctionContextImpl::GetConstFnAttr(): "
                 << Print(call_instr);
      is_corrupt_ = true;
      break;
    }
  }

  // There is an llvm bug (#10957) that causes the first step of the verifier to always
  // abort the process if it runs into an issue and ignores ReturnStatusAction.  This
  // would cause impalad to go down if one query has a problem.  To work around this, we
  // will copy that step here and not abort on error. Adapted from the pre-verifier
  // function pass.
  // TODO: doesn't seem there is much traction in getting this fixed but we'll see
  for (llvm::Function::iterator i = fn->begin(), e = fn->end(); i != e; ++i) {
    if (i->empty() || !i->back().isTerminator()) {
      LOG(ERROR) << "Basic block must end with terminator: \n" << Print(&(*i));
      is_corrupt_ = true;
      break;
    }
  }

  if (!is_corrupt_) {
    string str;
    llvm::raw_string_ostream stream(str);
    is_corrupt_ = verifyFunction(*fn, &stream);
    if (is_corrupt_) LOG(ERROR) << str;
  }

  if (is_corrupt_) {
    string fn_name = fn->getName(); // llvm has some fancy operator overloading
    LOG(ERROR) << "Function corrupt: " << fn_name <<"\nFunction Dump: "
        << LlvmCodeGen::Print(fn);
    return false;
  }
  return true;
}

void LlvmCodeGen::SetNoInline(llvm::Function* function) const {
  function->removeFnAttr(llvm::Attribute::AlwaysInline);
  function->removeFnAttr(llvm::Attribute::InlineHint);
  function->addFnAttr(llvm::Attribute::NoInline);
}

void LlvmCodeGen::SetCPUAttrs(llvm::Function* function) {
  // Set all functions' "target-cpu" and "target-features" to match the
  // host's CPU's features. It's assumed that the functions don't use CPU
  // features which the host doesn't support. CreateFromMemory() checks
  // the features of the host's CPU and loads the module compatible with
  // the host's CPU.
  function->addFnAttr("target-cpu", cpu_name_);
  function->addFnAttr("target-features", target_features_attr_);
}

LlvmCodeGen::FnPrototype::FnPrototype(
    LlvmCodeGen* codegen, const string& name, llvm::Type* ret_type)
  : codegen_(codegen), name_(name), ret_type_(ret_type) {
  DCHECK(!codegen_->is_compiled_) << "Not valid to add additional functions";
}

llvm::Function* LlvmCodeGen::FnPrototype::GeneratePrototype(
    LlvmBuilder* builder, llvm::Value** params) {
  vector<llvm::Type*> arguments;
  arguments.reserve(args_.size());
  for (int i = 0; i < args_.size(); ++i) {
    arguments.push_back(args_[i].type);
  }
  llvm::FunctionType* prototype = llvm::FunctionType::get(ret_type_, arguments, false);

  llvm::Function* fn = llvm::Function::Create(
      prototype, llvm::GlobalValue::ExternalLinkage, name_, codegen_->module_);
  DCHECK(fn != NULL);

  // Name the arguments
  int idx = 0;
  for (llvm::Function::arg_iterator iter = fn->arg_begin(); iter != fn->arg_end();
       ++iter, ++idx) {
    iter->setName(args_[idx].name);
    if (params != NULL) params[idx] = &*iter;
  }

  if (builder != NULL) {
    llvm::BasicBlock* entry_block =
        llvm::BasicBlock::Create(codegen_->context(), "entry", fn);
    // Add it to the llvm module via the builder and to the list of handcrafted functions
    // that are a part of the module.
    builder->SetInsertPoint(entry_block);
    codegen_->handcrafted_functions_.push_back(fn);
  }
  return fn;
}

Status LlvmCodeGen::LoadFunction(const TFunction& fn, const string& symbol,
    const ColumnType* return_type, const vector<ColumnType>& arg_types,
    int num_fixed_args, bool has_varargs, llvm::Function** llvm_fn,
    LibCacheEntry** cache_entry) {
  DCHECK_GE(arg_types.size(), num_fixed_args);
  DCHECK(has_varargs || arg_types.size() == num_fixed_args);
  DCHECK(!has_varargs || arg_types.size() > num_fixed_args);
  // from_utc_timestamp() and to_utc_timestamp() have inline ASM that cannot be JIT'd.
  // TimestampFunctions::AddSub() contains a try/catch which doesn't work in JIT'd
  // code. Always use the interpreted version of these functions.
  // TODO: fix these built-in functions so we don't need 'broken_builtin' below.
  bool broken_builtin = fn.name.function_name == "from_utc_timestamp"
      || fn.name.function_name == "to_utc_timestamp"
      || symbol.find("AddSub") != string::npos;
  if (fn.binary_type == TFunctionBinaryType::NATIVE
      || (fn.binary_type == TFunctionBinaryType::BUILTIN && broken_builtin)) {
    // In this path, we are calling a precompiled native function, either a UDF
    // in a .so or a builtin using the UDF interface.
    void* fn_ptr;
    Status status = LibCache::instance()->GetSoFunctionPtr(
        fn.hdfs_location, symbol, fn.last_modified_time, &fn_ptr, cache_entry);
    if (!status.ok() && fn.binary_type == TFunctionBinaryType::BUILTIN) {
      // Builtins symbols should exist unless there is a version mismatch.
      status.AddDetail(
          ErrorMsg(TErrorCode::MISSING_BUILTIN, fn.name.function_name, symbol).msg());
    }
    RETURN_IF_ERROR(status);
    DCHECK(fn_ptr != NULL);

    // Per the x64 ABI, DecimalVals are returned via a DecimalVal* output argument.
    // So, the return type is void.
    bool is_decimal = return_type != NULL && return_type->type == TYPE_DECIMAL;
    llvm::Type* llvm_return_type = return_type == NULL || is_decimal ?
        void_type() :
        CodegenAnyVal::GetLoweredType(this, *return_type);

    // Convert UDF function pointer to Function*. Start by creating a function
    // prototype for it.
    FnPrototype prototype(this, symbol, llvm_return_type);

    if (is_decimal) {
      // Per the x64 ABI, DecimalVals are returned via a DecmialVal* output argument
      llvm::Type* output_type = CodegenAnyVal::GetUnloweredPtrType(this, *return_type);
      prototype.AddArgument("output", output_type);
    }

    // The "FunctionContext*" argument.
    prototype.AddArgument("ctx", GetNamedPtrType("class.impala_udf::FunctionContext"));

    // The "fixed" arguments for the UDF function, followed by the variable arguments,
    // if any.
    for (int i = 0; i < num_fixed_args; ++i) {
      llvm::Type* arg_type = CodegenAnyVal::GetUnloweredPtrType(this, arg_types[i]);
      prototype.AddArgument(Substitute("fixed_arg_$0", i), arg_type);
    }

    if (has_varargs) {
      prototype.AddArgument("num_var_arg", i32_type());
      // Get the vararg type from the first vararg.
      prototype.AddArgument(
          "var_arg", CodegenAnyVal::GetUnloweredPtrType(this, arg_types[num_fixed_args]));
    }

    // Create a Function* with the generated type. This is only a function
    // declaration, not a definition, since we do not create any basic blocks or
    // instructions in it.
    *llvm_fn = prototype.GeneratePrototype(nullptr, nullptr);
#ifdef __aarch64__
    if (is_decimal) {
      // Mark first argument as sret
      (*llvm_fn)->addAttribute(1, llvm::Attribute::StructRet);
    }
#endif
    // Associate the dynamically loaded function pointer with the Function* we defined.
    // This tells LLVM where the compiled function definition is located in memory.
    execution_engine()->addGlobalMapping(*llvm_fn, fn_ptr);
    // Disable the codegen cache because codegen cache uses the llvm module bitcode as
    // the key while the bitcode doesn't contain the global function mapping of the
    // execution engine. If the mapping is changed during running, like udf recreation,
    // the function mapping in the cache could point to an old address and lead to a
    // crash while calling the udf,  so block the codegen cache for native udfs.
    // Builtin functions should not have the issue, because they should not change
    // during runtime.
    if (fn.binary_type == TFunctionBinaryType::NATIVE) {
      // Should be before compilation.
      DCHECK(!is_compiled_);
      codegen_cache_enabled_ = false;
    }
  } else if (fn.binary_type == TFunctionBinaryType::BUILTIN) {
    // In this path, we're running a builtin with the UDF interface. The IR is
    // in the llvm module. Builtin functions may use Expr::GetConstant(). Clone the
    // function so that we can replace constants in the copied function.
    *llvm_fn = GetFunction(symbol, true);
    if (*llvm_fn == NULL) {
      // Builtins symbols should exist unless there is a version mismatch.
      return Status(Substitute("Builtin '$0' with symbol '$1' does not exist. Verify "
                               "that all your impalads are the same version.",
          fn.name.function_name, symbol));
    }
    // Rename the function to something more readable than the mangled name.
    string demangled_name = SymbolsUtil::DemangleNoArgs((*llvm_fn)->getName().str());
    (*llvm_fn)->setName(demangled_name);
  } else {
    // We're running an IR UDF.
    DCHECK_EQ(fn.binary_type, TFunctionBinaryType::IR);

    // Link the UDF module into this query's main module so the UDF's functions are
    // available in the main module.
    RETURN_IF_ERROR(LinkModuleFromHdfs(fn.hdfs_location, fn.last_modified_time));

    *llvm_fn = GetFunction(symbol, true);
    if (*llvm_fn == NULL) {
      return Status(Substitute("Unable to load function '$0' from LLVM module '$1'",
          symbol, fn.hdfs_location));
    }
    // Rename the function to something more readable than the mangled name.
    string demangled_name = SymbolsUtil::DemangleNoArgs((*llvm_fn)->getName().str());
    (*llvm_fn)->setName(demangled_name);
  }
  return Status::OK();
}

int LlvmCodeGen::ReplaceCallSites(
    llvm::Function* caller, llvm::Function* new_fn, const string& target_name) {
  DCHECK(!is_compiled_);
  DCHECK(caller->getParent() == module_);
  DCHECK(caller != NULL);
  DCHECK(new_fn != NULL);
  DCHECK(find(handcrafted_functions_.begin(), handcrafted_functions_.end(), new_fn)
          == handcrafted_functions_.end()
      || finalized_functions_.find(new_fn) != finalized_functions_.end());

  vector<llvm::CallInst*> call_sites;
  FindCallSites(caller, target_name, &call_sites);
  int replaced = 0;
  for (llvm::CallInst* call_instr : call_sites) {
    // Replace the called function
    call_instr->setCalledFunction(new_fn);
    ++replaced;
  }
  return replaced;
}

int LlvmCodeGen::ReplaceCallSitesWithValue(
    llvm::Function* caller, llvm::Value* replacement, const string& target_name) {
  DCHECK(!is_compiled_);
  DCHECK(caller->getParent() == module_);
  DCHECK(caller != NULL);
  DCHECK(replacement != NULL);

  vector<llvm::CallInst*> call_sites;
  FindCallSites(caller, target_name, &call_sites);
  int replaced = 0;
  for (llvm::CallInst* call_instr : call_sites) {
    call_instr->replaceAllUsesWith(replacement);
    ++replaced;
  }
  return replaced;
}

int LlvmCodeGen::ReplaceCallSitesWithBoolConst(llvm::Function* caller, bool constant,
    const string& target_name) {
  llvm::Value* replacement = GetBoolConstant(constant);
  return ReplaceCallSitesWithValue(caller, replacement, target_name);
}

int LlvmCodeGen::InlineConstFnAttrs(const FunctionContext::TypeDesc& ret_type,
    const vector<FunctionContext::TypeDesc>& arg_types, llvm::Function* fn) {
  int replaced = 0;
  for (llvm::inst_iterator iter = inst_begin(fn), end = inst_end(fn); iter != end;) {
    // Increment iter now so we don't mess it up modifying the instruction below
    llvm::Instruction* instr = &*(iter++);

    // Look for call instructions
    if (!llvm::isa<llvm::CallInst>(instr)) continue;
    llvm::CallInst* call_instr = llvm::cast<llvm::CallInst>(instr);
    llvm::Function* called_fn = call_instr->getCalledFunction();

    // Look for call to FunctionContextImpl::GetConstFnAttr().
    if (called_fn == nullptr ||
        called_fn->getName() != FunctionContextImpl::GET_CONST_FN_ATTR_SYMBOL) {
      continue;
    }

    // 't' and 'i' arguments must be constant
    llvm::ConstantInt* t_arg =
        llvm::dyn_cast<llvm::ConstantInt>(call_instr->getArgOperand(1));
    llvm::ConstantInt* i_arg =
        llvm::dyn_cast<llvm::ConstantInt>(call_instr->getArgOperand(2));
    // This optimization is only applied to built-ins which should have constant args.
    DCHECK(t_arg != nullptr)
        << "Non-constant 't' argument to FunctionContextImpl::GetConstFnAttr()";
    DCHECK(i_arg != nullptr)
        << "Non-constant 'i' argument to FunctionContextImpl::GetConstFnAttr";

    // Replace the called function with the appropriate constant
    FunctionContextImpl::ConstFnAttr t_val =
        static_cast<FunctionContextImpl::ConstFnAttr>(t_arg->getSExtValue());
    int i_val = static_cast<int>(i_arg->getSExtValue());
    DCHECK(state_ != nullptr);
    // All supported constants are currently integers.
    call_instr->replaceAllUsesWith(GetI32Constant(FunctionContextImpl::GetConstFnAttr(
        state_->query_options().decimal_v2, state_->query_options().utf8_mode, ret_type,
        arg_types, t_val, i_val)));
    call_instr->eraseFromParent();
    ++replaced;
  }
  return replaced;
}

void LlvmCodeGen::FindCallSites(
    llvm::Function* caller, const string& target_name, vector<llvm::CallInst*>* results) {
  for (llvm::inst_iterator iter = inst_begin(caller); iter != inst_end(caller); ++iter) {
    llvm::Instruction* instr = &*iter;
    // Look for call instructions. Note that we'll ignore invoke and other related
    // instructions that are not a plain function call.
    if (llvm::CallInst::classof(instr)) {
      llvm::CallInst* call_instr = reinterpret_cast<llvm::CallInst*>(instr);
      llvm::Function* callee = call_instr->getCalledFunction();
      // Check for substring match.
      if (callee != NULL && callee->getName().find(target_name) != string::npos) {
        results->push_back(call_instr);
      }
    }
  }
}

llvm::Function* LlvmCodeGen::CloneFunction(llvm::Function* fn) {
  DCHECK(!is_compiled_);
  llvm::ValueToValueMapTy dummy_vmap;
  // Verifies that 'fn' has been materialized already. Callers are expected to use
  // GetFunction() to obtain the Function object.
  DCHECK(!fn->isMaterializable());
  // CloneFunction() automatically gives the new function a unique name
  llvm::Function* fn_clone = llvm::CloneFunction(fn, dummy_vmap);
  fn_clone->copyAttributesFrom(fn);
  return fn_clone;
}

llvm::Function* LlvmCodeGen::FinalizeFunction(llvm::Function* function) {
  SetCPUAttrs(function);
  if (!VerifyFunction(function)) return NULL;
  finalized_functions_.insert(function);
  if (FLAGS_dump_ir) {
    string fn_name = function->getName();
    LOG(INFO) << "Dump of Function "<< fn_name << ": " << LlvmCodeGen::Print(function);
  }
  return function;
}

Status LlvmCodeGen::MaterializeModule() {
  llvm::Error err = module_->materializeAll();
  if (UNLIKELY(err)) {
    string err_string;
    llvm::handleAllErrors(
        move(err), [&](llvm::ErrorInfoBase& eib) { err_string = eib.message(); });
    return Status(Substitute("Failed to materialize module $0: $1",
        module_->getName().str(), err_string));
  }
  return Status::OK();
}

// It's okay to call this function even if the module has been materialized.
Status LlvmCodeGen::FinalizeLazyMaterialization() {
  for (llvm::Function& fn : module_->functions()) {
    if (fn.isMaterializable()) {
      DCHECK(!module_->isMaterialized());
      // Unmaterialized functions can still have their declarations around. LLVM asserts
      // these unmaterialized functions' linkage types are external / external weak.
      fn.setLinkage(llvm::Function::ExternalLinkage);
      // DCE may claim the personality function is still referenced by unmaterialized
      // functions when it is deleted by DCE. Similarly, LLVM may complain if comdats
      // reference unmaterialized functions but their definition cannot be found.
      // Since the unmaterialized functions are not used anyway, just remove their
      // personality functions and comdats.
      fn.setPersonalityFn(NULL);
      fn.setComdat(NULL);
      fn.setIsMaterializable(false);
    }
  }
  // All unused functions are now not materializable so it should be quick to call
  // materializeAll(). We need to call this function in order to destroy the
  // materializer so that DCE will not assert fail.
  return MaterializeModule();
}

bool LlvmCodeGen::LookupCache(CodeGenCacheKey& cache_key) {
  DCHECK(!cache_key.empty());
  CodeGenCacheEntry entry;
  CodeGenCache* cache = ExecEnv::GetInstance()->codegen_cache();
  DCHECK(cache != nullptr);
  Status lookup_status = cache->Lookup(cache_key,
      state_->query_options().codegen_cache_mode, &entry, &engine_cache_cached_);
  bool entry_exists = lookup_status.ok() && !entry.Empty();
  LOG(INFO) << DebugCacheEntryString(cache_key, true /*is_lookup*/,
      CodeGenCacheModeAnalyzer::is_debug(state_->query_options().codegen_cache_mode),
      entry_exists);
  if (entry_exists) {
    // Fallback to normal procedure if function names hashcode is not expected.
    // The names hashcode should be the same unless there is a collision on the
    // key, we expect this case is very rare.
    if (function_names_hashcode_ != entry.function_names_hashcode) {
      LOG(WARNING)
          << "The codegen cache entry contains a different function names hashcode: "
          << " function names hashcode expected: " << function_names_hashcode_
          << " actual: " << entry.function_names_hashcode
          << " key hash_code=" << cache_key.hash_code();
      cache->IncHitOrMissCount(/*hit*/ false);
      return false;
    }

    if (entry.opt_level < state_->query_options().codegen_opt_level) {
      // Requested optimization level is higher than cached entry, so treat as a miss.
      VLOG(2) << "Overwriting codegen cache entry at " << entry.opt_level
          << " with optimization level " << state_->query_options().codegen_opt_level;
      cache->IncHitOrMissCount(/*hit*/ false);
      return false;
    }

    // Because we cache all the compiled codegened functions, the cached number of
    // functions should be the same as the total optimized function number.
    COUNTER_SET(num_cached_functions_, entry.num_opt_functions);
    COUNTER_SET(num_functions_, entry.num_functions);
    COUNTER_SET(num_instructions_, entry.num_instructions);
    COUNTER_SET(num_opt_functions_, entry.num_opt_functions);
    COUNTER_SET(num_opt_instructions_, entry.num_opt_instructions);
  }
  cache->IncHitOrMissCount(/*hit*/ entry_exists);
  return entry_exists;
}

string LlvmCodeGen::GetAllFunctionNames() {
  stringstream result;
  // The way to concat would be like "function1,function2".
  // The function names are sorted in 'fns_to_jit_compile_'.
  constexpr char separator = ',';
  for (auto& entry : fns_to_jit_compile_) {
    const llvm::StringRef& fn_name = entry.first;
    result << fn_name.data() << separator;
  }
  return result.str();
}

void LlvmCodeGen::GenerateFunctionNamesHashCode() {
  string function_names = GetAllFunctionNames();
  // Use the same hash seed as the codegen cache key.
  function_names_hashcode_ = HashUtil::MurmurHash2_64(function_names.c_str(),
      function_names.length(), CodeGenCacheKeyConstructor::CODEGEN_CACHE_HASH_SEED_CONST);
}

Status LlvmCodeGen::StoreCache(CodeGenCacheKey& cache_key) {
  DCHECK(!cache_key.empty());
  Status store_status = ExecEnv::GetInstance()->codegen_cache()->Store(
      cache_key, this, state_->query_options().codegen_cache_mode,
      state_->query_options().codegen_opt_level);
  LOG(INFO) << DebugCacheEntryString(cache_key, false /*is_lookup*/,
      CodeGenCacheModeAnalyzer::is_debug(state_->query_options().codegen_cache_mode),
      store_status.ok());
  return store_status;
}

void LlvmCodeGen::PruneModule() {
  SCOPED_TIMER(function_prune_timer_);
  // Before running any other optimization passes, run the internalize pass, giving it
  // the names of all functions registered by AddFunctionToJit(), followed by the
  // global dead code elimination pass. This causes all functions not registered to be
  // JIT'd to be marked as internal, and any internal functions that are not used are
  // deleted by DCE pass. This greatly decreases compile time by removing unused code.
  llvm::ModuleAnalysisManager module_analysis_manager;
  llvm::PassBuilder pass_builder(execution_engine()->getTargetMachine());
  pass_builder.registerModuleAnalyses(module_analysis_manager);

  llvm::ModulePassManager module_pass_manager;
  module_pass_manager.addPass(
      llvm::InternalizePass([this](const llvm::GlobalValue& gv) {
        return fns_to_jit_compile_.count(gv.getName()) > 0;
      }));
  module_pass_manager.addPass(llvm::GlobalDCEPass());
  module_pass_manager.run(*module_, module_analysis_manager);
}

Status LlvmCodeGen::FinalizeModule(string* module_id) {
  DCHECK(!is_compiled_);
  is_compiled_ = true;

  if (FLAGS_unopt_module_dir.size() != 0) {
    string path = FLAGS_unopt_module_dir + "/" + id_ + "_unopt.ll";
    fstream f(path.c_str(), fstream::out | fstream::trunc);
    if (f.fail()) {
      LOG(ERROR) << "Could not save IR to: " << path;
    } else {
      f << GetIR(true);
      f.close();
      LOG(INFO) << "Saved unoptimized IR to " << path;
    }
  }

  if (is_corrupt_) return Status("Module is corrupt.");
  SCOPED_TIMER(profile_->total_time_counter());
  SCOPED_THREAD_COUNTER_MEASUREMENT(llvm_thread_counters_);

  // Clean up handcrafted functions that have not been finalized. Clean up is done by
  // deleting the function from the module. Any reference to deleted functions in the
  // module will crash LLVM and thus Impala during finalization of the module.
  stringstream ss;
  for (llvm::Function* fn : handcrafted_functions_) {
    if (finalized_functions_.find(fn) == finalized_functions_.end()) {
      ss << fn->getName().str() << "\n";
      fn->eraseFromParent();
    }
  }
  string non_finalized_fns_str = ss.str();
  if (!non_finalized_fns_str.empty()) {
    LOG(INFO) << "For query " << PrintId(state_->query_id())
              << " the following functions were not finalized and have been removed from "
                 "the module:\n"
              << non_finalized_fns_str;
  }

  // Don't waste time optimizing module if there are no functions to JIT. This can happen
  // if the codegen object is created but no functions are successfully codegen'd.
  if (fns_to_jit_compile_.empty()) {
    DestroyModule();
    return Status::OK();
  }

  RETURN_IF_ERROR(FinalizeLazyMaterialization());
  PruneModule();

  bool codegen_cache_enabled = state_->codegen_cache_enabled() && codegen_cache_enabled_;
  CodeGenCacheKey cache_key;
  bool cache_hit = false;
  if (codegen_cache_enabled) {
    string bitcode;
    SCOPED_TIMER(codegen_cache_lookup_timer_);
    {
      SCOPED_TIMER(module_bitcode_gen_timer_);
      llvm::raw_string_ostream bitcode_stream(bitcode);
      llvm::WriteBitcodeToFile(module_, bitcode_stream);
      bitcode_stream.flush();
    }
    CodeGenCacheKeyConstructor::construct(bitcode, &cache_key);
    // Generate the function names hashcode no matter the look up result, will be used
    // in the cache store process if look up failed.
    GenerateFunctionNamesHashCode();
    DCHECK(!cache_key.empty());
    // Set the module id for the use of ObjectCache.
    module_->setModuleIdentifier(cache_key.hash_code().str());
    cache_hit = LookupCache(cache_key);
  }

  if (!cache_hit) {
    // Update counters before final optimization, but after removing unused functions.
    // This gives us a rough measure of how much work the optimization and compilation
    // must do. If found in cache, counters will be restored from the cache entry.
    InstructionCounter counter;
    counter.visit(*module_);
    COUNTER_SET(num_functions_, counter.GetCount(InstructionCounter::TOTAL_FUNCTIONS));
    COUNTER_SET(num_instructions_, counter.GetCount(InstructionCounter::TOTAL_INSTS));

    if (optimizations_enabled_ && !FLAGS_disable_optimization_passes) {
      RETURN_IF_ERROR(OptimizeModule());
      counter.ResetCount();
      counter.visit(*module_);
    }
    COUNTER_SET(
        num_opt_functions_, counter.GetCount(InstructionCounter::TOTAL_FUNCTIONS));
    COUNTER_SET(num_opt_instructions_, counter.GetCount(InstructionCounter::TOTAL_INSTS));
  }

  if (FLAGS_opt_module_dir.size() != 0) {
    string path = FLAGS_opt_module_dir + "/" + id_ + "_opt.ll";
    fstream f(path.c_str(), fstream::out | fstream::trunc);
    if (f.fail()) {
      LOG(ERROR) << "Could not save IR to: " << path;
    } else {
      f << GetIR(true);
      f.close();
      LOG(INFO) << "Saved optimized IR to " << path;
    }
  }

  if (codegen_cache_enabled) {
    if (cache_hit) {
      DCHECK(engine_cache_cached_ != nullptr);
      execution_engine()->setObjectCache(engine_cache_cached_.get());
    } else {
      execution_engine()->setObjectCache(engine_cache_.get());
    }
  }
  {
    SCOPED_TIMER(compile_timer_);
    // Finalize module, which compiles all functions.
    execution_engine()->finalizeObject();
  }
  SetFunctionPointers();
  Status store_cache_status;
  if (codegen_cache_enabled && !cache_hit) {
    SCOPED_TIMER(codegen_cache_save_timer_);
    store_cache_status = StoreCache(cache_key);
  }

  // Track the memory consumed by the runtime compiled code.
  // If codegen cache is enabled, the part stored to the cache will be taken care by
  // codegen cache to track the memory consumption.
  int64_t bytes_allocated = memory_manager_->bytes_allocated();
  if (!mem_tracker_->TryConsume(bytes_allocated)) {
    const string& msg = Substitute(
        "Failed to allocate '$0' bytes for compiled code module", bytes_allocated);
    return mem_tracker_->MemLimitExceeded(NULL, msg, bytes_allocated);
  }
  memory_manager_->set_bytes_tracked(bytes_allocated);

  // Get the module id before module destruction.
  if (module_id != nullptr) *module_id = module_->getModuleIdentifier();
  DestroyModule();
  return Status::OK();
}

Status LlvmCodeGen::FinalizeModuleAsync(RuntimeProfile::EventSequence* event_sequence) {
  DCHECK(event_sequence != nullptr);
  Status thread_start_status = Thread::Create("async-codegen", "async-codegen",
      [this, event_sequence]() {
        SCOPED_THREAD_COUNTER_MEASUREMENT(compile_thread_counters_);
        VLOG(2) << "Starting async code generation.";

        Status status = DebugAction(state_->query_options(),
            "BEFORE_CODEGEN_IN_ASYNC_CODEGEN_THREAD");
        if (status.ok()) {
          status = this->FinalizeModule();
        }

        const std::string status_msg = status.ok() ? "OK." : status.msg().msg();
        auto log_level = status.ok() ? 2 : 1;
        event_sequence->MarkEvent("AsyncCodegenFinished");
        VLOG(log_level) << "Finished async code generation with result: " << status;
      }, &async_compile_thread_);

  event_sequence->MarkEvent("AsyncCodegenStarted");

  RETURN_IF_ERROR(DebugAction(state_->query_options(),
        "AFTER_STARTING_ASYNC_CODEGEN_IN_FRAGMENT_THREAD"));
  return thread_start_status;
}

/// TODO: In asynchronous mode, return early if the query is cancelled or finished.
Status LlvmCodeGen::OptimizeModule() {
  SCOPED_TIMER(optimization_timer_);

  llvm::LoopAnalysisManager LAM;
  llvm::FunctionAnalysisManager FAM;
  llvm::CGSCCAnalysisManager CGAM;
  llvm::ModuleAnalysisManager MAM;

  // This pass manager will construct optimizations passes that are "typical" for
  // c/c++ programs.  We're relying on llvm to pick the best passes for us.
  // TODO: we can likely muck with this to get better compile speeds or write
  // our own passes.  Our subexpression elimination optimization can be rolled into
  // a pass.
  llvm::PassBuilder pass_builder(execution_engine()->getTargetMachine());
  pass_builder.registerModuleAnalyses(MAM);
  pass_builder.registerCGSCCAnalyses(CGAM);
  pass_builder.registerFunctionAnalyses(FAM);
  pass_builder.registerLoopAnalyses(LAM);
  pass_builder.crossRegisterProxies(LAM, FAM, CGAM, MAM);

  TCodeGenOptLevel::type opt_level = state_->query_options().codegen_opt_level;
  llvm::PassBuilder::OptimizationLevel opt;
  // GCC's -Werror=switch errors if a case is not covered.
  switch (opt_level) {
    case TCodeGenOptLevel::O0:
      // Default optimization pipeline requires O1 or greater, so for O0 we skip.
      return Status::OK();
    case TCodeGenOptLevel::O1:
      opt = llvm::PassBuilder::OptimizationLevel::O1;
      break;
    case TCodeGenOptLevel::Os:
      opt = llvm::PassBuilder::OptimizationLevel::Os;
      break;
    case TCodeGenOptLevel::O2:
      opt = llvm::PassBuilder::OptimizationLevel::O2;
      break;
    case TCodeGenOptLevel::O3:
      opt = llvm::PassBuilder::OptimizationLevel::O3;
      break;
  }
  llvm::ModulePassManager pass_manager = pass_builder.buildPerModuleDefaultPipeline(opt);

  int64_t estimated_memory = ESTIMATED_OPTIMIZER_BYTES_PER_INST
      * num_instructions_->value();
  if (!mem_tracker_->TryConsume(estimated_memory)) {
    const string& msg = Substitute(
        "Codegen failed to reserve '$0' bytes for optimization", estimated_memory);
    return mem_tracker_->MemLimitExceeded(NULL, msg, estimated_memory);
  }

  // Create and run module pass manager
  pass_manager.run(*module_, MAM);
  if (FLAGS_print_llvm_ir_instruction_count) {
    for (auto& entry : fns_to_jit_compile_) {
      InstructionCounter counter;
      llvm::Function* llvm_function = entry.second.first;
      const llvm::StringRef& llvm_function_name = entry.first;
      counter.visit(*llvm_function);
      VLOG(1) << llvm_function_name.data();
      VLOG(1) << counter.PrintCounters();
    }
  }

  mem_tracker_->Release(estimated_memory);
  return Status::OK();
}

bool LlvmCodeGen::SetFunctionPointers(CodeGenCache* cache,
    const CodeGenCacheKey* cache_key) {
  // Get pointers to all codegen'd functions.
  for (auto& entry : fns_to_jit_compile_) {
    const llvm::StringRef& function_name = entry.first;

    LlvmFunctionWithFnPtrTargets& fn_with_targets = entry.second;
    llvm::Function* function = fn_with_targets.first;
    std::vector<CodegenFnPtrBase*>& jitted_fn_ptrs = fn_with_targets.second;

    void* jitted_function = nullptr;
    if (cache != nullptr) {
      DCHECK(cache_key != nullptr);
      // engine_cache_cached_ is used to keep the life of the object cache
      // in case the object cache is evicted in the global cache.
      DCHECK(engine_cache_cached_ != nullptr);
      // Using the function getFunctionAddress() with a non-existent function name would
      // hit an assertion during the test, could be a bug in llvm 5, need to review after
      // upgrade llvm. But because we already checked the names hashcode for key collision
      // cases, we expect all the functions should be in the cached execution engine.
      jitted_function =
          reinterpret_cast<void*>(execution_engine()->getFunctionAddress(function_name));
      if (jitted_function == nullptr) {
        LOG(WARNING) << "Failed to get a jitted function from cache: "
                     << function_name.data()
                     << " key hash_code=" << cache_key->hash_code();
        cache->IncHitOrMissCount(/*hit*/ false);
        return false;
      }
    } else {
      DCHECK(cache_key == nullptr);
      jitted_function = execution_engine()->getPointerToFunction(function);
      DCHECK(jitted_function != nullptr) << "Failed to jit " << function_name.data();
    }

    DCHECK(jitted_function != nullptr);
    for (CodegenFnPtrBase* jitted_fn_ptr : jitted_fn_ptrs) {
      jitted_fn_ptr->store(jitted_function);
    }
  }

  return true;
}

void LlvmCodeGen::DestroyModule() {
  // Clear all references to LLVM objects owned by the module.
  cross_compiled_functions_.clear();
  handcrafted_functions_.clear();
  registered_exprs_map_.clear();
  registered_exprs_.clear();
  llvm_intrinsics_.clear();
  hash_fns_.clear();
  fns_to_jit_compile_.clear();
  execution_engine()->removeModule(module_);
  module_ = NULL;
}

void LlvmCodeGen::AddFunctionToJit(llvm::Function* fn, CodegenFnPtrBase* fn_ptr) {
  DCHECK(finalized_functions_.find(fn) != finalized_functions_.end())
      << "Attempted to add a non-finalized function to Jit: " << fn->getName().str();
  DCHECK(!is_compiled_);
  llvm::Type* decimal_val_type = GetNamedType(CodegenAnyVal::LLVM_DECIMALVAL_NAME);
  if (fn->getReturnType() == decimal_val_type) {
    // Per the x86 calling convention ABI, DecimalVals should be returned via an extra
    // first DecimalVal* argument. We generate non-compliant functions that return the
    // DecimalVal directly, which we can call from generated code, but not from compiled
    // native code.  To avoid accidentally calling a non-compliant function from native
    // code, call 'function' from an ABI-compliant wrapper.
    stringstream name;
    name << fn->getName().str() << "ABIWrapper";
    LlvmCodeGen::FnPrototype prototype(this, name.str(), void_type_);
    // Add return argument
    prototype.AddArgument(NamedVariable("result", decimal_val_type->getPointerTo()));
    // Add regular arguments
    for (llvm::Function::arg_iterator arg = fn->arg_begin(); arg != fn->arg_end();
         ++arg) {
      prototype.AddArgument(NamedVariable(arg->getName(), arg->getType()));
    }
    LlvmBuilder builder(context());
    llvm::Value* args[fn->arg_size() + 1];
    llvm::Function* fn_wrapper = prototype.GeneratePrototype(&builder, &args[0]);
    fn_wrapper->addFnAttr(llvm::Attribute::AlwaysInline);
    // Mark first argument as sret (not sure if this is necessary but it can't hurt)
    fn_wrapper->addAttribute(1, llvm::Attribute::StructRet);
    // Call 'fn' and store the result in the result argument
    llvm::Value* result = builder.CreateCall(
        fn, llvm::ArrayRef<llvm::Value*>({&args[1], fn->arg_size()}), "result");
    builder.CreateStore(result, args[0]);
    builder.CreateRetVoid();
    fn = FinalizeFunction(fn_wrapper);
    DCHECK(fn != NULL);
  }

  AddFunctionToJitInternal(fn, fn_ptr);
}

void LlvmCodeGen::AddFunctionToJitInternal(llvm::Function* fn, CodegenFnPtrBase* fn_ptr) {
  DCHECK(fn != nullptr);
  DCHECK(fn_ptr != nullptr);
  const llvm::StringRef& fn_name = fn->getName();

  auto it = fns_to_jit_compile_.find(fn_name);
  if (it == fns_to_jit_compile_.end()) {
    fns_to_jit_compile_[fn_name] = make_pair(fn, vector<CodegenFnPtrBase*>{fn_ptr});
  } else {
    it->second.second.push_back(fn_ptr);
  }
}

void LlvmCodeGen::CodegenDebugTrace(
    LlvmBuilder* builder, const char* str, llvm::Value* v1) {
  LOG(ERROR) << "Remove IR codegen debug traces before checking in.";

  // Call printf by embedding the string into the module and getting a pointer to it.
  llvm::Value* const llvm_str =
      GetStringConstant(builder, Substitute("LLVM Trace: $0", str));

  llvm::Function* printf = module_->getFunction("printf");
  DCHECK(printf != nullptr);

  vector<llvm::Value*> calling_args;
  calling_args.push_back(llvm_str);
  if (v1 != NULL) calling_args.push_back(v1);
  builder->CreateCall(printf, calling_args);
}

Status LlvmCodeGen::GetSymbols(const string& file, const string& module_id,
    unordered_set<string>* symbols) {
  ObjectPool pool;
  scoped_ptr<LlvmCodeGen> codegen;
  RETURN_IF_ERROR(CreateFromFile(nullptr, &pool, nullptr, file, module_id, &codegen));
  for (const llvm::Function& fn : codegen->module_->functions()) {
    if (fn.isMaterializable()) symbols->insert(fn.getName());
  }
  codegen->Close();
  return Status::OK();
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
void LlvmCodeGen::CodegenMinMax(LlvmBuilder* builder, const ColumnType& type,
    llvm::Value* src, llvm::Value* dst_slot_ptr, bool min, llvm::Function* fn) {
  llvm::Value* dst = builder->CreateLoad(dst_slot_ptr, "dst_val");

  llvm::Value* compare = NULL;
  switch (type.type) {
    case TYPE_NULL:
      compare = false_value();
      break;
    case TYPE_BOOLEAN:
      if (min) {
        // For min, return x && y
        compare = builder->CreateAnd(src, dst);
      } else {
        // For max, return x || y
        compare = builder->CreateOr(src, dst);
      }
      break;
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_DATE:
    case TYPE_BIGINT:
    case TYPE_DECIMAL:
      if (min) {
        compare = builder->CreateICmpSLT(src, dst);
      } else {
        compare = builder->CreateICmpSGT(src, dst);
      }
      break;
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
      if (min) {
        // OLT is true if 'src' < 'dst' and neither 'src' nor 'dst' is 'nan'.
        compare = builder->CreateFCmpOLT(src, dst);
      } else {
        // OGT is true if 'src' > 'dst' and neither 'src' nor 'dst' is 'nan'.
        compare = builder->CreateFCmpOGT(src, dst);
      }
      // UNE is true if the operands are not equal or if either operand is a 'nan'. Since
      // we're comparing 'src' to itself, the UNE will only be true if 'src' is 'nan'.
      compare = builder->CreateOr(compare, builder->CreateFCmpUNE(src, src));
      break;
    default:
      DCHECK(false);
  }

  if (type.type == TYPE_BOOLEAN) {
    builder->CreateStore(compare, dst_slot_ptr);
  } else {
    llvm::BasicBlock *ret_v1, *ret_v2;
    CreateIfElseBlocks(fn, "ret_v1", "ret_v2", &ret_v1, &ret_v2);

    builder->CreateCondBr(compare, ret_v1, ret_v2);
    builder->SetInsertPoint(ret_v1);
    builder->CreateStore(src, dst_slot_ptr);
    builder->CreateBr(ret_v2);
    builder->SetInsertPoint(ret_v2);
  }
}

// Intrinsics are loaded one by one.  Some are overloaded (e.g. memcpy) and the types must
// be specified.
// TODO: is there a better way to do this?
Status LlvmCodeGen::LoadIntrinsics() {
  // Load memcpy
  {
    llvm::Type* types[] = {ptr_type(), ptr_type(), i32_type()};
    llvm::Function* fn =
        llvm::Intrinsic::getDeclaration(module_, llvm::Intrinsic::memcpy, types);
    if (fn == NULL) {
      return Status("Could not find memcpy intrinsic.");
    }
    llvm_intrinsics_[llvm::Intrinsic::memcpy] = fn;
  }

  // TODO: where is the best place to put this?
  struct {
    llvm::Intrinsic::ID id;
    const char* error;
  } non_overloaded_intrinsics[] = {
#ifdef __aarch64__
      {llvm::Intrinsic::aarch64_crc32cb, "aarch64 crc32_u8"},
      {llvm::Intrinsic::aarch64_crc32ch, "aarch64 crc32_u16"},
      {llvm::Intrinsic::aarch64_crc32cw, "aarch64 crc32_u32"},
      {llvm::Intrinsic::aarch64_crc32cx, "aarch64 crc32_u64"},
#else
      {llvm::Intrinsic::x86_sse42_crc32_32_8, "sse4.2 crc32_u8"},
      {llvm::Intrinsic::x86_sse42_crc32_32_16, "sse4.2 crc32_u16"},
      {llvm::Intrinsic::x86_sse42_crc32_32_32, "sse4.2 crc32_u32"},
      {llvm::Intrinsic::x86_sse42_crc32_64_64, "sse4.2 crc32_u64"},
#endif
  };
  const int num_intrinsics =
      sizeof(non_overloaded_intrinsics) / sizeof(non_overloaded_intrinsics[0]);

  for (int i = 0; i < num_intrinsics; ++i) {
    llvm::Intrinsic::ID id = non_overloaded_intrinsics[i].id;
    llvm::Function* fn = llvm::Intrinsic::getDeclaration(module_, id);
    if (fn == NULL) {
      stringstream ss;
      ss << "Could not find " << non_overloaded_intrinsics[i].error << " intrinsic";
      return Status(ss.str());
    }
    llvm_intrinsics_[id] = fn;
  }

  return Status::OK();
}

void LlvmCodeGen::CodegenMemcpy(
    LlvmBuilder* builder, llvm::Value* dst, llvm::Value* src, int size) {
  DCHECK_GE(size, 0);
  if (size == 0) return;
  llvm::Value* size_val = GetI64Constant(size);
  CodegenMemcpy(builder, dst, src, size_val);
}

void LlvmCodeGen::CodegenMemcpy(
    LlvmBuilder* builder, llvm::Value* dst, llvm::Value* src, llvm::Value* size) {
  DCHECK(dst->getType()->isPointerTy()) << Print(dst);
  DCHECK(src->getType()->isPointerTy()) << Print(src);
  builder->CreateMemCpy(dst, src, size, /* no alignment */ 0);
}

void LlvmCodeGen::CodegenMemset(
    LlvmBuilder* builder, llvm::Value* dst, int value, int size) {
  DCHECK(dst->getType()->isPointerTy()) << Print(dst);
  DCHECK_GE(size, 0);
  if (size == 0) return;
  llvm::Value* value_const = GetI8Constant(value);
  builder->CreateMemSet(dst, value_const, size, /* no alignment */ 0);
}

void LlvmCodeGen::CodegenClearNullBits(
    LlvmBuilder* builder, llvm::Value* tuple_ptr, const TupleDescriptor& tuple_desc) {
  llvm::Value* int8_ptr = builder->CreateBitCast(tuple_ptr, ptr_type(), "int8_ptr");
  llvm::Value* null_bytes_offset = GetI32Constant(tuple_desc.null_bytes_offset());
  llvm::Value* null_bytes_ptr =
      builder->CreateInBoundsGEP(int8_ptr, null_bytes_offset, "null_bytes_ptr");
  CodegenMemset(builder, null_bytes_ptr, 0, tuple_desc.num_null_bytes());
}

llvm::Value* LlvmCodeGen::CodegenMemPoolAllocate(LlvmBuilder* builder,
    llvm::Value* pool_val, llvm::Value* size_val, const char* name) {
  DCHECK(pool_val != nullptr);
  DCHECK(size_val->getType()->isIntegerTy());
  DCHECK_LE(size_val->getType()->getIntegerBitWidth(), 64);
  DCHECK_EQ(pool_val->getType(), GetStructPtrType<MemPool>());
  // Extend 'size_val' to i64 if necessary
  if (size_val->getType()->getIntegerBitWidth() < 64) {
    size_val = builder->CreateSExt(size_val, i64_type());
  }
  llvm::Function* allocate_fn = GetFunction(IRFunction::MEMPOOL_ALLOCATE, false);
  llvm::Value* alignment = GetI32Constant(MemPool::DEFAULT_ALIGNMENT);
  llvm::Value* fn_args[] = {pool_val, size_val, alignment};
  return builder->CreateCall(allocate_fn, fn_args, name);
}

llvm::Value* LlvmCodeGen::CodegenArrayAt(
    LlvmBuilder* builder, llvm::Value* array, int idx, const char* name) {
  DCHECK(array->getType()->isPointerTy() || array->getType()->isArrayTy())
      << Print(array->getType());
  llvm::Value* ptr = builder->CreateConstGEP1_32(array, idx);
  return builder->CreateLoad(ptr, name);
}

llvm::Value* LlvmCodeGen::CodegenCallFunction(LlvmBuilder* builder,
    IRFunction::Type ir_type, llvm::ArrayRef<llvm::Value*> args, const char* name) {
  llvm::Function* fn = GetFunction(ir_type, false);
  DCHECK(fn != nullptr);
  return builder->CreateCall(fn, args, name);
}

void LlvmCodeGen::ClearHashFns() {
  hash_fns_.clear();
}

// Codegen to compute hash for a particular byte size.  Loops are unrolled in this
// process.  For the case where num_bytes == 11, we'd do this by calling
//   1. crc64 (for first 8 bytes)
//   2. crc16 (for bytes 9, 10)
//   3. crc8 (for byte 11)
// The resulting IR looks like:
// define i32 @CrcHash11(i8* %data, i32 %len, i32 %seed) {
// entry:
//   %0 = zext i32 %seed to i64
//   %1 = bitcast i8* %data to i64*
//   %2 = getelementptr i64* %1, i32 0
//   %3 = load i64* %2
//   %4 = call i64 @llvm.x86.sse42.crc32.64.64(i64 %0, i64 %3)
//   %5 = trunc i64 %4 to i32
//   %6 = getelementptr i8* %data, i32 8
//   %7 = bitcast i8* %6 to i16*
//   %8 = load i16* %7
//   %9 = call i32 @llvm.x86.sse42.crc32.32.16(i32 %5, i16 %8)
//   %10 = getelementptr i8* %6, i32 2
//   %11 = load i8* %10
//   %12 = call i32 @llvm.x86.sse42.crc32.32.8(i32 %9, i8 %11)
//   ret i32 %12
// }
llvm::Function* LlvmCodeGen::GetHashFunction(int num_bytes) {
  if (IS_AARCH64 || IsCPUFeatureEnabled(CpuInfo::SSE4_2)) {
    if (num_bytes == -1) {
      // -1 indicates variable length, just return the generic loop based
      // hash fn.
      return GetFunction(IRFunction::HASH_CRC, false);
    }

    map<int, llvm::Function*>::iterator cached_fn = hash_fns_.find(num_bytes);
    if (cached_fn != hash_fns_.end()) {
      return cached_fn->second;
    }

    // Generate a function to hash these bytes
    stringstream ss;
    ss << "CrcHash" << num_bytes;
    FnPrototype prototype(this, ss.str(), i32_type());
    prototype.AddArgument(LlvmCodeGen::NamedVariable("data", ptr_type()));
    prototype.AddArgument(LlvmCodeGen::NamedVariable("len", i32_type()));
    prototype.AddArgument(LlvmCodeGen::NamedVariable("seed", i32_type()));

    llvm::Value* args[3];
    LlvmBuilder builder(context());
    llvm::Function* fn = prototype.GeneratePrototype(&builder, &args[0]);
    llvm::Value* data = args[0];
    llvm::Value* result = args[2];
#ifdef __aarch64__
    llvm::Function* crc8_fn = llvm_intrinsics_[llvm::Intrinsic::aarch64_crc32cb];
    llvm::Function* crc16_fn = llvm_intrinsics_[llvm::Intrinsic::aarch64_crc32ch];
    llvm::Function* crc32_fn = llvm_intrinsics_[llvm::Intrinsic::aarch64_crc32cw];
    llvm::Function* crc64_fn = llvm_intrinsics_[llvm::Intrinsic::aarch64_crc32cx];
#else
    llvm::Function* crc8_fn = llvm_intrinsics_[llvm::Intrinsic::x86_sse42_crc32_32_8];
    llvm::Function* crc16_fn = llvm_intrinsics_[llvm::Intrinsic::x86_sse42_crc32_32_16];
    llvm::Function* crc32_fn = llvm_intrinsics_[llvm::Intrinsic::x86_sse42_crc32_32_32];
    llvm::Function* crc64_fn = llvm_intrinsics_[llvm::Intrinsic::x86_sse42_crc32_64_64];
#endif

    // Generate the crc instructions starting with the highest number of bytes
    if (num_bytes >= 8) {
#ifndef __aarch64__
      llvm::Value* result_64 = builder.CreateZExt(result, i64_type());
#endif
      llvm::Value* ptr = builder.CreateBitCast(data, i64_ptr_type());
      int i = 0;
      while (num_bytes >= 8) {
        llvm::Value* index[] = {GetI32Constant(i++)};
        llvm::Value* d = builder.CreateLoad(builder.CreateInBoundsGEP(ptr, index));
#ifdef __aarch64__
        result = builder.CreateCall(crc64_fn, llvm::ArrayRef<llvm::Value*>({result, d}));
#else
        result_64 =
            builder.CreateCall(crc64_fn, llvm::ArrayRef<llvm::Value*>({result_64, d}));
#endif
        num_bytes -= 8;
      }
#ifndef __aarch64__
      result = builder.CreateTrunc(result_64, i32_type());
#endif
      llvm::Value* index[] = {GetI32Constant(i * 8)};
      // Update data to past the 8-byte chunks
      data = builder.CreateInBoundsGEP(data, index);
    }

    if (num_bytes >= 4) {
      DCHECK_LT(num_bytes, 8);
      llvm::Value* ptr = builder.CreateBitCast(data, i32_ptr_type());
      llvm::Value* d = builder.CreateLoad(ptr);
      result = builder.CreateCall(crc32_fn, llvm::ArrayRef<llvm::Value*>({result, d}));
      llvm::Value* index[] = {GetI32Constant(4)};
      data = builder.CreateInBoundsGEP(data, index);
      num_bytes -= 4;
    }

    if (num_bytes >= 2) {
      DCHECK_LT(num_bytes, 4);
      llvm::Value* ptr = builder.CreateBitCast(data, i16_ptr_type());
      llvm::Value* d = builder.CreateLoad(ptr);
#ifdef __aarch64__
      d = builder.CreateZExt(d, i32_type());
#endif
      result = builder.CreateCall(crc16_fn, llvm::ArrayRef<llvm::Value*>({result, d}));
      llvm::Value* index[] = {GetI16Constant(2)};
      data = builder.CreateInBoundsGEP(data, index);
      num_bytes -= 2;
    }

    if (num_bytes > 0) {
      DCHECK_EQ(num_bytes, 1);
      llvm::Value* d = builder.CreateLoad(data);
#ifdef __aarch64__
      d = builder.CreateZExt(d, i32_type());
#endif
      result = builder.CreateCall(crc8_fn, llvm::ArrayRef<llvm::Value*>({result, d}));
      --num_bytes;
    }
    DCHECK_EQ(num_bytes, 0);

    llvm::Value* shift_16 = GetI32Constant(16);
    llvm::Value* upper_bits = builder.CreateShl(result, shift_16);
    llvm::Value* lower_bits = builder.CreateLShr(result, shift_16);
    result = builder.CreateOr(upper_bits, lower_bits);
    builder.CreateRet(result);

    fn = FinalizeFunction(fn);
    if (fn != NULL) {
      hash_fns_[num_bytes] = fn;
    }
    return fn;
  } else {
    return GetMurmurHashFunction(num_bytes);
  }
}

static llvm::Function* GetLenOptimizedHashFn(
    LlvmCodeGen* codegen, IRFunction::Type f, int len) {
  llvm::Function* fn = codegen->GetFunction(f, false);
  DCHECK(fn != NULL);
  if (len != -1) {
    // Clone this function since we're going to modify it by replacing the
    // length with num_bytes.
    fn = codegen->CloneFunction(fn);
    llvm::Value* len_arg = codegen->GetArgument(fn, 1);
    len_arg->replaceAllUsesWith(codegen->GetI32Constant(len));
  }
  return codegen->FinalizeFunction(fn);
}

llvm::Function* LlvmCodeGen::GetMurmurHashFunction(int len) {
  return GetLenOptimizedHashFn(this, IRFunction::HASH_MURMUR, len);
}

void LlvmCodeGen::ReplaceInstWithValue(llvm::Instruction* from, llvm::Value* to) {
  llvm::BasicBlock::iterator iter(from);
  llvm::ReplaceInstWithValue(from->getParent()->getInstList(), iter, to);
}

llvm::Argument* LlvmCodeGen::GetArgument(llvm::Function* fn, int i) {
  DCHECK_LE(i, fn->arg_size());
  llvm::Function::arg_iterator iter = fn->arg_begin();
  for (int j = 0; j < i; ++j) ++iter;
  return &*iter;
}

llvm::Value* LlvmCodeGen::GetPtrTo(
    LlvmBuilder* builder, llvm::Value* v, const char* name) {
  llvm::Value* ptr = CreateEntryBlockAlloca(*builder, v->getType(), name);
  builder->CreateStore(v, ptr);
  return ptr;
}

llvm::Constant* LlvmCodeGen::ConstantToGVPtr(
    llvm::Type* type, llvm::Constant* ir_constant, const string& name) {
  llvm::GlobalVariable* gv = new llvm::GlobalVariable(
      *module_, type, true, llvm::GlobalValue::PrivateLinkage, ir_constant, name);
  return llvm::ConstantExpr::getGetElementPtr(
      NULL, gv, llvm::ArrayRef<llvm::Constant*>({GetI32Constant(0)}));
}

llvm::Constant* LlvmCodeGen::ConstantsToGVArrayPtr(llvm::Type* element_type,
    llvm::ArrayRef<llvm::Constant*> ir_constants, const string& name) {
  llvm::ArrayType* array_type = llvm::ArrayType::get(element_type, ir_constants.size());
  llvm::Constant* array_const = llvm::ConstantArray::get(array_type, ir_constants);
  return ConstantToGVPtr(array_type, array_const, name);
}

std::unordered_set<string> LlvmCodeGen::ApplyCpuAttrWhitelist(
    const std::unordered_set<string>& cpu_attrs) {
  std::unordered_set<string> result;
  vector<string> attr_whitelist;
  boost::split(attr_whitelist, FLAGS_llvm_cpu_attr_whitelist, boost::is_any_of(","));
  for (const string& attr : cpu_attrs) {
    DCHECK_GE(attr.size(), 1);
    DCHECK(attr[0] == '-' || attr[0] == '+') << attr;
    if (attr[0] == '-') {
      // Already disabled - copy it over unmodified.
      result.insert(attr);
      continue;
    }
    const string attr_name = attr.substr(1);
    auto it = std::find(attr_whitelist.begin(), attr_whitelist.end(), attr_name);
    if (it != attr_whitelist.end()) {
      // In whitelist - copy it over unmodified.
      result.insert(attr);
    } else {
      // Not in whitelist - disable it.
      result.insert("-" + attr_name);
    }
  }
  return result;
}

void LlvmCodeGen::DiagnosticHandler::DiagnosticHandlerFn(
    const llvm::DiagnosticInfo& info, void* context) {
  if (info.getSeverity() == llvm::DiagnosticSeverity::DS_Error) {
    LlvmCodeGen* codegen = reinterpret_cast<LlvmCodeGen*>(context);
    codegen->diagnostic_handler_.error_str_.clear();
    llvm::raw_string_ostream error_msg(codegen->diagnostic_handler_.error_str_);
    llvm::DiagnosticPrinterRawOStream diagnostic_printer(error_msg);
    diagnostic_printer << "LLVM diagnostic error: ";
    info.print(diagnostic_printer);
    error_msg.flush();
    if (codegen->state_) {
      LOG(INFO) << "Query " << PrintId(codegen->state_->query_id()) << " encountered a "
          << codegen->diagnostic_handler_.error_str_;
    }
  }
}

string LlvmCodeGen::DiagnosticHandler::GetErrorString() {
  if (!error_str_.empty()) {
    string return_msg(move(error_str_)); // Also clears error_str_.
    return return_msg;
  }
  return "";
}

string LlvmCodeGen::DebugCacheEntryString(CodeGenCacheKey& key, bool is_lookup,
    bool debug_mode, bool success) const {
  stringstream out;
  if (is_lookup) {
    out << "Look up codegen cache ";
  } else {
    out << "Store to codegen cache ";
  }
  if (success) {
    out << "succeeded. ";
  } else {
    if (is_lookup) {
      out << "missed. ";
    } else {
      out << "failed. ";
    }
  }
  out << "CodeGen Cache Key hash_code=" << key.hash_code();
  if (UNLIKELY(debug_mode)) {
    out << "\nFragment Plan: " << apache::thrift::ThriftDebugString(state_->fragment())
        << "\n";
    out << "CodeGen Functions: \n";
    for (auto& entry : fns_to_jit_compile_) {
      const llvm::StringRef& fn_name = entry.first;
      out << "  " << fn_name.data() << "\n";
    }
  }
  return out.str();
}
}

namespace boost {

/// Handler for exceptions in cross-compiled functions.
/// When boost is configured with BOOST_NO_EXCEPTIONS, it calls these handlers instead of
/// throwing the exception.
[[noreturn]] void throw_exception(std::exception const& e) {
  LOG(FATAL) << "Cannot handle exceptions in codegen'd code " << e.what();
}

[[noreturn]] void throw_exception(
    std::exception const& e, boost::source_location const& loc) {
  LOG(FATAL) << loc.file_name() << ":" << loc.line() << "] " << loc.function_name()
             << ": Cannot handle exceptions in codegen'd code " << e.what();
}
}
