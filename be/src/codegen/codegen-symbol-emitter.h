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


#ifndef IMPALA_CODEGEN_JIT_SYMBOL_EMITTER_H
#define IMPALA_CODEGEN_JIT_SYMBOL_EMITTER_H

#include <boost/unordered_map.hpp>
#include <iosfwd>
#include <llvm/ExecutionEngine/JITEventListener.h>

#include "util/spinlock.h"

namespace llvm {
  class DIContext;
  namespace object {
    class SymbolRef;
  }
}

namespace impala {

/// Class to emit debug symbols and associated info from jitted object files.
/// The methods of this listener are called whenever the query fragment emits
/// a compiled machine code object. We can then process any debug symbols
/// associated with the code object and emit useful information such as disassembly
/// and symbols for perf profiling.
class CodegenSymbolEmitter : public llvm::JITEventListener {
 public:
  CodegenSymbolEmitter(std::string id)
      : id_(id), emit_perf_map_(false), non_freed_objects_(0)
  { }

  ~CodegenSymbolEmitter();

  /// Write the current contents of 'perf_map_' to /tmp/perf-<pid>.map
  /// Atomically updates the current map by writing to a temporary file then moving it.
  static void WritePerfMap();

  /// Called whenever MCJIT module code is emitted.
  void NotifyObjectEmitted(const llvm::object::ObjectFile &obj,
     const llvm::RuntimeDyld::LoadedObjectInfo &loaded_obj) override;

  /// Called whenever MCJIT module code is freed.
  void NotifyFreeingObject(const llvm::object::ObjectFile &obj) override;

  void set_emit_perf_map(bool emit_perf_map) { emit_perf_map_ = emit_perf_map; }

  void set_asm_path(const std::string& asm_path) { asm_path_ = asm_path; }

 private:
  struct PerfMapEntry {
    std::string symbol;
    uint64_t addr;
    uint64_t size;
  };

  /// Process the given 'symbol' with 'size'. For function symbols, append to
  /// 'perf_map_entries' if 'emit_perf_map_' is true and write disassembly to 'asm_file'
  /// if it is open.
  void ProcessSymbol(llvm::DIContext* debug_ctx, const llvm::object::SymbolRef& symbol,
      uint64_t size, std::vector<PerfMapEntry>* perf_map_entries,
      std::ofstream& asm_file);

  /// Implementation of WritePerfMap(). 'perf_map_lock_' must be held by caller.
  static void WritePerfMapLocked();

  /// Emit disassembly for the function. If symbols are present for the code object,
  /// the symbols will be interleaved with the disassembly.
  void EmitFunctionAsm(llvm::DIContext* debug_ctx, const std::string& fn_symbol,
      uint64_t addr, uint64_t size, std::ofstream& asm_file);

  /// Identifier to append to symbols, e.g. a fragment instance id. The identifier is
  /// passed by the caller when the CodegenSymbolEmitter is constructed. Without this
  /// identifier, many distinct codegen'd functions will have the same symbol.
  std::string id_;

  /// If true, emit perf map info to /tmp/perf-<pid>.map.
  bool emit_perf_map_;

  /// The number of object files that have been emitted but not freed yet. The counter is
  /// incremented in NotifyObjectEmitted() and decremented in NotifyFreeingObject(). At
  /// the time of the destruction of this object, this should be 0 - if it is greater than
  /// 0, the LLVM execution engine to which this object is subscribed is still alive and
  /// it will try to notify this object when the object file is freed (most likely when
  /// the execution engine itself is destroyed), leading to use-after-free. See
  /// IMPALA-12306 for more.
  int non_freed_objects_;

  /// File to emit disassembly to. If empty string, don't emit.
  std::string asm_path_;

  /// Global lock to protect 'perf_map_' and writes to /tmp/perf-<pid>.map.
  static SpinLock perf_map_lock_;

  /// All current entries that should be emitted into the perf map file.
  /// Maps the address of each ObjectFile's data to the symbols in the object.
  static boost::unordered_map<const void*, std::vector<PerfMapEntry>> perf_map_;
};

}

#endif
