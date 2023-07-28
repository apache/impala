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

#include "codegen/codegen-symbol-emitter.h"

#include <unistd.h>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <boost/scoped_ptr.hpp>
#include <llvm-c/Disassembler.h>
#include <llvm/CodeGen/MachineFunction.h>
#include <llvm/DebugInfo/DIContext.h>
#include <llvm/DebugInfo/DWARF/DWARFContext.h>
#include <llvm/IR/DebugInfo.h>
#include <llvm/IR/Function.h>
#include <llvm/Object/SymbolSize.h>
#include <llvm/Support/Debug.h>
#include "llvm/Support/raw_ostream.h"

#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "util/error-util.h"

#include "common/names.h"

using std::hex;
using std::rename;

DEFINE_bool_hidden(codegen_symbol_emitter_log_successful_destruction_test_only, false,
    "Log when a 'CodegenSymbolCache' object is destroyed correctly, i.e. "
    "'non_freed_objects_' is zero. Only used for testing.");

namespace impala {

SpinLock CodegenSymbolEmitter::perf_map_lock_;
unordered_map<const void*, vector<CodegenSymbolEmitter::PerfMapEntry>>
    CodegenSymbolEmitter::perf_map_;

CodegenSymbolEmitter::~CodegenSymbolEmitter() {
  if (non_freed_objects_ != 0) {
    stringstream s;
    s << "The difference between the number of emitted and freed object files "
        << "should be zero, but was " << non_freed_objects_ << ".";
    const string& msg = s.str();
    DCHECK(false) << msg;
    LOG(WARNING) << msg;
  } else if (FLAGS_codegen_symbol_emitter_log_successful_destruction_test_only) {
    LOG(INFO) << "Successful destruction of CodegenSymbolEmitter object.";
  }
}

void CodegenSymbolEmitter::NotifyObjectEmitted(const llvm::object::ObjectFile& obj,
    const llvm::RuntimeDyld::LoadedObjectInfo& loaded_obj) {
  non_freed_objects_++;
  vector<PerfMapEntry> perf_map_entries;

  ofstream asm_file;
  if (!asm_path_.empty()) {
    asm_file.open(asm_path_, fstream::out | fstream::trunc);
    if (asm_file.fail()) {
      // Log error and continue if we can't write the disassembly to a file. Note that
      // fstream operations don't throw exceptions by default unless configured to do so.
      LOG(ERROR) << "Could not save disassembly to: " << asm_path_;
    }
  }

  llvm::object::OwningBinary<llvm::object::ObjectFile> debug_obj_owner =
      loaded_obj.getObjectForDebug(obj);
  const llvm::object::ObjectFile& debug_obj = *debug_obj_owner.getBinary();
  llvm::DWARFContextInMemory dwarf_ctx(debug_obj);

  // Use symbol info to iterate functions in the object.
  for (const std::pair<llvm::object::SymbolRef, uint64_t>& pair :
      computeSymbolSizes(debug_obj)) {
    ProcessSymbol(&dwarf_ctx, pair.first, pair.second, &perf_map_entries, asm_file);
  }

  if (asm_file.is_open()) {
    asm_file.close();
    LOG(INFO) << "Saved disassembly to " << asm_path_;
  }

  ofstream perf_map_file;
  if (emit_perf_map_) {
    lock_guard<SpinLock> perf_map_lock(perf_map_lock_);
    DCHECK(perf_map_.find(obj.getData().data()) == perf_map_.end());
    perf_map_[obj.getData().data()] = std::move(perf_map_entries);
    WritePerfMapLocked();
  }
}

void CodegenSymbolEmitter::NotifyFreeingObject(const llvm::object::ObjectFile& obj) {
  non_freed_objects_--;
  if (emit_perf_map_) {
    lock_guard<SpinLock> perf_map_lock(perf_map_lock_);
    DCHECK(perf_map_.find(obj.getData().data()) != perf_map_.end());
    perf_map_.erase(obj.getData().data());
    WritePerfMapLocked();
  }
}

void CodegenSymbolEmitter::ProcessSymbol(llvm::DIContext* debug_ctx,
    const llvm::object::SymbolRef& symbol, uint64_t size,
    vector<PerfMapEntry>* perf_map_entries, ofstream& asm_file) {
  llvm::Expected<llvm::object::SymbolRef::Type> symType = symbol.getType();
  if (!symType || symType.get() != llvm::object::SymbolRef::ST_Function) return;

  llvm::Expected<llvm::StringRef> name_or_err = symbol.getName();
  llvm::Expected<uint64_t> addr_or_err = symbol.getAddress();
  if (!name_or_err || !addr_or_err) return;

  uint64_t addr = addr_or_err.get();
  // Append id to symbol to disambiguate different instances of jitted functions.
  string fn_symbol = Substitute("$0:$1", name_or_err.get().data(), id_);

  if (emit_perf_map_) {
    PerfMapEntry entry;
    entry.symbol = fn_symbol;
    entry.addr = addr;
    entry.size = size;
    perf_map_entries->push_back(entry);
  }

  if (asm_file.is_open()) {
    EmitFunctionAsm(debug_ctx, fn_symbol, addr, size, asm_file);
  }
}

void CodegenSymbolEmitter::WritePerfMap() {
  lock_guard<SpinLock> perf_map_lock(perf_map_lock_);
  WritePerfMapLocked();
}

void CodegenSymbolEmitter::WritePerfMapLocked() {
  perf_map_lock_.DCheckLocked();
  string perf_map_path = Substitute("/tmp/perf-$0.map", getpid());
  string tmp_perf_map_path = perf_map_path + ".tmp";

  ofstream tmp_perf_map_file;
  tmp_perf_map_file.open(tmp_perf_map_path, fstream::out | fstream::trunc);
  if (tmp_perf_map_file.fail()) {
    LOG(ERROR) << "Could not write perf map to: " << tmp_perf_map_path;
    return;
  }

  for (pair<const void*, vector<PerfMapEntry>> entries: perf_map_) {
    for (PerfMapEntry& entry: entries.second) {
      // Write perf.map file. Each line has format <address> <size> <label>
      tmp_perf_map_file << hex << entry.addr << " " << entry.size << " "
                        << entry.symbol << "\n";
    }
  }
  tmp_perf_map_file.close();

  // Atomically move the temprorary file to the new one.
  if (rename(tmp_perf_map_path.c_str(), perf_map_path.c_str()) != 0) {
    string err_msg = GetStrErrMsg();
    LOG(ERROR) << "Failed to move perf map from: " << tmp_perf_map_path << " to "
               << perf_map_path << ": " << err_msg;
  }
}

void CodegenSymbolEmitter::EmitFunctionAsm(llvm::DIContext* debug_ctx,
    const string& fn_symbol, uint64_t addr, uint64_t size, ofstream& asm_file) {
  DCHECK(asm_file.is_open());
  llvm::DILineInfoTable di_lines = debug_ctx->getLineInfoForAddressRange(addr, size);
  auto di_line_it = di_lines.begin();

  // LLVM's C disassembler API is much simpler than the C++ API, so let's use it.
  string triple = llvm::sys::getProcessTriple();
  LLVMDisasmContextRef disasm = LLVMCreateDisasm(triple.c_str(), NULL, 0, NULL, NULL);
  if (disasm == NULL) {
    LOG(WARNING) << "Could not create LLVM disassembler for target triple " << triple;
    return;
  }

  char line_buf[2048];
  uint8_t* code = reinterpret_cast<uint8_t*>(addr);
  uint8_t* code_end = code + size;

  asm_file << "Disassembly for " << fn_symbol << " (" << reinterpret_cast<void*>(addr)
           << "):" << "\n";

  while (code < code_end) {
    uint64_t inst_addr = reinterpret_cast<uint64_t>(code);
    // Emit any debug symbols before instruction.
    for (; di_line_it != di_lines.end() && di_line_it->first <= inst_addr; ++di_line_it) {
      llvm::DILineInfo line = di_line_it->second;
      asm_file << "\t" << line.FileName << ":" << line.FileName << ":"
               << line.FunctionName << ":" << line.Line << ":" << line.Column << "\n";
    }

    size_t inst_len = LLVMDisasmInstruction(disasm, code, code_end - code, 0, line_buf,
        sizeof(line_buf));
    if (inst_len == 0) {
      LOG(WARNING) << "Invalid instruction at " << static_cast<void*>(code);
      break;
    }

    uint64_t offset = inst_addr - addr;
    asm_file << offset << ":\t" << line_buf << "\n";
    code += inst_len;
  }

  for (; di_line_it != di_lines.end(); ++di_line_it) {
    llvm::DILineInfo line = di_line_it->second;
    asm_file << "\t" << line.FileName << ":" << line.FileName << ":"
             << line.FunctionName << ":" << line.Line << ":" << line.Column << "\n";
  }
  asm_file << "\n";
}

}
