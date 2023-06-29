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

#pragma once

#include <memory>

#include "codegen/codegen-symbol-emitter.h"

#include <llvm/ExecutionEngine/ExecutionEngine.h>

namespace impala {

/// Class that encapsulates a non-NULL 'llvm::ExecutionEngine' and other objects the
/// lifetimes of which are tied to the execution engine but are not owned by it.
class LlvmExecutionEngineWrapper {
  public:
   LlvmExecutionEngineWrapper(std::unique_ptr<llvm::ExecutionEngine> execution_engine,
      std::unique_ptr<CodegenSymbolEmitter> symbol_emitter)
     : symbol_emitter_(std::move(symbol_emitter)),
       execution_engine_(std::move(execution_engine)) {
     DCHECK(execution_engine_ != nullptr);
   }

   ~LlvmExecutionEngineWrapper() {
     execution_engine_.reset();
     symbol_emitter_.reset();
   }

   llvm::ExecutionEngine* execution_engine() {
     return execution_engine_.get();
   }

  private:
   /// The symbol emitter associated with 'execution_engine_'. Methods on
   /// 'symbol_emitter_' are called by 'execution_engine_' when code is emitted or
   /// freed. The lifetime of the symbol emitter must be longer than
   /// 'execution_engine_'.
   std::unique_ptr<CodegenSymbolEmitter> symbol_emitter_;

   /// Execution/Jitting engine.
   std::unique_ptr<llvm::ExecutionEngine> execution_engine_;
};

} // namespace impala
