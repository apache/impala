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

#include "codegen/codegen-callgraph.h"

#include <llvm/IR/Constants.h>
#include <llvm/IR/Function.h>
#include <llvm/IR/GlobalVariable.h>

#include "runtime/lib-cache.h"

#include "common/names.h"

using namespace llvm;
using namespace strings;

namespace impala {

bool CodegenCallGraph::IsDefinedInImpalad(const string& fn_name) {
  void* fn_ptr = nullptr;
  Status status =
      LibCache::instance()->GetSoFunctionPtr("", fn_name, &fn_ptr, nullptr, true);
  return status.ok();
}

void CodegenCallGraph::FindGlobalUsers(User* val, vector<GlobalObject*>* users) {
  for (Use& u: val->uses()) {
    User* user = u.getUser();
    if (isa<Instruction>(user)) {
      Instruction* inst = dyn_cast<Instruction>(u.getUser());
      users->push_back(inst->getFunction());
    } else if (isa<GlobalVariable>(user)) {
      GlobalVariable* gv = cast<GlobalVariable>(user);
      string val_name = gv->getName();
      // We strip global ctors and dtors out of the modules as they are not run.
      if (val_name.find("llvm.global_ctors") == string::npos &&
          val_name.find("llvm.global_dtors") == string::npos) {
        users->push_back(gv);;
      }
    } else if (isa<Constant>(user)) {
      FindGlobalUsers(user, users);
    } else {
      DCHECK(false) << "Unknown user's types for " << val->getName().str();
    }
  }
}

void CodegenCallGraph::Init(Module* module) {
  DCHECK(!inited_);
  // Create a mapping of functions to their referenced functions.
  for (Function& fn: module->functions()) {
    if (fn.isIntrinsic() || fn.isDeclaration()) continue;
    string fn_name = fn.getName();
    // Create an entry for a function if it doesn't exist already.
    // This creates entries for functions which don't have any callee.
    if (call_graph_.find(fn_name) == call_graph_.end()) {
      call_graph_.emplace(fn_name, unordered_set<string>());
    }
    vector<GlobalObject*> users;
    FindGlobalUsers(&fn, &users);
    for (GlobalValue* val: users) {
      const string& caller_name = val->getName();
      DCHECK(isa<GlobalVariable>(val) || isa<Function>(val));
      // 'call_graph_' contains functions which need to be materialized when a certain
      // IR Function is materialized. We choose to include functions referenced by
      // another IR function in the map even if it's defined in Impalad binary so it
      // can be inlined for further optimization. This is not applicable for functions
      // referenced by global variables only.
      if (isa<GlobalVariable>(val)) {
        if (IsDefinedInImpalad(fn_name)) continue;
        fns_referenced_by_gv_.insert(fn_name);
      } else {
        // There may not be an entry for 'caller_name' yet. Create an entry if needed.
        call_graph_[caller_name].insert(fn_name);
      }
    }
  }
  inited_ = true;
}

}
