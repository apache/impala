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

#ifndef IMPALA_CODEGEN_CODEGEN_CALLGRAPH_H
#define IMPALA_CODEGEN_CODEGEN_CALLGRAPH_H

#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include <llvm/IR/Module.h>

#include "codegen/llvm-codegen.h"

namespace impala {

/// This class implements the callgraph of functions in a LLVM module.
class CodegenCallGraph {
 public:
  /// Creates an uninitialized call graph.
  CodegenCallGraph() : inited_(false) { }

  /// Initializes the call graph based on the functions defined in 'module'.
  void Init(llvm::Module* module);

  /// Returns the callees of the function with name 'fn_name'.
  /// Returns NULL if the function is not found in the call graph.
  const boost::unordered_set<std::string>* GetCallees(const std::string& fn_name) const {
    DCHECK(inited_);
    CallGraph::const_iterator iter = call_graph_.find(fn_name);
    return LIKELY(iter != call_graph_.end()) ? &iter->second : nullptr;
  }

  /// Returns the set of functions referenced by global variables in 'module'
  const boost::unordered_set<std::string>& fns_referenced_by_gv() const {
    DCHECK(inited_);
    return fns_referenced_by_gv_;
  }

 private:
  bool inited_;

  /// Map of functions' names to their callee functions' names.
  typedef boost::unordered_map<std::string, boost::unordered_set<std::string>> CallGraph;
  CallGraph call_graph_;

  /// The set of all functions referenced by global variables.
  /// They always need to be materialized.
  boost::unordered_set<std::string> fns_referenced_by_gv_;

  /// Returns true if the function 'fn' is defined in the Impalad native code.
  static bool IsDefinedInImpalad(const std::string& fn);

  /// Find all global variables and functions which reference the llvm::Value 'val'
  /// and append them to 'users'.
  static void FindGlobalUsers(llvm::User* val, std::vector<llvm::GlobalObject*>* users);
};

}

#endif
