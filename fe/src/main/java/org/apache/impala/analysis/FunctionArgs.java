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

package org.apache.impala.analysis;

import java.util.ArrayList;

import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

// Wrapper class around argument types and if it has varArgs
public class FunctionArgs implements ParseNode {
  private final ArrayList<TypeDef> argTypeDefs_;
  private boolean hasVarArgs_;

  // Result of analysis.
  private ArrayList<Type> argTypes_;

  public FunctionArgs() {
    argTypeDefs_ = Lists.newArrayList();
    hasVarArgs_ = false;
  }

  public FunctionArgs(ArrayList<TypeDef> argTypeDefs, boolean varArgs) {
    argTypeDefs_ = argTypeDefs;
    hasVarArgs_ = varArgs;
    if (varArgs) Preconditions.checkState(argTypeDefs.size() > 0);
  }

  public void setHasVarArgs(boolean b) { {
    Preconditions.checkState(argTypeDefs_.size() > 0);
    hasVarArgs_ = b; }
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    ArrayList<Type> argTypes = Lists.newArrayListWithCapacity(argTypeDefs_.size());
    for (TypeDef typeDef: argTypeDefs_) {
      typeDef.analyze(analyzer);
      argTypes.add(typeDef.getType());
    }
    argTypes_ = argTypes;
  }

  public ArrayList<TypeDef> getArgTypeDefs() { return argTypeDefs_; }
  public ArrayList<Type> getArgTypes() { return argTypes_; }
  public boolean hasVarArgs() { return hasVarArgs_; }

  @Override
  public String toSql() { return null; }
}
