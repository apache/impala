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
import java.util.List;

import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import static org.apache.impala.analysis.ToSqlOptions.DEFAULT;

// Wrapper class around argument types and if it has varArgs
public class FunctionArgs extends StmtNode {
  private final List<TypeDef> argTypeDefs_;
  private boolean hasVarArgs_;

  // Result of analysis.
  private List<Type> argTypes_;

  public FunctionArgs() {
    argTypeDefs_ = new ArrayList<>();
    hasVarArgs_ = false;
  }

  public FunctionArgs(List<TypeDef> argTypeDefs, boolean varArgs) {
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
    List<Type> argTypes = Lists.newArrayListWithCapacity(argTypeDefs_.size());
    for (TypeDef typeDef: argTypeDefs_) {
      typeDef.analyze(analyzer);
      argTypes.add(typeDef.getType());
    }
    argTypes_ = argTypes;
  }

  public List<TypeDef> getArgTypeDefs() { return argTypeDefs_; }
  public List<Type> getArgTypes() { return argTypes_; }
  public boolean hasVarArgs() { return hasVarArgs_; }

  @Override
  public final String toSql() {
    return toSql(DEFAULT);
  }

  @Override
  public String toSql(ToSqlOptions options) {
    return null;
  }
}
