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

package com.cloudera.impala.catalog;

import java.util.ArrayList;

import com.cloudera.impala.analysis.FunctionArgs;
import com.cloudera.impala.analysis.FunctionName;
import com.cloudera.impala.analysis.HdfsURI;

/**
 * Internal representation of a UDF.
 * TODO: unify this with builtins.
 */
public class Udf extends Function {
  // The name inside the binary at location_ that contains this particular
  // UDF. e.g. org.example.MyUdf.class.
  private String symbolName_;

  public Udf(FunctionName fnName, FunctionArgs args, PrimitiveType retType) {
    super(fnName, args.argTypes, retType, args.hasVarArgs);
  }

  public Udf(FunctionName fnName, ArrayList<PrimitiveType> argTypes,
    PrimitiveType retType, HdfsURI location, String symbolName) {
    super(fnName, argTypes, retType, false);
    setLocation(location);
    setSymbolName(symbolName);
  }

  public void setSymbolName(String s) { symbolName_ = s; }
  public String getSymbolName() { return symbolName_; }
}
