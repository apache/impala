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

import java.util.List;

import com.cloudera.impala.analysis.ColumnType;
import com.cloudera.impala.analysis.FunctionArgs;
import com.cloudera.impala.analysis.FunctionName;
import com.cloudera.impala.analysis.HdfsURI;
import com.cloudera.impala.thrift.TFunction;
import com.cloudera.impala.thrift.TUda;

/**
 * Internal representation of a UDA.
 */
public class Uda extends Function {
  private ColumnType intermediateType_;

  // The name inside the binary at location_ that contains this particular
  private String updateFnName_;
  private String initFnName_;
  private String serializeFnName_;
  private String mergeFnName_;
  private String finalizeFnName_;

  public Uda(FunctionName fnName, FunctionArgs args, PrimitiveType retType) {
    super(fnName, args.argTypes, retType, args.hasVarArgs);
  }

  public Uda(FunctionName fnName, List<PrimitiveType> argTypes,
      PrimitiveType retType, ColumnType intermediateType,
      HdfsURI location, String updateFnName, String initFnName,
      String serializeFnName, String mergeFnName, String finalizeFnName) {
    super(fnName, argTypes, retType, false);
    setLocation(location);
    intermediateType_ = intermediateType;
    updateFnName_ = updateFnName;
    initFnName_ = initFnName;
    serializeFnName_ = serializeFnName;
    mergeFnName_ = mergeFnName;
    finalizeFnName_ = finalizeFnName;
  }

  public String getUpdateFnName() { return updateFnName_; }
  public String getInitFnName() { return initFnName_; }
  public String getSerializeFnName() { return serializeFnName_; }
  public String getMergeFnName() { return mergeFnName_; }
  public String getFinalizeFnName() { return finalizeFnName_; }
  public ColumnType getIntermediateType() { return intermediateType_; }

  public void setUpdateFnName(String fn) { updateFnName_ = fn; }
  public void setInitFnName(String fn) { initFnName_ = fn; }
  public void setSerializeFnName(String fn) { serializeFnName_ = fn; }
  public void setMergeFnName(String fn) { mergeFnName_ = fn; }
  public void setFinalizeFnName(String fn) { finalizeFnName_ = fn; }
  public void setIntermediateType(ColumnType t) { intermediateType_ = t; }

  @Override
  public TFunction toThrift() {
    TFunction fn = super.toThrift();
    TUda uda = new TUda();
    uda.setUpdate_fn_name(updateFnName_);
    uda.setInit_fn_name(initFnName_);
    if (serializeFnName_ == null) uda.setSerialize_fn_name(serializeFnName_);
    uda.setMerge_fn_name(mergeFnName_);
    uda.setFinalize_fn_name(finalizeFnName_);
    uda.setIntermediate_type(intermediateType_.toThrift());
    fn.setUda(uda);
    return fn;
  }
}
