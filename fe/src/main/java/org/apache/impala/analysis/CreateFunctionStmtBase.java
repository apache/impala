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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.permission.FsAction;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.Function;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TCreateFunctionParams;
import org.apache.impala.thrift.TFunctionBinaryType;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Base class for CREATE [] FUNCTION.
 */
public abstract class CreateFunctionStmtBase extends StatementBase {

  // Enums for valid keys for optional arguments.
  public enum OptArg {
    COMMENT,
    SYMBOL,           // Only used for Udfs
    PREPARE_FN,       // Only used for Udfs
    CLOSE_FN,         // Only used for Udfs
    UPDATE_FN,        // Only used for Udas
    INIT_FN,          // Only used for Udas
    SERIALIZE_FN,     // Only used for Udas
    MERGE_FN,         // Only used for Udas
    FINALIZE_FN       // Only used for Udas
  };

  protected final FunctionName fnName_;
  protected final FunctionArgs args_;
  protected final TypeDef retTypeDef_;
  protected final HdfsUri location_;
  protected final Map<CreateFunctionStmtBase.OptArg, String> optArgs_;
  protected final boolean ifNotExists_;

  // Result of analysis.
  protected Function fn_;

  // Db object for function fn_. Set in analyze().
  protected FeDb db_;

  // Set in analyze()
  protected String sqlString_;

  protected CreateFunctionStmtBase(FunctionName fnName, FunctionArgs args,
      TypeDef retTypeDef, HdfsUri location, boolean ifNotExists,
      Map<CreateFunctionStmtBase.OptArg, String> optArgs) {
    // The return and arg types must either be both null or non-null.
    Preconditions.checkState(!(args == null ^ retTypeDef == null));
    fnName_ = fnName;
    args_ = args;
    retTypeDef_ = retTypeDef;
    location_ = location;
    ifNotExists_ = ifNotExists;
    optArgs_ = optArgs;
  }

  public boolean getIfNotExists() { return ifNotExists_; }
  public boolean hasSignature() { return args_ != null; }

  public TCreateFunctionParams toThrift() {
    TCreateFunctionParams params = new TCreateFunctionParams(fn_.toThrift());
    params.setIf_not_exists(getIfNotExists());
    params.setFn(fn_.toThrift());
    return params;
  }

  // Returns optArg[key], first validating that it is set.
  public String checkAndGetOptArg(OptArg key)
      throws AnalysisException {
    if (!optArgs_.containsKey(key)) {
      throw new AnalysisException("Argument '" + key + "' must be set.");
    }
    return optArgs_.get(key);
  }

  protected void checkOptArgNotSet(OptArg key)
      throws AnalysisException {
    if (optArgs_.containsKey(key)) {
      throw new AnalysisException("Optional argument '" + key + "' should not be set.");
    }
  }

  // Returns the function's binary type based on the path extension.
  private TFunctionBinaryType getBinaryType() throws AnalysisException {
    TFunctionBinaryType binaryType = null;
    String binaryPath = fn_.getLocation().getLocation();
    int suffixIndex = binaryPath.lastIndexOf(".");
    if (suffixIndex != -1) {
      String suffix = binaryPath.substring(suffixIndex + 1);
      if (suffix.equalsIgnoreCase("jar")) {
        binaryType = TFunctionBinaryType.JAVA;
      } else if (suffix.equalsIgnoreCase("so")) {
        binaryType = TFunctionBinaryType.NATIVE;
      } else if (suffix.equalsIgnoreCase("ll")) {
        binaryType = TFunctionBinaryType.IR;
      }
    }
    if (binaryType == null) {
      throw new AnalysisException("Unknown binary type: '" + binaryPath +
          "'. Binary must end in .jar, .so or .ll");
    }
    return binaryType;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    // Validate function name is legal
    fnName_.analyze(analyzer, false);

    if (hasSignature()) {
      // Validate function arguments and return type.
      args_.analyze(analyzer);
      retTypeDef_.analyze(analyzer);
      fn_ = createFunction(fnName_, args_.getArgTypes(), retTypeDef_.getType(),
          args_.hasVarArgs());
    } else {
      fn_ = createFunction(fnName_, null, null, false);
    }

    analyzer.registerPrivReq(builder ->
        builder.onFunction(fn_.dbName(), fn_.signatureString())
            .allOf(Privilege.CREATE)
            .build());

    db_ = analyzer.getDb(fn_.dbName(), true);
    Function existingFn = db_.getFunction(fn_, Function.CompareMode.IS_INDISTINGUISHABLE);
    if (existingFn != null && !ifNotExists_) {
      throw new AnalysisException(Analyzer.FN_ALREADY_EXISTS_ERROR_MSG +
          existingFn.signatureString());
    }

    location_.analyze(analyzer, Privilege.ALL, FsAction.READ);
    fn_.setLocation(location_);

    // Check the file type from the binary type to infer the type of the UDA
    fn_.setBinaryType(getBinaryType());

    // Forbid unsupported and complex types.
    if (hasSignature()) {
      List<Type> refdTypes = Lists.newArrayList(fn_.getReturnType());
      refdTypes.addAll(Lists.newArrayList(fn_.getArgs()));
      for (Type t: refdTypes) {
        if (!t.isSupported() || t.isComplexType()) {
          throw new AnalysisException(
              String.format("Type '%s' is not supported in UDFs/UDAs.", t.toSql()));
        }
      }
    } else if (fn_.getBinaryType() != TFunctionBinaryType.JAVA) {
      throw new AnalysisException(
          String.format("Native functions require a return type and/or " +
              "argument types: %s", fn_.getFunctionName()));
    }

    // Check if the function can be persisted. We persist all native/IR functions
    // and also JAVA functions added without signature. Only JAVA functions added
    // with signatures aren't persisted.
    if (getBinaryType() == TFunctionBinaryType.JAVA && hasSignature()) {
      fn_.setIsPersistent(false);
    } else {
      fn_.setIsPersistent(true);
    }
  }

  public FunctionName getFunctionName() {
    return fnName_;
  }

  public HdfsUri getLocation() {
    return location_;
  }
  /**
   * Creates a concrete function.
   */
  protected abstract Function createFunction(FunctionName fnName,
      List<Type> argTypes, Type retType, boolean hasVarArgs);
}
