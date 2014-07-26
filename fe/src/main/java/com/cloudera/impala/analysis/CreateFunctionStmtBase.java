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

package com.cloudera.impala.analysis;

import java.util.HashMap;
import java.util.List;

import com.cloudera.impala.authorization.AuthorizeableFn;
import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.authorization.PrivilegeRequest;
import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.Function;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TCreateFunctionParams;
import com.cloudera.impala.thrift.TFunctionBinaryType;
import com.google.common.collect.Lists;

/**
 * Base class for CREATE [] FUNCTION.
 */
public class CreateFunctionStmtBase extends StatementBase {
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

  protected final Function fn_;
  protected final boolean ifNotExists_;

  // Additional arguments to create function.
  protected final HashMap<OptArg, String> optArgs_;

  // Set in analyze()
  protected String sqlString_;

  protected CreateFunctionStmtBase(Function fn, HdfsUri location, boolean ifNotExists,
      HashMap<OptArg, String> optArgs) {
    fn_ = fn;
    fn_.setLocation(location);
    ifNotExists_ = ifNotExists;
    optArgs_ = optArgs;
  }

  public String getComment() { return optArgs_.get(OptArg.COMMENT); }
  public boolean getIfNotExists() { return ifNotExists_; }

  public TCreateFunctionParams toThrift() {
    TCreateFunctionParams params = new TCreateFunctionParams(fn_.toThrift());
    params.setIf_not_exists(getIfNotExists());
    params.setFn(fn_.toThrift());
    return params;
  }

  // Returns optArg[key], first validating that it is set.
  protected String checkAndGetOptArg(OptArg key)
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
        binaryType = TFunctionBinaryType.HIVE;
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
    fn_.getFunctionName().analyze(analyzer);

    // For now, if authorization is enabled, the user needs ALL on the server
    // to create functions.
    // TODO: this is not the right granularity but acceptable for now.
    analyzer.registerPrivReq(new PrivilegeRequest(
        new AuthorizeableFn(fn_.signatureString()), Privilege.ALL));

    Db builtinsDb = analyzer.getCatalog().getDb(Catalog.BUILTINS_DB);
    if (builtinsDb.containsFunction(fn_.getName())) {
      throw new AnalysisException("Function cannot have the same name as a builtin: " +
          fn_.getFunctionName().getFunction());
    }

    // Validate function arguments
    for (Type t: fn_.getArgs()) {
      t.analyze();
    }
    fn_.getReturnType().analyze();

    // Forbid unsupported and complex types.
    List<Type> refdTypes = Lists.newArrayList(fn_.getReturnType());
    refdTypes.addAll(Lists.newArrayList(fn_.getArgs()));
    for (Type t: refdTypes) {
      if (!t.isSupported() || t.isComplexType()) {
        throw new AnalysisException(
            String.format("Type '%s' is not supported in UDFs/UDAs.", t.toSql()));
      }
    }

    if (analyzer.getDb(fn_.dbName(), Privilege.CREATE) == null) {
      throw new AnalysisException(Analyzer.DB_DOES_NOT_EXIST_ERROR_MSG + fn_.dbName());
    }

    Function existingFn = analyzer.getCatalog().getFunction(
        fn_, Function.CompareMode.IS_INDISTINGUISHABLE);
    if (existingFn != null && !ifNotExists_) {
      throw new AnalysisException(Analyzer.FN_ALREADY_EXISTS_ERROR_MSG +
          existingFn.signatureString());
    }

    fn_.getLocation().analyze(analyzer, Privilege.CREATE);

    // Check the file type from the binary type to infer the type of the UDA
    fn_.setBinaryType(getBinaryType());
  }
}
