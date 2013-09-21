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

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.Function;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TCreateFunctionParams;
import com.cloudera.impala.thrift.TFunctionBinaryType;
import com.cloudera.impala.thrift.TFunctionName;
import com.cloudera.impala.thrift.TPrimitiveType;
import com.google.common.collect.Lists;

/**
 * Base class for CREATE [] FUNCTION.
 */
public class CreateFunctionStmtBase extends StatementBase {
  // Enums for valid keys for optional arguments.
  public enum OptArg {
    COMMENT,
    SYMBOL,           // Only used for Udfs
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

  protected CreateFunctionStmtBase(Function fn, HdfsURI location, boolean ifNotExists,
      HashMap<OptArg, String> optArgs) {
    fn_ = fn;
    fn_.setLocation(location);
    ifNotExists_ = ifNotExists;
    optArgs_ = optArgs;
  }

  public String getComment() { return optArgs_.get(OptArg.COMMENT); }
  public boolean getIfNotExists() { return ifNotExists_; }

  protected TCreateFunctionParams toThrift() {
    TCreateFunctionParams params = new TCreateFunctionParams();
    params.setFn_name(new TFunctionName(fn_.dbName(), fn_.functionName()));
    params.setFn_binary_type(fn_.getBinaryType());

    params.setLocation(fn_.getLocation().toString());
    List<TPrimitiveType> types = Lists.newArrayList();
    if (fn_.getNumArgs() > 0) {
      for (PrimitiveType t: fn_.getArgs()) {
        types.add(t.toThrift());
      }
    }
    params.setArg_types(types);

    params.setRet_type(fn_.getReturnType().toThrift());
    params.setComment(getComment());
    params.setIf_not_exists(getIfNotExists());
    return params;
  }

  // Returns true if the symbol fnName exists in the function's binary.
  protected boolean symbolExists(String symbol) {
    // TODO: implement this
    return true;
  }

  protected void reportSymbolNotFound(String symbol)
      throws AnalysisException {
    throw new AnalysisException("Could not find symbol '" + symbol +
        "' in: " + fn_.getLocation().getLocation());
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
  public void analyze(Analyzer analyzer)
      throws AnalysisException, AuthorizationException {
    // Validate function name is legal
    fn_.getName().analyze(analyzer);

    // Validate DB is legal
    String dbName = analyzer.getTargetDbName(fn_.getName());
    fn_.getName().setDb(dbName);
    if (analyzer.getCatalog().getDb(
        dbName, analyzer.getUser(), Privilege.CREATE) == null) {
      throw new AnalysisException(Analyzer.DB_DOES_NOT_EXIST_ERROR_MSG + dbName);
    }
    if (analyzer.getCatalog().getFunction(fn_, true) != null && !ifNotExists_) {
      throw new AnalysisException(Analyzer.FN_ALREADY_EXISTS_ERROR_MSG +
          fn_.signatureString());
    }

    fn_.getLocation().analyze(analyzer, Privilege.CREATE);

    // Check the file type from the binary type to infer the type of the UDA
    fn_.setBinaryType(getBinaryType());
  }
}
