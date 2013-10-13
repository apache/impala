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

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.Function;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.service.FeSupport;
import com.cloudera.impala.thrift.TCreateFunctionParams;
import com.cloudera.impala.thrift.TFunction;
import com.cloudera.impala.thrift.TFunctionBinaryType;
import com.cloudera.impala.thrift.TFunctionName;
import com.cloudera.impala.thrift.TSymbolLookupParams;
import com.cloudera.impala.thrift.TSymbolLookupResult;
import com.google.common.base.Preconditions;

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
    TFunction fn = new TFunction();
    fn.setFn_name(new TFunctionName(fn_.dbName(), fn_.functionName()));
    fn.setFn_binary_type(fn_.getBinaryType());
    fn.setLocation(fn_.getLocation().toString());
    fn.setArg_types(PrimitiveType.toThrift(fn_.getArgs()));
    fn.setRet_type(fn_.getReturnType().toThrift());
    fn.setHas_var_args(fn_.hasVarArgs());
    fn.setComment(getComment());

    TCreateFunctionParams params = new TCreateFunctionParams(fn);
    params.setIf_not_exists(getIfNotExists());
    return params;
  }

  // Returns the resolved symbol in the binary. The BE will do a lookup of 'symbol'
  // in the binary and try to resolve unmangled names.
  // If this function is expecting a return argument, retArgType is that type. It should
  // be null if this function isn't expecting a return argument.
  protected String lookupSymbol(String symbol, ColumnType retArgType, boolean hasVarArgs,
      ColumnType... argTypes) throws AnalysisException {
    if (symbol.length() == 0) reportSymbolNotFound(symbol);
    if (fn_.getBinaryType() == TFunctionBinaryType.HIVE) {
      // TODO: add this when hive udfs go in.
      return symbol;
    }
    Preconditions.checkState(fn_.getBinaryType() == TFunctionBinaryType.NATIVE ||
        fn_.getBinaryType() == TFunctionBinaryType.IR);

    TSymbolLookupParams lookup = new TSymbolLookupParams();
    lookup.location =  fn_.getLocation().toString();
    lookup.symbol = symbol;
    lookup.fn_binary_type = fn_.getBinaryType();
    lookup.arg_types = ColumnType.toThrift(argTypes);
    lookup.has_var_args = hasVarArgs;
    if (retArgType != null) lookup.setRet_arg_type(retArgType.toThrift());

    try {
      TSymbolLookupResult result = FeSupport.LookupSymbol(lookup);
      switch (result.result_code) {
        case SYMBOL_FOUND:
          return result.symbol;
        case BINARY_NOT_FOUND:
          throw new AnalysisException(
              "Could not load binary: " + fn_.getLocation().getLocation());
        case SYMBOL_NOT_FOUND:
          reportSymbolNotFound(symbol);
        default:
          // Should never get here.
          throw new AnalysisException("Internal Error");
      }
    } catch (InternalException e) {
      throw new AnalysisException("Could not find symbol.", e);
    }
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
    // For now, if authorization is enabled, the user needs ALL on the server
    // to create functions.
    // TODO: this is not the right granularity but acceptable for now.
    analyzer.getCatalog().checkCreateDropFunctionAccess(analyzer.getUser());

    // Validate function name is legal
    fn_.getFunctionName().analyze(analyzer);

    // Validate DB is legal
    String dbName = analyzer.getTargetDbName(fn_.getFunctionName());
    fn_.getFunctionName().setDb(dbName);
    if (analyzer.getCatalog().getDb(
        dbName, analyzer.getUser(), Privilege.CREATE) == null) {
      throw new AnalysisException(Analyzer.DB_DOES_NOT_EXIST_ERROR_MSG + dbName);
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
