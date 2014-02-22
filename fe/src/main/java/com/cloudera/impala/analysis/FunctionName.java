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

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TFunctionName;

/**
 * Class to represent a function name. Function names are specified as
 * db.function_name.
 */
public class FunctionName {
  private String db_;
  private final String fn_;
  boolean isBuiltin_;

  public FunctionName(String dbName, String fn) {
    db_ = dbName;
    fn_ = fn.toLowerCase();
    if (db_ != null) db_ = db_.toLowerCase();
  }

  public FunctionName(String fn) {
    db_ = null;
    fn_ = fn.toLowerCase();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof FunctionName)) return false;
    FunctionName o = (FunctionName)obj;
    if ((db_ == null || o.db_ == null) && (db_ != o.db_)) {
      if (db_ == null && o.db_ != null) return false;
      if (db_ != null && o.db_ == null) return false;
      if (!db_.equalsIgnoreCase(o.db_)) return false;
    }
    return fn_.equalsIgnoreCase(o.fn_);
  }

  public FunctionName(TFunctionName thriftName) {
    db_ = thriftName.db_name.toLowerCase();
    fn_ = thriftName.function_name.toLowerCase();
  }

  public String getDb() { return db_; }
  public String getFunction() { return fn_; }
  public boolean isFullyQualified() { return db_ != null; }
  public boolean isBuiltin() { return isBuiltin_; }

  @Override
  public String toString() {
    if (db_ == null || isBuiltin_) return fn_;
    return db_ + "." + fn_;
  }

  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (fn_.length() == 0) {
      throw new AnalysisException("Function name can not be empty.");
    }
    for (int i = 0; i < fn_.length(); ++i) {
      if (!isValidCharacter(fn_.charAt(i))) {
        throw new AnalysisException(
            "Function names must be all alphanumeric or underscore. " +
            "Invalid name: " + fn_);
      }
    }
    if (Character.isDigit(fn_.charAt(0))) {
      throw new AnalysisException("Function cannot start with a digit: " + fn_);
    }

    // Resolve the database for this function.
    if (!isFullyQualified()) {
      Db builtinDb = analyzer.getCatalog().getBuiltinsDb();
      if (builtinDb.containsFunction(fn_)) {
        // If it isn't fully qualified and is the same name as a builtin, use
        // the builtin.
        db_ = Catalog.BUILTINS_DB;
        isBuiltin_ = true;
      } else {
        db_ = analyzer.getDefaultDb();
        isBuiltin_ = false;
      }
    } else {
      isBuiltin_ = db_.equals(Catalog.BUILTINS_DB);
    }
  }

  private boolean isValidCharacter(char c) {
    return Character.isLetterOrDigit(c) || c == '_';
  }

  public TFunctionName toThrift() {
    TFunctionName name = new TFunctionName(fn_);
    name.setDb_name(db_);
    return name;
  }

  public static FunctionName fromThrift(TFunctionName fnName) {
    return new FunctionName(fnName.getDb_name(), fnName.getFunction_name());
  }
}
