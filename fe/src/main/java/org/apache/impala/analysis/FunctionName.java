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
import java.util.Objects;

import org.apache.commons.lang.StringUtils;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TFunctionName;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Class to represent a function name. Function names are specified as
 * db.function_name.
 */
public class FunctionName {
  // Only set for parsed function names.
  private final List<String> fnNamePath_;

  // Set/validated during analysis.
  private String db_;
  private String fn_;
  private boolean isBuiltin_ = false;
  private boolean isAnalyzed_ = false;

  /**
   * C'tor for parsed function names. The function names could be invalid. The validity
   * is checked during analysis.
   */
  public FunctionName(List<String> fnNamePath) {
    fnNamePath_ = fnNamePath;
  }

  public FunctionName(String dbName, String fn) {
    db_ = (dbName != null) ? dbName.toLowerCase() : null;
    fn_ = fn.toLowerCase();
    fnNamePath_ = null;
  }

  public FunctionName(String fn) {
    this(null, fn);
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

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), db_.toLowerCase(), fn_.toLowerCase());
  }

  public String getDb() { return db_; }
  public void setDb(String db) { db_ = db; }
  public String getFunction() { return fn_; }
  public void setFunction(String fn) { fn_ = fn; }
  public boolean isFullyQualified() { return db_ != null; }
  public boolean isBuiltin() { return isBuiltin_; }
  public List<String> getFnNamePath() { return fnNamePath_; }

  @Override
  public String toString() {
    // The fnNamePath_ is not always set.
    if (!isAnalyzed_ && fnNamePath_ != null) return Joiner.on(".").join(fnNamePath_);
    if (db_ == null || isBuiltin_) return fn_;
    return db_ + "." + fn_;
  }

  public void analyze(Analyzer analyzer) throws AnalysisException {
    analyze(analyzer, true);
  }

  /**
   * Path resolution happens as follows.
   *
   * Fully-qualified function name:
   * - Set the database name to the database name specified.
   *
   * Non-fully-qualified function name:
   * - When preferBuiltinsDb is true:
   *   - If the function name specified has the same name as a built-in function,
   *     set the database name to _impala_builtins.
   *   - Else if the query option of 'FALLBACK_DB_FOR_FUNCTIONS' being set and the
   *     function exists in fallback database, set the database name to fallback
   *     database.
   *   - Else, set the database name to the current session DB name.
   * - When preferBuiltinsDb is false: set the database name to current session DB name.
   *   For CREATE/DROP FUNCTION and GRANT/REVOKE <privilege> ON USER_DEFINED_FN
   *   statements, preferBuiltinsDb is false.
   */
  public void analyze(Analyzer analyzer, boolean preferBuiltinsDb)
      throws AnalysisException {
    if (isAnalyzed_) return;
    analyzeFnNamePath();
    if (fn_.isEmpty()) throw new AnalysisException("Function name cannot be empty.");
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
    Db builtinDb = BuiltinsDb.getInstance();
    if (!isFullyQualified()) {
      if (preferBuiltinsDb && builtinDb.containsFunction(fn_)) {
        db_ = BuiltinsDb.NAME;
      } else if (preferBuiltinsDb && fallbackDbContainsFn(analyzer)) {
        db_ = analyzer.getFallbackDbForFunctions();
      } else {
        db_ = analyzer.getDefaultDb();
      }
    }
    Preconditions.checkNotNull(db_);
    isBuiltin_ = db_.equals(BuiltinsDb.NAME) &&
        builtinDb.containsFunction(fn_);
    isAnalyzed_ = true;
  }

  private boolean fallbackDbContainsFn(Analyzer analyzer) throws AnalysisException {
    String dbName = analyzer.getFallbackDbForFunctions();
    if (StringUtils.isEmpty(dbName)) {
      return false;
    }
    // Execute a UDF of the fallback database in a SELECT statement, the requesting user
    // has be to granted a) the SELECT privilege on the UDF, and b) any one of the
    // INSERT, REFRESH, SELECT privileges on the fallback database.
    analyzer.registerPrivReq(builder ->
        builder.allOf(Privilege.SELECT).onFunction(dbName, getFunction()).build());
    FeDb feDb = analyzer.getDb(dbName, Privilege.VIEW_METADATA, false);
    return feDb != null && feDb.containsFunction(fn_);
  }

  private void analyzeFnNamePath() throws AnalysisException {
    if (fnNamePath_ == null) return;
    if (fnNamePath_.size() > 2 || fnNamePath_.isEmpty()) {
      throw new AnalysisException(
          String.format("Invalid function name: '%s'. Expected [dbname].funcname.",
              Joiner.on(".").join(fnNamePath_)));
    } else if (fnNamePath_.size() > 1) {
      db_ = fnNamePath_.get(0).toLowerCase();
      fn_ = fnNamePath_.get(1).toLowerCase();
    } else {
      Preconditions.checkState(fnNamePath_.size() == 1);
      fn_ = fnNamePath_.get(0).toLowerCase();
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

  public static String thriftToString(TFunctionName fnName) {
    return fromThrift(fnName).toString();
  }
}
