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

package org.apache.impala.calcite.operators;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.impala.catalog.AggregateFunction;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.FeCatalog;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.Function;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The ImpalaOperatorTable is used to hold all the operators and to resolve
 * the functions that are being validated.
 *
 * The main method used to resolve the operators is lookupOperatorOverloads.
 * The method mutates the operatorList and fills it with the matching operator.
 * If Calcite contains an operator that matches the name passed in, the Calcite
 * operator is the one that is used. It is preferable to use the Calcite operator
 * since Calcite performs optimizations based on the operator class.
 *
 * If a Calcite operator is not found, we check the Impala functions to see if
 * the function name is known by Impala. If so, an ImpalaOperator class is generated
 * on the fly.
 *
 * There are some special functions where we would prefer to use the Impala operator
 * over the Calcite operator due to some incompatibility. These are specified in
 * USE_IMPALA_OPERATOR.
 *
 * TODO: IMPALA-13095: Handle UDFs
 */
public class ImpalaOperatorTable extends ReflectiveSqlOperatorTable {
  protected static final Logger LOG =
      LoggerFactory.getLogger(ImpalaOperatorTable.class.getName());

  private final FeCatalog catalog_;
  private final FeDb db_;

  // Functions that have been used by the query. This is currently only used for
  // UDF functions. It is needed to see if the user implementing the query is
  // authorized to used the function.
  private final Set<Function> usedFunctions_;

  public static Set<String> USE_IMPALA_OPERATOR =
      ImmutableSet.<String> builder()
      .add("year")
      .add("month")
      .add("week")
      .add("day")
      .add("hour")
      .add("minute")
      .add("second")
      .add("millisecond")
      .add("dayofmonth")
      .add("dayofyear")
      .add("weekofyear")
      .add("corr")
      .add("covar_pop")
      .add("covar_samp")
      .add("coalesce")
      .add("lag")
      .add("trim")
      .add("extract")
      .add("regr_count")
      .add("localtime")
      .add("translate")
      .add("sleep")
      .add("user")
      .add("abs")
      .add("first_value")
      .add("last_value")
      .add("typeof")
      .build();

  private static ImpalaOperatorTable INSTANCE;

  private ImpalaOperatorTable(FeCatalog catalog, FeDb db, boolean trackFunctionsUsed) {
    catalog_ = catalog;
    db_ = (db == null) ? BuiltinsDb.getInstance() : db;
    usedFunctions_ = trackFunctionsUsed ? new HashSet<>() : null;
  }

  /**
   * lookupOperatorOverloads: See class comment above for details.
   */
  @Override
  public void lookupOperatorOverloads(SqlIdentifier opName, SqlFunctionCategory category,
      SqlSyntax syntax, List<SqlOperator> operatorList, SqlNameMatcher nameMatcher) {


    // The operatorList contains the found function. There is a separate operator table
    // for UDFs. If the function was found in the UDF, the operatorList will contain the
    // function and there is no need to add another one.
    if (operatorList.size() > 0) {
      return;
    }

    ImpalaCustomOperatorTable.instance().lookupOperatorOverloads(opName, category, syntax,
        operatorList, nameMatcher);

    if (operatorList.size() >= 1 || opName.isStar() || opName.names.size() > 2) {
      return;
    }

    String funcName =
        opName.names.size() == 1 ? opName.names.get(0) : opName.names.get(1);
    String dbName = opName.names.size() == 1 ? null : opName.names.get(0);

    String lowercaseFuncName = funcName.toLowerCase();

    // A little hack. We need our own version of "cast" when it is explicit. But
    // we need to use a different name for the function ("explicit_cast") and a
    // different type (SqlKind.OTHER instead of SqlKind.CAST) in order to avoid
    // conflict problems within Calcite processing. In order to find our operator,
    // we look for "cast" and use the "explicit_cast" name as defined in
    // ImpalaCastFunction
    if (lowercaseFuncName.equals("cast")) {
      operatorList.add(ImpalaCastFunction.INSTANCE);
      return;
    }

    if (!USE_IMPALA_OPERATOR.contains(lowercaseFuncName)) {
      // Check Calcite operator table for existence.
      SqlStdOperatorTable.instance().lookupOperatorOverloads(opName, category, syntax,
          operatorList, nameMatcher);
      Preconditions.checkState(operatorList.size() <= 1);
      if (operatorList.size() == 1) {
        return;
      }
    }

    // The catalog_ variable is null for the operator table that handles builtins. So
    // if the dbName is not null, just return here.  This case will be handled
    // by the operator table object that handles when the database is supplied and can
    // be fetched by catalog_.getDb(<dbName>)
    if (dbName != null && catalog_ == null) {
      return;
    }
    FeDb dbToUse = (dbName == null || catalog_== null) ? db_ : catalog_.getDb(dbName);
    if (dbToUse == null) {
      return;
    }

    // Check Impala Builtins for existence: TODO: IMPALA-13095: handle UDFs
    List<Function> functions;
    try {
      functions = dbToUse.getFunctions(lowercaseFuncName);
      if (functions.size() == 0) {
        return;
      }
    } catch (Exception e) {
      return;
    }

    if (usedFunctions_ != null) {
      usedFunctions_.add(functions.get(0));
    }

    SqlOperator impalaOp = (functions.get(0) instanceof AggregateFunction)
        ? new ImpalaAggOperator(dbToUse, funcName)
        : new ImpalaOperator(dbToUse, funcName);

    operatorList.add(impalaOp);
  }

  public Set<Function> getUsedFunctions() {
    return usedFunctions_;
  }

  public static synchronized void create() {
    if (INSTANCE != null) {
      return;
    }
    INSTANCE = create(null, BuiltinsDb.getInstance(), false);
  }

  public static ImpalaOperatorTable create(FeCatalog catalog, FeDb db,
      boolean trackFunctionsUsed) {
    return new ImpalaOperatorTable(catalog, db, trackFunctionsUsed);
  }

  public static ImpalaOperatorTable getInstance() {
    return INSTANCE;
  }
}
