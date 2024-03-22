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
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.apache.impala.calcite.functions.FunctionResolver;
import org.apache.impala.catalog.BuiltinsDb;
import org.apache.impala.catalog.Db;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
 * TODO: IMPALA-13095: Handle UDFs
 */
public class ImpalaOperatorTable extends ReflectiveSqlOperatorTable {
  protected static final Logger LOG =
      LoggerFactory.getLogger(ImpalaOperatorTable.class.getName());

  private static ImpalaOperatorTable INSTANCE;

  /**
   * lookupOperatorOverloads: See class comment above for details.
   */
  @Override
  public void lookupOperatorOverloads(SqlIdentifier opName, SqlFunctionCategory category,
      SqlSyntax syntax, List<SqlOperator> operatorList, SqlNameMatcher nameMatcher) {


    // Check Calcite operator table for existence.
    SqlStdOperatorTable.instance().lookupOperatorOverloads(opName, category, syntax,
        operatorList, nameMatcher);
    Preconditions.checkState(operatorList.size() <= 1);
    if (operatorList.size() == 1) {
      return;
    }

    // There shouldn't be more than one opName with our usage, so throw an exception
    // if this happens.
    if (opName.names.size() > 1) {
      throw new RuntimeException("Cannot handle identifier with more than one name: " +
          opName);
    }

    // Check Impala Builtins for existence: TODO: IMPALA-13095: handle UDFs
    if (!BuiltinsDb.getInstance().containsFunction(opName.getSimple())) {
      return;
    }

    operatorList.add(new ImpalaOperator(opName.getSimple()));
  }

  public static synchronized void create(Db db) {
    if (INSTANCE != null) {
      return;
    }
    INSTANCE = new ImpalaOperatorTable();
  }

  public static ImpalaOperatorTable getInstance() {
    return INSTANCE;
  }
}
