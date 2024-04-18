/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.impala.calcite.operators;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql2rel.ReflectiveConvertletTable;
import org.apache.calcite.sql2rel.SqlRexConvertlet;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.impala.calcite.operators.ImpalaCustomOperatorTable;

/**
 *
 */
public class ImpalaConvertletTable extends ReflectiveConvertletTable {
  public static final ImpalaConvertletTable INSTANCE =
      new ImpalaConvertletTable();

  public ImpalaConvertletTable() {
    addAlias(ImpalaCustomOperatorTable.PERCENT_REMAINDER, SqlStdOperatorTable.MOD);
  }

  @Override
  public SqlRexConvertlet get(SqlCall call) {
    // If we were using Calcite's PERCENT_REMAINDER, the "addAlias" is defined
    // in StandardConvertletTable. But since the PERCENT_REMAINDER is overridden
    // with an Impala version (which derives the Impala return type), we need
    // to handle the alias in our own convertlet.
    if (call.getOperator().equals(ImpalaCustomOperatorTable.PERCENT_REMAINDER)) {
      return super.get(call);
    }

    return StandardConvertletTable.INSTANCE.get(call);
  }

}
