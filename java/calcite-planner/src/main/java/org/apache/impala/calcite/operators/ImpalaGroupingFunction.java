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

package org.apache.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

/**
 * Grouping operator for Impala
 *
 * We use the Calcite operator for the grouping function (deriving from
 * SqlGroupingFunction) since Calcite understands known operator types.
 * However, we can't use the SqlGroupingFunction directly because
 * Calcite's version returns a BIGINT type. We override this in the
 * getReturnTypeInference.
 *
 * One unfortunate hack is that this had to be declared in the same package
 * as the SqlGroupingFunction package (org.apache.calcite.sql.fun) since the
 * constructor there is protected.
 */
public class ImpalaGroupingFunction extends SqlGroupingFunction {

  public ImpalaGroupingFunction() {
    super("GROUPING");
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.TINYINT;
  }

  @Override
  public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    return getReturnTypeInference().inferReturnType(opBinding);
  }
}
