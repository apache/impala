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

import org.apache.impala.common.AnalysisException;

/**
 * Represents an ALTER TABLE tbl SET OWNER [USER|ROLE] owner statement.
 */
public class AlterTableSetOwnerStmt extends AlterTableOrViewSetOwnerStmt {
  public AlterTableSetOwnerStmt(TableName tableName, Owner owner) {
    super(tableName, owner);
  }

  @Override
  public String getOperation() { return "SET OWNER"; }

  @Override
  protected void validateType(TableRef tableRef) throws AnalysisException {
    if (tableRef instanceof InlineViewRef) {
      throw new AnalysisException(String.format(
          "ALTER TABLE not allowed on a view: %s", tableName_));
    }
  }
}