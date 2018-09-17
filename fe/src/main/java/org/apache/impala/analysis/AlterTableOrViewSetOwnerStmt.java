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

import com.google.common.base.Preconditions;
import org.apache.impala.authorization.Privilege;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TAlterTableOrViewSetOwnerParams;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableType;
import org.apache.impala.util.MetaStoreUtil;

/**
 * A base class for ALTER TABLE/VIEW SET OWNER.
 */
public abstract class AlterTableOrViewSetOwnerStmt extends AlterTableStmt {
  protected final Owner owner_;

  public AlterTableOrViewSetOwnerStmt(TableName tableName, Owner owner) {
    super(tableName);
    Preconditions.checkNotNull(owner);
    owner_ = owner;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    String ownerName = owner_.getOwnerName();
    if (ownerName.length() > MetaStoreUtil.MAX_OWNER_LENGTH) {
      throw new AnalysisException(String.format("Owner name exceeds maximum length of " +
          "%d characters. The given owner name has %d characters.",
          MetaStoreUtil.MAX_OWNER_LENGTH, ownerName.length()));
    }
    tableName_ = analyzer.getFqTableName(tableName_);
    // Require ALL with GRANT OPTION privilege.
    TableRef tableRef = new TableRef(tableName_.toPath(), null, Privilege.ALL,
        /* grant option */ true);
    tableRef = analyzer.resolveTableRef(tableRef);
    Preconditions.checkNotNull(tableRef);
    tableRef.analyze(analyzer);
    validateType(tableRef);
  }

  /**
   * Validates the type of the given TableRef.
   */
  protected abstract void validateType(TableRef tableRef) throws AnalysisException;

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = new TAlterTableParams();
    params.setTable_name(tableName_.toThrift());
    TAlterTableOrViewSetOwnerParams ownerParams = new TAlterTableOrViewSetOwnerParams();
    ownerParams.setOwner_type(owner_.getOwnerType());
    ownerParams.setOwner_name(owner_.getOwnerName());
    params.setAlter_type(TAlterTableType.SET_OWNER);
    params.setSet_owner_params(ownerParams);
    return params;
  }
}
