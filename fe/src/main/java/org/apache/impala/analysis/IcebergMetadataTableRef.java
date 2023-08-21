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

import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.catalog.iceberg.IcebergMetadataTable;
import org.apache.impala.common.AnalysisException;

import com.google.common.base.Preconditions;

/**
 * TableRef class for Iceberg metadata tables.
 */
public class IcebergMetadataTableRef extends TableRef {

  private String metadataTableName_;

  public IcebergMetadataTableRef(TableRef tableRef, Path resolvedPath) {
    super(tableRef);
    Preconditions.checkState(resolvedPath.isResolved());
    Preconditions.checkState(resolvedPath.isRootedAtTable());
    Preconditions.checkState(resolvedPath.getRootTable() instanceof IcebergMetadataTable);
    resolvedPath_ = resolvedPath;
    IcebergMetadataTable iceMTbl = (IcebergMetadataTable)resolvedPath.getRootTable();
    FeIcebergTable iceTbl = iceMTbl.getBaseTable();
    metadataTableName_ = iceMTbl.getMetadataTableName();
    if (hasExplicitAlias()) return;
    aliases_ = new String[] {
      iceTbl.getTableName().toString().toLowerCase(),
      iceTbl.getName().toLowerCase()};
  }

  public String getMetadataTableName() {
    return metadataTableName_;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed_) return;
    IcebergMetadataTable rootTable = (IcebergMetadataTable)resolvedPath_.getRootTable();
    FeTable iceRootTable = rootTable.getBaseTable();
    analyzer.registerAuthAndAuditEvent(iceRootTable, priv_, requireGrantOption_);
    analyzeTimeTravel(analyzer);
    desc_ = analyzer.registerTableRef(this);
    isAnalyzed_ = true;
    analyzeHints(analyzer);
    analyzeJoin(analyzer);
  }

}
