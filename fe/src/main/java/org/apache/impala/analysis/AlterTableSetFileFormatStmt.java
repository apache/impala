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
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableSetFileFormatParams;
import org.apache.impala.thrift.TAlterTableType;
import org.apache.impala.thrift.THdfsFileFormat;

/**
 * Represents an ALTER TABLE [PARTITION partitionSet] SET FILEFORMAT statement.
 */
public class AlterTableSetFileFormatStmt extends AlterTableSetStmt {
  private final THdfsFileFormat fileFormat_;

  public AlterTableSetFileFormatStmt(TableName tableName,
      PartitionSet partitionSet, THdfsFileFormat fileFormat) {
    super(tableName, partitionSet);
    this.fileFormat_ = fileFormat;
  }

  public THdfsFileFormat getFileFormat() { return fileFormat_; }

  @Override
  public String getOperation() { return "SET FILEFORMAT"; }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.SET_FILE_FORMAT);
    TAlterTableSetFileFormatParams fileFormatParams =
        new TAlterTableSetFileFormatParams(fileFormat_);
    if (getPartitionSet() != null) {
      fileFormatParams.setPartition_set(getPartitionSet().toThrift());
    }
    params.setSet_file_format_params(fileFormatParams);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    FeTable tbl = getTargetTable();
    if (tbl instanceof FeKuduTable) {
      throw new AnalysisException("ALTER TABLE SET FILEFORMAT is not supported " +
          "on Kudu tables: " + tbl.getFullName());
    }

    if (tbl instanceof FeIcebergTable) {
      throw new AnalysisException("ALTER TABLE SET FILEFORMAT is not supported " +
          "on Iceberg tables: " + tbl.getFullName());
    }
  }
}
