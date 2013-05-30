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

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.catalog.AuthorizationException;
import com.cloudera.impala.catalog.FileFormat;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TAlterTableParams;
import com.cloudera.impala.thrift.TAlterTableSetFileFormatParams;
import com.cloudera.impala.thrift.TAlterTableType;

/**
 * Represents an ALTER TABLE [PARTITION partitionSpec] SET FILEFORMAT statement.
 */
public class AlterTableSetFileFormatStmt extends AlterTableStmt {
  private final FileFormat fileFormat;
  private final PartitionSpec partitionSpec;

  public AlterTableSetFileFormatStmt(TableName tableName,
      PartitionSpec partitionSpec, FileFormat fileFormat) {
    super(tableName);
    this.fileFormat = fileFormat;
    this.partitionSpec = partitionSpec;
    if (partitionSpec != null) {
      partitionSpec.setTableName(tableName);
    }
  }

  public FileFormat getFileFormat() {
    return fileFormat;
  }

  public PartitionSpec getPartitionSpec() {
    return partitionSpec;
  }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.SET_FILE_FORMAT);
    TAlterTableSetFileFormatParams fileFormatParams =
        new TAlterTableSetFileFormatParams(fileFormat.toThrift());
    if (partitionSpec != null) {
      fileFormatParams.setPartition_spec(partitionSpec.toThrift());
    }
    params.setSet_file_format_params(fileFormatParams);
    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException,
      AuthorizationException {
    super.analyze(analyzer);
    // Altering the table, rather than the partition.
    if (partitionSpec == null) {
      return;
    }

    partitionSpec.setPartitionShouldExist();
    partitionSpec.setPrivilegeRequirement(Privilege.ALTER);
    partitionSpec.analyze(analyzer);
  }
}
