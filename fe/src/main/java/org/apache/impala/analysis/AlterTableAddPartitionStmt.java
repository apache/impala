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
import com.google.common.base.Joiner;

import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeKuduTable;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TAlterTableAddPartitionParams;
import org.apache.impala.thrift.TAlterTableParams;
import org.apache.impala.thrift.TAlterTableSetFileFormatParams;
import org.apache.impala.thrift.TAlterTableType;
import org.apache.impala.thrift.THdfsFileFormat;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Represents an ALTER TABLE ADD PARTITION statement.
 */
public class AlterTableAddPartitionStmt extends AlterTableStmt {
  private final boolean ifNotExists_;
  private final List<PartitionDef> partitions_;

  private THdfsFileFormat fileFormat;

  public AlterTableAddPartitionStmt(TableName tableName,
      boolean ifNotExists, List<PartitionDef> partitions) {
    super(tableName);
    Preconditions.checkNotNull(partitions);
    Preconditions.checkState(!partitions.isEmpty());
    partitions_ = partitions;
    // If 'ifNotExists' is true, no error is raised if a partition with the same spec
    // already exists. If multiple partitions are specified, the statement will ignore
    // those that exist and add the rest.
    ifNotExists_ = ifNotExists;
    for (PartitionDef p: partitions_) {
      p.setTableName(tableName);
      if (!ifNotExists_) p.setPartitionShouldNotExist();
    }
  }

  public AlterTableAddPartitionStmt(TableName tableName,
      boolean ifNotExists, List<PartitionDef> partitions, THdfsFileFormat fileFormat) {
    this(tableName, ifNotExists, partitions);
    this.fileFormat = fileFormat;
  }

  public boolean getIfNotExists() { return ifNotExists_; }

  @Override
  public String getOperation() {
    StringBuilder sb = new StringBuilder("ADD ");
    if (ifNotExists_) sb.append("IF NOT EXISTS ");
    sb.append("PARTITION");
    return sb.toString();
  }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder sb = new StringBuilder("ALTER TABLE ");
    if (getDb() != null) sb.append(getDb() + ".");
    sb.append(getTbl()).append(" ADD");
    if (ifNotExists_) sb.append(" IF NOT EXISTS");
    for (PartitionDef p : partitions_) sb.append(" " + p.toSql(options));

    if (fileFormat != null) {
      sb.append(" SET FILEFORMAT ");
      sb.append(fileFormat);
    }

    return sb.toString();
  }

  @Override
  public TAlterTableParams toThrift() {
    TAlterTableAddPartitionParams addPartParams = new TAlterTableAddPartitionParams();
    addPartParams.setIf_not_exists(ifNotExists_);
    for (PartitionDef p: partitions_) addPartParams.addToPartitions(p.toThrift());
    TAlterTableParams params = super.toThrift();
    params.setAlter_type(TAlterTableType.ADD_PARTITION);
    params.setAdd_partition_params(addPartParams);

    if (fileFormat != null) {
      TAlterTableSetFileFormatParams fileFormatParams =
          new TAlterTableSetFileFormatParams();
      fileFormatParams.setFile_format(fileFormat);
      params.setSet_file_format_params(fileFormatParams);
    }

    return params;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    FeTable table = getTargetTable();
    if (table instanceof FeKuduTable) {
      throw new AnalysisException("ALTER TABLE ADD PARTITION is not supported for " +
          "Kudu tables: " + table.getTableName());
    } else if (table instanceof FeIcebergTable) {
      throw new AnalysisException("ALTER TABLE ADD PARTITION is not supported for " +
          "Iceberg tables: " + table.getTableName());
    }
    Set<String> partitionSpecs = new HashSet<>();
    for (PartitionDef p: partitions_) {
      p.analyze(analyzer);

      // Make sure no duplicate partition specs are specified
      if (!partitionSpecs.add(p.getPartitionSpec().toCanonicalString())) {
        throw new AnalysisException(String.format("Duplicate partition spec: (%s)",
            Joiner.on(", ").join(p.getPartitionSpec().getPartitionSpecKeyValues())));
      }
    }
  }

  @Override
  protected void checkTransactionalTable() throws AnalysisException {
  }
}
