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

import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * Base class for ALTER TABLE statements that work against Partition specs
 */
public abstract class AlterTablePartitionSpecStmt extends AlterTableStmt {
  protected final List<PartitionKeyValue> partitionSpec;

  // The value Hive is configured to use for NULL partition key values.
  // Set during analysis.
  private String nullPartitionKeyValue;

  public AlterTablePartitionSpecStmt(TableName tableName,
      List<PartitionKeyValue> partitionSpec) {
    super(tableName);
    Preconditions.checkState(partitionSpec != null && partitionSpec.size() > 0);
    this.partitionSpec = Lists.newArrayList(partitionSpec);
  }

  public List<PartitionKeyValue> getPartitionSpec() {
    return partitionSpec;
  }

  // The value Hive is configured to use for NULL partition key values.
  // Set during analysis.
  protected String getNullPartitionKeyValue() {
    Preconditions.checkNotNull(nullPartitionKeyValue);
    return nullPartitionKeyValue;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);
    Table table = getTargetTable();
    String tableName = getDb() + "." + getTbl();

    // Make sure the target table is partitioned.
    if (table.getMetaStoreTable().getPartitionKeysSize() == 0) {
      throw new AnalysisException("Table is not partitioned: " + tableName);
    }

    // Make sure static partition key values only contain constant exprs.
    for (PartitionKeyValue kv: partitionSpec) {
      kv.analyze(analyzer);
    }

    // Get all keys in the target table.
    Set<String> targetPartitionKeys = Sets.newHashSet();
    for (FieldSchema fs: table.getMetaStoreTable().getPartitionKeys()) {
      targetPartitionKeys.add(fs.getName().toLowerCase());
    }

    // All partition keys need to be specified.
    if (targetPartitionKeys.size() != partitionSpec.size()) {
      throw new AnalysisException(String.format("Items in partition spec must exactly " +
          "match the partition columns in the table definition: %s (%d vs %d)",
          tableName, partitionSpec.size(), targetPartitionKeys.size()));
    }

    Set<String> keyNames = Sets.newHashSet();
    // Validate each partition key/value specified, ensuring a matching partition column
    // exists in the target table, no duplicate keys were specified, and that all the
    // column types are compatible.
    for (PartitionKeyValue pk: partitionSpec) {
      if (!keyNames.add(pk.getColName().toLowerCase())) {
        throw new AnalysisException("Duplicate partition key name: " + pk.getColName());
      }

      Column c = table.getColumn(pk.getColName());
      if (c == null) {
        throw new AnalysisException(String.format(
            "Partition column '%s' not found in table: %s", pk.getColName(), tableName));
      } else if (!targetPartitionKeys.contains(pk.getColName().toLowerCase())) {
        throw new AnalysisException(String.format(
            "Column '%s' is not a partition column in table: %s",
             pk.getColName(), tableName));
      } else if (pk.getValue() instanceof NullLiteral) {
        // No need for further analysis checks of this partition key value.
        continue;
      }

      PrimitiveType colType = c.getType();
      PrimitiveType literalType = pk.getValue().getType();
      PrimitiveType compatibleType =
          PrimitiveType.getAssignmentCompatibleType(colType, literalType);
      if (!compatibleType.isValid()) {
        throw new AnalysisException(String.format("Target table not compatible.\n" +
            "Incompatible types '%s' and '%s' in column '%s'", colType.toString(),
            literalType.toString(), pk.getColName()));
      }
      // Check for loss of precision with the partition value
      if (compatibleType != colType) {
        throw new AnalysisException(
            String.format("Partition key value may result in loss of precision.\n" +
            "Would need to cast '%s' to '%s' for partition column: %s",
            pk.getValue().toSql(), colType.toString(), pk.getColName()));
      }
    }
    // Only HDFS tables are partitioned.
    Preconditions.checkState(table instanceof HdfsTable);
    HdfsTable hdfsTable = (HdfsTable) table;
    nullPartitionKeyValue = hdfsTable.getNullPartitionKeyValue();
  }
}
