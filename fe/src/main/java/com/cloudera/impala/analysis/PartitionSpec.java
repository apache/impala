package com.cloudera.impala.analysis;

import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.HdfsTable;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.thrift.TPartitionKeyValue;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/*
 * Represents a partition spec - a collection of partition key/values.
 */
public class PartitionSpec implements ParseNode {
  private final ImmutableList<PartitionKeyValue> partitionSpec_;
  private TableName tableName_;
  private Boolean partitionShouldExist_;
  private Privilege privilegeRequirement_;

  // Flag to determine if the partition already exists in the target table.
  // Set during analysis.
  private Boolean partitionExists_;

 // The value Hive is configured to use for NULL partition key values.
 // Set during analysis.
 private String nullPartitionKeyValue_;

  public PartitionSpec(List<PartitionKeyValue> partitionSpec) {
    this.partitionSpec_ = ImmutableList.copyOf(partitionSpec);
  }

  public List<PartitionKeyValue> getPartitionSpecKeyValues() {
    return partitionSpec_;
  }

  public String getTbl() { return tableName_.getTbl(); }
  public void setTableName(TableName tableName) { this.tableName_ = tableName; }
  public boolean partitionExists() {
    Preconditions.checkNotNull(partitionExists_);
    return partitionExists_;
  }

  // The value Hive is configured to use for NULL partition key values.
  // Set during analysis.
  public String getNullPartitionKeyValue() {
    Preconditions.checkNotNull(nullPartitionKeyValue_);
    return nullPartitionKeyValue_;
  }

  // If set, an additional analysis check will be performed to validate the target table
  // contains the given partition spec.
  public void setPartitionShouldExist() { partitionShouldExist_ = Boolean.TRUE; }

  // If set, an additional analysis check will be performed to validate the target table
  // does not contain the given partition spec.
  public void setPartitionShouldNotExist() { partitionShouldExist_ = Boolean.FALSE; }

  // Set the privilege requirement for this partition spec. Must be set prior to
  // analysis.
  public void setPrivilegeRequirement(Privilege privilege) {
    privilegeRequirement_ = privilege;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    Preconditions.checkNotNull(tableName_);
    Preconditions.checkNotNull(privilegeRequirement_);

    // Skip adding an audit event when analyzing partitions. The parent table should
    // be audited outside of the PartitionSpec.
    Table table = analyzer.getTable(tableName_, privilegeRequirement_, false);
    String tableName = table.getDb().getName() + "." + getTbl();

    // Make sure the target table is partitioned.
    if (table.getMetaStoreTable().getPartitionKeysSize() == 0) {
      throw new AnalysisException("Table is not partitioned: " + tableName);
    }

    // Make sure static partition key values only contain constant exprs.
    for (PartitionKeyValue kv: partitionSpec_) {
      kv.analyze(analyzer);
    }

    // Get all keys in the target table.
    Set<String> targetPartitionKeys = Sets.newHashSet();
    for (FieldSchema fs: table.getMetaStoreTable().getPartitionKeys()) {
      targetPartitionKeys.add(fs.getName().toLowerCase());
    }

    // All partition keys need to be specified.
    if (targetPartitionKeys.size() != partitionSpec_.size()) {
      throw new AnalysisException(String.format("Items in partition spec must exactly " +
          "match the partition columns in the table definition: %s (%d vs %d)",
          tableName, partitionSpec_.size(), targetPartitionKeys.size()));
    }

    Set<String> keyNames = Sets.newHashSet();
    // Validate each partition key/value specified, ensuring a matching partition column
    // exists in the target table, no duplicate keys were specified, and that all the
    // column types are compatible.
    for (PartitionKeyValue pk: partitionSpec_) {
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

      Type colType = c.getType();
      Type literalType = pk.getValue().getType();
      Type compatibleType =
          Type.getAssignmentCompatibleType(colType, literalType, false);
      if (!compatibleType.isValid()) {
        throw new AnalysisException(String.format("Value of partition spec (column=%s) "
            + "has incompatible type: '%s'. Expected type: '%s'.",
            pk.getColName(), literalType, colType));
      }
      // Check for loss of precision with the partition value
      if (!compatibleType.equals(colType)) {
        throw new AnalysisException(
            String.format("Partition key value may result in loss of precision.\n" +
            "Would need to cast '%s' to '%s' for partition column: %s",
            pk.getValue().toSql(), colType.toString(), pk.getColName()));
      }
    }
    // Only HDFS tables are partitioned.
    Preconditions.checkState(table instanceof HdfsTable);
    HdfsTable hdfsTable = (HdfsTable) table;
    nullPartitionKeyValue_ = hdfsTable.getNullPartitionKeyValue();

    partitionExists_ = hdfsTable.getPartition(partitionSpec_) != null;
    if (partitionShouldExist_ != null) {
      if (partitionShouldExist_ && !partitionExists_) {
          throw new AnalysisException("Partition spec does not exist: (" +
              Joiner.on(", ").join(partitionSpec_) + ").");
      } else if (!partitionShouldExist_ && partitionExists_) {
          throw new AnalysisException("Partition spec already exists: (" +
              Joiner.on(", ").join(partitionSpec_) + ").");
      }
    }
  }

  /*
   * Returns the Thrift representation of this PartitionSpec.
   */
  public List<TPartitionKeyValue> toThrift() {
    List<TPartitionKeyValue> thriftPartitionSpec = Lists.newArrayList();
    for (PartitionKeyValue kv: partitionSpec_) {
      String value = PartitionKeyValue.getPartitionKeyValueString(
          kv.getLiteralValue(),  getNullPartitionKeyValue());
      thriftPartitionSpec.add(new TPartitionKeyValue(kv.getColName(), value));
    }
    return thriftPartitionSpec;
  }

  @Override
  public String toSql() {
    List<String> partitionSpecStr = Lists.newArrayList();
    for (PartitionKeyValue kv: partitionSpec_) {
      partitionSpecStr.add(kv.getColName() + "=" + kv.getValue().toSql());
    }
    return String.format("PARTITION (%s)", Joiner.on(", ").join(partitionSpecStr));
  }
}
