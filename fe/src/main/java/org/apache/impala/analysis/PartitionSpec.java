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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TPartitionKeyValue;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Represents a partition spec - a collection of partition key/values.
 */
public class PartitionSpec extends PartitionSpecBase {
  private final ImmutableList<PartitionKeyValue> partitionSpec_;

  // Flag to determine if the partition already exists in the target table.
  // Set during analysis.
  private Boolean partitionExists_;

  public PartitionSpec(List<PartitionKeyValue> partitionSpec) {
    this.partitionSpec_ = ImmutableList.copyOf(partitionSpec);
  }

  public List<PartitionKeyValue> getPartitionSpecKeyValues() { return partitionSpec_; }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    super.analyze(analyzer);

    // Make sure static partition key values only contain constant exprs.
    for (PartitionKeyValue kv: partitionSpec_) {
      kv.analyze(analyzer);
    }

    // Get all keys in the target table.
    Set<String> targetPartitionKeys = Sets.newHashSet();
    for (FieldSchema fs: table_.getMetaStoreTable().getPartitionKeys()) {
      targetPartitionKeys.add(fs.getName().toLowerCase());
    }

    // All partition keys need to be specified.
    if (targetPartitionKeys.size() != partitionSpec_.size()) {
      throw new AnalysisException(String.format("Items in partition spec must exactly " +
          "match the partition columns in the table definition: %s (%d vs %d)",
          tableName_, partitionSpec_.size(), targetPartitionKeys.size()));
    }

    Set<String> keyNames = Sets.newHashSet();
    // Validate each partition key/value specified, ensuring a matching partition column
    // exists in the target table, no duplicate keys were specified, and that all the
    // column types are compatible.
    for (PartitionKeyValue pk: partitionSpec_) {
      if (!keyNames.add(pk.getColName().toLowerCase())) {
        throw new AnalysisException("Duplicate partition key name: " + pk.getColName());
      }

      Column c = table_.getColumn(pk.getColName());
      if (c == null) {
        throw new AnalysisException(String.format(
            "Partition column '%s' not found in table: %s", pk.getColName(), tableName_));
      } else if (!targetPartitionKeys.contains(pk.getColName().toLowerCase())) {
        throw new AnalysisException(String.format(
            "Column '%s' is not a partition column in table: %s",
             pk.getColName(), tableName_));
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
    partitionExists_ = table_.getPartition(partitionSpec_) != null;
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

  /**
   * Utility method that returns the concatenated string of key=value pairs ordered by
   * key. Since analyze() ensures that there are no duplicate keys in partition specs,
   * this method provides a uniquely comparable string representation for this object.
   */
  public String toCanonicalString() {
    List<PartitionKeyValue> sortedPartitionSpec = Lists.newArrayList(partitionSpec_);
    Collections.sort(sortedPartitionSpec, PartitionKeyValue.getColNameComparator());
    return Joiner.on(",").join(sortedPartitionSpec);
  }
}
