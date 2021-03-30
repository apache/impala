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

import java.util.List;

import org.apache.impala.common.AnalysisException;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.thrift.TIcebergPartitionSpec;

/**
 * Represents the partitioning of a Iceberg table as defined in the PARTITION BY SPEC
 * clause of a CREATE TABLE statement. Iceberg supported kinds of partition.
 * Examples:
 * PARTITION BY SPEC
 * (
 * dt identity,
 * event_time hour,
 * event_time day,
 * event_time month
 * )
 */
public class IcebergPartitionSpec extends StmtNode {
  // Partition id from iceberg PartitionSpec
  private int partitionId_;

  private List<IcebergPartitionField> icebergPartitionFields_;

  public IcebergPartitionSpec(int partitionId, List<IcebergPartitionField> fields) {
    partitionId_ = partitionId;
    icebergPartitionFields_ = fields;
  }

  public IcebergPartitionSpec(List<IcebergPartitionField> fields) {
    this(0, fields);
  }

  public List<IcebergPartitionField> getIcebergPartitionFields() {
    return icebergPartitionFields_;
  }

  public boolean hasPartitionFields() {
    return icebergPartitionFields_ != null && (!icebergPartitionFields_.isEmpty());
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (!hasPartitionFields()) return;
    for (IcebergPartitionField field : icebergPartitionFields_) {
      field.analyze(analyzer);
    }
  }

  @Override
  public final String toSql() {
    return toSql(ToSqlOptions.DEFAULT);
  }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder builder = new StringBuilder();
    builder.append("(");
    if (hasPartitionFields()) {
      builder.append("\n");
      for (IcebergPartitionField field : icebergPartitionFields_) {
        builder.append(String.format("  %s,\n", field.toSql()));
      }
    }
    builder.append(")");
    return builder.toString();
  }

  public TIcebergPartitionSpec toThrift() {
    TIcebergPartitionSpec result = new TIcebergPartitionSpec();
    result.setPartition_id(partitionId_);
    if (!hasPartitionFields()) return result;
    for (IcebergPartitionField field : icebergPartitionFields_) {
      result.addToPartition_fields(field.toThrift());
    }
    return result;
  }
}
