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
import java.util.ArrayList;
import java.util.List;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TIcebergPartitionSpec;

/**
 * Represents the partitioning of a Iceberg table as defined in the PARTITIONED BY SPEC
 * clause of a CREATE TABLE statement. Iceberg supported kinds of partition.
 * Examples:
 * PARTITIONED BY SPEC
 * (
 * dt,
 * hour(event_time),
 * day(event_time),
 * month(event_time)
 * )
 */
public class IcebergPartitionSpec extends StmtNode {
  // Partition spec id from iceberg PartitionSpec
  private int specId_;

  private List<IcebergPartitionField> icebergPartitionFields_;

  public IcebergPartitionSpec(int partitionId, List<IcebergPartitionField> fields) {
    specId_ = partitionId;
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

  public int getSpecId() { return specId_; }

  public int getIcebergPartitionFieldsSize() {
    if (!hasPartitionFields()) return 0;
    return getIcebergPartitionFields().size();
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
      builder.append("\n  ");
      List<String> fieldsSql = new ArrayList<>();
      for (IcebergPartitionField field : icebergPartitionFields_) {
        fieldsSql.add(field.toSql());
      }
      builder.append(Joiner.on(",\n  ").join(fieldsSql));
    }
    builder.append("\n)");
    return builder.toString();
  }

  public TIcebergPartitionSpec toThrift() {
    TIcebergPartitionSpec result = new TIcebergPartitionSpec();
    result.setSpec_id(specId_);
    if (!hasPartitionFields()) return result;
    for (IcebergPartitionField field : icebergPartitionFields_) {
      result.addToPartition_fields(field.toThrift());
    }
    return result;
  }
}
