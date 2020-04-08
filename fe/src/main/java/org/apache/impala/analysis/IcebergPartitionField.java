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

import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TIcebergPartitionField;
import org.apache.impala.thrift.TIcebergPartitionTransform;

/**
 * Represents a PartitionField of iceberg
 */
public class IcebergPartitionField extends StmtNode {
  // The id of the source field in iceberg table Schema, you can get these source
  // fields by Schema.columns(), the return type is List<NestedField>.
  private int sourceId_;

  // The field id from Iceberg PartitionField, which across all the table
  // metadata's partition specs
  private int fieldId_;

  //Column name
  private String fieldName_;

  //Column partition type
  private TIcebergPartitionTransform fieldType_;

  public IcebergPartitionField(int sourceId, int fieldId, String fieldName,
                               TIcebergPartitionTransform fieldType) {
    sourceId_ = sourceId;
    fieldId_ = fieldId;
    fieldName_ = fieldName;
    fieldType_ = fieldType;
  }

  public IcebergPartitionField(String fieldName, TIcebergPartitionTransform fieldType) {
    this(0, 0, fieldName, fieldType);
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    return;
  }

  @Override
  public final String toSql() {
    return toSql(ToSqlOptions.DEFAULT);
  }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder builder = new StringBuilder();
    builder.append(fieldName_ + " " + fieldType_.toString());
    return builder.toString();
  }

  public TIcebergPartitionField toThrift() {
    TIcebergPartitionField result = new TIcebergPartitionField();
    result.setField_id(fieldId_);
    result.setSource_id(sourceId_);
    result.setField_name(fieldName_);
    result.setField_type(fieldType_);
    return result;
  }
}
