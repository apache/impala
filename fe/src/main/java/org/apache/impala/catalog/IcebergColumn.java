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

package org.apache.impala.catalog;

import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TColumnDescriptor;

/**
 * Represents an Iceberg column.
 *
 * This class extends Column with the Iceberg-specific field id. Field ids are used in
 * schema evolution to uniquely identify columns. The Parquet writer is required to
 * set the 'field_id' metadata field for the written files. We only use field ids during
 * writing, so for now we only set the fields id for top-level columns (Impala Parquet
 * writer cannot write complex types).
 * TODO: add field ids for the nested fields and use them in schema resolution in the
 * Parquet reader.
 */
public class IcebergColumn extends Column {
  private final int fieldId_;
  // Keep key and value field id for column with Map type.
  private final int fieldMapKeyId_;
  private final int fieldMapValueId_;
  // False for required Iceberg field, true for optional Iceberg field
  private final boolean isNullable_;

  public IcebergColumn(String name, Type type, String comment, int position,
      int fieldId, int fieldMapKeyId, int fieldMapValueId, boolean isNullable) {
    super(name.toLowerCase(), type, comment, position);
    fieldId_ = fieldId;
    fieldMapKeyId_ = fieldMapKeyId;
    fieldMapValueId_ = fieldMapValueId;
    isNullable_ = isNullable;
  }

  public static IcebergColumn cloneWithNullability(IcebergColumn source,
      boolean isNullable) {
    return new IcebergColumn(source.name_, source.type_, source.comment_,
        source.position_, source.fieldId_, source.fieldMapKeyId_, source.fieldMapKeyId_,
        isNullable);
  }

  public int getFieldId() {
    return fieldId_;
  }

  public boolean isNullable() { return isNullable_; }

  @Override
  public TColumn toThrift() {
    TColumn tcol = super.toThrift();
    tcol.setIs_iceberg_column(true);
    tcol.setIceberg_field_id(fieldId_);
    tcol.setIceberg_field_map_key_id(fieldMapKeyId_);
    tcol.setIceberg_field_map_value_id(fieldMapValueId_);
    tcol.setIs_nullable(isNullable_);
    return tcol;
  }

  @Override
  public TColumnDescriptor toDescriptor() {
    TColumnDescriptor desc = super.toDescriptor();
    desc.setIcebergFieldId(fieldId_);
    desc.setIcebergFieldMapKeyId(fieldMapKeyId_);
    desc.setIcebergFieldMapValueId(fieldMapValueId_);
    return desc;
  }
}
