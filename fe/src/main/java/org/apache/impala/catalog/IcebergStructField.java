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

import java.util.Objects;

import org.apache.impala.thrift.TColumnType;
import org.apache.impala.thrift.TStructField;
import org.apache.impala.thrift.TTypeNode;

/**
 * Represents an Iceberg NestedField.
 *
 * This class extends StructField with the Iceberg-specific field id. We keep field id
 * by this class, so we can use field id to resolve column on backend. Now we can use
 * 'set PARQUET_FALLBACK_SCHEMA_RESOLUTION=FIELD_ID' or
 * 'set PARQUET_FALLBACK_SCHEMA_RESOLUTION=2' to choose field id resolving.
 */
public class IcebergStructField extends StructField {
  private final int fieldId_;

  public IcebergStructField(String name, Type type, String comment, int fieldId) {
    super(name, type, comment);
    fieldId_ = fieldId;
  }

  @Override
  public void toThrift(TColumnType container, TTypeNode node) {
    TStructField field = new TStructField();
    field.setName(name_);
    if (comment_ != null) field.setComment(comment_);
    field.setField_id(fieldId_);
    node.struct_fields.add(field);
    type_.toThrift(container);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof IcebergStructField)) return false;
    IcebergStructField otherStructField = (IcebergStructField) other;
    return otherStructField.name_.equals(name_) && otherStructField.type_.equals(type_)
        && (otherStructField.fieldId_ == fieldId_);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name_, type_, fieldId_);
  }
}
