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

package org.apache.impala.catalog.paimon;

import java.util.Objects;

import org.apache.impala.catalog.StructField;
import org.apache.impala.catalog.Type;
import org.apache.impala.thrift.TColumnType;
import org.apache.impala.thrift.TStructField;
import org.apache.impala.thrift.TTypeNode;

/**
 * Represents a Paimon StructField.
 *
 * This class extends StructField with Paimon-specific field.
 * Paimon uses field IDs for schema evolution and compatibility, similar to Iceberg.
 * We keep field id by this class, so we can use field id to resolve column on backend.
 */
public class PaimonStructField extends StructField {
  private final int fieldId_;
  // False for required Paimon field, true for optional Paimon field
  private final boolean isNullable_;

  public PaimonStructField(String name, Type type, String comment, int fieldId) {
    this(name, type, comment, fieldId, true);
  }

  public PaimonStructField(
      String name, Type type, String comment, int fieldId, boolean isNullable) {
    super(name, type, comment);
    fieldId_ = fieldId;
    isNullable_ = isNullable;
  }

  public int getFieldId() { return fieldId_; }

  public boolean isNullable() { return isNullable_; }

  @Override
  public void toThrift(TColumnType container, TTypeNode node) {
    TStructField field = new TStructField();
    field.setName(name_);
    if (comment_ != null) field.setComment(comment_);
    field.setField_id(fieldId_);
    // Paimon-specific metadata - nullable and key properties could be added to
    // extended metadata if the Thrift definition supports it
    field.setIs_nullable(isNullable_);
    node.struct_fields.add(field);
    type_.toThrift(container);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof PaimonStructField)) return false;
    PaimonStructField otherStructField = (PaimonStructField) other;
    return otherStructField.name_.equals(name_) && otherStructField.type_.equals(type_)
        && otherStructField.fieldId_ == fieldId_
        && otherStructField.isNullable_ == isNullable_;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name_, type_, fieldId_, isNullable_);
  }

  @Override
  public String toString() {
    return String.format("PaimonStructField{name=%s, type=%s, fieldId=%d, nullable=%s}",
        name_, type_, fieldId_, isNullable_);
  }
}