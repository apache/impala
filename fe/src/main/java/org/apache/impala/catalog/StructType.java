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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.impala.thrift.TColumnType;
import org.apache.impala.thrift.TTypeNode;
import org.apache.impala.thrift.TTypeNodeType;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Describes a STRUCT type. STRUCT types have a list of named struct fields.
 */
public class StructType extends Type {
  private final Map<String, StructField> fieldMap_ = new HashMap<>();
  private final List<StructField> fields_;

  public StructType(List<StructField> fields) {
    Preconditions.checkNotNull(fields);
    fields_ = fields;
    for (int i = 0; i < fields_.size(); ++i) {
      fields_.get(i).setPosition(i);
      fieldMap_.put(fields_.get(i).getName().toLowerCase(), fields_.get(i));
    }
  }

  public StructType() {
    fields_ = new ArrayList<>();
  }

  @Override
  public String toSql(int depth) {
    if (depth >= MAX_NESTING_DEPTH) return "STRUCT<...>";
    List<String> fieldsSql = new ArrayList<>();
    for (StructField f: fields_) fieldsSql.add(f.toSql(depth + 1));
    return String.format("STRUCT<%s>", Joiner.on(",").join(fieldsSql));
  }

  @Override
  protected String prettyPrint(int lpad) {
    String leftPadding = StringUtils.repeat(' ', lpad);
    List<String> fieldsSql = new ArrayList<>();
    for (StructField f: fields_) fieldsSql.add(f.prettyPrint(lpad + 2));
    return String.format("%sSTRUCT<\n%s\n%s>",
        leftPadding, Joiner.on(",\n").join(fieldsSql), leftPadding);
  }

  public void addField(StructField field) {
    field.setPosition(fields_.size());
    fields_.add(field);
    fieldMap_.put(field.getName().toLowerCase(), field);
  }

  public List<StructField> getFields() { return fields_; }

  public StructField getField(String fieldName) {
    return fieldMap_.get(fieldName.toLowerCase());
  }

  public void clearFields() {
    fields_.clear();
    fieldMap_.clear();
  }

  /**
   * Update field by parameter, currently we use this method to update Iceberg
   * tables' field.
   */
  public void updateFields(int pos, StructField field) {
    if (fields_.size() >= pos) {
      fieldMap_.put(field.getName(), field);
      fields_.set(pos, field);
    }
  }

  /**
   * The size of a struct slot is the sum of the size of its children. Don't have to
   * count for null indicators as they are not stored on the level of the struct slot,
   * instead it's on the topmost tuple's level.
   */
  @Override
  public int getSlotSize() {
    int size = 0;
    for (StructField structField : fields_) {
      size += structField.getType().getSlotSize();
    }
    return size;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof StructType)) return false;
    StructType otherStructType = (StructType) other;
    return otherStructType.getFields().equals(fields_);
  }

  @Override
  public int hashCode() {
    return fields_.hashCode();
  }

  @Override
  public void toThrift(TColumnType container) {
    TTypeNode node = new TTypeNode();
    container.types.add(node);
    Preconditions.checkNotNull(fields_);
    Preconditions.checkState(!fields_.isEmpty());
    node.setType(TTypeNodeType.STRUCT);
    node.setStruct_fields(new ArrayList<>());
    for (StructField field: fields_) {
      field.toThrift(container, node);
    }
  }
}
