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

import org.apache.commons.lang3.StringUtils;

import org.apache.impala.thrift.TColumnType;
import org.apache.impala.thrift.TStructField;
import org.apache.impala.thrift.TTypeNode;

import java.util.Objects;

/**
 * TODO: Support comments for struct fields. The Metastore does not properly store
 * comments of struct fields. We set comment_ to null to avoid compatibility issues.
 */
public class StructField {
  protected final String name_;
  protected final Type type_;
  protected final String comment_;
  protected int position_;  // in struct
  // True, if the field shouldn't be included in star expansion.
  protected boolean isHidden_ = false;

  public StructField(String name, Type type, String comment, boolean isHidden) {
    // Impala expects field names to be in lower case, but type strings stored in the HMS
    // are not guaranteed to be lower case.
    name_ = name.toLowerCase();
    type_ = type;
    comment_ = comment;
    isHidden_ = isHidden;
  }

  public StructField(String name, Type type, String comment) {
    this(name, type, comment, false);
  }

  public StructField(String name, Type type) {
    this(name, type, null);
  }

  public String getComment() { return comment_; }
  public String getName() { return name_; }
  public Type getType() { return type_; }
  public int getPosition() { return position_; }
  public void setPosition(int position) { position_ = position; }
  public boolean isHidden() { return isHidden_; }

  public String toSql(int depth) {
    String typeSql = (depth < Type.MAX_NESTING_DEPTH) ? type_.toSql(depth) : "...";
    StringBuilder sb = new StringBuilder(name_);
    if (type_ != null) sb.append(":" + typeSql);
    if (comment_ != null) sb.append(String.format(" COMMENT '%s'", comment_));
    return sb.toString();
  }

  /**
   * Pretty prints this field with lpad number of leading spaces.
   * Calls prettyPrint(lpad) on this field's type.
   */
  public String prettyPrint(int lpad) {
    String leftPadding = StringUtils.repeat(' ', lpad);
    StringBuilder sb = new StringBuilder(leftPadding + name_);
    if (type_ != null) {
      // Pass in the padding to make sure nested fields are aligned properly,
      // even if we then strip the top-level padding.
      String typeStr = type_.prettyPrint(lpad);
      typeStr = typeStr.substring(lpad);
      sb.append(":" + typeStr);
    }
    if (comment_ != null) sb.append(String.format(" COMMENT '%s'", comment_));
    return sb.toString();
  }

  public void toThrift(TColumnType container, TTypeNode node) {
    TStructField field = new TStructField();
    field.setName(name_);
    if (comment_ != null) field.setComment(comment_);
    node.struct_fields.add(field);
    type_.toThrift(container);
  }

  /**
   * Implements equals and hashCode so a.equals(b) implies a.hashCode()==b.hashCode().
   */
  @Override
  public boolean equals(Object other) {
    if (!(other instanceof StructField)) return false;
    StructField otherStructField = (StructField) other;
    return otherStructField.name_.equals(name_) && otherStructField.type_.equals(type_);
  }
  @Override
  public int hashCode() {
    return Objects.hash(name_, type_);
  }
}
