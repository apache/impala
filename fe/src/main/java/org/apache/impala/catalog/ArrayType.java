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
import org.apache.impala.thrift.TTypeNode;
import org.apache.impala.thrift.TTypeNodeType;
import com.google.common.base.Preconditions;

/**
 * Describes an ARRAY type.
 */
public class ArrayType extends Type {
  private final Type itemType_;

  public ArrayType(Type itemType) {
    itemType_ = itemType;
  }

  public Type getItemType() { return itemType_; }

  @Override
  public String toSql(int depth) {
    if (depth >= MAX_NESTING_DEPTH) return "ARRAY<...>";
    return String.format("ARRAY<%s>", itemType_.toSql(depth + 1));
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ArrayType)) return false;
    ArrayType otherArrayType = (ArrayType) other;
    return otherArrayType.itemType_.equals(itemType_);
  }

  @Override
  public int hashCode() {
    // Add 1 to differentiate between e.g. array<int> and array<array<int>>.
    return 1 + itemType_.hashCode();
  }

  @Override
  public void toThrift(TColumnType container) {
    TTypeNode node = new TTypeNode();
    container.types.add(node);
    Preconditions.checkNotNull(itemType_);
    node.setType(TTypeNodeType.ARRAY);
    itemType_.toThrift(container);
  }

  @Override
  protected String prettyPrint(int lpad) {
    String leftPadding = StringUtils.repeat(' ', lpad);
    if (itemType_.isScalarType()) return leftPadding + toSql();
    // Pass in the padding to make sure nested fields are aligned properly,
    // even if we then strip the top-level padding.
    String structStr = itemType_.prettyPrint(lpad);
    structStr = structStr.substring(lpad);
    return String.format("%sARRAY<%s>", leftPadding, structStr);
  }
}
