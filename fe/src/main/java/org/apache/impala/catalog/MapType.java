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

import org.apache.commons.lang3.StringUtils;

import org.apache.impala.thrift.TColumnType;
import org.apache.impala.thrift.TTypeNode;
import org.apache.impala.thrift.TTypeNodeType;
import com.google.common.base.Preconditions;

/**
 * Describes a MAP type. MAP types have a scalar key and an arbitrarily-typed value.
 */
public class MapType extends Type {
  private final Type keyType_;
  private final Type valueType_;

  public MapType(Type keyType, Type valueType) {
    Preconditions.checkNotNull(keyType);
    Preconditions.checkNotNull(valueType);
    keyType_ = keyType;
    valueType_ = valueType;
  }

  public Type getKeyType() { return keyType_; }
  public Type getValueType() { return valueType_; }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof MapType)) return false;
    MapType otherMapType = (MapType) other;
    return otherMapType.keyType_.equals(keyType_) &&
        otherMapType.valueType_.equals(valueType_);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyType_, valueType_);
  }

  @Override
  public String toSql(int depth) {
    if (depth >= MAX_NESTING_DEPTH) return "MAP<...>";
    return String.format("MAP<%s,%s>",
        keyType_.toSql(depth + 1), valueType_.toSql(depth + 1));
  }

  @Override
  protected String prettyPrint(int lpad) {
    String leftPadding = StringUtils.repeat(' ', lpad);
    if (valueType_.isScalarType()) return leftPadding + toSql();
    // Pass in the padding to make sure nested fields are aligned properly,
    // even if we then strip the top-level padding.
    String structStr = valueType_.prettyPrint(lpad);
    structStr = structStr.substring(lpad);
    return String.format("%sMAP<%s,%s>", leftPadding, keyType_.toSql(), structStr);
  }

  @Override
  public void toThrift(TColumnType container) {
    TTypeNode node = new TTypeNode();
    container.types.add(node);
    Preconditions.checkNotNull(keyType_);
    Preconditions.checkNotNull(valueType_);
    node.setType(TTypeNodeType.MAP);
    keyType_.toThrift(container);
    valueType_.toThrift(container);
  }
}
