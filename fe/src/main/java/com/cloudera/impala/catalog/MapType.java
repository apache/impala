package com.cloudera.impala.catalog;

import org.apache.commons.lang3.StringUtils;

import com.cloudera.impala.thrift.TColumnType;
import com.cloudera.impala.thrift.TTypeNode;
import com.cloudera.impala.thrift.TTypeNodeType;
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
  public String toSql() {
    return String.format("MAP<%s,%s>", keyType_.toSql(), valueType_.toSql());
  }

  @Override
  protected String prettyPrint(int lpad) {
    String leftPadding = StringUtils.repeat(' ', lpad);
    if (!valueType_.isStructType()) return leftPadding + toSql();
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
