package com.cloudera.impala.catalog;

import com.cloudera.impala.thrift.TColumnType;
import com.cloudera.impala.thrift.TTypeNode;
import com.cloudera.impala.thrift.TTypeNodeType;
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
  public String toSql() {
    return String.format("ARRAY<%s>", itemType_.toSql());
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ArrayType)) return false;
    ArrayType otherArrayType = (ArrayType) other;
    return otherArrayType.itemType_.equals(itemType_);
  }

  @Override
  public void toThrift(TColumnType container) {
    TTypeNode node = new TTypeNode();
    container.types.add(node);
    Preconditions.checkNotNull(itemType_);
    node.setType(TTypeNodeType.ARRAY);
    itemType_.toThrift(container);
  }
}

