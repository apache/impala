package com.cloudera.impala.catalog;

import com.cloudera.impala.thrift.TColumnType;
import com.cloudera.impala.thrift.TTypeNode;
import com.cloudera.impala.thrift.TTypeNodeType;
import com.google.common.base.Preconditions;

/**
 * Describes an ARRAY type.
 */
public class ArrayType extends Type {
  private final Type elementType_;

  public ArrayType(Type elementType) {
    Preconditions.checkNotNull(elementType);
    elementType_ = elementType;
  }

  public Type getElementType() { return elementType_; }

  @Override
  public String toSql() {
    return String.format("ARRAY<%s>", elementType_.toSql());
  }

  @Override
  public void toThrift(TColumnType container) {
    TTypeNode node = new TTypeNode();
    container.types.add(node);
    Preconditions.checkNotNull(elementType_);
    node.setType(TTypeNodeType.ARRAY);
    elementType_.toThrift(container);
  }

  @Override
  public boolean matchesType(Type t) { return false; }
}
