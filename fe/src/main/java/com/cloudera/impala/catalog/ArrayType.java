package com.cloudera.impala.catalog;

import org.apache.commons.lang3.StringUtils;

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
    if (!itemType_.isStructType()) return leftPadding + toSql();
    // Pass in the padding to make sure nested fields are aligned properly,
    // even if we then strip the top-level padding.
    String structStr = itemType_.prettyPrint(lpad);
    structStr = structStr.substring(lpad);
    return String.format("%sARRAY<%s>", leftPadding, structStr);
  }
}
