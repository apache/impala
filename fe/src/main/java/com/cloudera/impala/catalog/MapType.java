package com.cloudera.impala.catalog;

import com.cloudera.impala.common.AnalysisException;
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
    keyType_ = keyType;
    valueType_ = valueType;
  }

  @Override
  public void analyze() throws AnalysisException {
    if (isAnalyzed_) return;
    Preconditions.checkNotNull(keyType_);
    Preconditions.checkNotNull(valueType_);
    keyType_.analyze();
    if (keyType_.isComplexType()) {
      throw new AnalysisException(
          "Map type cannot have a complex-typed key: " + toSql());
    }
    valueType_.analyze();
    isAnalyzed_ = true;
  }

  @Override
  public String toSql() {
    return String.format("MAP<%s,%s>", keyType_.toSql(), valueType_.toSql());
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

  @Override
  public boolean matchesType(Type t) { return false; }
}
