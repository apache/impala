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

package org.apache.impala.common;

import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.transforms.Transforms;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.IcebergPartitionExpr;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.IcebergColumn;
import org.apache.impala.thrift.TIcebergPartitionTransformType;

public class IcebergPartitionPredicateConverter extends IcebergPredicateConverter {
  public IcebergPartitionPredicateConverter(Schema schema, Analyzer analyzer) {
    super(schema, analyzer);
  }

  @Override
  protected Term getTerm(Expr expr) throws ImpalaRuntimeException {
    if(!(expr instanceof IcebergPartitionExpr)) {
      throw new ImpalaRuntimeException("Unsupported expression type: " + expr);
    }
    IcebergPartitionExpr partitionExpr = (IcebergPartitionExpr) expr;
    Column column = getColumnFromSlotRef(partitionExpr.getSlotRef());
    if(!(column instanceof IcebergColumn)){
      throw new ImpalaRuntimeException(
          String.format("Invalid column type %s for column: %s",
              column.getType(), column));
    }
    IcebergColumn icebergColumn = (IcebergColumn) column;
    if (partitionExpr.getTransform().getTransformType().equals(
        TIcebergPartitionTransformType.IDENTITY)) {
      return new Term(Expressions.ref(column.getName()),icebergColumn);
    }
    return new Term((UnboundTerm<Object>) Expressions.transform(column.getName(),
        Transforms.fromString(partitionExpr.getTransform().toSql())), icebergColumn);
  }
}
