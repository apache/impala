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

import java.util.UUID;
import org.apache.iceberg.expressions.Expression.Operation;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.NamedReference;
import org.apache.iceberg.expressions.UnboundTerm;
import org.apache.iceberg.transforms.Transforms;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.IcebergPartitionExpr;
import org.apache.impala.analysis.StringLiteral;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.IcebergColumn;
import org.apache.impala.thrift.TIcebergPartitionTransformType;
import org.apache.impala.util.IcebergUtil;

public class IcebergPartitionPredicateConverter extends IcebergPredicateConverter {
  private final FeIcebergTable table_;

  public IcebergPartitionPredicateConverter(FeIcebergTable table, Analyzer analyzer) {
    super(table.getIcebergSchema(), analyzer);
    table_ = table;
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
      return new Term(Expressions.ref(column.getName()), icebergColumn);
    }
    return new Term((UnboundTerm<Object>) Expressions.transform(column.getName(),
        Transforms.fromString(partitionExpr.getTransform().toSql())), icebergColumn);
  }

  @Override
  protected Object getIcebergUuidValue(Term term, Operation op, StringLiteral literal)
      throws ImpalaRuntimeException {
    // Safe for (in)equality on a field that is identity-partitioned in every spec.
    // Iceberg evaluates the filter exactly on each file's partition value, and equality
    // does not depend on byte order. Data file bounds are equal within an identity
    // partition. != and NOT IN only prune manifests whose summary holds a single value.
    // = and IN also prune manifests by their partition summary bounds, which assumes the
    // summaries were built with the Iceberg Java library's signed comparator. This holds
    // for writers that use the library (Impala, Trino, Spark). A writer that orders UUIDs
    // unsigned (e.g. PyIceberg's min()/max()) would produce summaries that make
    // ManifestEvaluator skip manifests with matching partitions.
    boolean isIdentityTerm = term.term_ instanceof NamedReference;
    boolean isEqualityOp = op == Operation.EQ || op == Operation.NOT_EQ
        || op == Operation.IN || op == Operation.NOT_IN;
    if (isIdentityTerm && isEqualityOp
        && IcebergUtil.isIdentityPartitionedInAllSpecs(table_, term.referencedColumn_)) {
      return UUID.fromString(literal.getUnescapedValue());
    }
    throw new ImpalaRuntimeException(
        "UUID values can only be used with =, !=, IN or NOT IN on columns that are " +
        "identity partitioned in every partition spec");
  }
}
