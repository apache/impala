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

package org.apache.impala.analysis;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.function.Function;
import org.apache.iceberg.PartitionSpec;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TIcebergPartitionTransformType;
import org.apache.impala.util.IcebergUtil;

class IcebergPartitionExpressionRewriter {
  private final Analyzer analyzer_;
  private final org.apache.iceberg.PartitionSpec partitionSpec_;

  public IcebergPartitionExpressionRewriter(Analyzer analyzer,
      org.apache.iceberg.PartitionSpec partitionSpec) {
    this.analyzer_ = analyzer;
    this.partitionSpec_ = partitionSpec;
  }

  /**
   * Rewrites SlotRefs and FunctionCallExprs as IcebergPartitionExpr. SlotRefs targeting a
   * columns are rewritten to an IcebergPartitionExpr where the transform type is
   * IDENTITY. FunctionExprs are checked whether the function name matches any Iceberg
   * transform name, if it matches, then it gets rewritten to an IcebergPartitionExpr
   * where the transform is located from the function name, and the parameter (if there's
   * any) for the transform is saved as well, and the targeted column's SlotRef is also
   * saved in the IcebergPartitionExpr. The resulting IcebergPartitionExpr then replaces
   * the original SlotRef/FunctionCallExpr. For Date transforms (year, month, day, hour),
   * an implicit conversion happens during the rewriting: string literals formed as
   * Iceberg partition values like "2023", "2023-12", ... are parsed and transformed
   * automatically to their numeric counterparts.
   *
   * @param expr incoming expression tree
   * @return Expr where SlotRefs and FunctionCallExprs are replaced with
   * IcebergPartitionExpr
   * @throws AnalysisException when expression rewrite fails
   */
  public Expr rewrite(Expr expr) throws AnalysisException {
    if (expr instanceof BinaryPredicate) {
      BinaryPredicate binaryPredicate = (BinaryPredicate) expr;
      return rewrite(binaryPredicate);
    }
    if (expr instanceof CompoundPredicate) {
      CompoundPredicate compoundPredicate = (CompoundPredicate) expr;
      return rewrite(compoundPredicate);
    }
    if (expr instanceof SlotRef) {
      SlotRef slotRef = (SlotRef) expr;
      return rewrite(slotRef);
    }
    if (expr instanceof FunctionCallExpr) {
      FunctionCallExpr functionCallExpr = (FunctionCallExpr) expr;
      return rewrite(functionCallExpr);
    }
    if (expr instanceof IsNullPredicate) {
      IsNullPredicate isNullPredicate = (IsNullPredicate) expr;
      return rewrite(isNullPredicate);
    }
    if (expr instanceof InPredicate) {
      InPredicate isNullPredicate = (InPredicate) expr;
      return rewrite(isNullPredicate);
    }
    throw new AnalysisException(
        "Invalid partition filtering expression: " + expr.toSql());
  }

  private BinaryPredicate rewrite(BinaryPredicate binaryPredicate)
      throws AnalysisException {
    Expr term = binaryPredicate.getChild(0);
    Expr literal = binaryPredicate.getChild(1);
    IcebergPartitionExpr partitionExpr;
    if (term instanceof SlotRef) {
      partitionExpr = rewrite((SlotRef) term);
      binaryPredicate.getChildren().set(0, partitionExpr);
    } else if (term instanceof FunctionCallExpr) {
      partitionExpr = rewrite((FunctionCallExpr) term);
      binaryPredicate.getChildren().set(0, partitionExpr);
    } else {
      return binaryPredicate;
    }
    if (!(literal instanceof LiteralExpr)) {
      return binaryPredicate;
    }
    TIcebergPartitionTransformType transformType = partitionExpr.getTransform()
        .getTransformType();
    if (!IcebergUtil.isDateTimeTransformType(transformType)) { return binaryPredicate; }
    rewriteDateTransformConstants((LiteralExpr) literal, transformType,
        numericLiteral -> binaryPredicate.getChildren().set(1, numericLiteral));
    return binaryPredicate;
  }

  private InPredicate rewrite(InPredicate inPredicate)
      throws AnalysisException {
    Expr term = inPredicate.getChild(0);
    List<Expr> literals = inPredicate.getChildren()
        .subList(1, inPredicate.getChildCount());
    IcebergPartitionExpr partitionExpr;
    if (term instanceof SlotRef) {
      partitionExpr = rewrite((SlotRef) term);
      inPredicate.getChildren().set(0, partitionExpr);
    } else if (term instanceof FunctionCallExpr) {
      partitionExpr = rewrite((FunctionCallExpr) term);
      inPredicate.getChildren().set(0, partitionExpr);
    } else {
      return inPredicate;
    }
    TIcebergPartitionTransformType transformType = partitionExpr.getTransform()
        .getTransformType();
    for (int i = 0; i < literals.size(); ++i) {
      if (!(literals.get(i) instanceof LiteralExpr)) {
        return inPredicate;
      }
      LiteralExpr literal = (LiteralExpr) literals.get(i);
      int affectedChildId = i + 1;
      if (!IcebergUtil.isDateTimeTransformType(transformType)) { continue; }
      rewriteDateTransformConstants(literal, transformType,
          numericLiteral -> inPredicate.getChildren()
              .set(affectedChildId, numericLiteral));
    }
    return inPredicate;
  }

  private void rewriteDateTransformConstants(LiteralExpr literal,
      TIcebergPartitionTransformType transformType,
      Function<NumericLiteral, ?> rewrite) {
    Preconditions.checkState(IcebergUtil.isDateTimeTransformType(transformType));
    if (transformType.equals(TIcebergPartitionTransformType.YEAR)
        && literal instanceof NumericLiteral) {
      NumericLiteral numericLiteral = (NumericLiteral) literal;
      long longValue = numericLiteral.getLongValue();
      long target = longValue + IcebergUtil.ICEBERG_EPOCH_YEAR;
      analyzer_.addWarning(String.format(
          "The YEAR transform expects years normalized to %d: %d is targeting year %d",
          IcebergUtil.ICEBERG_EPOCH_YEAR, longValue, target));
      return;
    }
    if (!(literal instanceof StringLiteral)) { return; }
    try {
      Integer dateTimeTransformValue = IcebergUtil.getDateTimeTransformValue(
          transformType,
          literal.getStringValue());
      rewrite.apply(NumericLiteral.create(dateTimeTransformValue));
    } catch (ImpalaRuntimeException ignore) {}
  }

  private CompoundPredicate rewrite(CompoundPredicate compoundPredicate)
      throws AnalysisException {
    Expr left = compoundPredicate.getChild(0);
    Expr right = compoundPredicate.getChild(1);
    compoundPredicate.setChild(0, rewrite(left));
    compoundPredicate.setChild(1, rewrite(right));
    return compoundPredicate;
  }

  private IcebergPartitionExpr rewrite(SlotRef slotRef) {
    return new IcebergPartitionExpr(slotRef, partitionSpec_);
  }

  private IcebergPartitionExpr rewrite(FunctionCallExpr functionCallExpr)
      throws AnalysisException {
    return new IcebergPartitionExpr(functionCallExpr, partitionSpec_);
  }

  private IsNullPredicate rewrite(IsNullPredicate isNullPredicate)
      throws AnalysisException {
    Expr child = isNullPredicate.getChild(0);

    if (child instanceof SlotRef) {
      isNullPredicate.getChildren().set(0, rewrite(child));
    }
    if (child instanceof FunctionCallExpr) {
      isNullPredicate.getChildren()
          .set(0, new IcebergPartitionExpr((FunctionCallExpr) child, partitionSpec_));
    }
    return isNullPredicate;
  }
}
