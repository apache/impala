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

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.transforms.Transforms;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.IcebergColumn;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TExprNode;
import org.apache.impala.thrift.TIcebergPartitionTransformType;
import org.apache.impala.util.IcebergUtil;

/**
 * IcebergPartitionExpr represents a partitioned Iceberg table column with its transform
 * and its value. This class is used as a temporary state for Iceberg table's partition
 * scanning. Later on it gets transformed to an Iceberg Expression.
 */
public class IcebergPartitionExpr extends Expr {
  private final IcebergPartitionTransform transform_;
  private final SlotRef slotRef_;
  private final PartitionSpec partitionSpec_;

  public IcebergPartitionExpr(SlotRef slotRef, PartitionSpec partitionSpec) {
    this(new IcebergPartitionTransform(TIcebergPartitionTransformType.IDENTITY), slotRef,
        partitionSpec);
  }

  protected IcebergPartitionExpr(IcebergPartitionTransform transform, SlotRef slotRef,
      PartitionSpec partitionSpec) {
    transform_ = transform;
    slotRef_ = slotRef;
    partitionSpec_ = partitionSpec;
  }

  public IcebergPartitionExpr(FunctionCallExpr callExpr, PartitionSpec partitionSpec)
      throws AnalysisException {
    partitionSpec_ = partitionSpec;
    FunctionParams params = callExpr.getParams();
    if (params.size() > 2 && params.size() < 1) {
      throw new AnalysisException("Invalid partition predicate: " + callExpr.toSql());
    }
    Expr slotRefExpr;
    Expr transformParamExpr;
    Optional<Integer> transformParam = Optional.empty();

    if (params.size() == 1) { // transform(column)
      slotRefExpr = params.exprs().get(0);
    } else { // transform(transform_param, column)
      transformParamExpr = params.exprs().get(0);
      slotRefExpr = params.exprs().get(1);

      if (!(transformParamExpr instanceof LiteralExpr)) {
        throw new AnalysisException("Invalid transform parameter: " + transformParamExpr);
      }
      LiteralExpr literal = (LiteralExpr) transformParamExpr;
      String stringValue = literal.getStringValue();
      try {
        int parsedInt = Integer.parseInt(stringValue);
        transformParam = Optional.of(parsedInt);
      } catch (NumberFormatException e) {
        throw new AnalysisException("Invalid transform parameter value: " + stringValue);
      }
    }

    if (!(slotRefExpr instanceof SlotRef)) {
      throw new AnalysisException("Invalid partition predicate: " + callExpr.toSql());
    }

    String functionName = callExpr.getFnName().getFunction();
    if (functionName == null) {
      List<String> fnNamePath = callExpr.getFnName().getFnNamePath();
      if (fnNamePath.size() > 1) {
        throw new AnalysisException("Invalid partition transform: " + fnNamePath);
      }
      functionName = fnNamePath.get(0);
    }
    String transformString = functionName.toUpperCase();

    slotRef_ = (SlotRef) slotRefExpr;

    try{
      if (transformParam.isPresent()) {
        transform_ =
            IcebergUtil.getPartitionTransform(transformString, transformParam.get());
      } else {
        transform_ = IcebergUtil.getPartitionTransform(transformString);
      }
    }
    catch (ImpalaRuntimeException e) {
      throw new AnalysisException(e.getMessage());
    }

    if (transform_.getTransformType().equals(TIcebergPartitionTransformType.VOID)) {
      throw new AnalysisException(
          "VOID transform is not supported for partition selection");
    }
  }

  public SlotRef getSlotRef() { return slotRef_; }

  public IcebergPartitionTransform getTransform() { return transform_; }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    slotRef_.analyze(analyzer);
    transform_.analyze(analyzer);
    SlotDescriptor desc = slotRef_.getDesc();
    IcebergColumn icebergColumn = (IcebergColumn)desc.getColumn();

    List<PartitionField> partitionFields =
        partitionSpec_.getFieldsBySourceId(icebergColumn.getFieldId());
    // Removing VOID transforms
    partitionFields = partitionFields.stream().filter(
        partitionField -> !partitionField.transform()
            .equals(Transforms.alwaysNull())).collect(
        Collectors.toList());

    if (partitionFields.isEmpty()) {
      throw new AnalysisException(
          "Partition exprs cannot contain non-partition column(s): " + toSql());
    }
    TIcebergPartitionTransformType transformType = transform_.getTransformType();
    if (!transformType.equals(TIcebergPartitionTransformType.IDENTITY)) {
      for (PartitionField field : partitionFields) {
        TIcebergPartitionTransformType transformTypeFromSpec;
        try {
          transformTypeFromSpec =
              IcebergUtil.getPartitionTransformType(field.transform().toString());
        }
        catch (ImpalaRuntimeException e) {
          throw new AnalysisException(e.getCause());
        }
        if (!transformTypeFromSpec.equals(transformType)) {
          throw new AnalysisException(
              String.format("Can't filter column '%s' with transform type: '%s'",
                  slotRef_.toSql(), transformType));
        }
      }
    }
    if (transformType.equals(TIcebergPartitionTransformType.TRUNCATE)
        || transformType.equals(TIcebergPartitionTransformType.IDENTITY)) {
      type_ = slotRef_.type_;
    }
    if (transformType.equals(TIcebergPartitionTransformType.YEAR)
        || transformType.equals(TIcebergPartitionTransformType.BUCKET)
        || transformType.equals(TIcebergPartitionTransformType.MONTH)
        || transformType.equals(TIcebergPartitionTransformType.DAY)
        || transformType.equals(TIcebergPartitionTransformType.HOUR)) {
      type_ = Type.INT;
    }
  }

  @Override
  protected float computeEvalCost() {
    return UNKNOWN_COST;
  }

  @Override
  protected String toSqlImpl(ToSqlOptions options) {
    return transform_.toSql(slotRef_.toSql());
  }

  @Override
  protected void toThrift(TExprNode msg) {
    /* Keeping it empty to avoid constant folding */
  }

  @Override
  public Expr clone() {
    return new IcebergPartitionExpr(transform_, slotRef_, partitionSpec_);
  }
}
