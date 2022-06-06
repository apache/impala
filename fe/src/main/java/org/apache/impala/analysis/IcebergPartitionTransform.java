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

import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TIcebergPartitionTransform;
import org.apache.impala.thrift.TIcebergPartitionTransformType;

/**
 * Represents the partition transform part of an Iceberg partition field.
*/
public class IcebergPartitionTransform extends StmtNode {

    // Stores the transform type such as HOUR, YEAR, etc.
    private TIcebergPartitionTransformType transformType_;

    // Stores the parameter of BUCKET or TRUNCATE partition transforms (numBuckets,
    // width respectively). This is null for partition transforms that don't have a
    // parameter.
    private Integer transformParam_;

    // Constructor for parameterless partition transforms.
    public IcebergPartitionTransform(TIcebergPartitionTransformType transformType) {
        this(transformType, null);
    }

    public IcebergPartitionTransform(TIcebergPartitionTransformType transformType,
        Integer transformParam) {
      transformType_ = transformType;
      transformParam_ = transformParam;
    }

    public TIcebergPartitionTransformType getTransformType() {
      return transformType_;
    }

    public Integer getTransformParam() {
      return transformParam_;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
      if (transformType_ == TIcebergPartitionTransformType.BUCKET ||
          transformType_ == TIcebergPartitionTransformType.TRUNCATE) {
        if (transformParam_ == null) {
          throw new AnalysisException("BUCKET and TRUNCATE partition transforms should " +
              "have a parameter.");
        }
        if (transformParam_ <= 0) {
          throw new AnalysisException("The parameter of a partition transform should " +
              "be greater than zero.");
        }
      } else {
        if (transformParam_ != null) {
          throw new AnalysisException("Only BUCKET and TRUNCATE partition transforms " +
              "accept a parameter.");
        }
      }
    }

  @Override
  public final String toSql() {
    return toSql(ToSqlOptions.DEFAULT);
  }

  @Override
  public String toSql(ToSqlOptions options) {
    StringBuilder builder = new StringBuilder();
    builder.append(transformType_.toString());
    if (transformParam_ != null) builder.append("[").append(transformParam_).append("]");
    return builder.toString();
  }

  public final String toSql(String colName) {
    return toSql(colName, ToSqlOptions.DEFAULT);
  }

  public String toSql(String colName, ToSqlOptions options) {
    StringBuilder builder = new StringBuilder();
    if (transformType_ != TIcebergPartitionTransformType.IDENTITY) {
      builder.append(transformType_.toString()).append ("(");
      if (transformParam_ != null) {
        builder.append(transformParam_.toString()).append(", ");
      }
    }
    builder.append(colName);
    if (transformType_ != TIcebergPartitionTransformType.IDENTITY) {
      builder.append(")");
    }
    return builder.toString();
  }

  public TIcebergPartitionTransform toThrift() {
    TIcebergPartitionTransform transform = new TIcebergPartitionTransform();
    transform.setTransform_type(transformType_);
    if (transformParam_ != null) transform.setTransform_param(transformParam_);
    return transform;
  }
}