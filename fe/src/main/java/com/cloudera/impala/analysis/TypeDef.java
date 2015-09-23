// Copyright 2015 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.analysis;

import java.util.Set;

import org.apache.hadoop.hive.metastore.MetaStoreUtils;

import com.cloudera.impala.catalog.ArrayType;
import com.cloudera.impala.catalog.MapType;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.ScalarType;
import com.cloudera.impala.catalog.StructField;
import com.cloudera.impala.catalog.StructType;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * Represents an anonymous type definition, e.g., used in DDL and CASTs.
 */
public class TypeDef implements ParseNode {
  private boolean isAnalyzed_;
  private final Type parsedType_;

  public TypeDef(Type parsedType) {
    parsedType_ = parsedType;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException {
    if (isAnalyzed_) return;
    // Check the max nesting depth before calling the recursive analyze() to avoid
    // a stack overflow.
    if (parsedType_.exceedsMaxNestingDepth()) {
      throw new AnalysisException(String.format(
          "Type exceeds the maximum nesting depth of %s:\n%s",
          Type.MAX_NESTING_DEPTH, parsedType_.toSql()));
    }
    analyze(parsedType_, analyzer);
    isAnalyzed_ = true;
  }

  private void analyze(Type type, Analyzer analyzer) throws AnalysisException {
    if (type.isScalarType()) {
      analyzeScalarType((ScalarType) type, analyzer);
    } else if (type.isStructType()) {
      analyzeStructType((StructType) type, analyzer);
    } else if (type.isArrayType()) {
      ArrayType arrayType = (ArrayType) type;
      analyze(arrayType.getItemType(), analyzer);
    } else {
      Preconditions.checkState(type.isMapType());
      analyzeMapType((MapType) type, analyzer);
    }
  }

  private void analyzeScalarType(ScalarType scalarType, Analyzer analyzer)
      throws AnalysisException {
    PrimitiveType type = scalarType.getPrimitiveType();
    switch (type) {
      case CHAR:
      case VARCHAR: {
        String name;
        int maxLen;
        if (type == PrimitiveType.VARCHAR) {
          name = "Varchar";
          maxLen = ScalarType.MAX_VARCHAR_LENGTH;
        } else if (type == PrimitiveType.CHAR) {
          name = "Char";
          maxLen = ScalarType.MAX_CHAR_LENGTH;
        } else {
          Preconditions.checkState(false);
          return;
        }
        int len = scalarType.getLength();
        if (len <= 0) {
          throw new AnalysisException(name + " size must be > 0: " + len);
        }
        if (scalarType.getLength() > maxLen) {
          throw new AnalysisException(
              name + " size must be <= " + maxLen + ": " + len);
        }
        break;
      }
      case DECIMAL: {
        int precision = scalarType.decimalPrecision();
        int scale = scalarType.decimalScale();
        if (precision > ScalarType.MAX_PRECISION) {
          throw new AnalysisException("Decimal precision must be <= " +
              ScalarType.MAX_PRECISION + ": " + precision);
        }
        if (precision == 0) {
          throw new AnalysisException("Decimal precision must be > 0: " + precision);
        }
        if (scale > precision) {
          throw new AnalysisException("Decimal scale (" + scale + ") must be <= " +
              "precision (" + precision + ")");
        }
      }
      default: break;
    }
  }

  private void analyzeStructType(StructType structType, Analyzer analyzer)
      throws AnalysisException {
    // Check for duplicate field names.
    Set<String> fieldNames = Sets.newHashSet();
    for (StructField f: structType.getFields()) {
      analyze(f.getType(), analyzer);
      if (!fieldNames.add(f.getName().toLowerCase())) {
        throw new AnalysisException(
            String.format("Duplicate field name '%s' in struct '%s'",
                f.getName(), toSql()));
      }
      // Check whether the column name meets the Metastore's requirements.
      if (!MetaStoreUtils.validateName(f.getName().toLowerCase())) {
        throw new AnalysisException("Invalid struct field name: " + f.getName());
      }
    }
  }

  private void analyzeMapType(MapType mapType, Analyzer analyzer)
      throws AnalysisException {
    analyze(mapType.getKeyType(), analyzer);
    if (mapType.getKeyType().isComplexType()) {
      throw new AnalysisException(
          "Map type cannot have a complex-typed key: " + mapType.toSql());
    }
    analyze(mapType.getValueType(), analyzer);
  }

  public Type getType() { return parsedType_; }

  @Override
  public String toString() { return parsedType_.toSql(); }

  @Override
  public String toSql() { return parsedType_.toSql(); }
}
