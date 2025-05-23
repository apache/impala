/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.impala.calcite.schema;

import java.util.List;

import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdSize;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.impala.catalog.Column;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * ImpalaRelMdSize is an extension of the RelMdSize
 * Calcite class which returns the size of a given column datatype.
 */
public class ImpalaRelMdSize extends RelMdSize {

  public static final ImpalaRelMdSize INSTANCE = new ImpalaRelMdSize();

  public static final RelMetadataProvider SOURCE =
          ReflectiveRelMetadataProvider.reflectiveSource(INSTANCE,
                  BuiltInMethod.AVERAGE_COLUMN_SIZES.method,
                  BuiltInMethod.AVERAGE_ROW_SIZE.method);

  //~ Methods ----------------------------------------------------------------

  @Override
  public List<Double> averageColumnSizes(TableScan scan, RelMetadataQuery mq) {
    // Obtain list of col stats, or use default if they are not available
    final ImmutableList.Builder<Double> list = ImmutableList.builder();
    CalciteTable table = (CalciteTable) scan.getTable();
    Preconditions.checkState(
        scan.getRowType().getFieldCount() == table.getColumns().size());
    for (int i = 0; i < table.getColumns().size(); ++i) {
      Column c = table.getColumn(i);
      double avgSize = c.getStats().getAvgSize();
      if (avgSize == -1) {
        avgSize = averageTypeValueSize(
            table.getRowType().getFieldList().get(i).getType());
      }
      list.add(avgSize);
    }
    return list.build();
  }

  @Override
  public Double averageTypeValueSize(RelDataType type) {
    switch (type.getSqlTypeName()) {
    case BOOLEAN:
    case TINYINT:
      return 1d;
    case SMALLINT:
      return 2d;
    case INTEGER:
    case FLOAT:
    case REAL:
    case DECIMAL:
    case DATE:
    case TIME:
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
      return 4d;
    case BIGINT:
    case DOUBLE:
    case TIMESTAMP:
    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
      return 8d;
    case BINARY:
      return (double) type.getPrecision();
    case VARBINARY:
      return Math.min(type.getPrecision(), 100d);
    case CHAR:
      return (double) type.getPrecision() * BYTES_PER_CHARACTER;
    case VARCHAR:
      // Even in large (say VARCHAR(2000)) columns most strings are small
      return Math.min((double) type.getPrecision() * BYTES_PER_CHARACTER, 100d);
    case ROW:
      Double average = 0.0;
      for (RelDataTypeField field : type.getFieldList()) {
        average += averageTypeValueSize(field.getType());
      }
      return average;
    default:
      return null;
    }
  }
}
