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

package org.apache.impala.catalog;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.util.IcebergSchemaConverter;

import org.apache.iceberg.types.Types.NestedField;

/**
 * Iceberg equality delete table is created on the fly during planning. It belongs to an
 * actual Iceberg table (referred to as 'baseTable_'), but has a schema that corresponds
 * to the file schema of equality delete files. Therefore with the help of it we can
 * do an ANTI JOIN between data files and equality delete files.
 */
public class IcebergEqualityDeleteTable extends IcebergDeleteTable  {

  public IcebergEqualityDeleteTable(FeIcebergTable baseTable, String name,
      Set<FileDescriptor> deleteFiles, List<Integer> equalityIds, long deleteRecordsCount)
      throws ImpalaRuntimeException {
    super(baseTable, name, deleteFiles, deleteRecordsCount);

    int columnPos = 0;
    for (Integer eqId : equalityIds) {
      ++columnPos;
      NestedField field = baseTable.getIcebergSchema().findField(eqId);
      Type colType = IcebergSchemaConverter.toImpalaType(field.type());
      if (colType.isComplexType()) {
        throw new ImpalaRuntimeException(
            "Equality ID for nested types isn't supported: '" + field.name() + "'");
      } else if (colType.isFloatingPointType()) {
        throw new ImpalaRuntimeException(
            "Equality ID for floating point types isn't supported: '" +
            field.name() + "'");
      }

      Column equalityCol = new IcebergColumn(field.name(), colType, field.doc(),
          columnPos, field.fieldId(), INVALID_MAP_KEY_ID, INVALID_MAP_VALUE_ID,
          field.isOptional());
      addColumn(equalityCol);
    }
  }

  @Override
  public List<VirtualColumn> getVirtualColumns() {
    return Arrays.asList(VirtualColumn.ICEBERG_DATA_SEQUENCE_NUMBER);
  }
}