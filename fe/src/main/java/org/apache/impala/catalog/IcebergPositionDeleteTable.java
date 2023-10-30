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

import java.util.Collections;
import java.util.Set;

import org.apache.impala.catalog.HdfsPartition.FileDescriptor;
import org.apache.impala.thrift.TColumnStats;

/**
 * Iceberg position delete table is created on the fly during planning. It belongs to an
 * actual Iceberg table (referred to as 'baseTable_'), but has a schema that corresponds
 * to the file schema of position delete files. Therefore with the help of it we can
 * do an ANTI JOIN between data files and position delete files.
 */
public class IcebergPositionDeleteTable extends IcebergDeleteTable  {
  public static String FILE_PATH_COLUMN = "file_path";
  public static String POS_COLUMN = "pos";

  public IcebergPositionDeleteTable(FeIcebergTable baseTable) {
    this(baseTable, baseTable.getName() + "-POSITION-DELETE", Collections.emptySet(), 0,
        new TColumnStats());
  }

  public IcebergPositionDeleteTable(FeIcebergTable baseTable, String name,
      Set<FileDescriptor> deleteFiles,
      long deleteRecordsCount, TColumnStats filePathsStats) {
    super(baseTable, name, deleteFiles, deleteRecordsCount);
    Column filePath = new IcebergColumn(FILE_PATH_COLUMN, Type.STRING, /*comment=*/"",
        colsByPos_.size(), IcebergTable.V2_FILE_PATH_FIELD_ID, INVALID_MAP_KEY_ID,
        INVALID_MAP_VALUE_ID, /*nullable=*/false);
    Column pos = new IcebergColumn(POS_COLUMN, Type.BIGINT, /*comment=*/"",
        colsByPos_.size(), IcebergTable.V2_POS_FIELD_ID, INVALID_MAP_KEY_ID,
        INVALID_MAP_VALUE_ID, /*nullable=*/false);
    filePath.updateStats(filePathsStats);
    pos.updateStats(getPosStats(pos));
    addColumn(filePath);
    addColumn(pos);
  }

  private TColumnStats getPosStats(Column pos) {
    TColumnStats colStats = new TColumnStats();
    colStats.num_distinct_values = deleteRecordsCount_;
    colStats.num_nulls = 0;
    colStats.max_size = pos.getType().getSlotSize();
    return colStats;
  }
}
