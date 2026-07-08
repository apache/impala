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

package org.apache.impala.calcite.schema;

import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.impala.analysis.Analyzer;
import org.apache.impala.analysis.BaseTableRef;
import org.apache.impala.analysis.TimeTravelSpec;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.FeIcebergTable.Utils;
import org.apache.impala.catalog.IcebergColumn;
import org.apache.impala.catalog.IcebergTimeTravelTable;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.util.IcebergUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Calcite {@link org.apache.calcite.schema.Table} wrapper for an Iceberg table.
 */
public class CalciteIcebergTable extends CalciteTable {
  protected static final Logger LOG =
      LoggerFactory.getLogger(CalciteIcebergTable.class.getName());

  // lazily populated
  private Boolean hasDeleteFiles_;

  // lazily populated
  private Long recordCount_;

  // Special name for the time travel object. It consists of the normal table
  // name plus the "_tt_<hashcode of TimeTravelSpec>" string.
  private final String timeTravelName_;

  public CalciteIcebergTable(FeIcebergTable table, CalciteCatalogReader reader,
      Analyzer analyzer) throws ImpalaException {
    super(table, reader, analyzer);
    timeTravelName_ = null;
  }

  public CalciteIcebergTable(FeIcebergTable table, CalciteCatalogReader reader,
      Analyzer analyzer, String timeTravelName) throws ImpalaException {
    super(table, reader, analyzer);
    timeTravelName_ = timeTravelName;
  }

  public FeIcebergTable getFeIcebergTable() {
    return (FeIcebergTable) getFeTable();
  }

  @Override
  public boolean isOnlyClusteredCols(Collection<String> fieldNames) {
    FeIcebergTable table = getFeIcebergTable();
    for (String fieldName : fieldNames) {
      Column c = table.getColumn(fieldName);
      if (c instanceof IcebergColumn) {
        if (!IcebergUtil.canUsePartitionKeyScan(table, (IcebergColumn) c)) {
          return false;
        }
      }
    }
    return true;
  }

  /* Checks if the count star optimization can be applied to this table.
   * The count star optimization check here is only for Iceberg V1 tables.
   * Iceberg V2 tables with delete files do not use the countStarSlot_ and
   * are optimized in a different way.
   */
  @Override
  public boolean canApplyCountStarOptimization() {
    try {
      return super.canApplyCountStarOptimization() &&
          getRecordCount() > 0 && !hasDeleteFiles();
    } catch (Exception e) {
      LOG.info("Exception caught while checking for Iceberg count star optimization:" +
          e);
      return false;
    }
  }

  public boolean hasDeleteFiles() throws ImpalaException {
    if (hasDeleteFiles_ == null) {
      hasDeleteFiles_ = FeIcebergTable.Utils.hasDeleteFiles(getFeIcebergTable(),
          getTimeTravelSpec());
    }
    return hasDeleteFiles_;
  }

  public long getRecordCount() throws ImpalaException {
    // If record count has not been fetched yet, fetch and cache it.
    if (recordCount_ == null) {
      FeIcebergTable feIcebergTable = getFeIcebergTable();
      recordCount_ = hasDeleteFiles()
          ? Utils.getRecordCountV2(feIcebergTable, getTimeTravelSpec())
          : Utils.getRecordCountV1(feIcebergTable.getIcebergApiTable(),
              getTimeTravelSpec());
    }
    return recordCount_;
  }

  /* Checks if this is an Iceberg V2 table and sets the optimization flag. */
  public void testAndSetOptimizeCountStarForIcebergV2(BaseTableRef tableRef) {
    try {
      if (hasDeleteFiles() && getRecordCount() > 0) {
        tableRef.setOptimizeCountStarForIcebergV2(true);
      }
    } catch (Exception e) {
      LOG.info("Failed setting optimization count star for Iceberg V2 flag.");
    }
  }

  @Override
  public boolean isTemporal() {
    return timeTravelName_ != null;
  }

  @Override
  public List<String> getQualifiedName() {
    List<String> qualifiedTableName = super.getQualifiedName();
    if (!(getFeIcebergTable() instanceof IcebergTimeTravelTable)) {
      return qualifiedTableName;
    }
    // all fields except the last are the same in the array.
    List<String> newNames = new ArrayList<>();
    for (int i = 0; i < qualifiedTableName.size() - 1; ++i) {
      newNames.add(qualifiedTableName.get(i));
    }
    // last "table name" string contains the table name plus an appended
    // "_tt_<hashcode of TimeTravelSpec>" string.
    newNames.add(timeTravelName_.toUpperCase());
    return newNames;
  }

  public TimeTravelSpec getTimeTravelSpec() {
    return getFeIcebergTable() instanceof IcebergTimeTravelTable
        ? ((IcebergTimeTravelTable) getFeIcebergTable()).getTimeTravelSpec()
        : null;
  }
}
