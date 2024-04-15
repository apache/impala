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

package org.apache.impala.catalog.local;

import java.util.Set;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeSystemTable;
import org.apache.impala.catalog.local.MetaProvider.TableMetaRef;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.InternalException;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.thrift.TSystemTable;
import org.apache.impala.thrift.TSystemTableName;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableType;
import org.apache.impala.util.TResultRowBuilder;

import com.google.common.base.Preconditions;

/**
 * System table instance loaded from {@link LocalCatalog}.
 *
 * System tables are identified by the TBL_PROP_SYSTEM_TABLE table parameter.
 */
public class LocalSystemTable extends LocalTable implements FeSystemTable {
  public static LocalSystemTable load(LocalDb db, Table msTbl, TableMetaRef ref) {
    Preconditions.checkNotNull(db);
    Preconditions.checkNotNull(msTbl);
    return new LocalSystemTable(db, msTbl, ref);
  }

  private LocalSystemTable(LocalDb db, Table msTbl, TableMetaRef ref) {
    super(db, msTbl, ref);
  }

  @Override // FeSystemTable
  public TSystemTableName getSystemTableName() {
    return TSystemTableName.valueOf(getName().toUpperCase());
  }

  @Override
  public long getNumRows() {
    try {
      // Return an estimate of the number of live queries assuming balanced load across
      // coordinators.
      return FeSupport.NumLiveQueries() * FeSupport.GetCoordinators().getAddressesSize();
    } catch (InternalException e) {
      return super.getNumRows();
    }
  }

  /**
   * Returns statistics on this table as a tabular result set. Used for the
   * SHOW TABLE STATS statement. The schema of the returned TResultSet is set
   * inside this method.
   */
  @Override // FeSystemTable
  public TResultSet getTableStats() {
    TResultSet result = new TResultSet();
    TResultSetMetadata resultSchema = new TResultSetMetadata();
    resultSchema.addToColumns(new TColumn("#Rows", Type.BIGINT.toThrift()));
    result.setSchema(resultSchema);
    TResultRowBuilder rowBuilder = new TResultRowBuilder();
    rowBuilder.add(getNumRows());
    result.addToRows(rowBuilder.get());
    return result;
  }

  @Override
  public TTableDescriptor toThriftDescriptor(
      int tableId, Set<Long> referencedPartitions) {
    TTableDescriptor tableDescriptor = new TTableDescriptor(tableId,
        TTableType.SYSTEM_TABLE, FeCatalogUtils.getTColumnDescriptors(this),
        getNumClusteringCols(), getName(), getDb().getName());
    tableDescriptor.setSystemTable(getTSystemTable());
    return tableDescriptor;
  }

  /**
   * Returns a thrift structure for the system table.
   */
  private TSystemTable getTSystemTable() {
    return new TSystemTable(getSystemTableName());
  }
}
