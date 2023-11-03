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
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.analysis.TableName;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TImpalaTableType;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableStats;

/**
 * Frontend interface for interacting with a table.
 */
public interface FeTable {
  Comparator<FeTable> NAME_COMPARATOR = new Comparator<FeTable>() {
    @Override
    public int compare(FeTable t1, FeTable t2) {
      return t1.getFullName().compareTo(t2.getFullName());
    }
  };

  // Internal table property that specifies the number of rows in the table.
  public static final String NUM_ROWS = "numRows";

  // Internal table property that specifies which user the table was last modified by.
  public static final String LAST_MODIFIED_BY = "last_modified_by";

  // Internal table property that specifies when the table was last modified.
  public static final String LAST_MODIFIED_TIME = "last_modified_time";

  // Internal table property that specifies the catalog service id.
  public static final String CATALOG_SERVICE_ID = "impala.events.catalogServiceId";

  // Internal table property that specifies the catalog version of the table.
  public static final String CATALOG_VERSION = "impala.events.catalogVersion";

  /** @see CatalogObject#isLoaded() */
  boolean isLoaded();

  /**
   * @return the metastore.api.Table object this Table was created from. Returns null
   * if the derived Table object was not created from a metastore Table (ex. InlineViews).
   */
  Table getMetaStoreTable();

  /**
   * @return the Hive StorageHandler class name that should be used for this table,
   * or null if no storage handler is needed.
   */
  String getStorageHandlerClassName();

  /**
   * @return the type of catalog object -- either TABLE or VIEW.
   */
  TCatalogObjectType getCatalogObjectType();

  /**
   * @return the short name of this table (e.g. "my_table")
   */
  String getName();

  /**
   * @return the full name of this table (e.g. "my_db.my_table")
   */
  String getFullName();

  /**
   * @return the table name in structured form
   */
  TableName getTableName();

  /**
   * @return the general type of this table (e.g. "TABLE" or "VIEW")
   */
  TImpalaTableType getTableType();

  /**
   * @return the comment of this table
   */
  String getTableComment();

  /**
   * @return the columns in this table
   */
  List<Column> getColumns();

  /**
   * @return the virtual columns of this table
   */
  default List<VirtualColumn> getVirtualColumns() {
    return Collections.emptyList();
  }

  /**
   * @return an unmodifiable list of all columns, but with partition columns at the end of
   * the list rather than the beginning. This is equivalent to the order in
   * which Hive enumerates columns.
   */
  List<Column> getColumnsInHiveOrder();

  /**
   * @return a list of the column names ordered by position.
   */
  List<String> getColumnNames();

  /**
   * @return SQL constraints for the table.
   */
  SqlConstraints getSqlConstraints();

  /**
   * @return an unmodifiable list of all partition columns.
   */
  List<Column> getClusteringColumns();

  /**
   * @return an unmodifiable list of all columns excluding any partition columns.
   */
  List<Column> getNonClusteringColumns();

  int getNumClusteringCols();

  boolean isClusteringColumn(Column c);

  /**
   * Return true when the column is used in a computed partition, e.g. in Iceberg
   * partition transforms.
   */
  default boolean isComputedPartitionColumn(Column c) { return false; }

  /**
   * Case-insensitive lookup.
   *
   * @return null if the column with 'name' is not found.
   */
  Column getColumn(String name);

  /**
   * @return the type of this table (array of struct) that mirrors the columns.
   */
  ArrayType getType();

  /**
   * @return the database that that contains this table
   */
  FeDb getDb();

  /**
   * @return the estimated number of rows in this table (or -1 if unknown)
   */
  long getNumRows();

  /**
   * @return the stats for this table
   */
  TTableStats getTTableStats();

  /**
   * @return the Thrift table descriptor for this table
   */
  TTableDescriptor toThriftDescriptor(int tableId, Set<Long> referencedPartitions);

  /**
   * @return the write id for this table
   */
  long getWriteId();

  /**
   * @return the valid write id list for this table
   */
  ValidWriteIdList getValidWriteIds();

  /**
   * @return the owner user for this table. If the table is not loaded or the owner is
   * missing returns null.
   */
  String getOwnerUser();

}
