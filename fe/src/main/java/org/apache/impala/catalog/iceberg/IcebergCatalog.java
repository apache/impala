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

package org.apache.impala.catalog.iceberg;

import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.impala.catalog.FeIcebergTable;
import org.apache.impala.catalog.IcebergTableLoadingException;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.common.ImpalaRuntimeException;

/**
 * Interface for Iceberg catalogs. Only contains a minimal set of methods to make
 * it easy to add support for new Iceberg catalogs. Methods that can be implemented in a
 * catalog-agnostic way should be placed in IcebergUtil.
 */
public interface IcebergCatalog {
  /**
   * Creates an Iceberg table in this catalog.
   */
  Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> properties) throws ImpalaRuntimeException;

  /**
   * Loads a native Iceberg table based on the information in 'feTable'.
   */
  Table loadTable(FeIcebergTable feTable) throws TableLoadingException;

  /**
   * Loads a native Iceberg table based on 'tableId' or 'tableLocation'.
   * @param tableId is the Iceberg table identifier to load the table via the catalog
   *     interface, e.g. HadoopCatalog.
   * @param tableLocation is the filesystem path to load the table via the HadoopTables
   *     interface.
   * @param properties provides information for table loading when Iceberg Catalogs
   *     is being used.
   */
   Table loadTable(TableIdentifier tableId, String tableLocation,
      Map<String, String> properties) throws IcebergTableLoadingException;

  /**
   * Drops the table from this catalog.
   * If purge is true, delete all data and metadata files in the table.
   * Return true if the table was dropped, false if the table did not exist
   */
  boolean dropTable(FeIcebergTable feTable, boolean purge);

  /**
   * Drops the table from this catalog using database name and table name.
   * @param dbName the database name
   * @param tblName the table name
   * @param purge whether to drop data/metadata files or not
   * @return true if table was dropped, false if the table did not exist
   */
  boolean dropTable(String dbName, String tblName, boolean purge);

  /**
   * Renames Iceberg table.
   * For HadoopTables, Iceberg does not supported 'renameTable' method
   * For HadoopCatalog, Iceberg implement 'renameTable' method with Exception threw
   */
  void renameTable(FeIcebergTable feTable, TableIdentifier newTableId);

  /**
   * Some of the implemetation methods might be running on native threads as they might
   * be invoked via JNI. In that case the context class loader for those threads are
   * null. 'Catalogs' uses JNDI to load the catalog implementations, e.g. HadoopCatalog
   * or HiveCatalog. JNDI uses the context class loader, but as it is null it falls back
   * to the bootstrap class loader that doesn't have the Iceberg classes on its classpath.
   * To avoid ClassNotFoundException we set the context class loader to the class loader
   * that loaded this class.
   */
  default void setContextClassLoader() {
    if (Thread.currentThread().getContextClassLoader() != null) return;
    Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
  }
}
