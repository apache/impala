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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.ArrayType;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeDb;
import org.apache.impala.catalog.FeTable;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableStats;

import com.google.common.base.Preconditions;

/**
 * Table instance loaded from {@link LocalCatalog}.
 *
 * This class is not thread-safe. A new instance is created for
 * each catalog instance.
 */
class LocalTable implements FeTable {
  private final LocalDb db_;
  /** The lower-case name of the table. */
  private final String name_;

  public static LocalTable load(LocalDb db, String tblName) {
    // TODO: change this function to instantiate the appropriate
    // subclass based on the table type (eg view, hbase table, etc)
    return new LocalTable(db, tblName);
  }

  public LocalTable(LocalDb db, String tblName) {
    Preconditions.checkNotNull(db);
    Preconditions.checkNotNull(tblName);
    Preconditions.checkArgument(tblName.toLowerCase().equals(tblName));
    this.db_ = db;
    this.name_ = tblName;
  }

  @Override
  public boolean isLoaded() {
    return true;
  }

  @Override
  public Table getMetaStoreTable() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public String getStorageHandlerClassName() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public String getName() {
    return name_;
  }

  @Override
  public String getFullName() {
    return db_.getName() + "." + name_;
  }

  @Override
  public TableName getTableName() {
    return new TableName(db_.getName(), name_);
  }

  @Override
  public ArrayList<Column> getColumns() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public List<Column> getColumnsInHiveOrder() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public List<String> getColumnNames() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public List<Column> getClusteringColumns() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public List<Column> getNonClusteringColumns() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public int getNumClusteringCols() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public boolean isClusteringColumn(Column c) {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public Column getColumn(String name) {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public ArrayType getType() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public FeDb getDb() {
    return db_;
  }

  @Override
  public long getNumRows() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public TTableStats getTTableStats() {
    throw new UnsupportedOperationException("TODO");
  }

  @Override
  public TTableDescriptor toThriftDescriptor(int tableId, Set<Long> referencedPartitions) {
    throw new UnsupportedOperationException("TODO");
  }
}
