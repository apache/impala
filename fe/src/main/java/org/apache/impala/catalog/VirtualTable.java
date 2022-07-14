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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.analysis.TableName;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TImpalaTableType;
import org.apache.impala.util.AcidUtils;

/**
 * Tables that aren't actually exist in HMS. E.g. Iceberg position delete tables.
 * Metadata tables can be another use case for this class.
 * Using old table schema and stats during time-travel might be another use case.
 */
public abstract class VirtualTable implements FeTable {
  protected final Table msTable_;
  protected final FeDb db_;
  protected final String name_;
  protected final String owner_;

  // colsByPos[i] refers to the ith column in the table. The first numClusteringCols are
  // the clustering columns.
  protected final List<Column> colsByPos_ = new ArrayList<>();

  // map from lowercase column name to Column object.
  protected final Map<String, Column> colsByName_ = new HashMap<>();

  // Number of clustering columns.
  protected int numClusteringCols_ = 0;

  // Type of this table (array of struct) that mirrors the columns. Useful for analysis.
  protected final ArrayType type_ = new ArrayType(new StructType());

  public VirtualTable(org.apache.hadoop.hive.metastore.api.Table msTable, FeDb db,
      String name, String owner) {
    msTable_ = msTable;
    db_ = db;
    name_ = name;
    owner_ = owner;
  }

  protected void addColumn(Column col) {
    colsByPos_.add(col);
    colsByName_.put(col.getName().toLowerCase(), col);
    ((StructType) type_.getItemType()).addField(
        new StructField(col.getName(), col.getType(), col.getComment()));
  }

  @Override
  public String getTableComment() { return null; }

  @Override
  public boolean isLoaded() { return true; }

  @Override
  public Table getMetaStoreTable() { return msTable_; }

  @Override
  public String getStorageHandlerClassName() { return null; }

  @Override
  public TCatalogObjectType getCatalogObjectType() { return TCatalogObjectType.TABLE; }

  @Override
  public TImpalaTableType getTableType() { return TImpalaTableType.TABLE; }

  @Override
  public FeDb getDb() { return db_; }

  @Override
  public String getName() { return name_; }

  @Override
  public String getFullName() { return (db_ != null ? db_.getName() + "." : "") + name_; }

  @Override
  public TableName getTableName() {
    return new TableName(db_ != null ? db_.getName() : null, name_);
  }

  @Override
  public List<Column> getColumns() { return colsByPos_; }

  @Override
  public List<Column> getColumnsInHiveOrder() {
    List<Column> columns = Lists.newArrayList(getNonClusteringColumns());
    if (getMetaStoreTable() != null &&
        AcidUtils.isFullAcidTable(getMetaStoreTable().getParameters())) {
      // Remove synthetic "row__id" column.
      Preconditions.checkState(columns.get(0).getName().equals("row__id"));
      columns.remove(0);
    }
    columns.addAll(getClusteringColumns());
    return Collections.unmodifiableList(columns);
  }

  @Override
  public List<Column> getClusteringColumns() {
    return Collections.unmodifiableList(colsByPos_.subList(0, numClusteringCols_));
  }

  @Override
  public List<Column> getNonClusteringColumns() {
    return Collections.unmodifiableList(colsByPos_.subList(numClusteringCols_,
        colsByPos_.size()));
  }

  @Override
  public List<String> getColumnNames() { return Column.toColumnNames(colsByPos_); }

  @Override
  public SqlConstraints getSqlConstraints() { return null; }

  @Override
  public int getNumClusteringCols() { return numClusteringCols_; }

  @Override
  public boolean isClusteringColumn(Column c) {
    return c.getPosition() < numClusteringCols_;
  }

  @Override // FeTable
  public Column getColumn(String name) { return colsByName_.get(name.toLowerCase()); }

  @Override
  public ArrayType getType() { return type_; }

  @Override
  public long getWriteId() { return 0; }

  @Override
  public ValidWriteIdList getValidWriteIds() { return null; }

  @Override
  public String getOwnerUser() { return owner_; }
}
