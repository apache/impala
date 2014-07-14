// Copyright 2012 Cloudera Inc.
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

package com.cloudera.impala.catalog;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.thrift.TColumn;
import com.cloudera.impala.thrift.TColumnStats;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Internal representation of column-related metadata.
 * Owned by Catalog instance.
 */
public class Column {
  private final static Logger LOG = LoggerFactory.getLogger(Column.class);

  protected final String name_;
  protected final Type type_;
  protected final String comment_;
  protected int position_;  // in table

  protected final ColumnStats stats_;

  public Column(String name, Type type, int position) {
    this(name, type, null, position);
  }

  public Column(String name, Type type, String comment, int position) {
    name_ = name;
    type_ = type;
    comment_ = comment;
    position_ = position;
    stats_ = new ColumnStats(type);
  }

  public String getComment() { return comment_; }
  public String getName() { return name_; }
  public Type getType() { return type_; }
  public int getPosition() { return position_; }
  public void setPosition(int position) { this.position_ = position; }
  public ColumnStats getStats() { return stats_; }

  public boolean updateStats(ColumnStatisticsData statsData) {
    boolean statsDataCompatibleWithColType = stats_.update(type_, statsData);
    LOG.debug("col stats: " + name_ + " #distinct=" + stats_.getNumDistinctValues());
    return statsDataCompatibleWithColType;
  }

  public void updateStats(TColumnStats statsData) {
    stats_.update(type_, statsData);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this.getClass())
                  .add("name_", name_)
                  .add("type_", type_)
                  .add("comment_", comment_)
                  .add("stats", stats_)
                  .add("position_", position_).toString();
  }

  public static Column fromThrift(TColumn columnDesc) {
    String comment = columnDesc.isSetComment() ? columnDesc.getComment() : null;
    Preconditions.checkState(columnDesc.isSetPosition());
    int position = columnDesc.getPosition();
    Column col;
    if (columnDesc.isIs_hbase_column()) {
      // HBase table column. The HBase column qualifier (column name) is not be set for
      // the HBase row key, so it being set in the thrift struct is not a precondition.
      Preconditions.checkState(columnDesc.isSetColumn_family());
      Preconditions.checkState(columnDesc.isSetIs_binary());
      col = new HBaseColumn(columnDesc.getColumnName(), columnDesc.getColumn_family(),
          columnDesc.getColumn_qualifier(), columnDesc.isIs_binary(),
          Type.fromThrift(columnDesc.getColumnType()), comment, position);
    } else {
      // Hdfs table column.
      col = new Column(columnDesc.getColumnName(),
          Type.fromThrift(columnDesc.getColumnType()), comment, position);
    }
    if (columnDesc.isSetCol_stats()) col.updateStats(columnDesc.getCol_stats());
    return col;
  }

  public TColumn toThrift() {
    TColumn colDesc = new TColumn(name_, type_.toThrift());
    if (comment_ != null) colDesc.setComment(comment_);
    colDesc.setPosition(position_);
    colDesc.setCol_stats(getStats().toThrift());
    return colDesc;
  }
}
