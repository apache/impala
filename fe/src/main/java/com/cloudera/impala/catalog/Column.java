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

import com.cloudera.impala.thrift.TColumnDesc;
import com.cloudera.impala.thrift.TColumnStatsData;
import com.google.common.base.Objects;

/**
 * Internal representation of column-related metadata.
 * Owned by Catalog instance.
 */
public class Column {
  private final static Logger LOG = LoggerFactory.getLogger(Column.class);

  private final String name;
  private final PrimitiveType type;
  private final String comment;
  private int position;  // in table

  private final ColumnStats stats;

  public Column(String name, PrimitiveType type, int position) {
    this(name, type, null, position);
  }

  public Column(String name, PrimitiveType type, String comment, int position) {
    this.name = name;
    this.type = type;
    this.comment = comment;
    this.position = position;
    this.stats = new ColumnStats(type);
  }

  public String getComment() {
    return comment;
  }

  public String getName() {
    return name;
  }

  public PrimitiveType getType() {
    return type;
  }

  public int getPosition() {
    return position;
  }

  public void setPosition(int position) {
    this.position = position;
  }

  public ColumnStats getStats() { return stats; }

  public boolean updateStats(ColumnStatisticsData statsData) {
    boolean statsDataCompatibleWithColType = stats.update(type, statsData);
    LOG.debug("col stats: " + name + " #distinct=" + stats.getNumDistinctValues());
    return statsDataCompatibleWithColType;
  }

  public void updateStats(TColumnStatsData statsData) {
    stats.update(type, statsData);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this.getClass())
                  .add("name", name)
                  .add("type", type)
                  .add("position", position).toString();
  }

  public static Column fromThrift(TColumnDesc columnDesc, int position) {
    // TODO: Should 'position' be part of TColumnDesc?
    String comment = columnDesc.isSetComment() ? columnDesc.getComment() : null;
    Column col = new Column(columnDesc.getColumnName(),
        PrimitiveType.fromThrift(columnDesc.getColumnType()), comment, position);
    if (columnDesc.isSetCol_stats()) col.updateStats(columnDesc.getCol_stats());
    return col;
  }
}
