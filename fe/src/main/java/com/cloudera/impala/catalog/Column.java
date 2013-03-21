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

import com.google.common.base.Objects;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;

/**
 * Internal representation of column-related metadata.
 * Owned by Catalog instance.
 */
public class Column {
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

  public void updateStats(ColumnStatisticsData statsData) {
    stats.update(type, statsData);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this.getClass())
                  .add("name", name)
                  .add("type", type)
                  .add("position", position).toString();
  }
}
