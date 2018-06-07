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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/**
 * Static utility functions shared between FeCatalog implementations.
 */
public abstract class FeCatalogUtils {
  /**
   * Gets the ColumnType from the given FieldSchema by using Impala's SqlParser.
   *
   * The type can either be:
   *   - Supported by Impala, in which case the type is returned.
   *   - A type Impala understands but is not yet implemented (e.g. date), the type is
   *     returned but type.IsSupported() returns false.
   *   - A supported type that exceeds an Impala limit, e.g., on the nesting depth.
   *   - A type Impala can't understand at all, and a TableLoadingException is thrown.
   *
   * Throws a TableLoadingException if the FieldSchema could not be parsed. In this
   * case, 'tableName' is included in the error message.
   */
  public static Type parseColumnType(FieldSchema fs, String tableName)
     throws TableLoadingException {
    Type type = Type.parseColumnType(fs.getType());
    if (type == null) {
      throw new TableLoadingException(String.format(
          "Unsupported type '%s' in column '%s' of table '%s'",
          fs.getType(), fs.getName(), tableName));
    }
    if (type.exceedsMaxNestingDepth()) {
      throw new TableLoadingException(String.format(
          "Type exceeds the maximum nesting depth of %s:\n%s",
          Type.MAX_NESTING_DEPTH, type.toSql()));
    }
    return type;
  }

  /**
   * Convert a list of HMS FieldSchemas to internal Column types.
   * @throws TableLoadingException if any type is invalid
   */
  public static ImmutableList<Column> fieldSchemasToColumns(
      Iterable<FieldSchema> fieldSchemas,
      String tableName) throws TableLoadingException {
    int pos = 0;
    ImmutableList.Builder<Column> ret = ImmutableList.builder();
    for (FieldSchema s : fieldSchemas) {
      Type type = parseColumnType(s, tableName);
      ret.add(new Column(s.getName(), type, s.getComment(), pos));
      ++pos;
    }
    return ret.build();
  }

  /**
   * Validate that the clustering columns are valid for a table
   *
   * TODO(todd): consider refactoring to combine with
   * HdfsTable.addColumnsFromFieldSchema
   *
   * @throws TableLoadingException if the columns are invalid
   */
  public static void validateClusteringColumns(
      Iterable<Column> cols, String tableName)
      throws TableLoadingException {
    // Check if we support partitioning on columns of such a type.
    for (Column c : cols) {
      Type type = c.getType();
      if (!type.supportsTablePartitioning()) {
        throw new TableLoadingException(
            String.format("Failed to load metadata for table '%s' because of " +
                "unsupported partition-column type '%s' in partition column '%s'",
                tableName, type.toString(), c.getName()));
      }
    }
  }

  /**
   * Returns the value of the ROW_COUNT constant, or -1 if not found.
   */
  public static long getRowCount(Map<String, String> parameters) {
    return getLongParam(StatsSetupConst.ROW_COUNT, parameters);
  }

  public static long getTotalSize(Map<String, String> parameters) {
    return getLongParam(StatsSetupConst.TOTAL_SIZE, parameters);
  }

  private static long getLongParam(String key, Map<String, String> parameters) {
    if (parameters == null) return -1;
    String value = parameters.get(key);
    if (value == null) return -1;
    try {
      return Long.valueOf(value);
    } catch (NumberFormatException exc) {
      // ignore
    }
    return -1;
  }

  /**
   * Convenience method to load exactly one partition from a table.
   *
   * TODO(todd): upon moving to Java8 this could be a default method
   * in FeFsTable.
   */
  public static FeFsPartition loadPartition(FeFsTable table,
      long partitionId) {
    Collection<? extends FeFsPartition> partCol = table.loadPartitions(
        Collections.singleton(partitionId));
    if (partCol.size() != 1) {
      throw new AssertionError(String.format(
          "expected exactly one result fetching partition ID %s from table %s " +
          "(got %s)", partitionId, table.getFullName(), partCol.size()));
    }
    return Iterables.getOnlyElement(partCol);
  }

  /**
   * Load all partitions from the given table.
   */
  public static Collection<? extends FeFsPartition> loadAllPartitions(
      FeFsTable table) {
    return table.loadPartitions(table.getPartitionIds());
  }
}
