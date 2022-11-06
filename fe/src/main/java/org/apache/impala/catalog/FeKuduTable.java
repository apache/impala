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
import java.util.List;

import javax.xml.bind.DatatypeConverter;

import org.apache.impala.analysis.KuduPartitionParam;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.util.KuduUtil;
import org.apache.impala.util.TResultRowBuilder;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.LocatedTablet;
import org.apache.kudu.client.PartitionSchema;
import org.apache.kudu.client.PartitionSchema.HashBucketSchema;
import org.apache.kudu.client.PartitionSchema.RangeSchema;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Frontend interface for interacting with a Kudu-backed table.
 */
public interface FeKuduTable extends FeTable {
  /**
   * Return the comma-separated list of masters for the Kudu cluster
   * backing this table.
   */
  String getKuduMasterHosts();

  /**
   * Return the name of the Kudu table backing this table.
   */
  String getKuduTableName();

  /**
   * Return true if the primary key is unique.
   */
  boolean isPrimaryKeyUnique();

  /**
   * Return true if the table has auto-incrementing column.
   */
  boolean hasAutoIncrementingColumn();

  /**
   * Return the names of the columns that make up the primary key
   * of this table.
   */
  List<String> getPrimaryKeyColumnNames();

  /**
   * Return the Kudu partitioning clause information.
   */
  List<KuduPartitionParam> getPartitionBy();

  /**
   * Utility functions for acting on FeKuduTable.
   *
   * When we fully move to Java 8, these can become default methods of the
   * interface.
   */
  public static abstract class Utils {
    /**
     * Returns the range-based partitioning of the given table if it exists,
     * null otherwise.
     */
    private static KuduPartitionParam getRangePartitioning(FeKuduTable table) {
      for (KuduPartitionParam partitionParam: table.getPartitionBy()) {
        if (partitionParam.getType() == KuduPartitionParam.Type.RANGE) {
          return partitionParam;
        }
      }
      return null;
    }

    /**
     * Returns the column names of the table's range-based partitioning or an empty
     * list if the table doesn't have a range-based partitioning.
     */
    public static List<String> getRangePartitioningColNames(FeKuduTable table) {
      KuduPartitionParam rangePartitioning = getRangePartitioning(table);
      if (rangePartitioning == null) return Collections.<String>emptyList();
      return rangePartitioning.getColumnNames();
    }

    public static List<KuduPartitionParam> loadPartitionByParams(
        org.apache.kudu.client.KuduTable kuduTable) {
      List<KuduPartitionParam> ret = new ArrayList<>();

      Preconditions.checkNotNull(kuduTable);
      Schema tableSchema = kuduTable.getSchema();
      PartitionSchema partitionSchema = kuduTable.getPartitionSchema();
      for (HashBucketSchema hashBucketSchema: partitionSchema.getHashBucketSchemas()) {
        List<String> columnNames = new ArrayList<>();
        for (int colId: hashBucketSchema.getColumnIds()) {
          columnNames.add(getColumnNameById(tableSchema, colId));
        }
        ret.add(KuduPartitionParam.createHashParam(columnNames,
            hashBucketSchema.getNumBuckets()));
      }
      RangeSchema rangeSchema = partitionSchema.getRangeSchema();
      List<Integer> columnIds = rangeSchema.getColumns();
      if (columnIds.isEmpty()) return ret;
      List<String> columnNames = new ArrayList<>();
      for (int colId: columnIds) columnNames.add(getColumnNameById(tableSchema, colId));
      // We don't populate the split values because Kudu's API doesn't currently support
      // retrieving the split values for range partitions.
      // TODO: File a Kudu JIRA.
      ret.add(KuduPartitionParam.createRangeParam(columnNames, null));

      return ret;
    }

    public static TResultSet getTableStats(FeKuduTable table)
        throws ImpalaRuntimeException {
      TResultSet result = new TResultSet();
      TResultSetMetadata resultSchema = new TResultSetMetadata();
      result.setSchema(resultSchema);

      // Build column header
      resultSchema.addToColumns(new TColumn("#Rows", Type.BIGINT.toThrift()));
      resultSchema.addToColumns(new TColumn("#Partitions", Type.BIGINT.toThrift()));
      resultSchema.addToColumns(new TColumn("Size", Type.STRING.toThrift()));
      resultSchema.addToColumns(new TColumn("Format", Type.STRING.toThrift()));
      resultSchema.addToColumns(new TColumn("Location", Type.STRING.toThrift()));

      KuduClient client = KuduUtil.getKuduClient(table.getKuduMasterHosts());
      try {
        // Call openTable to ensure we get the latest metadata for the Kudu table.
        org.apache.kudu.client.KuduTable kuduTable =
            client.openTable(table.getKuduTableName());
        List<LocatedTablet> tablets = kuduTable.getTabletsLocations(
            BackendConfig.INSTANCE.getKuduClientTimeoutMs());
        TResultRowBuilder tResultRowBuilder = new TResultRowBuilder();
        tResultRowBuilder.add(table.getNumRows());
        tResultRowBuilder.add(tablets.size());
        tResultRowBuilder.addBytes(kuduTable.getTableStatistics().getOnDiskSize());
        tResultRowBuilder.add("KUDU");
        tResultRowBuilder.add(table.getKuduMasterHosts());
        result.addToRows(tResultRowBuilder.get());
      } catch (Exception e) {
        throw new ImpalaRuntimeException("Error accessing Kudu for table stats.", e);
      }
      return result;
    }

    public static TResultSet getPartitions(FeKuduTable table)
        throws ImpalaRuntimeException {
      TResultSet result = new TResultSet();
      TResultSetMetadata resultSchema = new TResultSetMetadata();
      result.setSchema(resultSchema);

      resultSchema.addToColumns(new TColumn("Start Key", Type.STRING.toThrift()));
      resultSchema.addToColumns(new TColumn("Stop Key", Type.STRING.toThrift()));
      resultSchema.addToColumns(new TColumn("Leader Replica", Type.STRING.toThrift()));
      resultSchema.addToColumns(new TColumn("#Replicas", Type.INT.toThrift()));

      KuduClient client = KuduUtil.getKuduClient(table.getKuduMasterHosts());
      try {
        // Call openTable to ensure we get the latest metadata for the Kudu table.
        org.apache.kudu.client.KuduTable kuduTable =
            client.openTable(table.getKuduTableName());
        List<LocatedTablet> tablets =
            kuduTable.getTabletsLocations(BackendConfig.INSTANCE.getKuduClientTimeoutMs());
        if (tablets.isEmpty()) {
          TResultRowBuilder builder = new TResultRowBuilder();
          result.addToRows(
              builder.add("N/A").add("N/A").add("N/A").add("-1").get());
          return result;
        }
        for (LocatedTablet tab: tablets) {
          TResultRowBuilder builder = new TResultRowBuilder();
          builder.add(DatatypeConverter.printHexBinary(
              tab.getPartition().getPartitionKeyStart()));
          builder.add(DatatypeConverter.printHexBinary(
              tab.getPartition().getPartitionKeyEnd()));
          LocatedTablet.Replica leader = tab.getLeaderReplica();
          if (leader == null) {
            // Leader might be null, if it is not yet available (e.g. during
            // leader election in Kudu)
            builder.add("Leader N/A");
          } else {
            builder.add(leader.getRpcHost() + ":" + leader.getRpcPort().toString());
          }
          builder.add(tab.getReplicas().size());
          result.addToRows(builder.get());
        }
      } catch (Exception e) {
        throw new ImpalaRuntimeException("Error accessing Kudu for table partitions.", e);
      }
      return result;
    }

    public static TResultSet getRangePartitions(FeKuduTable table, boolean showHashSchema)
        throws ImpalaRuntimeException {
      TResultSet result = new TResultSet();
      TResultSetMetadata resultSchema = new TResultSetMetadata();
      result.setSchema(resultSchema);

      // Build column header
      String header = "RANGE (" + Joiner.on(',').join(
          Utils.getRangePartitioningColNames(table)) + ")";
      if (showHashSchema) {
        header += " HASH SCHEMA";
      }
      resultSchema.addToColumns(new TColumn(header, Type.STRING.toThrift()));
      KuduClient client = KuduUtil.getKuduClient(table.getKuduMasterHosts());
      try {
        // Call openTable to ensure we get the latest metadata for the Kudu table.
        org.apache.kudu.client.KuduTable kuduTable =
            client.openTable(table.getKuduTableName());
        // The Kudu table API will return the partitions in sorted order by value.
        long timeout = BackendConfig.INSTANCE.getKuduClientTimeoutMs();
        List<String> partitions = showHashSchema ?
            kuduTable.getFormattedRangePartitionsWithHashSchema(timeout) :
            kuduTable.getFormattedRangePartitions(timeout);
        if (partitions.isEmpty()) {
          TResultRowBuilder builder = new TResultRowBuilder();
          result.addToRows(builder.add("").get());
          return result;
        }
        for (String partition: partitions) {
          TResultRowBuilder builder = new TResultRowBuilder();
          builder.add(partition);
          result.addToRows(builder.get());
        }
      } catch (Exception e) {
        throw new ImpalaRuntimeException("Error accessing Kudu for table partitions.", e);
      }
      return result;
    }

    /**
     * Returns the name of a Kudu column with id 'colId'.
     */
    private static String getColumnNameById(Schema tableSchema, int colId) {
      Preconditions.checkNotNull(tableSchema);
      ColumnSchema col = tableSchema.getColumnByIndex(tableSchema.getColumnIndex(colId));
      Preconditions.checkNotNull(col);
      return col.getName();
    }
  }
}
