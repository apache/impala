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

package org.apache.impala.util;

import static org.apache.impala.catalog.Table.TBL_PROP_EXTERNAL_TABLE_PURGE;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.TableMigrationUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.impala.analysis.TableName;
import org.apache.impala.catalog.FeCatalog;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.IcebergTable;
import org.apache.impala.catalog.iceberg.IcebergCatalog;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TConvertTableRequest;
import org.apache.impala.thrift.TIcebergCatalog;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.thrift.TTableName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes the migration of a legacy Hive table to Iceberg table.
 *
 * This is an in-place migration where an Iceberg table is created on the same location
 * as the Hive table and the existing files are appended to the Iceberg table. The file
 * metadata creation is done using TableMigrationUtil from Iceberg.
 */
public class MigrateTableUtil {

  private static final Logger LOG = LoggerFactory.getLogger(MigrateTableUtil.class);
  private static final long RETRY_TIMEOUT_MS = 3600000; // 1 hour
  private static final int RETRY_DELAY_MS = 300;

  private MigrateTableUtil() { }

  /**
   * Create an external Iceberg table using the data of the HDFS table.
   */
  public static void migrateToIcebergTable(
      IMetaStoreClient hmsClient,
      TConvertTableRequest request,
      FeFsTable table,
      TQueryOptions queryOptions) throws ImpalaRuntimeException {
    LOG.info("Migrating table to Iceberg: " + table.getFullName());
    Schema schema =
        IcebergSchemaConverter.convertToIcebergSchema(table.getMetaStoreTable());
    PartitionSpec spec = IcebergSchemaConverter.createIcebergPartitionSpec(
        table.getMetaStoreTable(), schema);
    String fileFormat = getFileFormat(table.getMetaStoreTable().getSd());
    Preconditions.checkNotNull(fileFormat);
    Map<String, String> props = Maps.newHashMap(request.getProperties());
    props.put(IcebergTable.ICEBERG_FILE_FORMAT, fileFormat);
    if (IcebergUtil.isHiveCatalog(props)) {
      props.put(TBL_PROP_EXTERNAL_TABLE_PURGE, "true");
    }
    String location = table.getLocation();
    TIcebergCatalog tCatalog = IcebergUtil.getTIcebergCatalog(props);
    IcebergCatalog catalog = IcebergUtil.getIcebergCatalog(tCatalog, location);
    TTableName name = request.getTable_name();
    TableIdentifier id = TableIdentifier.of(name.getDb_name(), name.getTable_name());

    Table icebergTable = catalog.createTable(id, schema, spec, location, props);
    Preconditions.checkNotNull(icebergTable);

    TableName tableName = TableName.fromThrift(name);
    try {
      if (IcebergUtil.isHiveCatalog(props)) {
        waitForTableToBeCreated(hmsClient , tableName);
      }

      importDataFilesInHdfsTable(table, icebergTable, queryOptions);
    } catch (Exception e) {
      // If the migration failed for some reason, we clean up the Iceberg table.
      if (IcebergUtil.isHiveCatalog(props)) {
        // For tables in Hive Catalog we have to use the Iceberg API to make sure it's
        // removed from HMS. Using purge='false' to guarantee that the files remain on
        // disk.
        catalog.dropTable(tableName.getDb(), tableName.getTbl(), false);
      }
      // We drop the metadata folder, otherwise running MIGRATE TABLE the next time could
      // fail because Iceberg might think that there is already an existing Iceberg table.
      Path metadataPath = new Path(location, IcebergTable.METADATA_FOLDER_NAME);
      FileSystemUtil.deleteIfExists(metadataPath);

      throw new ImpalaRuntimeException("Failed to import data into Iceberg table\n", e);
    }
  }

  private static void waitForTableToBeCreated(IMetaStoreClient hmsClient,
      TableName tableName) throws ImpalaRuntimeException {
    if (getHmsTableNoThrow(hmsClient, tableName.getDb(), tableName.getTbl()) != null) {
      return;
    }
    long att = 0;
    try (ThreadNameAnnotator nameAnnotator = new ThreadNameAnnotator(
        "waiting for " + tableName + " to be created")) {
      long begin = System.currentTimeMillis();
      long end;
      do {
        try {
          Thread.sleep(RETRY_DELAY_MS);
          LOG.info("Waiting for " + tableName + " to be created, attempt: " + ++att);
        } catch (InterruptedException e) {
          // Ignore
        }
        end = System.currentTimeMillis();
      } while (getHmsTableNoThrow(hmsClient, tableName.getDb(),
          tableName.getTbl()) == null && (end - begin < RETRY_TIMEOUT_MS));
    }

    if (getHmsTableNoThrow(hmsClient, tableName.getDb(), tableName.getTbl()) == null) {
      throw new ImpalaRuntimeException("Failed to wait for " + tableName +
          " to be created");
    }
  }

  private static org.apache.hadoop.hive.metastore.api.Table getHmsTableNoThrow(
      IMetaStoreClient hiveClient, String dbName, String tblName) {
    try {
      return hiveClient.getTable(dbName, tblName);
    } catch (Exception e) {
      return null;
    }
  }

  public static String getFileFormat(StorageDescriptor sd) {
    for (String fileFormat : ImmutableList.of(FileFormat.PARQUET.name().toLowerCase(),
        FileFormat.ORC.name().toLowerCase(),
        FileFormat.AVRO.name().toLowerCase())) {
      if (sd.getInputFormat().toLowerCase().contains(fileFormat)) {
        return fileFormat;
      }
    }
    return null;
  }

  private static void importDataFilesInHdfsTable(FeFsTable hdfsTable, Table icebergTable,
      TQueryOptions queryOptions) throws ImpalaRuntimeException {
    Params params = Params.of(
        hdfsTable.getMetaStoreTable().getSd().getInputFormat(),
        icebergTable.spec(),
        icebergTable.schema(),
        MetricsConfig.forTable(icebergTable),
        getDegreeOfParallelism(queryOptions),
        icebergTable.newAppend(),
        getDebugAction(queryOptions));

    if (hdfsTable.isPartitioned()) {
      importDataFiles(hdfsTable, params);
    } else {
      importDataFiles(hdfsTable.getMetaStoreTable().getSd().getLocation(), params);
    }
    params.append_.commit();
  }

  private static void importDataFiles(FeFsTable hdfsTable, Params params)
      throws ImpalaRuntimeException {
    List<? extends FeFsPartition> partitions =
        hdfsTable.loadPartitions(hdfsTable.getPartitionIds());
    for (FeFsPartition part : partitions) {
      String partitionName = part.getPartitionName();
      Map<String, String> partitionKeys = Collections.emptyMap();
      try {
        partitionKeys = Warehouse.makeSpecFromName(partitionName);
      } catch (MetaException e) {
        throw new ImpalaRuntimeException(
            "Unable to create partition keys for " + partitionName, e);
      }

      importDataFilesImpl(partitionKeys, part.getLocationPath(), params);
    }
  }

  private static void importDataFiles(String location, Params params)
          throws ImpalaRuntimeException {
    importDataFilesImpl(Collections.emptyMap(), new Path(location), params);
  }

  private static void importDataFilesImpl(Map<String, String> partitionKeys,
      Path location, Params params) throws ImpalaRuntimeException {
    try {
      LOG.info("Creating Iceberg metadata for folder: " + location.toString() + " using "
          + params.threadNum_ + " thread(s).");

      if (params.debugAction_.equalsIgnoreCase("CONVERT_TABLE_FAIL_ICEBERG_CALL")) {
        throw new IllegalArgumentException("Exception thrown by debug action.");
      }

      List<DataFile> dataFiles = TableMigrationUtil.listPartition(
              partitionKeys,
              location.toString(),
              params.format_,
              params.spec_,
              FileSystemUtil.getConfiguration(),
              params.metricsConfig_,
              null, // NameMapping mapping
              params.threadNum_);

      dataFiles.forEach(params.append_::appendFile);
    } catch (Exception e) {
      throw new ImpalaRuntimeException(
          "Unable load data files for location: " + location.toString(), e);
    }
  }

  private static int getDegreeOfParallelism(TQueryOptions queryOptions) {
    int threadNum = Runtime.getRuntime().availableProcessors();
    if (queryOptions.isSetNum_threads_for_table_migration() &&
        queryOptions.num_threads_for_table_migration > 0) {
      threadNum = Math.min(queryOptions.num_threads_for_table_migration,
          Runtime.getRuntime().availableProcessors());
    }
    return threadNum;
  }

  private static String getDebugAction(TQueryOptions queryOptions) {
    if (!queryOptions.isSetDebug_action()) return "";
    return queryOptions.getDebug_action();
  }

  private static class Params {
    final String format_;
    final PartitionSpec spec_;
    final Schema schema_;
    final MetricsConfig metricsConfig_;
    final int threadNum_;
    final AppendFiles append_;
    final String debugAction_;

    private Params(String format, PartitionSpec spec, Schema schema,
        MetricsConfig metricsConfig, int threadNum, AppendFiles append,
        String debugAction) {
      format_ = format;
      spec_ = spec;
      schema_ = schema;
      metricsConfig_ = metricsConfig;
      threadNum_ = threadNum;
      append_ = append;
      debugAction_ = debugAction;
    }

    static Params of(String format, PartitionSpec spec, Schema schema,
        MetricsConfig metricsConfig, int threadNum, AppendFiles append,
        String debugAction) {
      return new Params(
          format,
          spec,
          schema,
          metricsConfig,
          threadNum,
          append,
          debugAction);
    }
  }
}
