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

package org.apache.impala.catalog.paimon;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TCreateTableParams;
import org.apache.impala.thrift.TDropTableOrViewParams;
import org.apache.impala.util.EventSequence;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * This is a helper for the CatalogOpExecutor to provide Paimon related DDL functionality
 * such as creating and dropping tables from Paimon.
 */
public class PaimonCatalogOpExecutor {
  public static final Logger LOG = LoggerFactory.getLogger(PaimonCatalogOpExecutor.class);
  public static final String LOADED_PAIMON_TABLE = "Loaded paimon table";
    public static final String CREATED_PAIMON_TABLE =
            "Created table using Paimon Catalog ";
  /**
   * Create Paimon table by Paimon api
   * Return value is table object from Paimon
   */
  public static String createTable(Identifier identifier, String location,
      TCreateTableParams params, org.apache.hadoop.hive.metastore.api.Table newTable)
      throws ImpalaRuntimeException {
    try {
      List<TColumn> all_columns = Lists.newArrayList(params.getColumns());
      List<String> partitionKeys = Lists.newArrayList();
      List<String> primaryKeys = Lists.newArrayList();

      // handle partition columns
      if (params.getPartition_columnsSize() > 0) {
        Preconditions.checkArgument(
            !params.getTable_properties().containsKey(CoreOptions.PARTITION.key()));
        all_columns.addAll(params.getPartition_columns());
        partitionKeys.addAll(params.getPartition_columns()
                                 .stream()
                                 .map(c -> c.getColumnName().toLowerCase())
                                 .collect(Collectors.toList()));
        params.getTable_properties().remove(CoreOptions.PARTITION.key());
      }
      if (params.getTable_properties().containsKey(CoreOptions.PARTITION.key())) {
        Preconditions.checkArgument(params.getPartition_columnsSize() <= 0);
        partitionKeys.clear();
        List<String> newPartKeys = Arrays
                                       .stream(params.getTable_properties()
                                                   .get(CoreOptions.PARTITION.key())
                                                   .split(","))
                                       .collect(Collectors.toList());
        partitionKeys.addAll(newPartKeys);
        params.getTable_properties().remove(CoreOptions.PARTITION.key());
      }

      // handle primary keys
      if (params.getPrimary_key_column_namesSize() > 0) {
        primaryKeys.addAll(params.getPrimary_key_column_names());
        Preconditions.checkArgument(
            !params.getTable_properties().containsKey(CoreOptions.PRIMARY_KEY.key()));
      }

      if (params.getTable_properties().containsKey(CoreOptions.PRIMARY_KEY.key())) {
        primaryKeys.clear();
        List<String> exist = Arrays
                                 .stream(params.getTable_properties()
                                             .get(CoreOptions.PRIMARY_KEY.key())
                                             .split(","))
                                 .collect(Collectors.toList());
        primaryKeys.addAll(exist);
      }
      params.getTable_properties().put(
          CoreOptions.PRIMARY_KEY.key(), StringUtils.join(primaryKeys, ","));

      Path path = new Path(location);
      CatalogContext context = PaimonUtil.catalogContext(newTable, location);
      FileIO fileIO;
      try {
        fileIO = FileIO.get(path, context);
      } catch (IOException e) {
        throw new ImpalaRuntimeException("Failed to get file IO for paimon table", e);
      }

      SchemaManager schemaManager = new SchemaManager(fileIO, path);
      Optional<TableSchema> tableSchema = schemaManager.latest();
      if (!tableSchema.isPresent()) {
        Schema schema = PaimonUtil.genPaimonSchema(
            all_columns, partitionKeys, params.getTable_properties());
        schemaManager.createTable(schema);
        LOG.info("Create paimon table successful.");
      } else {
        throw new AlreadyExistsException(
            "Can't create paimon table, since the table location is not clean.");
      }
      return location;
    } catch (Exception ex) {
      throw new ImpalaRuntimeException("Failed to create paimon table", ex);
    }
  }

  /**
   * Populates HMS table schema based on the Paimon table's schema.
   */
  public static void populateExternalTableSchemaFromPaimonTable(
      org.apache.hadoop.hive.metastore.api.Table msTbl, Table tbl)
      throws TableLoadingException {
    try {
      List<FieldSchema> cols = Lists.newArrayList();
      List<FieldSchema> partCols = Lists.newArrayList();
      Set<String> partSet = tbl.partitionKeys()
                                .stream()
                                .map(String::toLowerCase)
                                .collect(Collectors.toSet());
      List<String> primaryKeys = tbl.primaryKeys()
                                     .stream()
                                     .map(String::toLowerCase)
                                     .collect(Collectors.toList());
      List<FieldSchema> hiveFields = PaimonUtil.convertToHiveSchema(tbl.rowType());

      for (int i = 0; i < tbl.rowType().getFields().size(); i++) {
        DataField dataField = tbl.rowType().getFields().get(i);
        if (partSet.contains(dataField.name().toLowerCase())) {
          partCols.add(hiveFields.get(i));
        } else {
          cols.add(hiveFields.get(i));
        }
      }
      msTbl.getSd().setCols(cols);
      msTbl.setPartitionKeys(partCols);
      Map<String, String> parameters = msTbl.getParameters();
      // Update primary key
      if (!primaryKeys.isEmpty()) {
        parameters.put(CoreOptions.PRIMARY_KEY.key(), StringUtils.join(primaryKeys, ","));
      }
    } catch (ImpalaRuntimeException e) {
      throw new TableLoadingException("Error while infer schema from underlying" +
              " paimon table", e);
    }
  }

  /**
   * Drop Paimon table by Paimon api
   * Return value is whether requires to drop hms table
   */
  public static boolean dropTable(org.apache.hadoop.hive.metastore.api.Table msTbl,
      org.apache.impala.catalog.Table existingTbl, EventSequence catalogTimeline,
      TDropTableOrViewParams param) throws ImpalaRuntimeException {
    if (msTbl == null) { return false; }
    Preconditions.checkArgument(existingTbl instanceof PaimonTable);
    boolean isSynchronizedPaimonTable =
        PaimonUtil.isSynchronizedTable(msTbl) || param.isPurge();

    if (isSynchronizedPaimonTable) {
      String location = msTbl.getSd().getLocation();
      Path path = new Path(location);
      CatalogContext context = PaimonUtil.catalogContext(msTbl, location);
      try {
        FileIO fileIO = FileIO.get(path, context);
        if (fileIO.exists(path)) {
          // clear the data
          fileIO.deleteDirectoryQuietly(path);
          fileIO.mkdirs(path);
        }
      } catch (IOException e) {
        LOG.warn("Delete directory '{}' fail for the paimon table.", path, e);
      }
      catalogTimeline.markEvent("Dropped table using Paimon");
    }
    return true;
  }
}