/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.impala.catalog.paimon;

import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.Db;
import org.apache.impala.catalog.Table;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.catalog.VirtualColumn;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TPaimonCatalog;
import org.apache.impala.thrift.TPaimonTable;
import org.apache.impala.thrift.TPaimonTableKind;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableType;
import org.apache.impala.util.EventSequence;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.types.DataField;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Representation of an Paimon table in the catalog cache.
 */
public class PaimonTable extends Table implements FePaimonTable {
  private static final Set<String> PAIMON_EXCLUDED_PROPERTIES = Sets.newHashSet();

  static {
      PAIMON_EXCLUDED_PROPERTIES.add(CoreOptions.PATH.key());
      PAIMON_EXCLUDED_PROPERTIES.add("owner");
  }

  // Paimon api table.
  private org.apache.paimon.table.Table table_;



  public PaimonTable(org.apache.hadoop.hive.metastore.api.Table msTable, Db db,
      String name, String owner) {
    super(msTable, db, name, owner);
  }

  @Override
  public TTableDescriptor toThriftDescriptor(
      int tableId, Set<Long> referencedPartitions) {
    TTableDescriptor tableDescriptor =
        new TTableDescriptor(tableId, TTableType.PAIMON_TABLE, getTColumnDescriptors(),
            numClusteringCols_, name_, db_.getName());
    try {
      tableDescriptor.setPaimonTable(PaimonUtil.getTPaimonTable(this));
    } catch (IOException e) { throw new RuntimeException(e); }
    return tableDescriptor;
  }

  @Override
  public TTable toThrift() {
    TTable tTable = super.toThrift();
    try {
      tTable.setPaimon_table(PaimonUtil.getTPaimonTable(this));
    } catch (IOException e) { throw new RuntimeException(e); }
    return tTable;
  }

  @Override
  protected void loadFromThrift(TTable thriftTable) throws TableLoadingException {
    super.loadFromThrift(thriftTable);
    TPaimonTable tpaimon = thriftTable.getPaimon_table();
    try {
      Preconditions.checkArgument(tpaimon.getKind() == TPaimonTableKind.JNI);
      table_ = PaimonUtil.deserialize(ByteBuffer.wrap(tpaimon.getJni_tbl_obj()));
    } catch (Exception e) {
        throw new TableLoadingException("Failed to load paimon table from" +
                " thrift data.",e);
    }
  }

  @Override
  public TCatalogObjectType getCatalogObjectType() {
    return TCatalogObjectType.TABLE;
  }

  /**
   * Verify the table metadata.
   * @throws TableLoadingException when it is unsafe to load the table.
   */
  private void verifyTable(org.apache.hadoop.hive.metastore.api.Table msTbl)
      throws TableLoadingException {
      TPaimonCatalog catalog = PaimonUtil.getTPaimonCatalog(msTbl);
      if (catalog == TPaimonCatalog.HADOOP_CATALOG) {
          if (!msTbl.getParameters()
                  .containsKey(PaimonUtil.PAIMON_HADOOP_CATALOG_LOCATION)) {
            throw new TableLoadingException(
                    String.format(
                            "%s is required for paimon hadoop catalog table.",
                            PaimonUtil.PAIMON_HADOOP_CATALOG_LOCATION)
            );
          }
      }
  }

  /**
   * Load schema and partitioning schemes directly from Paimon.
   */
  public void loadSchemaFromPaimon()
      throws TableLoadingException, ImpalaRuntimeException {
    loadSchema();
    addVirtualColumns();
  }

  /**
   * Loads the HMS schema by Paimon schema
   */
  private void loadSchema() throws TableLoadingException {
    clearColumns();
    try {
      List<DataField> dataFields = getPaimonSchema().getFields();
      List<String> partitionKeys = getPaimonApiTable()
                                       .partitionKeys()
                                       .stream()
                                       .map(String::toLowerCase)
                                       .collect(Collectors.toList());
      List<FieldSchema> hiveFields = PaimonUtil.convertToHiveSchema(getPaimonSchema());
      List<Column> impalaFields = PaimonUtil.convertToImpalaSchema(getPaimonSchema());
      List<FieldSchema> hivePartitionedFields = Lists.newArrayList();
      List<FieldSchema> hiveNonPartitionedFields = Lists.newArrayList();
      List<Column> impalaNonPartitionedFields = Lists.newArrayList();
      List<Column> impalaPartitionedFields = Lists.newArrayList();
      // lookup the clustering columns
      for (String name : partitionKeys) {
        int colIndex = PaimonUtil.getFieldIndexByNameIgnoreCase(getPaimonSchema(), name);
        Preconditions.checkArgument(colIndex >= 0);
        hivePartitionedFields.add(hiveFields.get(colIndex));
        impalaPartitionedFields.add(impalaFields.get(colIndex));
      }
      // put non-clustering columns in natural order
      for (int i = 0; i < dataFields.size(); i++) {
        if (!partitionKeys.contains(dataFields.get(i).name().toLowerCase())) {
          hiveNonPartitionedFields.add(hiveFields.get(i));
          impalaNonPartitionedFields.add(impalaFields.get(i));
        }
      }
      // update hive ms table metadata
      if (!hivePartitionedFields.isEmpty()) {
        msTable_.setPartitionKeys(hivePartitionedFields);
      }
      msTable_.getSd().setCols(hiveNonPartitionedFields);
      // update impala table metadata
      int colPos = 0;
      for (Column col : impalaPartitionedFields) {
        col.setPosition(colPos++);
        addColumn(col);
      }
      for (Column col : impalaNonPartitionedFields) {
        col.setPosition(colPos++);
        addColumn(col);
      }
      numClusteringCols_ = impalaPartitionedFields.size();
      // sync table properties from underlying paimon table
      final Map<String, String> paimonProps =
          Maps.newHashMap(getPaimonApiTable().options());
      for (String key : PAIMON_EXCLUDED_PROPERTIES) { paimonProps.remove(key); }
      for (String key : paimonProps.keySet()) {
        msTable_.getParameters().put(key, paimonProps.get(key));
      }
    } catch (ImpalaRuntimeException e) {
      throw new TableLoadingException(e.getMessage(), e);
    }
  }

  /**
   * Add virtual columns for Paimon table, Paimon table has 4 metadata
   * columns: paimon_file_path,paimon_row_index,__paimon_partition,__paimon_bucket.
   * in these metadata columns,paimon_file_path,and paimon_row_index,__paimon_partition
   * will be mapped to existing virtual column INPUT_FILE_NAME, FILE_POSITION.
   * __paimon_partition,__paimon_bucket will be mapped to newly added
   * PARTITION_VALUE_SERIALIZED, BUCKET_ID separately.
   */
  private void addVirtualColumns() {
    addVirtualColumn(VirtualColumn.INPUT_FILE_NAME);
    addVirtualColumn(VirtualColumn.FILE_POSITION);
    addVirtualColumn(VirtualColumn.PARTITION_VALUE_SERIALIZED);
    addVirtualColumn(VirtualColumn.BUCKET_ID);
  }

  /**
   * Loads the metadata of a Paimon table.
   * <p>
   * Schema and partitioning schemes are loaded directly from Paimon. for column stats,
   * will try to loaded from HMS first, if they are absent, will load from Paimon table.
   * The function also updates the table schema in HMS in order to
   * propagate alterations made to the Pqimon table to HMS.
   * </p>
   */
  @Override
  public void load(boolean reuseMetadata, IMetaStoreClient msClient,
      org.apache.hadoop.hive.metastore.api.Table msTbl, String reason,
      EventSequence catalogTimeline) throws TableLoadingException {
    final Timer.Context context =
        getMetrics().getTimer(Table.LOAD_DURATION_METRIC).time();
    verifyTable(msTbl);
    try {
      // Copy the table to check later if anything has changed.
      msTable_ = msTbl;
      setTableStats(msTable_);
      // Load metadata from Paimon
      final Timer.Context ctxStorageLdTime =
          getMetrics().getTimer(Table.LOAD_DURATION_STORAGE_METADATA).time();
      try {
        table_ = PaimonUtil.createFileStoreTable(msTbl);
        catalogTimeline.markEvent("Loaded Paimon API table");
        loadSchemaFromPaimon();
        catalogTimeline.markEvent("Loaded schema from Paimon");
        applyPaimonTableStatsIfPresent();
        loadAllColumnStats(msClient, catalogTimeline);
        applyPaimonColumnStatsIfPresent();
        catalogTimeline.markEvent("Loaded stats from Paimon");
      } catch (Exception e) {
        throw new TableLoadingException(
            "Error loading metadata for Paimon table " + msTbl.getTableName(), e);
      } finally {
          catalogTimeline.markEvent("Loaded all from Paimon");
          storageMetadataLoadTime_ = ctxStorageLdTime.stop();
      }
      refreshLastUsedTime();
    } finally {
      context.stop();
    }
  }

  @Override
  public org.apache.paimon.table.Table getPaimonApiTable() {
    return table_;
  }

}
