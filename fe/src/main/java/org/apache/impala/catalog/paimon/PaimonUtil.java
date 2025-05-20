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

import static org.apache.impala.catalog.Table.isExternalPurgeTable;
import static org.apache.paimon.CoreOptions.PARTITION_DEFAULT_NAME;
import static org.apache.paimon.CoreOptions.PARTITION_GENERATE_LEGCY_NAME;
import static org.apache.paimon.utils.HadoopUtils.HADOOP_LOAD_DEFAULT_CONFIG;

import com.google.common.collect.Lists;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.DatabaseNotFoundException;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.MetaStoreClientPool;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.PrintUtils;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.thrift.TColumn;
import org.apache.impala.thrift.TDescribeHistoryParams;
import org.apache.impala.thrift.TGetTableHistoryResult;
import org.apache.impala.thrift.TGetTableHistoryResultItem;
import org.apache.impala.thrift.TPaimonCatalog;
import org.apache.impala.thrift.TPaimonTable;
import org.apache.impala.thrift.TPaimonTableKind;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TResultSetMetadata;
import org.apache.impala.thrift.TShowFilesParams;
import org.apache.impala.thrift.TTableStats;
import org.apache.impala.util.TResultRowBuilder;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.AbstractCatalog;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.CatalogUtils;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.hive.HiveTypeUtils;
import org.apache.paimon.hive.LocationKeyExtractor;
import org.apache.paimon.hive.utils.HiveUtils;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.stats.ColStats;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.FileStoreTableFactory;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.DeletionFile;
import org.apache.paimon.table.source.RawFile;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.table.system.SystemTableLoader;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypeFamily;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.thrift.TException;
import org.postgresql.shaded.com.ongres.scram.common.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class PaimonUtil {
  final static Logger LOG = LoggerFactory.getLogger(PaimonUtil.class);

  public static final String PAIMON_STORAGE_HANDLER =
      "org.apache.paimon.hive.PaimonStorageHandler";
  public static final String STORAGE_HANDLER = "storage_handler";
  public static final String PAIMON_CATALOG = "paimon.catalog";
  public static final String HIVE_CATALOG = "hive";
  public static final String PAIMON_PROPERTY_PREFIX = "";
  public static final String PAIMON_HADOOP_CATALOG_LOCATION = "paimon.catalog_location";
  public static final String PAIMON_TABLE_LOCATION = "paimon_location";
  public static final String PAIMON_TABLE_IDENTIFIER = "paimon.table_identifier";

  private static final HiveConf hiveConf_ = new HiveConf();
  public static Catalog catalog_ = null;
  private static final String metastoreClientClass_ =
      "org.apache.hadoop.hive.metastore.HiveMetaStoreClient";

  /**
   * Returns true if the given Metastore Table represents an Paimon table.
   * Versions of Hive/Paimon are inconsistent which Paimon related fields are set
   * (e.g., HIVE-6548 changed the input format to null).
   * For maximum compatibility consider all known fields that indicate an Paimon table.
   */
  public static boolean isPaimonTable(org.apache.hadoop.hive.metastore.api.Table msTbl) {
    if (msTbl.getParameters() != null
        && PAIMON_STORAGE_HANDLER.equals(
            msTbl.getParameters().getOrDefault(STORAGE_HANDLER, ""))) {
      return true;
    }
    StorageDescriptor sd = msTbl.getSd();
    if (sd == null) return false;
    if (sd.getInputFormat() != null
        && sd.getInputFormat().equals(HdfsFileFormat.PAIMON.inputFormat())) {
      return true;
    } else
      return sd.getSerdeInfo() != null && sd.getSerdeInfo().getSerializationLib() != null
          && sd.getSerdeInfo().getSerializationLib().equals(
              HdfsFileFormat.PAIMON.serializationLib());
  }

  public static ByteBuffer serialize(FePaimonTable paimonTable) throws IOException {
    return ByteBuffer.wrap(SerializationUtils.serialize(paimonTable.getPaimonApiTable()));
  }

  public static Table deserialize(ByteBuffer b) throws Exception {
    return SerializationUtils.deserialize(b.array());
  }

  /**
   * Get Thrift object for paimon table.
   */
  public static TPaimonTable getTPaimonTable(FePaimonTable paimonTable)
      throws IOException {
    TPaimonTable t_ = new TPaimonTable();
    t_.setKind(TPaimonTableKind.JNI);
    t_.setJni_tbl_obj(serialize(paimonTable));
    return t_;
  }

  /**
   * Converts Paimon schema to a Hive schema.
   */
  public static List<FieldSchema> convertToHiveSchema(RowType schema)
      throws ImpalaRuntimeException {
    List<FieldSchema> ret = new ArrayList<>();
    for (DataField dataField : schema.getFields()) {
      ret.add(new FieldSchema(dataField.name().toLowerCase(),
          HiveTypeUtils.toTypeInfo(dataField.type()).getTypeName(),
          dataField.description()));
    }
    return ret;
  }

  /**
   * Converts Paimon schema to an Impala schema.
   */
  public static List<Column> convertToImpalaSchema(RowType schema)
      throws ImpalaRuntimeException {
    List<Column> ret = new ArrayList<>();
    int pos = 0;
    for (DataField dataField : schema.getFields()) {
      Type colType = ImpalaTypeUtils.toImpalaType(dataField.type());
      ret.add(new Column(dataField.name().toLowerCase(), colType, pos++));
    }
    return ret;
  }

  /**
   * Generates Paimon schema from given columns.
   */
  public static Schema genPaimonSchema(List<TColumn> columns, List<String> partitionKeys,
      Map<String, String> options) {
    Schema.Builder schemaBuilder = Schema.newBuilder();
    for (TColumn column : columns) {
      schemaBuilder.column(column.getColumnName().toLowerCase(),
          ImpalaTypeUtils.fromImpalaType(Type.fromThrift(column.getColumnType())));
    }
    if (!partitionKeys.isEmpty()) { schemaBuilder.partitionKeys(partitionKeys); }
    if (!options.isEmpty()) { schemaBuilder.options(options); }
    return schemaBuilder.build();
  }

  /**
   * Returns the corresponding paimon catalog implementation.
   * TODO:
   */
  public static Catalog getPaimonCatalog(TPaimonCatalog catalog, boolean isExternal,
      String warehouse_location) throws ImpalaRuntimeException {
    switch (catalog) {
      case HADOOP_CATALOG: {
        Preconditions.checkNotNull(
            warehouse_location, "warehouse location should not be null");
        CatalogContext context = CatalogContext.create(new Path(warehouse_location));
        return CatalogFactory.createCatalog(context);
      }
      case HIVE_CATALOG: {
        try {
          String location = isExternal ?
              hiveConf_.get(HiveConf.ConfVars.HIVE_METASTORE_WAREHOUSE_EXTERNAL.varname) :
              hiveConf_.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname);
          Path path = new Path(location);
          Options catalogOptions = new Options();
          catalogOptions.set(CatalogOptions.WAREHOUSE, location);
          CatalogContext catalogContext = CatalogContext.create(catalogOptions);
          FileIO fileIO = FileIO.get(path, catalogContext);
          HiveCatalog externalWarehouseCatalog =
              new HiveCatalog(fileIO, hiveConf_, metastoreClientClass_, location);
          return externalWarehouseCatalog;
        } catch (Exception ex) {
          throw new ImpalaRuntimeException("failed to create hive catalog : ", ex);
        }
      }
      default: throw new ImpalaRuntimeException("Unexpected catalog type: " + catalog);
    }
  }

  /**
   * get Paimon Identifier object.
   */
  public static Identifier getTableIdentifier(String dbName, String tableName) {
    return new Identifier(dbName, tableName);
  }

  /**
   * Generates Paimon table identifier from HMS table object.
   */
  public static Identifier getTableIdentifier(
      org.apache.hadoop.hive.metastore.api.Table msTable) {
    String name = msTable.getParameters().get(PAIMON_TABLE_IDENTIFIER);
    if (name == null || name.isEmpty()) {
      return getTableIdentifier(
          msTable.getDbName().toLowerCase(), msTable.getTableName().toLowerCase());
    } else {
      if (!name.contains(".")) {
        return getTableIdentifier(msTable.getDbName(), name);
      } else {
        String[] names = name.split("\\.");
        return getTableIdentifier(names[0], names[1]);
      }
    }
  }

  /**
   * Convert paimon column stats to HMS ColumnStatisticsData.
   */
  public static Optional<ColumnStatisticsData> convertColStats(
      ColStats<?> colStats, DataField dataField) {
    ColumnStatisticsData columnStatisticsData = new ColumnStatisticsData();
    colStats.deserializeFieldsFromString(dataField.type());

    Set<DataTypeFamily> fieldFamilySet = dataField.type().getTypeRoot().getFamilies();
    if (fieldFamilySet.contains(DataTypeFamily.NUMERIC)) {
      if (fieldFamilySet.contains(DataTypeFamily.INTEGER_NUMERIC)) {
        // LONG_STATS
        LongColumnStatsData longColumnStatsData = new LongColumnStatsData();
        if (colStats.nullCount().isPresent()) {
          longColumnStatsData.setNumNulls(colStats.nullCount().getAsLong());
        }
        if (dataField.type() instanceof BigIntType) {
          if (colStats.min().isPresent()) {
            longColumnStatsData.setLowValue((Long) colStats.min().get());
          }
          if (colStats.max().isPresent()) {
            longColumnStatsData.setHighValue((Long) colStats.max().get());
          }
        } else if (dataField.type() instanceof IntType) {
          if (colStats.min().isPresent()) {
            longColumnStatsData.setLowValue((Integer) colStats.min().get());
          }
          if (colStats.max().isPresent()) {
            longColumnStatsData.setHighValue((Integer) colStats.max().get());
          }
        } else if (dataField.type() instanceof SmallIntType) {
          if (colStats.min().isPresent()) {
            longColumnStatsData.setLowValue((Short) colStats.min().get());
          }
          if (colStats.max().isPresent()) {
            longColumnStatsData.setHighValue((Short) colStats.max().get());
          }
        } else if (dataField.type() instanceof TinyIntType) {
          if (colStats.min().isPresent()) {
            longColumnStatsData.setLowValue((Byte) colStats.min().get());
          }
          if (colStats.max().isPresent()) {
            longColumnStatsData.setHighValue((Byte) colStats.max().get());
          }
        } else {
          LOG.warn(String.format("Column stats doesn't support data type %s",
                  dataField.type().asSQLString()));
          return Optional.empty();
        }

        if (colStats.distinctCount().isPresent()) {
          longColumnStatsData.setNumDVs(colStats.distinctCount().getAsLong());
        }
        columnStatisticsData.setLongStats(longColumnStatsData);
        return Optional.of(columnStatisticsData);
      } else if (fieldFamilySet.contains(DataTypeFamily.APPROXIMATE_NUMERIC)) {
        // DOUBLE_STATS
        DoubleColumnStatsData doubleColumnStatsData = new DoubleColumnStatsData();
        if (colStats.nullCount().isPresent()) {
          doubleColumnStatsData.setNumNulls(colStats.nullCount().getAsLong());
        }
        if (dataField.type() instanceof DoubleType) {
          if (colStats.min().isPresent()) {
            doubleColumnStatsData.setLowValue((Double) colStats.min().get());
          }
          if (colStats.max().isPresent()) {
            doubleColumnStatsData.setHighValue((Double) colStats.max().get());
          }
        } else if (dataField.type() instanceof FloatType) {
          if (colStats.min().isPresent()) {
            doubleColumnStatsData.setLowValue((Float) colStats.min().get());
          }
          if (colStats.max().isPresent()) {
            doubleColumnStatsData.setHighValue((Float) colStats.max().get());
          }
        } else {
            LOG.warn(String.format("Column stats doesn't support data type %s",
                    dataField.type().asSQLString()));
            return Optional.empty();
        }
        if (colStats.distinctCount().isPresent()) {
          doubleColumnStatsData.setNumDVs(colStats.distinctCount().getAsLong());
        }
        columnStatisticsData.setDoubleStats(doubleColumnStatsData);
        return Optional.of(columnStatisticsData);

      } else {
          LOG.warn(String.format("Column stats doesn't support data type %s",
                  dataField.type().asSQLString()));
          return Optional.empty();
      }
    } else if (fieldFamilySet.contains(DataTypeFamily.CHARACTER_STRING)) {
      // STRING_STATS
      StringColumnStatsData stringColumnStatsData = new StringColumnStatsData();
      if (colStats.nullCount().isPresent()) {
        stringColumnStatsData.setNumNulls(colStats.nullCount().getAsLong());
      }
      if (colStats.avgLen().isPresent()) {
        stringColumnStatsData.setAvgColLen(colStats.avgLen().getAsLong());
      }
      if (colStats.maxLen().isPresent()) {
        stringColumnStatsData.setMaxColLen(colStats.maxLen().getAsLong());
      }
      columnStatisticsData.setStringStats(stringColumnStatsData);
      return Optional.of(columnStatisticsData);

    } else if (fieldFamilySet.contains(DataTypeFamily.BINARY_STRING)) {
      // BINARY_STATS
      BinaryColumnStatsData binaryColumnStatsData = new BinaryColumnStatsData();
      if (colStats.nullCount().isPresent()) {
        binaryColumnStatsData.setNumNulls(colStats.nullCount().getAsLong());
      }
      if (colStats.avgLen().isPresent()) {
        binaryColumnStatsData.setAvgColLen(colStats.avgLen().getAsLong());
      }
      if (colStats.maxLen().isPresent()) {
        binaryColumnStatsData.setMaxColLen(colStats.maxLen().getAsLong());
      }
      columnStatisticsData.setBinaryStats(binaryColumnStatsData);
      return Optional.of(columnStatisticsData);

    } else if (dataField.type() instanceof BooleanType) {
      // BOOLEAN_STATS
      BooleanColumnStatsData booleanColumnStatsData = new BooleanColumnStatsData();
      if (colStats.nullCount().isPresent()) {
        booleanColumnStatsData.setNumNulls(colStats.nullCount().getAsLong());
      }
      columnStatisticsData.setBooleanStats(booleanColumnStatsData);
      return Optional.of(columnStatisticsData);

    } else {
        LOG.warn(String.format("Column stats doesn't support data type %s",
                dataField.type().asSQLString()));
        return Optional.empty();
    }
  }

  /**
   * Get FileStore Object for paimon table in HMS.
   */
  public static FileStoreTable createFileStoreTable(
      org.apache.hadoop.hive.metastore.api.Table table) throws MetaException {
    Options options = HiveUtils.extractCatalogConfig(FePaimonTable.jobConf);
    options.set(CoreOptions.PATH,
        LocationKeyExtractor.getPaimonLocation(FePaimonTable.jobConf, table));
    CatalogContext catalogContext;
    if (options.get(HADOOP_LOAD_DEFAULT_CONFIG)) {
      catalogContext = CatalogContext.create(options, FePaimonTable.jobConf);
    } else {
      catalogContext = CatalogContext.create(options);
    }
    return FileStoreTableFactory.create(catalogContext);
  }

  /**
   * Get FileStore Object for specified location
   */
  public static FileStoreTable createFileStoreTable(String tableLocation)
      throws MetaException {
    Options options = HiveUtils.extractCatalogConfig(FePaimonTable.jobConf);
    options.set(CoreOptions.PATH, tableLocation);
    CatalogContext catalogContext;
    if (options.get(HADOOP_LOAD_DEFAULT_CONFIG)) {
      catalogContext = CatalogContext.create(options, FePaimonTable.jobConf);
    } else {
      catalogContext = CatalogContext.create(options);
    }
    return FileStoreTableFactory.create(catalogContext);
  }

  /**
   * Return paimon catalog from table properties
   */
  public static List<String> fieldNames(RowType rowType) {
    return rowType.getFields()
        .stream()
        .map(DataField::name)
        .map(String::toLowerCase)
        .collect(Collectors.toList());
  }

  /**
   * check  if the given table has primary keys
   */
  public static boolean hasPrimaryKey(Table table) {
    return !table.primaryKeys().isEmpty();
  }

  /**
   * check  if the given table is partitioned
   */
  public static boolean hasPartition(Table table) {
    return !table.partitionKeys().isEmpty();
  }

  /**
   * lookup result for given table, max 1000 rows.
   */
  public static List<InternalRow> lookupInTable(Table table, List<Predicate> predicates) {
    return lookupInTable(table, predicates, 1000);
  }

  /**
   * lookup result for given table.
   */
  protected static List<InternalRow> lookupInTable(
      Table table, List<Predicate> predicates, int maxcount) {
    ReadBuilder readBuilder = table.newReadBuilder().withFilter(predicates);
    List<Split> splits = readBuilder.newScan().plan().splits();
    TableRead read = readBuilder.newRead();
    final List<InternalRow> internalRows = Lists.newArrayList();
    try {
      try (RecordReader<InternalRow> recordReader = read.createReader(splits)) {
        RecordReader.RecordIterator<InternalRow> recordIterator =
            recordReader.readBatch();
        InternalRow internalRow = null;
        while ((internalRow = recordIterator.next()) != null) {
          internalRows.add(internalRow);
          if (internalRows.size() >= maxcount) { break; }
        }
      }
    } catch (IOException ex) {
      LOG.warn("failed to read table", ex);
      return Lists.newArrayList();
    }
    return internalRows;
  }

  /**
   * check whether the given table support predicate pushdown.
   */
  public static boolean canApplyPredicatePushDown(Table table) {
    return table instanceof FileStoreTable;
  }

  /**
   * query the snapshot history for paimon table.
   */
  public static TGetTableHistoryResult getPaimonTableHistory(FePaimonTable feTable,
      TDescribeHistoryParams params) throws DatabaseNotFoundException {
    try {
      // Column indexes for paimon snapshot table.
      // Used to select required columns from snapshot table.
      // 5: commit_time
      // 0: snapshot id
      // 1: schema_id
      final int[] SNAPSHOT_TABLE_PROJECTION = {5, 0, 1};
      TGetTableHistoryResult historyResult = new TGetTableHistoryResult();
      FileStoreTable table = (FileStoreTable) feTable.getPaimonApiTable();
      org.apache.paimon.table.Table snapshotTable =
          SystemTableLoader.load("snapshots", table);
      PredicateBuilder predicateBuilder = new PredicateBuilder(snapshotTable.rowType());
      Optional<Predicate> predicataOpt = Optional.empty();
      if (params.isSetFrom_time()) {
        // DESCRIBE HISTORY <table> FROM <ts>
        // check if commit-time >= from_time
        predicataOpt = Optional.of(predicateBuilder.greaterOrEqual(
            0, Timestamp.fromEpochMillis(params.getFrom_time())));
      } else if (params.isSetBetween_start_time() && params.isSetBetween_end_time()) {
        predicataOpt = Optional.of(PredicateBuilder.and(
            predicateBuilder.greaterOrEqual(
                0, Timestamp.fromEpochMillis(params.getBetween_start_time())),
            predicateBuilder.lessOrEqual(
                0, Timestamp.fromEpochMillis(params.getBetween_end_time()))));
      }
      ReadBuilder readBuilder =
          snapshotTable.newReadBuilder().withProjection(SNAPSHOT_TABLE_PROJECTION);
      predicataOpt.ifPresent(readBuilder::withFilter);
      List<Split> splits = readBuilder.newScan().plan().splits();
      RecordReader<InternalRow> internalRowRecordReader =
          readBuilder.newRead().createReader(splits);
      List<TGetTableHistoryResultItem> result =
          com.google.common.collect.Lists.newArrayList();

      internalRowRecordReader.forEachRemaining(new Consumer<InternalRow>() {
        @Override
        public void accept(InternalRow internalRow) {
          TGetTableHistoryResultItem resultItem = new TGetTableHistoryResultItem();
          long snapshotId = internalRow.getLong(1);
          Timestamp timestamp = internalRow.getTimestamp(0, 9);
          resultItem.setCreation_time(timestamp.getMillisecond());
          resultItem.setSnapshot_id(snapshotId);
          // note: parent id and ancestor id is always null
          result.add(resultItem);
        }
      });
      historyResult.setResult(result);
      return historyResult;
    } catch (Exception ex) {
      throw new DatabaseNotFoundException("Failed to get snapshot: " + ex.getMessage());
    }
  }

  /**
   * Get HdfsFileFormat from a string, usually from table properties.
   * Returns PARQUET when 'format' is null. Returns null for invalid formats.
   */
  public static HdfsFileFormat getPaimonFileFormat(String format) {
    if ("PARQUET".equalsIgnoreCase(format) || format == null) {
      return HdfsFileFormat.PARQUET;
    } else if ("ORC".equalsIgnoreCase(format)) {
      return HdfsFileFormat.ORC;
    }
    return null;
  }

  /**
   * A table is synchronized table if its Managed table or if its a external table with
   * <code>external.table.purge</code> property set to true.
   * We need to create/drop/etc. synchronized tables through the Paimon APIs as well.
   */
  public static boolean isSynchronizedTable(
      org.apache.hadoop.hive.metastore.api.Table msTbl) {
    com.google.common.base.Preconditions.checkState(isPaimonTable(msTbl));
    return isManagedTable(msTbl) || isExternalPurgeTable(msTbl);
  }

  /**
   * Returns if this metastore table has managed table type
   */
  public static boolean isManagedTable(org.apache.hadoop.hive.metastore.api.Table msTbl) {
    return msTbl.getTableType().equalsIgnoreCase(TableType.MANAGED_TABLE.toString());
  }

  /**
   * get the location for the newly created paimon table.
   */
  public static String getPaimonCatalogLocation(
      MetaStoreClientPool.MetaStoreClient msClient,
      org.apache.hadoop.hive.metastore.api.Table msTable) throws TException {
    TPaimonCatalog catalog = getTPaimonCatalog(msTable);
    if (catalog == TPaimonCatalog.HADOOP_CATALOG) {
      // Using catalog location to create table
      // We cannot set location for 'hadoop.catalog' table in SQL
      String location = msTable.getParameters().get(PAIMON_HADOOP_CATALOG_LOCATION);
      Identifier table_identifier = getTableIdentifier(msTable);
      return AbstractCatalog.newTableLocation(location, table_identifier).toString();
    } else {
      // Using normal location as 'hadoop.tables' table location and create
      // table
      return MetastoreShim.getPathForNewTable(
          msClient.getHiveClient().getDatabase(msTable.getDbName()), msTable);
    }
  }

  /**
   * Get Paimon table catalog location with 'paimon.catalog_location' when using
   * 'hadoop.catalog'
   */
  public static String getPaimonCatalogLocation(
      org.apache.hadoop.hive.metastore.api.Table msTable) {
    return msTable.getParameters().get(PAIMON_HADOOP_CATALOG_LOCATION);
  }

  /**
   * Get Paimon table catalog type from hms table properties
   * use HiveCatalog as default
   */
  public static TPaimonCatalog getTPaimonCatalog(
      org.apache.hadoop.hive.metastore.api.Table msTable) {
    return getTPaimonCatalog(msTable.getParameters());
  }

  /**
   * Get Paimon table catalog type from  properties
   * use HiveCatalog as default
   */
  public static TPaimonCatalog getTPaimonCatalog(Map<String, String> props) {
    return getTPaimonCatalog(props.get(PAIMON_CATALOG));
  }

  /**
   * Get Paimon table catalog type string
   * use HiveCatalog as default
   */
  public static TPaimonCatalog getTPaimonCatalog(String catalog) {
    if ("hadoop".equalsIgnoreCase(catalog)) {
      return TPaimonCatalog.HADOOP_CATALOG;
    } else if (HIVE_CATALOG.equalsIgnoreCase(catalog) || catalog == null) {
      return TPaimonCatalog.HIVE_CATALOG;
    } else {
      return TPaimonCatalog.HIVE_CATALOG;
    }
  }

  /**
   * Extract column names from string
   */
  public static List<String> extractColumnNames(String value) {
    return Arrays.stream(value.split(",")).
            map(String::toLowerCase).collect(Collectors.toList());
  }

  /**
   * Create catalog context from HMS table and location
   */
  public static CatalogContext catalogContext(
      org.apache.hadoop.hive.metastore.api.Table table, String location) {
    Options options = HiveUtils.extractCatalogConfig(hiveConf_);
    options.set(CoreOptions.PATH, location);
    table.getParameters().forEach(options::set);
    return CatalogContext.create(options, hiveConf_);
  }

  /**
   * Create catalog context from HMS table and location
   */
  public static String partitionSpecToString(final Map<String, String> spec) {
    List<String> speclist = spec.keySet()
                                .stream()
                                .map(k -> String.join("=", k, spec.get(k)))
                                .collect(Collectors.toList());
    return String.join("/", speclist);
  }

  /**
   * Get partition stats for the given fe paimon table.
   */
  public static TResultSet doGetTableStats(FePaimonTable table) {
    TResultSet result = new TResultSet();
    TResultSetMetadata resultSchema = new TResultSetMetadata();
    result.setSchema(resultSchema);
    result.setRows(new ArrayList<>());

    resultSchema.addToColumns(new TColumn("Number Of Rows", Type.BIGINT.toThrift()));
    resultSchema.addToColumns(new TColumn("Number Of Bytes", Type.BIGINT.toThrift()));
    TTableStats stats = table.getTTableStats();
    {
      TResultRowBuilder builder = new TResultRowBuilder();
      builder.add(stats.getNum_rows());
      builder.add(stats.getTotal_file_bytes());
      result.addToRows(builder.get());
    }
    return result;
  }

  public static long localMillisToUTCMillis(long epochMillis) {
    ZoneId zone = ZoneId.systemDefault();
    ZoneOffset offset = zone.getRules().getOffset(Instant.now());
    return epochMillis + offset.getTotalSeconds() * 1000L;
  }

  /**
   * Get partition stats for the given fe paimon table.
   */
  public static TResultSet doGetPartitionStats(FePaimonTable table) {
    TResultSet result = new TResultSet();
    TResultSetMetadata resultSchema = new TResultSetMetadata();
    result.setSchema(resultSchema);
    result.setRows(new ArrayList<>());

    resultSchema.addToColumns(new TColumn("Partition", Type.STRING.toThrift()));
    resultSchema.addToColumns(new TColumn("Number Of Rows", Type.BIGINT.toThrift()));
    resultSchema.addToColumns(new TColumn("Number Of Files", Type.BIGINT.toThrift()));
    resultSchema.addToColumns(new TColumn("Number Of Bytes", Type.BIGINT.toThrift()));
    resultSchema.addToColumns(new TColumn("Last Creation Time", Type.BIGINT.toThrift()));

    List<Partition> partitions =
        CatalogUtils.listPartitionsFromFileSystem(table.getPaimonApiTable());
    for (Partition partition : partitions) {
      TResultRowBuilder builder = new TResultRowBuilder();
      builder.add(partitionSpecToString(partition.spec()));
      builder.add(partition.recordCount());
      builder.add(partition.fileCount());
      builder.add(partition.fileSizeInBytes());
      // epoch millis obtained via paimon api is from LocalDateTime
      // with default timezone. Different time zone will yield different values,
      // will remove default timezone offset for the epoch millis obtained via
      // piamon api.
      builder.add(localMillisToUTCMillis(partition.lastFileCreationTime()));
      result.addToRows(builder.get());
    }
    return result;
  }

  public static int getFieldIndexByNameIgnoreCase(RowType rowType, String fieldName) {
    for (int i = 0; i < rowType.getFields().size(); ++i) {
      if (rowType.getFields().get(i).name().equalsIgnoreCase(fieldName)) { return i; }
    }
    return -1;
  }

  private static Optional<Predicate> extractPartitionFilter(
      Table table, TShowFilesParams request) {
    Iterator<java.util.List<org.apache.impala.thrift.TPartitionKeyValue>> iter =
        request.getPartition_setIterator();
    PredicateBuilder predicateBuilder = new PredicateBuilder(table.rowType());
    List<Predicate> predicates =
        request.getPartition_set()
            .parallelStream()
            .map(l
                -> PredicateBuilder.and(
                    l.stream()
                        .map(kv
                            -> predicateBuilder.equal(getFieldIndexByNameIgnoreCase(
                                                          table.rowType(), kv.getName()),
                                kv.getValue()))
                        .collect(Collectors.toList())))
            .collect(Collectors.toList());
    if (!predicates.isEmpty()) {
      return Optional.of(PredicateBuilder.or(predicates));
    } else {
      return Optional.empty();
    }
  }

  /**
   * Get files for the given fe paimon table.
   */
  public static TResultSet doGetTableFiles(
      FePaimonTable paimon_table, TShowFilesParams request) {
    Table table = paimon_table.getPaimonApiTable();
    TResultSet result = new TResultSet();
    TResultSetMetadata resultSchema = new TResultSetMetadata();
    result.setSchema(resultSchema);
    resultSchema.addToColumns(new TColumn("Path", Type.STRING.toThrift()));
    resultSchema.addToColumns(new TColumn("Size", Type.STRING.toThrift()));
    resultSchema.addToColumns(new TColumn("Partition", Type.STRING.toThrift()));
    resultSchema.addToColumns(new TColumn("EC Policy", Type.STRING.toThrift()));
    resultSchema.addToColumns(new TColumn("Type", Type.STRING.toThrift()));
    ReadBuilder readBuilder = table.newReadBuilder();
    result.setRows(new ArrayList<>());
    if (request.isSetPartition_set()) {
      Optional<Predicate> predicate = extractPartitionFilter(table, request);
      if (predicate.isPresent()) {
        readBuilder = readBuilder.withFilter(predicate.get());
      }
    }

    TableScan.Plan plan = readBuilder.newScan().plan();

    Options options = Options.fromMap(table.options());

    InternalRowPartitionComputer computer =
        new InternalRowPartitionComputer(options.get(PARTITION_DEFAULT_NAME),
            table.rowType(), table.partitionKeys().toArray(new String[0]),
            options.get(PARTITION_GENERATE_LEGCY_NAME));

    for (Split split : plan.splits()) {
      if (!(split instanceof DataSplit)) continue;
      DataSplit dataSplit = (DataSplit) split;
      Optional<List<RawFile>> rawFiles = dataSplit.convertToRawFiles();
      if (rawFiles.isPresent()) {
        for (RawFile rawFile : rawFiles.get()) {
          TResultRowBuilder builder = new TResultRowBuilder();
          builder.add(rawFile.path());
          builder.add(PrintUtils.printBytes(rawFile.fileSize()));
          builder.add(
              partitionSpecToString(computer.generatePartValues(dataSplit.partition())));
          builder.add(FileSystemUtil.getErasureCodingPolicy(
              new org.apache.hadoop.fs.Path(rawFile.path())));
          builder.add("DATA");
          result.addToRows(builder.get());
        }
      }
      if (split.deletionFiles().isPresent()) {
        for (DeletionFile deletionFile : split.deletionFiles().get()) {
          if (deletionFile == null) break;
          TResultRowBuilder builder = new TResultRowBuilder();
          builder.add(deletionFile.path());
          builder.add(PrintUtils.printBytes(deletionFile.length()));
          builder.add(
              partitionSpecToString(computer.generatePartValues(dataSplit.partition())));
          builder.add(FileSystemUtil.getErasureCodingPolicy(
              new org.apache.hadoop.fs.Path(deletionFile.path())));
          builder.add("DELETE");
          result.addToRows(builder.get());
        }
      }
    }
    return result;
  }
}
