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

package org.apache.impala.catalog.local;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.impala.analysis.Expr;
import org.apache.impala.analysis.LiteralExpr;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.CatalogObject.ThriftObjectType;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeFsPartition;
import org.apache.impala.catalog.FeFsTable;
import org.apache.impala.catalog.HdfsFileFormat;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.PrunablePartition;
import org.apache.impala.catalog.local.MetaProvider.PartitionMetadata;
import org.apache.impala.catalog.local.MetaProvider.PartitionRef;
import org.apache.impala.catalog.local.MetaProvider.TableMetaRef;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.thrift.CatalogObjectsConstants;
import org.apache.impala.thrift.THdfsPartition;
import org.apache.impala.thrift.THdfsTable;
import org.apache.impala.thrift.TNetworkAddress;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableType;
import org.apache.impala.util.AvroSchemaConverter;
import org.apache.impala.util.AvroSchemaUtils;
import org.apache.impala.util.ListMap;
import org.apache.thrift.TException;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class LocalFsTable extends LocalTable implements FeFsTable {
  /**
   * Map from partition ID to partition spec.
   *
   * Set by loadPartitionSpecs().
   */
  ImmutableMap<Long, LocalPartitionSpec> partitionSpecs_;

  /**
   * For each partition column, a map from value to the set of partition IDs
   * having that value.
   *
   * Set by loadPartitionValueMap().
   */
  private List<TreeMap<LiteralExpr, Set<Long>>> partitionValueMap_;

  /**
   * For each partition column, the set of partition IDs having a NULL value
   * for that column.
   *
   * Set by loadPartitionValueMap().
   */
  private List<Set<Long>> nullPartitionIds_;

  /**
   * The value that will be stored in a partition name to indicate NULL.
   */
  private final String nullColumnValue_;

  /**
   * Map assigning integer indexes for the hosts containing blocks for this table.
   * This is updated as a side effect of LocalFsPartition.loadFileDescriptors().
   */
  private final ListMap<TNetworkAddress> hostIndex_ = new ListMap<>();

  /**
   * The Avro schema for this table. Non-null if this table is an Avro table.
   * If this table is not an Avro table, this is usually null, but may be
   * non-null in the case that an explicit external avro schema is specified
   * as a table property. Such a schema is used when querying Avro partitions
   * of non-Avro tables.
   */
  private final String avroSchema_;

  public static LocalFsTable load(LocalDb db, Table msTbl, TableMetaRef ref) {
    String fullName = msTbl.getDbName() + "." + msTbl.getTableName();

    // Set Avro schema if necessary.
    String avroSchema;
    ColumnMap cmap;
    try {
      // Load the avro schema if it's external (explicitly specified).
      avroSchema = loadAvroSchema(msTbl);

      // If the table's format is Avro, then we should override the columns
      // based on the schema (either inferred or explicit). Otherwise, even if
      // there is an Avro schema set, we don't override the table-level columns:
      // the Avro schema in that case is just used in case there is an Avro-formatted
      // partition.
      if (isAvroFormat(msTbl)) {
        if (avroSchema == null) {
          // No Avro schema was explicitly set in the table metadata, so infer the Avro
          // schema from the column definitions.
          Schema inferredSchema = AvroSchemaConverter.convertFieldSchemas(
              msTbl.getSd().getCols(), fullName);
          avroSchema = inferredSchema.toString();
        }

        List<FieldSchema> reconciledFieldSchemas = AvroSchemaUtils.reconcileAvroSchema(
            msTbl, avroSchema);
        Table msTblWithExplicitAvroSchema = msTbl.deepCopy();
        msTblWithExplicitAvroSchema.getSd().setCols(reconciledFieldSchemas);
        cmap = ColumnMap.fromMsTable(msTblWithExplicitAvroSchema);
      } else {
        cmap = ColumnMap.fromMsTable(msTbl);
      }

      return new LocalFsTable(db, msTbl, ref, cmap, avroSchema);
    } catch (AnalysisException e) {
      throw new LocalCatalogException("Failed to load Avro schema for table "
          + fullName);
    }
  }

  private LocalFsTable(LocalDb db, Table msTbl, TableMetaRef ref, ColumnMap cmap,
      String explicitAvroSchema) {
    super(db, msTbl, ref, cmap);

    // set NULL indicator string from table properties
    String tableNullFormat =
        msTbl.getParameters().get(serdeConstants.SERIALIZATION_NULL_FORMAT);
    nullColumnValue_ = tableNullFormat != null ? tableNullFormat :
        FeFsTable.DEFAULT_NULL_COLUMN_VALUE;

    avroSchema_ = explicitAvroSchema;
  }

  private static String loadAvroSchema(Table msTbl) throws AnalysisException {
    List<Map<String, String>> schemaSearchLocations = ImmutableList.of(
        msTbl.getSd().getSerdeInfo().getParameters(),
        msTbl.getParameters());

    // TODO(todd): we should consider moving this to the MetaProvider interface
    // so that it can more easily be cached rather than re-loaded from HDFS on
    // each table reference.
    return AvroSchemaUtils.getAvroSchema(schemaSearchLocations);
  }

  /**
   * Creates a temporary FsTable object populated with the specified properties.
   * This is used for CTAS statements.
   */
  public static LocalFsTable createCtasTarget(LocalDb db,
      Table msTbl) throws CatalogException {
    return new LocalFsTable(db, msTbl, /*ref=*/null, ColumnMap.fromMsTable(msTbl),
        /*explicitAvroSchema=*/null);
  }


  @Override
  public boolean isCacheable() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isLocationCacheable() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isMarkedCached() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public String getLocation() {
    return getMetaStoreTable().getSd().getLocation();
  }

  @Override
  public String getNullPartitionKeyValue() {
    return db_.getCatalog().getNullPartitionKeyValue();
  }

  @Override
  public String getHdfsBaseDir() {
    // TODO(todd): this is redundant with getLocation, it seems.
    return getLocation();
  }

  @Override
  public long getTotalHdfsBytes() {
    // TODO(todd): this is slow because it requires loading all partitions. Remove if possible.
    long size = 0;
    for (FeFsPartition p: loadPartitions(getPartitionIds())) {
      size += p.getSize();
    }
    return size;
  }

  @Override
  public boolean usesAvroSchemaOverride() {
    return isAvroFormat(msTable_);
  }

  @Override
  public Set<HdfsFileFormat> getFileFormats() {
    // TODO(todd): can we avoid loading all partitions here? this is called
    // for any INSERT query, even if the partition is specified.
    Collection<? extends FeFsPartition> parts;
    if (ref_ != null) {
      parts = FeCatalogUtils.loadAllPartitions(this);
    } else {
      // If this is a CTAS target, we don't want to try to load the partition list.
      parts = Collections.emptyList();
    }
    // In the case that we have no partitions added to the table yet, it's
    // important to add the "prototype" partition as a fallback.
    Iterable<FeFsPartition> partitionsToConsider = Iterables.concat(
        parts, Collections.singleton(createPrototypePartition()));
    return FeCatalogUtils.getFileFormats(partitionsToConsider);
  }

  @Override
  public boolean hasWriteAccessToBaseDir() {
    // TODO(todd): implement me properly
    return true;
  }

  @Override
  public String getFirstLocationWithoutWriteAccess() {
    // TODO(todd): implement me properly
    return null;
  }

  @Override
  public TResultSet getTableStats() {
    return HdfsTable.getTableStats(this);
  }

  @Override
  public FileSystemUtil.FsType getFsType() {
    Preconditions.checkNotNull(getHdfsBaseDir(),
            "LocalTable base dir is null");
    Path hdfsBaseDirPath = new Path(getHdfsBaseDir());
    Preconditions.checkNotNull(hdfsBaseDirPath.toUri().getScheme(),
        "Cannot get scheme from path " + getHdfsBaseDir());
    return FileSystemUtil.FsType.getFsType(hdfsBaseDirPath.toUri().getScheme());
  }

  @Override
  public TTableDescriptor toThriftDescriptor(int tableId,
      Set<Long> referencedPartitions) {
    if (referencedPartitions == null) {
      // null means "all partitions".
      referencedPartitions = getPartitionIds();
    }
    Map<Long, THdfsPartition> idToPartition = new HashMap<>();
    List<? extends FeFsPartition> partitions = loadPartitions(referencedPartitions);
    for (FeFsPartition partition : partitions) {
      idToPartition.put(partition.getId(),
          FeCatalogUtils.fsPartitionToThrift(partition,
              ThriftObjectType.DESCRIPTOR_ONLY));
    }

    THdfsPartition tPrototypePartition = FeCatalogUtils.fsPartitionToThrift(
        createPrototypePartition(), ThriftObjectType.DESCRIPTOR_ONLY);

    THdfsTable hdfsTable = new THdfsTable(getHdfsBaseDir(), getColumnNames(),
        getNullPartitionKeyValue(), nullColumnValue_, idToPartition,
        tPrototypePartition);

    if (avroSchema_ != null) {
      hdfsTable.setAvroSchema(avroSchema_);
    } else if (hasAnyAvroPartition(partitions)) {
      // Need to infer an Avro schema for the backend to use if any of the
      // referenced partitions are Avro, even if the table is mixed-format.
      hdfsTable.setAvroSchema(AvroSchemaConverter.convertFieldSchemas(
          getMetaStoreTable().getSd().getCols(), getFullName()).toString());
    }

    TTableDescriptor tableDesc = new TTableDescriptor(tableId, TTableType.HDFS_TABLE,
        FeCatalogUtils.getTColumnDescriptors(this),
        getNumClusteringCols(), name_, db_.getName());
    tableDesc.setHdfsTable(hdfsTable);
    return tableDesc;
  }

  private static boolean isAvroFormat(Table msTbl) {
    String inputFormat = msTbl.getSd().getInputFormat();
    return HdfsFileFormat.fromJavaClassName(inputFormat) == HdfsFileFormat.AVRO;
  }

  private static boolean hasAnyAvroPartition(List<? extends FeFsPartition> partitions) {
    for (FeFsPartition p : partitions) {
      if (p.getFileFormat() == HdfsFileFormat.AVRO) return true;
    }
    return false;
  }

  private LocalFsPartition createPrototypePartition() {
    Partition protoMsPartition = new Partition();

    // The prototype partition should not have a location set in its storage
    // descriptor, or else all inserted files will end up written into the
    // table directory instead of the new partition directories.
    StorageDescriptor sd = getMetaStoreTable().getSd().deepCopy();
    sd.unsetLocation();
    protoMsPartition.setSd(sd);

    protoMsPartition.setParameters(Collections.<String, String>emptyMap());
    LocalPartitionSpec spec = new LocalPartitionSpec(
        this, CatalogObjectsConstants.PROTOTYPE_PARTITION_ID);
    LocalFsPartition prototypePartition = new LocalFsPartition(
        this, spec, protoMsPartition, /*fileDescriptors=*/null, /*partitionStats=*/null,
        /*hasIncrementalStats=*/false);
    return prototypePartition;
  }

  @Override
  public Collection<? extends PrunablePartition> getPartitions() {
    loadPartitionSpecs();
    return partitionSpecs_.values();
  }

  @Override
  public Set<Long> getPartitionIds() {
    loadPartitionSpecs();
    return partitionSpecs_.keySet();
  }

  @Override
  public Map<Long, ? extends PrunablePartition> getPartitionMap() {
    loadPartitionSpecs();
    return partitionSpecs_;
  }

  @Override
  public TreeMap<LiteralExpr, Set<Long>> getPartitionValueMap(int col) {
    loadPartitionValueMap();
    return partitionValueMap_.get(col);
  }

  @Override
  public Set<Long> getNullPartitionIds(int colIdx) {
    loadPartitionValueMap();
    return nullPartitionIds_.get(colIdx);
  }

  @Override
  public List<? extends FeFsPartition> loadPartitions(Collection<Long> ids) {
    // TODO(todd) it seems like some queries actually call this multiple times.
    // Perhaps we should store the result in this class, instead of relying on
    // catalog-layer caching?
    Preconditions.checkState(partitionSpecs_ != null,
        "Cannot load partitions without having fetched partition IDs " +
        "from the same LocalFsTable instance");

    // Possible in the case that all partitions were pruned.
    if (ids.isEmpty()) return Collections.emptyList();

    List<PartitionRef> refs = new ArrayList<>();
    for (Long id : ids) {
      LocalPartitionSpec spec = partitionSpecs_.get(id);
      Preconditions.checkArgument(spec != null, "Invalid partition ID for table %s: %s",
          getFullName(), id);
      refs.add(Preconditions.checkNotNull(spec.getRef()));
    }
    Map<String, PartitionMetadata> partsByName;
    try {
      partsByName = db_.getCatalog().getMetaProvider().loadPartitionsByRefs(
          ref_, getClusteringColumnNames(), hostIndex_, refs);
    } catch (TException e) {
      throw new LocalCatalogException(
          "Could not load partitions for table " + getFullName(), e);
    }
    List<FeFsPartition> ret = Lists.newArrayListWithCapacity(ids.size());
    for (Long id : ids) {
      LocalPartitionSpec spec = partitionSpecs_.get(id);
      PartitionMetadata p = partsByName.get(spec.getRef().getName());
      if (p == null) {
        // TODO(todd): concurrent drop partition could result in this error.
        // Should we recover in a more graceful way from such an unexpected event?
        throw new LocalCatalogException(
            "Could not load expected partitions for table " + getFullName() +
            ": missing expected partition with name '" + spec.getRef().getName() +
            "' (perhaps it was concurrently dropped by another process)");
      }

      LocalFsPartition part = new LocalFsPartition(this, spec, p.getHmsPartition(),
          p.getFileDescriptors(), p.getPartitionStats(), p.hasIncrementalStats());
      ret.add(part);
    }
    return ret;
  }

  private List<String> getClusteringColumnNames() {
    List<String> names = Lists.newArrayListWithCapacity(getNumClusteringCols());
    for (Column c : getClusteringColumns()) {
      names.add(c.getName());
    }
    return names;
  }

  private void loadPartitionValueMap() {
    if (partitionValueMap_ != null) return;

    loadPartitionSpecs();
    List<TreeMap<LiteralExpr, Set<Long>>> valMapByCol =
        new ArrayList<>();
    List<Set<Long>> nullParts = new ArrayList<>();

    for (int i = 0; i < getNumClusteringCols(); i++) {
      valMapByCol.add(new TreeMap<>());
      nullParts.add(new HashSet<>());
    }
    for (LocalPartitionSpec partition : partitionSpecs_.values()) {
      List<LiteralExpr> vals = partition.getPartitionValues();
      for (int i = 0; i < getNumClusteringCols(); i++) {
        LiteralExpr val = vals.get(i);
        if (Expr.IS_NULL_LITERAL.apply(val)) {
          nullParts.get(i).add(partition.getId());
          continue;
        }

        Set<Long> ids = valMapByCol.get(i).get(val);
        if (ids == null) {
          ids = new HashSet<>();
          valMapByCol.get(i).put(val,  ids);
        }
        ids.add(partition.getId());
      }
    }
    partitionValueMap_ = valMapByCol;
    nullPartitionIds_ = nullParts;
  }

  private void loadPartitionSpecs() {
    if (partitionSpecs_ != null) return;
    if (ref_ == null) {
      // This is a CTAS target. Don't try to load metadata.
      partitionSpecs_ = ImmutableMap.of();
      return;
    }

    List<PartitionRef> partList;
    try {
      partList = db_.getCatalog().getMetaProvider().loadPartitionList(ref_);
    } catch (TException e) {
      throw new LocalCatalogException("Could not load partition names for table " +
          getFullName(), e);
    }
    ImmutableMap.Builder<Long, LocalPartitionSpec> b = new ImmutableMap.Builder<>();
    long id = 0;
    for (PartitionRef part: partList) {
      b.put(id, new LocalPartitionSpec(this, part, id));
      id++;
    }
    partitionSpecs_ = b.build();
  }

  /**
   * Override base implementation to populate column stats for
   * clustering columns based on the partition map.
   */
  @Override
  protected void loadColumnStats() {
    super.loadColumnStats();
    // TODO(todd): this is called for all tables even if not necessary,
    // which means we need to load all partition names, even if not
    // necessary.
    loadPartitionValueMap();
    for (int i = 0; i < getNumClusteringCols(); i++) {
      ColumnStats stats = getColumns().get(i).getStats();
      int nonNullParts = partitionValueMap_.get(i).size();
      int nullParts = nullPartitionIds_.get(i).size();
      stats.setNumDistinctValues(nonNullParts + (nullParts > 0 ? 1 : 0));

      // TODO(todd): this calculation ends up setting the num_nulls stat
      // to the number of partitions with null rows, not the number of rows.
      // However, it maintains the existing behavior from HdfsTable.
      stats.setNumNulls(nullParts);
    }
  }

  @Override
  public ListMap<TNetworkAddress> getHostIndex() {
    return hostIndex_;
  }
}
