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

import java.util.Arrays;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.Collections;
import java.util.List;
import java.util.Iterator;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.conf.HiveConf;

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.TableType;

import org.apache.log4j.Logger;

import com.cloudera.impala.analysis.TableName;
import com.cloudera.impala.catalog.Db.TableLoadingException;
import com.cloudera.impala.catalog.HiveStorageDescriptorFactory;
import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.MetaStoreClientPool;
import com.cloudera.impala.common.MetaStoreClientPool.MetaStoreClient;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;

import com.cloudera.impala.thrift.TColumnDef;
import com.cloudera.impala.thrift.TColumnDesc;
import com.cloudera.impala.thrift.TPartitionKeyValue;

/**
 * Thread safe interface for reading and updating metadata stored in the Hive MetaStore.
 * This class caches db-, table- and column-related metadata. Metadata updates (via DDL
 * operations like CREATE and DROP) are currently serialized for simplicity.
 * Although this class is thread safe, it does not guarantee consistency with the
 * MetaStore. It is important to keep in mind that there may be external (potentially
 * conflicting) concurrent metastore updates occurring at any time. This class does
 * guarantee any MetaStore updates done via this class will be reflected consistently.
 */
public class Catalog {
  public static final String DEFAULT_DB = "default";
  private static final Logger LOG = Logger.getLogger(Catalog.class);
  private static final int META_STORE_CLIENT_POOL_SIZE = 5;
  private final boolean lazy;
  private int nextTableId;
  private final MetaStoreClientPool metaStoreClientPool;
  // Lock used to synchronize metastore CREATE/DROP/ALTER TABLE/DATABASE requests.
  private final Object metastoreDdlLock = new Object();

  // map from db name to DB
  private final LazyDbMap dbs;

  // Tracks whether a Table/Db has all of its metadata loaded.
  enum MetadataLoadState {
    LOADED,
    UNINITIALIZED,
  }

  /**
   * Lazily loads database metadata on read (through 'get') and tracks the valid/known
   * database names. This class is thread safe.
   *
   * If a database has not yet been loaded successfully, get() will attempt to load it.
   * It is only possible to load metadata for databases that are in the known db name
   * map.
   *
   * Getting all the metadata is a heavy-weight operation, but Impala still needs
   * to know what databases exist (one use case is for SHOW commands). To support this,
   * there is a parallel mapping of known database names to their metadata load state.
   * When Impala starts up (and on refresh commands) the database name map is populated
   * with all database names available.
   *
   * Before loading any metadata, the database name map is checked to ensure the given
   * database is "known". If it is not, no metadata is loaded and an exception
   * is thrown.
   */
  private class LazyDbMap {
    // Cache of Db metadata with a key of lower-case database name
    private final LoadingCache<String, Db> dbMetadataCache =
        CacheBuilder.newBuilder()
            // TODO: Increase concurrency level once HIVE-3521 is resolved.
            .concurrencyLevel(1)
            .build(new CacheLoader<String, Db>() {
              public Db load(String dbName) throws DatabaseNotFoundException {
                return loadDb(dbName);
              }
            });

    // Map of lower-case database names to their metadata load state. It is only possible
    // to load metadata for databases that exist in this map.
    private final ConcurrentMap<String, MetadataLoadState> dbNameMap = new MapMaker()
        .makeMap();

    /**
     * Initializes the class with a list of valid database names and marks each
     * database's metadata as uninitialized.
     */
    public LazyDbMap(List<String> dbNames) {
      for (String dbName: dbNames) {
        dbNameMap.put(dbName.toLowerCase(), MetadataLoadState.UNINITIALIZED);
      }
    }

    /**
     * Invalidate the metadata for the given db name and marks the db metadata load
     * state as uninitialized. Invalidating the metadata will cause the next access to
     * the db to reload (synchronize) its metadata from the metastore.
     * If ifExists is true, this will only invalidate if the db name already exists in
     * the dbNameMap. If ifExists is false, the db metadata will be invalidated and the
     * metadata state will be set as UNINITIALIZED (potentially adding a new item to the
     * db name map).
     */
    public void invalidate(String dbName, boolean ifExists) {
      dbName = dbName.toLowerCase();
      if (ifExists) {
        if (dbNameMap.replace(dbName, MetadataLoadState.UNINITIALIZED) != null) {
          // TODO: Should we always invalidate the metadata cache even if the db
          // doesn't exist in the db name map?
          dbMetadataCache.invalidate(dbName);
        }
      } else {
        dbNameMap.put(dbName, MetadataLoadState.UNINITIALIZED);
        dbMetadataCache.invalidate(dbName);
      }
    }

    /**
     * Removes the database from the metadata cache
     */
    public void remove(String dbName) {
      dbName = dbName.toLowerCase();
      dbNameMap.remove(dbName);
      dbMetadataCache.invalidate(dbName);
    }

    /**
     * Returns all known database names.
     */
    public Set<String> getAllDbNames() {
      return dbNameMap.keySet();
    }

    /**
     * Returns the Db object corresponding to the supplied database name. The database
     * name must exist in the database name map for the metadata load to succeed. Returns
     * null if the database does not exist.
     *
     * The exact behavior is:
     * - If the database already exists in the metadata cache, its value will be returned.
     * - If the database is not present in the metadata cache AND the database exists in
     *   the known database map the metadata will be loaded
     * - If the database is not present the database name map, null is returned.
     */
    public Db get(String dbName) {
      try {
        return dbMetadataCache.get(dbName.toLowerCase());
      } catch (ExecutionException e) {
        // Search for the cause of the exception. If a load failed due to the database not
        // being found, callers should get 'null' instead of having to handle the
        // exception.
        Throwable cause = e.getCause();
        while(cause != null) {
          if (cause instanceof DatabaseNotFoundException) {
            return null;
          }
          cause = cause.getCause();
        }
        throw new IllegalStateException(e);
      }
    }

    private Db loadDb(String dbName) throws DatabaseNotFoundException {
      dbName = dbName.toLowerCase();
      MetadataLoadState metadataState = dbNameMap.get(dbName);

      // This database doesn't exist in the database name cache. Throw an exception.
      if (metadataState == null) {
        throw new DatabaseNotFoundException("Database not found: " + dbName);
      }

      // We should never have a case where we make it here and the metadata is marked
      // as already loaded.
      Preconditions.checkState(metadataState != MetadataLoadState.LOADED);
      MetaStoreClient msClient = getMetaStoreClient();
      Db db = null;
      try {
        db = Db.loadDb(Catalog.this, msClient.getHiveClient(), dbName, lazy);
      } finally {
        msClient.release();
      }

      // Mark the metadata as loaded. If the database was removed while loading then
      // throw a DatbaseNotFoundException.
      if (dbNameMap.replace(dbName, MetadataLoadState.LOADED) == null) {
        throw new DatabaseNotFoundException("Database not found: " + dbName);
      }
      return db;
    }
  }

  /**
   * Thrown by some methods when a table column is not found in the metastore
   */
  public static class ColumnNotFoundException extends ImpalaException {
    // Dummy serial ID to satisfy Eclipse
    private static final long serialVersionUID = -2203080667446640542L;

    public ColumnNotFoundException(String s) { super(s); }
  }

  /**
   * Thrown by some methods when a Partition is not found in the metastore
   */
  public static class PartitionNotFoundException extends ImpalaException {
    // Dummy serial ID to satisfy Eclipse
    private static final long serialVersionUID = -2203080667446640542L;

    public PartitionNotFoundException(String s) { super(s); }
  }


  /**
   * Thrown by some methods when a table can't be found in the metastore
   */
  public static class TableNotFoundException extends ImpalaException {
    // Dummy serial UID to avoid warnings
    private static final long serialVersionUID = -2203080667446640542L;

    public TableNotFoundException(String s) { super(s); }

    public TableNotFoundException(String s, Exception cause) { super(s, cause); }
  }

  /**
   * Thrown by some methods when a database is not found in the metastore
   */
  public static class DatabaseNotFoundException extends ImpalaException {
    // Dummy serial ID to satisfy Eclipse
    private static final long serialVersionUID = -2203080667446640542L;

    public DatabaseNotFoundException(String s) { super(s); }
  }


  public Catalog() {
    this(true, true);
  }

  /**
   * If lazy is true, tables are loaded on read, otherwise they are loaded eagerly in
   * the constructor. If raiseExceptions is false, exceptions will be logged and
   * swallowed. Otherwise, exceptions are re-raised.
   */
  public Catalog(boolean lazy, boolean raiseExceptions) {
    this.nextTableId = 0;
    this.lazy = lazy;

    MetaStoreClientPool clientPool = null;
    LazyDbMap dbMap = null;
    try {
      clientPool = new MetaStoreClientPool(META_STORE_CLIENT_POOL_SIZE);
      MetaStoreClient msClient = clientPool.getClient();

      try {
        dbMap = new LazyDbMap(msClient.getHiveClient().getAllDatabases());
      } finally {
        msClient.release();
      }

      if (!lazy) {
        // Load all the metadata
        for (String dbName: dbMap.getAllDbNames()) {
          dbMap.get(dbName);
        }
      }
    } catch (Exception e) {
      if (raiseExceptions) {
        // If exception is already an IllegalStateException, don't wrap it.
        if (e instanceof IllegalStateException) {
          throw (IllegalStateException) e;
        }
        throw new IllegalStateException(e);
      }

      LOG.error(e);
      LOG.error("Error initializing Catalog. Catalog may be empty.");
    }

    metaStoreClientPool = clientPool == null ? new MetaStoreClientPool(0) : clientPool;
    dbs = dbMap == null ? new LazyDbMap(new ArrayList<String>()) : dbMap;
  }

  /**
   * Release the Hive Meta Store Client resources. Can be called multiple times
   * (additional calls will be no-ops).
   */
  public void close() {
    metaStoreClientPool.close();
  }

  /**
   * Appends one or more columns to the given table, optionally replacing all existing
   * columns. After performing the operation the table metadata is marked as invalid and
   * will be reloaded on the next access.
   */
  public void alterTableAddReplaceCols(TableName tableName, List<TColumnDef> columns,
      boolean replaceExistingCols) throws MetaException, InvalidObjectException,
      org.apache.thrift.TException, DatabaseNotFoundException, TableNotFoundException,
       TableLoadingException {
    org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tableName);

    List<FieldSchema> newColumns = buildFieldSchemaList(columns);
    if (replaceExistingCols) {
      msTbl.getSd().setCols(newColumns);
    } else {
      // Append the new column to the existing list of columns.
      for (FieldSchema fs: buildFieldSchemaList(columns)) {
        msTbl.getSd().addToCols(fs);
      }
    }
    applyAlterTable(msTbl);
  }

  /**
   * Changes the column definition of an existing column. This can be used to rename a
   * column, add a comment to a column, or change the datatype of a column.
   */
  public void alterTableChangeCol(TableName tableName, String colName,
      TColumnDef newColDef) throws MetaException, InvalidObjectException,
      org.apache.thrift.TException, DatabaseNotFoundException, TableNotFoundException,
       TableLoadingException, ColumnNotFoundException {
    synchronized (metastoreDdlLock) {
      org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tableName);
      // Find the matching column name and change it.
      Iterator<FieldSchema> iterator = msTbl.getSd().getColsIterator();
      while (iterator.hasNext()) {
        FieldSchema fs = iterator.next();
        if (fs.getName().toLowerCase().equals(colName.toLowerCase())) {
          TColumnDesc colDesc = newColDef.getColumnDesc();
          fs.setName(colDesc.getColumnName());
          fs.setType(colDesc.getColumnType().toString().toLowerCase());
          // Don't overwrite the existing comment unless a new comment is given
          if (newColDef.getComment() != null) {
            fs.setComment(newColDef.getComment());
          }
          break;
        }
        if (!iterator.hasNext()) {
          throw new ColumnNotFoundException(
              String.format("Column name %s not found in table %s.", colName, tableName));
        }
      }
      applyAlterTable(msTbl);
    }
  }

  /**
   * Adds a new partition to the given table. After performing the operation the table
   * metadata is marked as invalid and will be reloaded on the next access.
   */
  public void alterTableAddPartition(TableName tableName,
      List<TPartitionKeyValue> partitionSpec, String location, boolean ifNotExists)
      throws MetaException, AlreadyExistsException, InvalidObjectException,
      org.apache.thrift.TException, DatabaseNotFoundException, TableNotFoundException,
      TableLoadingException {
    org.apache.hadoop.hive.metastore.api.Partition partition =
        new org.apache.hadoop.hive.metastore.api.Partition();
    if (ifNotExists && containsHdfsPartition(tableName.getDb(), tableName.getTbl(),
        partitionSpec)) {
      LOG.info(String.format("Skipping partition creation because (%s) already exists " +
          "and ifNotExists is true.", Joiner.on(", ").join(partitionSpec)));
      return;
    }

    synchronized (metastoreDdlLock) {
      org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tableName);
      partition.setDbName(tableName.getDb());
      partition.setTableName(tableName.getTbl());

      List<String> values = Lists.newArrayList();
      // Need to add in the values in the same order they are defined in the table.
      for (FieldSchema fs: msTbl.getPartitionKeys()) {
        for (TPartitionKeyValue kv: partitionSpec) {
          if (fs.getName().toLowerCase().equals(kv.getName().toLowerCase())) {
            values.add(kv.getValue());
          }
        }
      }
      partition.setValues(values);
      StorageDescriptor sd = msTbl.getSd().deepCopy();
      sd.setLocation(location);
      partition.setSd(sd);
      try {
        getMetaStoreClient().getHiveClient().add_partition(partition);
      } catch (AlreadyExistsException e) {
        if (!ifNotExists) {
          throw e;
        }
        LOG.info(String.format("Ignoring '%s' when adding partition to %s because" +
            " ifNotExists is true.", e, tableName));
      }
      invalidateTable(tableName.getDb(), tableName.getTbl(), true);
    }
  }

  /**
   * Drops an existing partition from the given table. After performing the operation the
   * table metadata is marked as invalid and will be reloaded on the next access.
   */
  public void alterTableDropPartition(TableName tableName,
      List<TPartitionKeyValue> partitionSpec, boolean ifExists) throws MetaException,
      NoSuchObjectException, org.apache.thrift.TException, DatabaseNotFoundException,
      TableNotFoundException, TableLoadingException {

    if (ifExists && !containsHdfsPartition(tableName.getDb(), tableName.getTbl(),
        partitionSpec)) {
      LOG.info(String.format("Skipping partition drop because (%s) does not exist " +
          "and ifExists is true.", Joiner.on(", ").join(partitionSpec)));
      return;
    }

    synchronized (metastoreDdlLock) {
      org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tableName);
      List<String> values = Lists.newArrayList();
      // Need to add in the values in the same order they are defined in the table.
      for (FieldSchema fs: msTbl.getPartitionKeys()) {
        for (TPartitionKeyValue kv: partitionSpec) {
          if (fs.getName().toLowerCase().equals(kv.getName().toLowerCase())) {
            values.add(kv.getValue());
          }
        }
      }
      try {
        getMetaStoreClient().getHiveClient().dropPartition(tableName.getDb(),
            tableName.getTbl(), values);
      } catch (NoSuchObjectException e) {
        if (!ifExists) {
          throw e;
        }
        LOG.info(String.format("Ignoring '%s' when dropping partition from %s because" +
            " ifExists is true.", e, tableName));
      }
      invalidateTable(tableName.getDb(), tableName.getTbl(), true);
    }
  }

  /**
   * Removes a column from the given table. After performing the operation the
   * table metadata is marked as invalid and will be reloaded on the next access.
   */
  public void alterTableDropCol(TableName tableName, String colName)
      throws MetaException, InvalidObjectException, org.apache.thrift.TException,
      DatabaseNotFoundException, TableNotFoundException, ColumnNotFoundException,
      TableLoadingException {
    synchronized (metastoreDdlLock) {
      org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tableName);

      // Find the matching column name and remove it.
      Iterator<FieldSchema> iterator = msTbl.getSd().getColsIterator();
      while (iterator.hasNext()) {
        FieldSchema fs = iterator.next();
        if (fs.getName().toLowerCase().equals(colName.toLowerCase())) {
          iterator.remove();
          break;
        }
        if (!iterator.hasNext()) {
          throw new ColumnNotFoundException(
              String.format("Column name %s not found in table %s.", colName, tableName));
        }
      }
      applyAlterTable(msTbl);
    }
  }

  /**
   * Renames an existing table. After renaming the table, the metadata is marked as
   * invalid and will be reloaded on the next access.
   */
  public void alterTableRename(TableName tableName, TableName newTableName)
      throws MetaException, InvalidObjectException, org.apache.thrift.TException,
      DatabaseNotFoundException, TableNotFoundException, TableLoadingException {
    synchronized (metastoreDdlLock) {
      org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tableName);
      msTbl.setDbName(newTableName.getDb());
      msTbl.setTableName(newTableName.getTbl());
      getMetaStoreClient().getHiveClient().alter_table(
          tableName.getDb(), tableName.getTbl(), msTbl);
      // Remove the old table name from the cache and then invalidate the new table.
      Db db = dbs.get(tableName.getDb());
      if (db != null) db.removeTable(tableName.getTbl());
      invalidateTable(newTableName.getDb(), newTableName.getTbl(), false);
    }
  }

  /**
   * Changes the file format for the given table or partition. This is a metadata only
   * operation, existing table data will not be converted to the new format. After
   * changing the file format the table metadata is marked as invalid and will be reloaded
   * on the next access.
   */
  public void alterTableSetFileFormat(TableName tableName,
      List<TPartitionKeyValue> partitionSpec, FileFormat fileFormat) throws MetaException,
      InvalidObjectException, org.apache.thrift.TException, DatabaseNotFoundException,
      PartitionNotFoundException, TableNotFoundException, TableLoadingException {
    Preconditions.checkState(partitionSpec == null || !partitionSpec.isEmpty());
    if (partitionSpec == null) {
      synchronized (metastoreDdlLock) {
        org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tableName);
        setStorageDescriptorFileFormat(msTbl.getSd(), fileFormat);
        applyAlterTable(msTbl);
      }
    } else {
      synchronized (metastoreDdlLock) {
        HdfsPartition partition = getHdfsPartition(
            tableName.getDb(), tableName.getTbl(), partitionSpec);
        org.apache.hadoop.hive.metastore.api.Partition msPartition =
            partition.getMetaStorePartition();
        Preconditions.checkNotNull(msPartition);
        setStorageDescriptorFileFormat(msPartition.getSd(), fileFormat);
        applyAlterPartition(tableName, msPartition);
      }
    }
  }

  /**
   * Helper method for setting the file format on a given storage descriptor.
   */
  private void setStorageDescriptorFileFormat(StorageDescriptor sd,
      FileFormat fileFormat) {
    StorageDescriptor tempSd =
        HiveStorageDescriptorFactory.createSd(fileFormat, RowFormat.DEFAULT_ROW_FORMAT);
    sd.setInputFormat(tempSd.getInputFormat());
    sd.setOutputFormat(tempSd.getOutputFormat());
    sd.getSerdeInfo().setSerializationLib(
        tempSd.getSerdeInfo().getSerializationLib());
  }

  /**
   * Changes the HDFS storage location for the given table. This is a metadata only
   * operation, existing table data will not be as part of changing the location.
   */
  public void alterTableSetLocation(TableName tableName,
      List<TPartitionKeyValue> partitionSpec, String location) throws MetaException,
      InvalidObjectException, org.apache.thrift.TException, DatabaseNotFoundException,
      PartitionNotFoundException, TableNotFoundException, TableLoadingException {
    Preconditions.checkState(partitionSpec == null || !partitionSpec.isEmpty());
    if (partitionSpec == null) {
      synchronized (metastoreDdlLock) {
        org.apache.hadoop.hive.metastore.api.Table msTbl = getMetaStoreTable(tableName);
        msTbl.getSd().setLocation(location);
        applyAlterTable(msTbl);
      }
    } else {
      synchronized (metastoreDdlLock) {
        HdfsPartition partition = getHdfsPartition(tableName.getDb(), tableName.getTbl(),
            partitionSpec);
        org.apache.hadoop.hive.metastore.api.Partition msPartition =
            partition.getMetaStorePartition();
        Preconditions.checkNotNull(msPartition);
        msPartition.getSd().setLocation(location);
        applyAlterPartition(tableName, msPartition);
      }
    }
  }

  /**
   * Applies an ALTER TABLE command to the metastore table. The caller should take the
   * metastoreDdlLock before calling this method.
   * Note: The metastore interface is not very safe because it only accepts a
   * an entire metastore.api.Table object rather than a delta of what to change. This
   * means an external modification to the table could be overwritten by an ALTER TABLE
   * command if the metadata is not completely in-sync. This affects both Hive and
   * Impala, but is more important in Impala because the metadata is cached for a
   * longer period of time.
   */
  private void applyAlterTable(org.apache.hadoop.hive.metastore.api.Table msTbl)
      throws MetaException, InvalidObjectException, org.apache.thrift.TException {
    MetaStoreClient msClient = getMetaStoreClient();
    try {
      msClient.getHiveClient().alter_table(
          msTbl.getDbName(), msTbl.getTableName(), msTbl);
    } finally {
      msClient.release();
      invalidateTable(msTbl.getDbName(), msTbl.getTableName(), true);
    }
  }

  private void applyAlterPartition(TableName tableName,
      org.apache.hadoop.hive.metastore.api.Partition msPartition) throws MetaException,
      InvalidObjectException, org.apache.thrift.TException {
    MetaStoreClient msClient = getMetaStoreClient();
    try {
      msClient.getHiveClient().alter_partition(
          tableName.getDb(), tableName.getTbl(), msPartition);
    } finally {
      msClient.release();
      invalidateTable(tableName.getDb(), tableName.getTbl(), true);
    }
  }

  /**
   * Creates a new database in the metastore and adds the db name to the internal
   * metadata cache, marking its metadata to be lazily loaded on the next access.
   * Re-throws any Hive Meta Store exceptions encountered during the create, these
   * may vary depending on the Meta Store connection type (thrift vs direct db).
   *
   * @param dbName - The name of the new database.
   * @param comment - Comment to attach to the database, or null for no comment.
   * @param location - Hdfs path to use as the default location for new table data or
   *                   null to use default location.
   * @param ifNotExists - If true, no errors are thrown if the database already exists
   */
  public void createDatabase(String dbName, String comment, String location,
      boolean ifNotExists) throws MetaException, AlreadyExistsException,
      InvalidObjectException, org.apache.thrift.TException {
    Preconditions.checkState(dbName != null && !dbName.isEmpty(),
        "Null or empty database name passed as argument to Catalog.createDatabase");
    if (ifNotExists && getDb(dbName) != null) {
      LOG.info("Skipping database creation because " + dbName + " already exists and " +
          "ifNotExists is true.");
      return;
    }
    org.apache.hadoop.hive.metastore.api.Database db =
        new org.apache.hadoop.hive.metastore.api.Database();
    db.setName(dbName);
    if (comment != null) {
      db.setDescription(comment);
    }
    if (location != null) {
      db.setLocationUri(location);
    }
    LOG.info("Creating database " + dbName);
    synchronized (metastoreDdlLock) {
      try {
        getMetaStoreClient().getHiveClient().createDatabase(db);
      } catch (AlreadyExistsException e) {
        if (!ifNotExists) {
          throw e;
        }
        LOG.info(String.format("Ignoring '%s' when creating database %s because " +
            "ifNotExists is true.", e, dbName));
      }
      dbs.invalidate(dbName, false);
    }
  }

  /**
   * Drops a database from the metastore and removes the database's metadata from the
   * internal cache. The database must be empty (contain no tables) for the drop operation
   * to succeed. Re-throws any Hive Meta Store exceptions encountered during the drop.
   *
   * @param dbName - The name of the database to drop
   * @param ifExists - If true, no errors will be thrown if the database does not exist.
   */
  public void dropDatabase(String dbName, boolean ifExists)
      throws MetaException, NoSuchObjectException, InvalidOperationException,
      org.apache.thrift.TException {
    Preconditions.checkState(dbName != null && !dbName.isEmpty(),
        "Null or empty database name passed as argument to Catalog.dropDatabase");
    LOG.info("Dropping database " + dbName);
    synchronized (metastoreDdlLock) {
      getMetaStoreClient().getHiveClient().dropDatabase(dbName, false, ifExists);
      dbs.remove(dbName);
    }
  }

  /**
   * Creates a new table in the metastore and adds the table name to the internal
   * metadata cache, marking its metadata to be lazily loaded on the next access.
   * Re-throws any Hive Meta Store exceptions encountered during the drop.
   *
   * @param tableName - The fully qualified name of the table to drop.
   * @param ifExists - If true, no errors will be thrown if the table does not exist.
   */
  public void dropTable(TableName tableName, boolean ifExists)
      throws MetaException, NoSuchObjectException, InvalidOperationException,
      org.apache.thrift.TException {
    checkTableNameFullyQualified(tableName);
    LOG.info(String.format("Dropping table %s", tableName));
    synchronized (metastoreDdlLock) {
      getMetaStoreClient().getHiveClient().dropTable(
          tableName.getDb(), tableName.getTbl(), true, ifExists);
      Db db = dbs.get(tableName.getDb());
      if (db != null) db.removeTable(tableName.getTbl());
    }
  }

  /**
   * Creates a new table in the metastore and adds an entry to the metadata cache to
   * lazily load the new metadata on the next access. Re-throws any Hive Meta Store
   * exceptions encountered during the create.
   *
   * @param tableName - Fully qualified name of the new table.
   * @param column - List of column definitions for the new table.
   * @param partitionColumn - List of partition column definitions for the new table.
   * @param owner - Owner of this table.
   * @param isExternal
   *    If true, table is created as external which means the data will not be deleted
   *    if dropped. External tables can also be created on top of existing data.
   * @param comment - Optional comment to attach to the table (null for no comment).
   * @param location - Hdfs path to use as the location for table data or null to use
   *                   default location.
   * @param ifNotExists - If true, no errors are thrown if the table already exists
   */
  public void createTable(TableName tableName, List<TColumnDef> columns,
      List<TColumnDef> partitionColumns, String owner, boolean isExternal, String comment,
      RowFormat rowFormat, FileFormat fileFormat, String location, boolean ifNotExists)
      throws MetaException, NoSuchObjectException, AlreadyExistsException,
      InvalidObjectException, org.apache.thrift.TException {
    checkTableNameFullyQualified(tableName);
    Preconditions.checkState(columns != null && columns.size() > 0,
        "Null or empty column list given as argument to Catalog.createTable");
    if (ifNotExists && containsTable(tableName.getDb(), tableName.getTbl())) {
      LOG.info(String.format("Skipping table creation because %s already exists and " +
          "ifNotExists is true.", tableName));
      return;
    }
    org.apache.hadoop.hive.metastore.api.Table tbl =
        new org.apache.hadoop.hive.metastore.api.Table();
    tbl.setDbName(tableName.getDb());
    tbl.setTableName(tableName.getTbl());
    tbl.setOwner(owner);
    tbl.setParameters(new HashMap<String, String>());

    if (comment != null) {
      tbl.getParameters().put("comment", comment);
    }
    if (isExternal) {
      tbl.setTableType(TableType.EXTERNAL_TABLE.toString());
      tbl.putToParameters("EXTERNAL", "TRUE");
    } else {
      tbl.setTableType(TableType.MANAGED_TABLE.toString());
    }

    StorageDescriptor sd = HiveStorageDescriptorFactory.createSd(fileFormat, rowFormat);
    if (location != null) {
      sd.setLocation(location);
    }
    List<FieldSchema> fsList = Lists.newArrayList();
    // Add in all the columns
    sd.setCols(buildFieldSchemaList(columns));
    tbl.setSd(sd);
    if (partitionColumns != null) {
      // Add in any partition keys that were specified
      tbl.setPartitionKeys(buildFieldSchemaList(partitionColumns));
    }

    LOG.info(String.format("Creating table %s", tableName));
    createTable(tbl, ifNotExists);
  }

  /**
   * Creates a new table in the metastore based on the definition of an existing table.
   * No data is copied as part of this process, it is a metadata only operation. If the
   * creation succeeds, an entry is added to the metadata cache to lazily load the new
   * table's metadata on the next access.
   *
   * @param tableName - Fully qualified name of the new table.
   * @param srcTableName - Fully qualified name of the old table.
   * @param owner - Owner of this table.
   * @param isExternal
   *    If true, table is created as external which means the data will not be deleted
   *    if dropped. External tables can also be created on top of existing data.
   * @param comment - Optional comment to attach to the table or an empty string for no
                      comment. Null to copy comment from the source table.
   * @param fileFormat - The file format for the new table or null to copy file format
   *                     from source table.
   * @param location - Hdfs path to use as the location for table data or null to use
   *                   default location.
   * @param ifNotExists - If true, no errors are thrown if the table already exists
   */
  public void createTableLike(TableName tableName, TableName srcTableName, String owner,
      boolean isExternal, String comment, FileFormat fileFormat, String location,
      boolean ifNotExists) throws MetaException, NoSuchObjectException,
      AlreadyExistsException, InvalidObjectException, org.apache.thrift.TException,
      ImpalaException, TableLoadingException {
    checkTableNameFullyQualified(tableName);
    checkTableNameFullyQualified(srcTableName);
    if (ifNotExists && containsTable(tableName.getDb(), tableName.getTbl())) {
      LOG.info(String.format("Skipping table creation because %s already exists and " +
          "ifNotExists is true.", tableName));
      return;
    }
    Table srcTable = getTable(srcTableName.getDb(), srcTableName.getTbl());
    org.apache.hadoop.hive.metastore.api.Table tbl =
        srcTable.getMetaStoreTable().deepCopy();
    tbl.setDbName(tableName.getDb());
    tbl.setTableName(tableName.getTbl());
    tbl.setOwner(owner);
    if (tbl.getParameters() == null) {
      tbl.setParameters(new HashMap<String, String>());
    }
    if (comment != null) {
      tbl.getParameters().put("comment", comment);
    }
    // The EXTERNAL table property should not be copied from the old table.
    if (isExternal) {
      tbl.setTableType(TableType.EXTERNAL_TABLE.toString());
      tbl.putToParameters("EXTERNAL", "TRUE");
    } else {
      tbl.setTableType(TableType.MANAGED_TABLE.toString());
      if (tbl.getParameters().containsKey("EXTERNAL")) {
        tbl.getParameters().remove("EXTERNAL");
      }
    }
    // The LOCATION property should not be copied from the old table. If the location
    // is null (the caller didn't specify a custom location) this will clear the value
    // and the table will use the default table location from the parent database.
    tbl.getSd().setLocation(location);
    if (fileFormat != null) {
      setStorageDescriptorFileFormat(tbl.getSd(), fileFormat);
    }
    LOG.info(String.format("Creating table %s LIKE %s", tableName, srcTableName));
    createTable(tbl, ifNotExists);
  }

  private void createTable(org.apache.hadoop.hive.metastore.api.Table newTable,
      boolean ifNotExists) throws MetaException, NoSuchObjectException,
      AlreadyExistsException, InvalidObjectException, org.apache.thrift.TException {
    MetaStoreClient msClient = getMetaStoreClient();
    synchronized (metastoreDdlLock) {
      try {
        msClient.getHiveClient().createTable(newTable);
      } catch (AlreadyExistsException e) {
        if (!ifNotExists) {
          throw e;
        }
        LOG.info(String.format("Ignoring '%s' when creating table %s.%s because " +
            "ifNotExists is true.", e, newTable.getDbName(), newTable.getTableName()));
      } finally {
        msClient.release();
      }
      invalidateTable(newTable.getDbName(), newTable.getTableName(), false);
    }
  }

  private static void checkTableNameFullyQualified(TableName tableName) {
    Preconditions.checkNotNull(tableName);
    Preconditions.checkState(tableName.isFullyQualified(),
        "Table name must be fully qualified: " + tableName);
  }

  private static List<FieldSchema> buildFieldSchemaList(List<TColumnDef> columnDefs) {
    List<FieldSchema> fsList = Lists.newArrayList();
    // Add in all the columns
    for (TColumnDef c: columnDefs) {
      TColumnDesc colDesc = c.getColumnDesc();
      FieldSchema fs = new FieldSchema(colDesc.getColumnName(),
          colDesc.getColumnType().toString().toLowerCase(), c.getComment());
      fsList.add(fs);
    }
    return fsList;
  }

  public TableId getNextTableId() {
    return new TableId(nextTableId++);
  }

  /**
   * Returns a managed meta store client from the client connection pool.
   */
  public MetaStoreClient getMetaStoreClient() {
    return metaStoreClientPool.getClient();
  }

  /**
   * Case-insensitive lookup. Returns null if the database does not exist.
   */
  public Db getDb(String db) {
    Preconditions.checkState(db != null && !db.isEmpty(),
        "Null or empty database name given as argument to Catalog.getDb");
    return dbs.get(db);
  }

  /**
   * Returns a list of tables in the supplied database that match
   * tablePattern. See filterStringsByPattern for details of the pattern match
   * semantics.
   *
   * dbName must not be null. tablePattern may be null (and thus matches
   * everything).
   *
   * Table names are returned unqualified.
   */
  public List<String> getTableNames(String dbName, String tablePattern)
      throws DatabaseNotFoundException {
    Preconditions.checkNotNull(dbName);
    List<String> matchingTables = Lists.newArrayList();

    Db db = getDb(dbName);
    if (db == null) {
      throw new DatabaseNotFoundException("Database '" + dbName + "' not found");
    }
    return filterStringsByPattern(db.getAllTableNames(), tablePattern);
  }

  /**
   * Returns a list of databases that match dbPattern. See
   * filterStringsByPattern for details of the pattern match semantics.
   *
   * dbPattern may be null (and thus matches
   * everything).
   */
  public List<String> getDbNames(String dbPattern) {
    return filterStringsByPattern(dbs.getAllDbNames(), dbPattern);
  }

  /**
   * Returns a list of all known databases in the Catalog.
   */
  public List<String> getAllDbNames() {
    return getDbNames(null);
  }

  /**
   * Implement Hive's pattern-matching semantics for SHOW statements. The only
   * metacharacters are '*' which matches any string of characters, and '|'
   * which denotes choice.  Doing the work here saves loading tables or
   * databases from the metastore (which Hive would do if we passed the call
   * through to the metastore client).
   *
   * If matchPattern is null, all strings are considered to match. If it is the
   * empty string, no strings match.
   */
  private List<String> filterStringsByPattern(Iterable<String> candidates,
      String matchPattern) {
    List<String> filtered = Lists.newArrayList();
    if (matchPattern == null) {
      filtered = Lists.newArrayList(candidates);
    } else {
      List<String> patterns = Lists.newArrayList();
      // Hive ignores pretty much all metacharacters, so we have to escape them.
      final String metaCharacters = "+?.^()]\\/{}";
      final Pattern regex = Pattern.compile("([" + Pattern.quote(metaCharacters) + "])");

      for (String pattern: Arrays.asList(matchPattern.split("\\|"))) {
        Matcher matcher = regex.matcher(pattern);
        pattern = matcher.replaceAll("\\\\$1").replace("*", ".*");
        patterns.add(pattern);
      }

      for (String candidate: candidates) {
        for (String pattern: patterns) {
          // Empty string matches nothing in Hive's implementation
          if (!pattern.isEmpty() && candidate.matches(pattern)) {
            filtered.add(candidate);
          }
        }
      }
    }
    Collections.sort(filtered, String.CASE_INSENSITIVE_ORDER);
    return filtered;
  }

  /**
   * Returns true if the table and the database exist in the Impala Catalog. Returns
   * false if the database does not exist or the table does not exist. This will
   * not trigger a metadata load for the given table name.
   */
  public boolean containsTable(String dbName, String tableName) {
    Db db = getDb(dbName);
    return db != null && db.containsTable(tableName);
  }

  /**
   * Returns true if the table contains the given partition spec, otherwise false.
   * This may will trigger a metadata load if the table metadata is not yet cached.
   * @throws DatabaseNotFoundException - If the database does not exist.
   * @throws TableNotFoundException - If the table does not exist.
   * @throws TableLoadingException - If there is an error loading the table metadata.
   */
  public boolean containsHdfsPartition(String dbName, String tableName,
      List<TPartitionKeyValue> partitionSpec) throws DatabaseNotFoundException,
      TableNotFoundException, TableLoadingException {
    try {
      return getHdfsPartition(dbName, tableName, partitionSpec) != null;
    } catch (PartitionNotFoundException e) {
      return false;
    }
  }

  /**
   * Returns the Table object for the given dbName/tableName. This will trigger a
   * metadata load if the table metadata is not yet cached.
   * @throws DatabaseNotFoundException - If the database does not exist.
   * @throws TableNotFoundException - If the table does not exist.
   * @throws TableLoadingException - If there is an error loading the table metadata.
   */
  public Table getTable(String dbName, String tableName) throws
      DatabaseNotFoundException, TableNotFoundException, TableLoadingException {
    Db db = getDb(dbName);
    if (db == null) {
      throw new DatabaseNotFoundException("Database not found: " + dbName);
    }
    Table table = db.getTable(tableName);
    if (table == null) {
      throw new TableNotFoundException(
          String.format("Table not found: %s.%s", dbName, tableName));
    }
    return table;
  }

  /**
   * Returns the HdfsPartition oject for the given dbName/tableName and partition spec.
   * This will trigger a metadata load if the table metadata is not yet cached.
   * @throws DatabaseNotFoundException - If the database does not exist.
   * @throws TableNotFoundException - If the table does not exist.
   * @throws PartitionNotFoundException - If the partition does not exist.
   * @throws TableLoadingException - If there is an error loading the table metadata.
   */
  public HdfsPartition getHdfsPartition(String dbName, String tableName,
      List<TPartitionKeyValue> partitionSpec) throws DatabaseNotFoundException,
      PartitionNotFoundException, TableNotFoundException, TableLoadingException {
    String partitionNotFoundMsg =
        "Partition not found: " + Joiner.on(", ").join(partitionSpec);
    Table table = getTable(dbName, tableName);
    // This is not an Hdfs table, throw an error.
    if (!(table instanceof HdfsTable)) {
      throw new PartitionNotFoundException(partitionNotFoundMsg);
    }
    // Get the HdfsPartition object for the given partition spec.
    HdfsPartition partition =
        ((HdfsTable) table).getPartitionFromThriftPartitionSpec(partitionSpec);
    if (partition == null) throw new PartitionNotFoundException(partitionNotFoundMsg);
    return partition;
  }

  /**
   * Returns a deep copy of the metastore.api.Table object for the given TableName.
   */
  private org.apache.hadoop.hive.metastore.api.Table getMetaStoreTable(
      TableName tableName) throws DatabaseNotFoundException, TableNotFoundException,
      TableLoadingException {
    checkTableNameFullyQualified(tableName);
    return getTable(tableName.getDb(), tableName.getTbl()).getMetaStoreTable().deepCopy();
  }

  /*
   * Marks the table as invalid so the next access will trigger a metadata load. If
   * the database does not exist no error is returned (there is nothing to invalidate).
   */
  public void invalidateTable(String dbName, String tableName, boolean ifExists) {
    Db db = getDb(dbName);
    if (db != null) {
      db.invalidateTable(tableName, ifExists);
    }
  }
}
