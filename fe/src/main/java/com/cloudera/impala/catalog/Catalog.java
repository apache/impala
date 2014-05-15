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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import com.cloudera.impala.analysis.ArithmeticExpr;
import com.cloudera.impala.analysis.BinaryPredicate;
import com.cloudera.impala.analysis.CaseExpr;
import com.cloudera.impala.analysis.CastExpr;
import com.cloudera.impala.analysis.CompoundPredicate;
import com.cloudera.impala.analysis.FunctionName;
import com.cloudera.impala.analysis.LikePredicate;
import com.cloudera.impala.builtins.ScalarBuiltins;
import com.cloudera.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import com.cloudera.impala.thrift.TCatalogObject;
import com.cloudera.impala.thrift.TFunction;
import com.cloudera.impala.thrift.TPartitionKeyValue;
import com.cloudera.impala.thrift.TTableName;
import com.cloudera.impala.util.PatternMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

/**
 * Thread safe interface for reading and updating metadata stored in the Hive MetaStore.
 * This class provides a storage API for caching CatalogObjects: databases, tables,
 * and functions and the relevant metadata to go along with them. Although this class is
 * thread safe, it does not guarantee consistency with the MetaStore. It is important
 * to keep in mind that there may be external (potentially conflicting) concurrent
 * metastore updates occurring at any time.
 * The CatalogObject storage hierarchy is:
 * Catalog -> Db -> Table
 *               -> Function
 * Each level has its own synchronization, so the cache of Dbs is synchronized and each
 * Db has a cache of tables which is synchronized independently.
 *
 * The catalog is populated with the impala builtins on startup. Builtins and user
 * functions are treated identically by the catalog. The builtins go in a specific
 * database that the user cannot modify.
 * Builtins are populated on startup in initBuiltins().
 */
public abstract class Catalog {
  // Initial catalog version.
  public final static long INITIAL_CATALOG_VERSION = 0L;
  public static final String DEFAULT_DB = "default";
  private static final int META_STORE_CLIENT_POOL_SIZE = 5;

  public static final String BUILTINS_DB = "_impala_builtins";

  protected final MetaStoreClientPool metaStoreClientPool_ = new MetaStoreClientPool(0);

  // Thread safe cache of database metadata. Uses an AtomicReference so reset()
  // operations can atomically swap dbCache_ references.
  // TODO: Update this to use a CatalogObjectCache?
  protected AtomicReference<ConcurrentHashMap<String, Db>> dbCache_ =
      new AtomicReference<ConcurrentHashMap<String, Db>>(
          new ConcurrentHashMap<String, Db>());

  // Cache of the DB containing the builtins.
  private static Db builtinsDb_;

  // Cache of data sources.
  protected final CatalogObjectCache<DataSource> dataSources_;

  /**
   * Creates a new instance of a Catalog. If initMetastoreClientPool is true, will
   * also add META_STORE_CLIENT_POOL_SIZE clients to metastoreClientPool_.
   */
  public Catalog(boolean initMetastoreClientPool) {
    if (initMetastoreClientPool) {
      metaStoreClientPool_.addClients(META_STORE_CLIENT_POOL_SIZE);
    }
    dataSources_ = new CatalogObjectCache<DataSource>();
    initBuiltins();
  }

  public Db getBuiltinsDb() { return builtinsDb_;}

  /**
   * Adds a new database to the catalog, replacing any existing database with the same
   * name. Returns the previous database with this name, or null if there was no
   * previous database.
   */
  public Db addDb(Db db) {
    return dbCache_.get().put(db.getName().toLowerCase(), db);
  }

  /**
   * Gets the Db object from the Catalog using a case-insensitive lookup on the name.
   * Returns null if no matching database is found.
   */
  public Db getDb(String dbName) {
    Preconditions.checkState(dbName != null && !dbName.isEmpty(),
        "Null or empty database name given as argument to Catalog.getDb");
    return dbCache_.get().get(dbName.toLowerCase());
  }

  /**
   * Removes a database from the metadata cache. Returns the value removed or null
   * if not database was removed as part of this operation. Used by DROP DATABASE
   * statements.
   */
  public Db removeDb(String dbName) {
    return dbCache_.get().remove(dbName.toLowerCase());
  }

  /**
   * Returns a list of databases that match dbPattern. See filterStringsByPattern
   * for details of the pattern match semantics.
   *
   * dbPattern may be null (and thus matches everything).
   */
  public List<String> getDbNames(String dbPattern) {
    return filterStringsByPattern(dbCache_.get().keySet(), dbPattern);
  }

  /**
   * Returns the Table object for the given dbName/tableName. This will trigger a
   * metadata load if the table metadata is not yet cached.
   */
  public Table getTable(String dbName, String tableName) throws
      DatabaseNotFoundException {
    Db db = getDb(dbName);
    if (db == null) {
      throw new DatabaseNotFoundException("Database '" + dbName + "' not found");
    }
    return db.getTable(tableName);
  }

  /**
   * Removes a table from the catalog and returns the table that was removed, or null
   * if the table/database does not exist.
   */
  public Table removeTable(TTableName tableName) {
    // Remove the old table name from the cache and add the new table.
    Db db = getDb(tableName.getDb_name());
    if (db == null) return null;
    return db.removeTable(tableName.getTable_name());
  }

  /**
   * Returns a list of tables in the supplied database that match
   * tablePattern. See filterStringsByPattern for details of the pattern match semantics.
   *
   * dbName must not be null, but tablePattern may be null (and thus matches
   * everything).
   *
   * Table names are returned unqualified.
   */
  public List<String> getTableNames(String dbName, String tablePattern)
      throws DatabaseNotFoundException {
    Preconditions.checkNotNull(dbName);
    Db db = getDb(dbName);
    if (db == null) {
      throw new DatabaseNotFoundException("Database '" + dbName + "' not found");
    }
    return filterStringsByPattern(db.getAllTableNames(), tablePattern);
  }

  public boolean containsTable(String dbName, String tableName) {
    Db db = getDb(dbName);
    return (db == null) ? false : db.containsTable(tableName);
  }

  /**
   * Adds a data source to the in-memory map of data sources. It is not
   * persisted to the metastore.
   * @return true if this item was added or false if the existing value was preserved.
   */
  public boolean addDataSource(DataSource dataSource) {
    return dataSources_.add(dataSource);
  }

  /**
   * Removes a data source from the in-memory map of data sources.
   * @return the item that was removed if it existed in the cache, null otherwise.
   */
  public DataSource removeDataSource(String dataSourceName) {
    Preconditions.checkNotNull(dataSourceName);
    return dataSources_.remove(dataSourceName.toLowerCase());
  }

  /**
   * Gets the specified data source.
   */
  public DataSource getDataSource(String dataSourceName) {
    Preconditions.checkNotNull(dataSourceName);
    return dataSources_.get(dataSourceName.toLowerCase());
  }

  /**
   * Gets a list of all data sources.
   */
  public List<DataSource> getDataSources() {
    return dataSources_.getValues();
  }

  /**
   * Returns a list of data sources names that match pattern. See filterStringsByPattern
   * for details of the pattern match semantics.
   *
   * pattern may be null (and thus matches everything).
   */
  public List<String> getDataSourceNames(String pattern) {
    return filterStringsByPattern(dataSources_.getAllNames(), pattern);
  }

  /**
   * Returns a list of data sources that match pattern. See filterStringsByPattern
   * for details of the pattern match semantics.
   *
   * pattern may be null (and thus matches everything).
   */
  public List<DataSource> getDataSources(String pattern) {
    List<String> names = filterStringsByPattern(dataSources_.getAllNames(), pattern);
    List<DataSource> dataSources = Lists.newArrayListWithCapacity(names.size());
    for (String name: names) {
      dataSources.add(dataSources_.get(name));
    }
    return dataSources;
  }

  /**
   * Adds a function to the catalog.
   * Returns true if the function was successfully added.
   * Returns false if the function already exists.
   * TODO: allow adding a function to a global scope. We probably want this to resolve
   * after the local scope.
   * e.g. if we had fn() and db.fn(). If the current database is 'db', fn() would
   * resolve first to db.fn().
   */
  public boolean addFunction(Function fn) {
    Db db = getDb(fn.dbName());
    if (db == null) return false;
    return db.addFunction(fn);
  }

  /**
   * Returns the function that best matches 'desc' that is registered with the
   * catalog using 'mode' to check for matching. If desc matches multiple functions
   * in the catalog, it will return the function with the strictest matching mode.
   */
  public Function getFunction(Function desc, Function.CompareMode mode) {
    Db db = getDb(desc.dbName());
    if (db == null) return null;
    return db.getFunction(desc, mode);
  }

  public static Function getBuiltin(Function desc, Function.CompareMode mode) {
    return builtinsDb_.getFunction(desc, mode);
  }

  /**
   * Removes a function from the catalog. Increments the catalog version and returns
   * the Function object that was removed if the function existed, otherwise returns
   * null.
   */
  public Function removeFunction(Function desc) {
    Db db = getDb(desc.dbName());
    if (db == null) return null;
    return db.removeFunction(desc);
  }

  /**
   * Returns true if there is a function with this function name. Parameters
   * are ignored.
   */
  public boolean containsFunction(FunctionName name) {
    Db db = getDb(name.getDb());
    if (db == null) return false;
    return db.containsFunction(name.getFunction());
  }

  /**
   * Release the Hive Meta Store Client resources. Can be called multiple times
   * (additional calls will be no-ops).
   */
  public void close() { metaStoreClientPool_.close(); }


  /**
   * Returns a managed meta store client from the client connection pool.
   */
  public MetaStoreClient getMetaStoreClient() { return metaStoreClientPool_.getClient(); }

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
      PatternMatcher matcher = PatternMatcher.createHivePatternMatcher(matchPattern);
      for (String candidate: candidates) {
        if (matcher.matches(candidate)) filtered.add(candidate);
      }
    }
    Collections.sort(filtered, String.CASE_INSENSITIVE_ORDER);
    return filtered;
  }

  /**
   * Returns the HdfsPartition object for the given dbName/tableName and partition spec.
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
   * Returns true if the table contains the given partition spec, otherwise false.
   * This may trigger a metadata load if the table metadata is not yet cached.
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
   * Gets the thrift representation of a catalog object, given the "object
   * description". The object description is just a TCatalogObject with only the
   * catalog object type and object name set.
   * If the object is not found, a CatalogException is thrown.
   */
  public TCatalogObject getTCatalogObject(TCatalogObject objectDesc)
      throws CatalogException {
    TCatalogObject result = new TCatalogObject();
    switch (objectDesc.getType()) {
      case DATABASE: {
        Db db = getDb(objectDesc.getDb().getDb_name());
        if (db == null) {
          throw new CatalogException(
              "Database not found: " + objectDesc.getDb().getDb_name());
        }
        result.setType(db.getCatalogObjectType());
        result.setCatalog_version(db.getCatalogVersion());
        result.setDb(db.toThrift());
        break;
      }
      case TABLE:
      case VIEW: {
        Table table = getTable(objectDesc.getTable().getDb_name(),
            objectDesc.getTable().getTbl_name());
        if (table == null) {
          throw new CatalogException("Table not found: " +
              objectDesc.getTable().getTbl_name());
        }
        result.setType(table.getCatalogObjectType());
        result.setCatalog_version(table.getCatalogVersion());
        result.setTable(table.toThrift());
        break;
      }
      case FUNCTION: {
        TFunction tfn = objectDesc.getFn();
        Function desc = Function.fromThrift(tfn);
        Function fn = getFunction(desc, Function.CompareMode.IS_INDISTINGUISHABLE);
        if (fn == null) {
          throw new CatalogException("Function not found: " + tfn);
        }
        result.setType(fn.getCatalogObjectType());
        result.setCatalog_version(fn.getCatalogVersion());
        result.setFn(fn.toThrift());
        break;
      }
      default: throw new IllegalStateException(
          "Unexpected TCatalogObject type: " + objectDesc.getType());
    }
    return result;
  }

  /**
   * Initializes all the builtins.
   */
  private void initBuiltins() {
    if (builtinsDb_ != null) {
      // Only in the FE test setup do we hit this case.
      addDb(builtinsDb_);
      return;
    }
    Preconditions.checkState(getDb(BUILTINS_DB) == null);
    Preconditions.checkState(builtinsDb_ == null);
    builtinsDb_ = new Db(BUILTINS_DB, this);
    builtinsDb_.setIsSystemDb(true);
    addDb(builtinsDb_);

    // Populate all aggregate builtins.
    initAggregateBuiltins();

    // Populate all scalar builtins.
    ArithmeticExpr.initBuiltins(builtinsDb_);
    BinaryPredicate.initBuiltins(builtinsDb_);
    CastExpr.initBuiltins(builtinsDb_);
    CaseExpr.initBuiltins(builtinsDb_);
    CompoundPredicate.initBuiltins(builtinsDb_);
    LikePredicate.initBuiltins(builtinsDb_);
    ScalarBuiltins.initBuiltins(builtinsDb_);
  }

  private static final Map<ColumnType, String> HLL_UPDATE_SYMBOL =
      ImmutableMap.<ColumnType, String>builder()
        .put(ColumnType.BOOLEAN,
            "9HllUpdateIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.TINYINT,
            "9HllUpdateIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.SMALLINT,
            "9HllUpdateIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.INT,
            "9HllUpdateIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.BIGINT,
            "9HllUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.FLOAT,
            "9HllUpdateIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.DOUBLE,
            "9HllUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.STRING,
            "9HllUpdateIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS3_")
        .put(ColumnType.TIMESTAMP,
            "9HllUpdateIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.DECIMAL,
            "9HllUpdateINS_10DecimalValEEEvPN10impala_udf15FunctionContextERKT_PNS3_9StringValE")
        .build();

  private static final Map<ColumnType, String> PC_UPDATE_SYMBOL =
      ImmutableMap.<ColumnType, String>builder()
        .put(ColumnType.BOOLEAN,
            "8PcUpdateIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.TINYINT,
            "8PcUpdateIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.SMALLINT,
            "8PcUpdateIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.INT,
            "8PcUpdateIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.BIGINT,
            "8PcUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.FLOAT,
            "8PcUpdateIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.DOUBLE,
            "8PcUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.STRING,
            "8PcUpdateIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS3_")
        .put(ColumnType.TIMESTAMP,
            "8PcUpdateIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
         .put(ColumnType.DECIMAL,
            "8PcUpdateINS_10DecimalValEEEvPN10impala_udf15FunctionContextERKT_PNS3_9StringValE")
        .build();

    private static final Map<ColumnType, String> PCSA_UPDATE_SYMBOL =
      ImmutableMap.<ColumnType, String>builder()
          .put(ColumnType.BOOLEAN,
              "10PcsaUpdateIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(ColumnType.TINYINT,
              "10PcsaUpdateIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(ColumnType.SMALLINT,
              "10PcsaUpdateIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(ColumnType.INT,
              "10PcsaUpdateIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(ColumnType.BIGINT,
              "10PcsaUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(ColumnType.FLOAT,
              "10PcsaUpdateIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(ColumnType.DOUBLE,
              "10PcsaUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(ColumnType.STRING,
              "10PcsaUpdateIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS3_")
          .put(ColumnType.TIMESTAMP,
              "10PcsaUpdateIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
          .put(ColumnType.DECIMAL,
              "10PcsaUpdateINS_10DecimalValEEEvPN10impala_udf15FunctionContextERKT_PNS3_9StringValE")
          .build();

  private static final Map<ColumnType, String> MIN_UPDATE_SYMBOL =
      ImmutableMap.<ColumnType, String>builder()
        .put(ColumnType.BOOLEAN,
            "3MinIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.TINYINT,
            "3MinIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.SMALLINT,
            "3MinIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.INT,
            "3MinIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.BIGINT,
            "3MinIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.FLOAT,
            "3MinIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.DOUBLE,
            "3MinIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.STRING,
            "3MinIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.TIMESTAMP,
            "3MinIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.DECIMAL,
            "3MinINS_10DecimalValEEEvPN10impala_udf15FunctionContextERKT_PS6_")
        .build();

  private static final Map<ColumnType, String> MAX_UPDATE_SYMBOL =
      ImmutableMap.<ColumnType, String>builder()
        .put(ColumnType.BOOLEAN,
            "3MaxIN10impala_udf10BooleanValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.TINYINT,
            "3MaxIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.SMALLINT,
            "3MaxIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.INT,
            "3MaxIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.BIGINT,
            "3MaxIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.FLOAT,
            "3MaxIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.DOUBLE,
            "3MaxIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.STRING,
            "3MaxIN10impala_udf9StringValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.TIMESTAMP,
            "3MaxIN10impala_udf12TimestampValEEEvPNS2_15FunctionContextERKT_PS6_")
        .put(ColumnType.DECIMAL,
            "3MaxINS_10DecimalValEEEvPN10impala_udf15FunctionContextERKT_PS6_")
        .build();

  private static final Map<ColumnType, String> STDDEV_UPDATE_SYMBOL =
      ImmutableMap.<ColumnType, String>builder()
        .put(ColumnType.TINYINT,
            "14KnuthVarUpdateIN10impala_udf10TinyIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.SMALLINT,
            "14KnuthVarUpdateIN10impala_udf11SmallIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.INT,
            "14KnuthVarUpdateIN10impala_udf6IntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.BIGINT,
            "14KnuthVarUpdateIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.FLOAT,
            "14KnuthVarUpdateIN10impala_udf8FloatValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .put(ColumnType.DOUBLE,
            "14KnuthVarUpdateIN10impala_udf9DoubleValEEEvPNS2_15FunctionContextERKT_PNS2_9StringValE")
        .build();

  // Populate all the aggregate builtins in the catalog.
  // null symbols indicate the function does not need that step of the evaluation.
  // An empty symbol indicates a TODO for the BE to implement the function.
  // TODO: We could also generate this in python but I'm not sure that is easier.
  private void initAggregateBuiltins() {
    final String prefix = "_ZN6impala18AggregateFunctions";
    final String initNullString = prefix +
        "14InitNullStringEPN10impala_udf15FunctionContextEPNS1_9StringValE";
    final String initNull = prefix +
        "8InitNullEPN10impala_udf15FunctionContextEPNS1_6AnyValE";
    final String stringValSerializeOrFinalize = prefix +
        "28StringValSerializeOrFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE";

    Db db = builtinsDb_;
    // Count (*)
    // TODO: the merge function should be Sum but the way we rewrite distincts
    // makes that not work.
    db.addBuiltin(AggregateFunction.createBuiltin(db, "count",
        new ArrayList<ColumnType>(),
        ColumnType.BIGINT, ColumnType.BIGINT,
        prefix + "8InitZeroIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextEPT_",
        prefix + "15CountStarUpdateEPN10impala_udf15FunctionContextEPNS1_9BigIntValE",
        prefix + "15CountStarUpdateEPN10impala_udf15FunctionContextEPNS1_9BigIntValE",
        null, null, false));

    for (ColumnType t : ColumnType.getSupportedTypes()) {
      if (t.isNull()) continue; // NULL is handled through type promotion.
      // Count
      db.addBuiltin(AggregateFunction.createBuiltin(db, "count",
          Lists.newArrayList(t), ColumnType.BIGINT, ColumnType.BIGINT,
          prefix + "8InitZeroIN10impala_udf9BigIntValEEEvPNS2_15FunctionContextEPT_",
          prefix + "11CountUpdateEPN10impala_udf15FunctionContextERKNS1_6AnyValEPNS1_9BigIntValE",
          prefix + "11CountUpdateEPN10impala_udf15FunctionContextERKNS1_6AnyValEPNS1_9BigIntValE",
          null, null, false));
      // Min
      String minMaxInit = t.isStringType() ? initNullString : initNull;
      String minMaxSerializeOrFinalize = t.isStringType() ?
          stringValSerializeOrFinalize : null;
      db.addBuiltin(AggregateFunction.createBuiltin(db, "min",
          Lists.newArrayList(t), t, t, minMaxInit,
          prefix + MIN_UPDATE_SYMBOL.get(t),
          prefix + MIN_UPDATE_SYMBOL.get(t),
          minMaxSerializeOrFinalize, minMaxSerializeOrFinalize, true));
      // Max
      db.addBuiltin(AggregateFunction.createBuiltin(db, "max",
          Lists.newArrayList(t), t, t, minMaxInit,
          prefix + MAX_UPDATE_SYMBOL.get(t),
          prefix + MAX_UPDATE_SYMBOL.get(t),
          minMaxSerializeOrFinalize, minMaxSerializeOrFinalize, true));
      // NDV
      // TODO: this needs to switch to CHAR(64) as the intermediate type
      db.addBuiltin(AggregateFunction.createBuiltin(db, "ndv",
          Lists.newArrayList(t), ColumnType.STRING, ColumnType.STRING,
          prefix + "7HllInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
          prefix + HLL_UPDATE_SYMBOL.get(t),
          prefix + "8HllMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
          stringValSerializeOrFinalize,
          prefix + "11HllFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
          true));

      // distinctpc
      // TODO: this needs to switch to CHAR(64) as the intermediate type
      db.addBuiltin(AggregateFunction.createBuiltin(db, "distinctpc",
          Lists.newArrayList(t), ColumnType.STRING, ColumnType.STRING,
          prefix + "6PcInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
          prefix + PC_UPDATE_SYMBOL.get(t),
          prefix + "7PcMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
          stringValSerializeOrFinalize,
          prefix + "10PcFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
          true));

      // distinctpcsa
      // TODO: this needs to switch to CHAR(64) as the intermediate type
      db.addBuiltin(AggregateFunction.createBuiltin(db, "distinctpcsa",
          Lists.newArrayList(t), ColumnType.STRING, ColumnType.STRING,
          prefix + "6PcInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
          prefix + PCSA_UPDATE_SYMBOL.get(t),
          prefix + "7PcMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
          stringValSerializeOrFinalize,
          prefix + "12PcsaFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
          true));

      if (STDDEV_UPDATE_SYMBOL.containsKey(t)) {
        db.addBuiltin(AggregateFunction.createBuiltin(db, "stddev",
            Lists.newArrayList(t), ColumnType.STRING, ColumnType.STRING,
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            stringValSerializeOrFinalize,
            prefix + "19KnuthStddevFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            true));
        db.addBuiltin(AggregateFunction.createBuiltin(db, "stddev_samp",
            Lists.newArrayList(t), ColumnType.STRING, ColumnType.STRING,
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            stringValSerializeOrFinalize,
            prefix + "19KnuthStddevFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            true));
        db.addBuiltin(AggregateFunction.createBuiltin(db, "stddev_pop",
            Lists.newArrayList(t), ColumnType.STRING, ColumnType.STRING,
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            stringValSerializeOrFinalize,
            prefix + "22KnuthStddevPopFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            true));
        db.addBuiltin(AggregateFunction.createBuiltin(db, "variance",
            Lists.newArrayList(t), ColumnType.STRING, ColumnType.STRING,
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            stringValSerializeOrFinalize,
            prefix + "16KnuthVarFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            true));
        db.addBuiltin(AggregateFunction.createBuiltin(db, "variance_samp",
            Lists.newArrayList(t), ColumnType.STRING, ColumnType.STRING,
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            stringValSerializeOrFinalize,
            prefix + "16KnuthVarFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            true));
        db.addBuiltin(AggregateFunction.createBuiltin(db, "variance_pop",
            Lists.newArrayList(t), ColumnType.STRING, ColumnType.STRING,
            prefix + "12KnuthVarInitEPN10impala_udf15FunctionContextEPNS1_9StringValE",
            prefix + STDDEV_UPDATE_SYMBOL.get(t),
            prefix + "13KnuthVarMergeEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
            stringValSerializeOrFinalize,
            prefix + "19KnuthVarPopFinalizeEPN10impala_udf15FunctionContextERKNS1_9StringValE",
            true));
      }
    }

    // Sum
    db.addBuiltin(AggregateFunction.createBuiltin(db, "sum",
        Lists.newArrayList(ColumnType.BIGINT), ColumnType.BIGINT, ColumnType.BIGINT,
        initNull,
        prefix + "3SumIN10impala_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
        prefix + "3SumIN10impala_udf9BigIntValES3_EEvPNS2_15FunctionContextERKT_PT0_",
        null, null, false));
    db.addBuiltin(AggregateFunction.createBuiltin(db, "sum",
        Lists.newArrayList(ColumnType.DOUBLE), ColumnType.DOUBLE, ColumnType.DOUBLE,
        initNull,
        prefix + "3SumIN10impala_udf9DoubleValES3_EEvPNS2_15FunctionContextERKT_PT0_",
        prefix + "3SumIN10impala_udf9DoubleValES3_EEvPNS2_15FunctionContextERKT_PT0_",
        null, null, false));
    db.addBuiltin(AggregateFunction.createBuiltin(db, "sum",
        Lists.newArrayList(ColumnType.DECIMAL), ColumnType.DECIMAL, ColumnType.DECIMAL,
        initNull,
        prefix + "9SumUpdateEPN10impala_udf15FunctionContextERKNS_10DecimalValEPS4_",
        prefix + "8SumMergeEPN10impala_udf15FunctionContextERKNS_10DecimalValEPS4_",
        null, null, false));

    for (ColumnType t: ColumnType.getNumericTypes()) {
      // Avg
      // TODO: because of avg rewrite, BE doesn't implement it yet.
      db.addBuiltin(AggregateFunction.createBuiltin(db, "avg",
          Lists.newArrayList(t), ColumnType.DOUBLE, ColumnType.DOUBLE,
          "", "", "", null, "", false));
    }
    // Avg(Timestamp)
    // TODO: why does this make sense? Avg(timestamp) returns a double.
    db.addBuiltin(AggregateFunction.createBuiltin(db, "avg",
        Lists.newArrayList(ColumnType.TIMESTAMP),
        ColumnType.DOUBLE, ColumnType.DOUBLE,
        "", "", "", null, "", false));

    // Group_concat(string)
    db.addBuiltin(AggregateFunction.createBuiltin(db, "group_concat",
        Lists.newArrayList(ColumnType.STRING),
        ColumnType.STRING, ColumnType.STRING,
        initNullString,
        prefix + "12StringConcatEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        prefix + "12StringConcatEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        stringValSerializeOrFinalize, stringValSerializeOrFinalize, false));
    // Group_concat(string, string)
    db.addBuiltin(AggregateFunction.createBuiltin(db, "group_concat",
        Lists.newArrayList(ColumnType.STRING, ColumnType.STRING),
        ColumnType.STRING, ColumnType.STRING,
        initNullString,
        prefix +
            "12StringConcatEPN10impala_udf15FunctionContextERKNS1_9StringValES6_PS4_",
        prefix + "12StringConcatEPN10impala_udf15FunctionContextERKNS1_9StringValEPS4_",
        stringValSerializeOrFinalize, stringValSerializeOrFinalize, false));
  }
}
