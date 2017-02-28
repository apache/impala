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

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.CachePoolEntry;
import org.apache.hadoop.hdfs.protocol.CachePoolInfo;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.ql.exec.FunctionUtils;
import org.apache.impala.authorization.SentryConfig;
import org.apache.impala.catalog.MetaStoreClientPool.MetaStoreClient;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.JniUtil;
import org.apache.impala.common.Pair;
import org.apache.impala.common.Reference;
import org.apache.impala.hive.executor.UdfExecutor;
import org.apache.impala.thrift.TCatalog;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TFunction;
import org.apache.impala.thrift.TGetAllCatalogObjectsResponse;
import org.apache.impala.thrift.TPartitionKeyValue;
import org.apache.impala.thrift.TPrivilege;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TUniqueId;
import org.apache.impala.util.PatternMatcher;
import org.apache.impala.util.SentryProxy;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Specialized Catalog that implements the CatalogService specific Catalog
 * APIs. The CatalogServiceCatalog manages loading of all the catalog metadata
 * and processing of DDL requests. For each DDL request, the CatalogServiceCatalog
 * will return the catalog version that the update will show up in. The client
 * can then wait until the statestore sends an update that contains that catalog
 * version.
 * The CatalogServiceCatalog also manages a global "catalog version". The version
 * is incremented and assigned to a CatalogObject whenever it is
 * added/modified/removed from the catalog. This means each CatalogObject will have a
 * unique version and assigned versions are strictly increasing.
 *
 * Table metadata for IncompleteTables (not fully loaded tables) are loaded in the
 * background by the TableLoadingMgr; tables can be prioritized for loading by calling
 * prioritizeLoad(). Background loading can also be enabled for the catalog, in which
 * case missing tables (tables that are not yet loaded) are submitted to the
 * TableLoadingMgr any table metadata is invalidated and on startup. The metadata of
 * fully loaded tables (e.g. HdfsTable, HBaseTable, etc) are updated in-place and don't
 * trigger a background metadata load through the TableLoadingMgr. Accessing a table
 * that is not yet loaded (via getTable()), will load the table's metadata on-demand,
 * out-of-band of the table loading thread pool.
 *
 * See the class comments in CatalogOpExecutor for a description of the locking protocol
 * that should be employed if both the catalog lock and table locks need to be held at
 * the same time.
 *
 * TODO: Consider removing on-demand loading and have everything go through the table
 * loading thread pool.
 */
public class CatalogServiceCatalog extends Catalog {
  private static final Logger LOG = Logger.getLogger(CatalogServiceCatalog.class);

  private static final int INITIAL_META_STORE_CLIENT_POOL_SIZE = 10;
  private final TUniqueId catalogServiceId_;

  // Fair lock used to synchronize reads/writes of catalogVersion_. Because this lock
  // protects catalogVersion_, it can be used to perform atomic bulk catalog operations
  // since catalogVersion_ cannot change externally while the lock is being held.
  // In addition to protecting catalogVersion_, it is currently used for the
  // following bulk operations:
  // * Building a delta update to send to the statestore in getCatalogObjects(),
  //   so a snapshot of the catalog can be taken without any version changes.
  // * During a catalog invalidation (call to reset()), which re-reads all dbs and tables
  //   from the metastore.
  // * During renameTable(), because a table must be removed and added to the catalog
  //   atomically (potentially in a different database).
  private final ReentrantReadWriteLock catalogLock_ = new ReentrantReadWriteLock(true);

  // Last assigned catalog version. Starts at INITIAL_CATALOG_VERSION and is incremented
  // with each update to the Catalog. Continued across the lifetime of the object.
  // Protected by catalogLock_.
  // TODO: Handle overflow of catalogVersion_ and nextTableId_.
  // TODO: The name of this variable is misleading and can be interpreted as a property
  // of the catalog server. Rename into something that indicates its role as a global
  // sequence number assigned to catalog objects.
  private long catalogVersion_ = INITIAL_CATALOG_VERSION;

  // Manages the scheduling of background table loading.
  private final TableLoadingMgr tableLoadingMgr_;

  private final boolean loadInBackground_;

  // Periodically polls HDFS to get the latest set of known cache pools.
  private final ScheduledExecutorService cachePoolReader_ =
      Executors.newScheduledThreadPool(1);

  // Proxy to access the Sentry Service and also periodically refreshes the
  // policy metadata. Null if Sentry Service is not enabled.
  private final SentryProxy sentryProxy_;

  // Local temporary directory to copy UDF Jars.
  private static String localLibraryPath_;

  /**
   * Initialize the CatalogServiceCatalog. If 'loadInBackground' is true, table metadata
   * will be loaded in the background. 'initialHmsCnxnTimeoutSec' specifies the time (in
   * seconds) CatalogServiceCatalog will wait to establish an initial connection to the
   * HMS before giving up. Using this setting allows catalogd and HMS to be started
   * simultaneously.
   */
  public CatalogServiceCatalog(boolean loadInBackground, int numLoadingThreads,
      int initialHmsCnxnTimeoutSec, SentryConfig sentryConfig, TUniqueId catalogServiceId,
      String kerberosPrincipal, String localLibraryPath) {
    super(INITIAL_META_STORE_CLIENT_POOL_SIZE, initialHmsCnxnTimeoutSec);
    catalogServiceId_ = catalogServiceId;
    tableLoadingMgr_ = new TableLoadingMgr(this, numLoadingThreads);
    loadInBackground_ = loadInBackground;
    try {
      // We want only 'true' HDFS filesystems to poll the HDFS cache (i.e not S3,
      // local, etc.)
      if (FileSystemUtil.getDefaultFileSystem() instanceof DistributedFileSystem) {
        cachePoolReader_.scheduleAtFixedRate(
            new CachePoolReader(), 0, 1, TimeUnit.MINUTES);
      }
    } catch (IOException e) {
      LOG.error("Couldn't identify the default FS. Cache Pool reader will be disabled.");
    }
    if (sentryConfig != null) {
      sentryProxy_ = new SentryProxy(sentryConfig, this, kerberosPrincipal);
    } else {
      sentryProxy_ = null;
    }
    localLibraryPath_ = new String("file://" + localLibraryPath);
  }

  // Timeout for acquiring a table lock
  // TODO: Make this configurable
  private static final long TBL_LOCK_TIMEOUT_MS = 7200000;
  // Time to sleep before retrying to acquire a table lock
  private static final int TBL_LOCK_RETRY_MS = 10;

  /**
   * Tries to acquire catalogLock_ and the lock of 'tbl' in that order. Returns true if it
   * successfully acquires both within TBL_LOCK_TIMEOUT_MS millisecs; both locks are held
   * when the function returns. Returns false otherwise and no lock is held in this case.
   */
  public boolean tryLockTable(Table tbl) {
    long begin = System.currentTimeMillis();
    long end;
    do {
      catalogLock_.writeLock().lock();
      if (tbl.getLock().tryLock()) {
        if (LOG.isTraceEnabled()) {
          end = System.currentTimeMillis();
          LOG.trace(String.format("Lock for table %s was acquired in %d msec",
              tbl.getFullName(), end - begin));
        }
        return true;
      }
      catalogLock_.writeLock().unlock();
      try {
        // Sleep to avoid spinning and allow other operations to make progress.
        Thread.sleep(TBL_LOCK_RETRY_MS);
      } catch (InterruptedException e) {
        // ignore
      }
      end = System.currentTimeMillis();
    } while (end - begin < TBL_LOCK_TIMEOUT_MS);
    return false;
  }

  /**
   * Reads the current set of cache pools from HDFS and updates the catalog.
   * Called periodically by the cachePoolReader_.
   */
  protected class CachePoolReader implements Runnable {

    /**
     * This constructor is needed to create a non-threaded execution of the class.
     */
    public CachePoolReader() {
      super();
    }

    public void run() {
      if (LOG.isTraceEnabled()) LOG.trace("Reloading cache pool names from HDFS");

      // Map of cache pool name to CachePoolInfo. Stored in a map to allow Set operations
      // to be performed on the keys.
      Map<String, CachePoolInfo> currentCachePools = Maps.newHashMap();
      try {
        DistributedFileSystem dfs = FileSystemUtil.getDistributedFileSystem();
        RemoteIterator<CachePoolEntry> itr = dfs.listCachePools();
        while (itr.hasNext()) {
          CachePoolInfo cachePoolInfo = itr.next().getInfo();
          currentCachePools.put(cachePoolInfo.getPoolName(), cachePoolInfo);
        }
      } catch (Exception e) {
        LOG.error("Error loading cache pools: ", e);
        return;
      }

      catalogLock_.writeLock().lock();
      try {
        // Determine what has changed relative to what we have cached.
        Set<String> droppedCachePoolNames = Sets.difference(
            hdfsCachePools_.keySet(), currentCachePools.keySet());
        Set<String> createdCachePoolNames = Sets.difference(
            currentCachePools.keySet(), hdfsCachePools_.keySet());
        // Add all new cache pools.
        for (String createdCachePool: createdCachePoolNames) {
          HdfsCachePool cachePool = new HdfsCachePool(
              currentCachePools.get(createdCachePool));
          cachePool.setCatalogVersion(
              CatalogServiceCatalog.this.incrementAndGetCatalogVersion());
          hdfsCachePools_.add(cachePool);
        }
        // Remove dropped cache pools.
        for (String cachePoolName: droppedCachePoolNames) {
          hdfsCachePools_.remove(cachePoolName);
          CatalogServiceCatalog.this.incrementAndGetCatalogVersion();
        }
      } finally {
        catalogLock_.writeLock().unlock();
      }
    }
  }

  /**
   * Adds a list of cache directive IDs for the given table name. Asynchronously
   * refreshes the table metadata once all cache directives complete.
   */
  public void watchCacheDirs(List<Long> dirIds, TTableName tblName) {
    tableLoadingMgr_.watchCacheDirs(dirIds, tblName);
  }

  /**
   * Prioritizes the loading of the given list TCatalogObjects. Currently only support
   * loading Table/View metadata since Db and Function metadata is not loaded lazily.
   */
  public void prioritizeLoad(List<TCatalogObject> objectDescs) {
    for (TCatalogObject catalogObject: objectDescs) {
      Preconditions.checkState(catalogObject.isSetTable());
      TTable table = catalogObject.getTable();
      tableLoadingMgr_.prioritizeLoad(new TTableName(table.getDb_name().toLowerCase(),
          table.getTbl_name().toLowerCase()));
    }
  }

  /**
   * Returns all known objects in the Catalog (Tables, Views, Databases, and
   * Functions). Some metadata may be skipped for objects that have a catalog
   * version < the specified "fromVersion". Takes a lock on the catalog to ensure this
   * update contains a consistent snapshot of all items in the catalog. While holding the
   * catalog lock, it locks each accessed table to protect against concurrent
   * modifications.
   */
  public TGetAllCatalogObjectsResponse getCatalogObjects(long fromVersion) {
    TGetAllCatalogObjectsResponse resp = new TGetAllCatalogObjectsResponse();
    resp.setObjects(new ArrayList<TCatalogObject>());
    resp.setMax_catalog_version(Catalog.INITIAL_CATALOG_VERSION);
    catalogLock_.readLock().lock();
    try {
      for (Db db: getDbs(PatternMatcher.MATCHER_MATCH_ALL)) {
        TCatalogObject catalogDb = new TCatalogObject(TCatalogObjectType.DATABASE,
            db.getCatalogVersion());
        catalogDb.setDb(db.toThrift());
        resp.addToObjects(catalogDb);

        for (String tblName: db.getAllTableNames()) {
          TCatalogObject catalogTbl = new TCatalogObject(TCatalogObjectType.TABLE,
              Catalog.INITIAL_CATALOG_VERSION);

          Table tbl = db.getTable(tblName);
          if (tbl == null) {
            LOG.error("Table: " + tblName + " was expected to be in the catalog " +
                "cache. Skipping table for this update.");
            continue;
          }

          // Protect the table from concurrent modifications.
          tbl.getLock().lock();
          try {
            // Only add the extended metadata if this table's version is >=
            // the fromVersion.
            if (tbl.getCatalogVersion() >= fromVersion) {
              try {
                catalogTbl.setTable(tbl.toThrift());
              } catch (Exception e) {
                if (LOG.isTraceEnabled()) {
                  LOG.trace(String.format("Error calling toThrift() on table %s.%s: %s",
                      db.getName(), tblName, e.getMessage()), e);
                }
                continue;
              }
              catalogTbl.setCatalog_version(tbl.getCatalogVersion());
            } else {
              catalogTbl.setTable(new TTable(db.getName(), tblName));
            }
          } finally {
            tbl.getLock().unlock();
          }
          resp.addToObjects(catalogTbl);
        }

        for (Function fn: db.getFunctions(null, new PatternMatcher())) {
          TCatalogObject function = new TCatalogObject(TCatalogObjectType.FUNCTION,
              fn.getCatalogVersion());
          function.setFn(fn.toThrift());
          resp.addToObjects(function);
        }
      }

      for (DataSource dataSource: getDataSources()) {
        TCatalogObject catalogObj = new TCatalogObject(TCatalogObjectType.DATA_SOURCE,
            dataSource.getCatalogVersion());
        catalogObj.setData_source(dataSource.toThrift());
        resp.addToObjects(catalogObj);
      }
      for (HdfsCachePool cachePool: hdfsCachePools_) {
        TCatalogObject pool = new TCatalogObject(TCatalogObjectType.HDFS_CACHE_POOL,
            cachePool.getCatalogVersion());
        pool.setCache_pool(cachePool.toThrift());
        resp.addToObjects(pool);
      }

      // Get all roles
      for (Role role: authPolicy_.getAllRoles()) {
        TCatalogObject thriftRole = new TCatalogObject();
        thriftRole.setRole(role.toThrift());
        thriftRole.setCatalog_version(role.getCatalogVersion());
        thriftRole.setType(role.getCatalogObjectType());
        resp.addToObjects(thriftRole);

        for (RolePrivilege p: role.getPrivileges()) {
          TCatalogObject privilege = new TCatalogObject();
          privilege.setPrivilege(p.toThrift());
          privilege.setCatalog_version(p.getCatalogVersion());
          privilege.setType(p.getCatalogObjectType());
          resp.addToObjects(privilege);
        }
      }

      // Each update should contain a single "TCatalog" object which is used to
      // pass overall state on the catalog, such as the current version and the
      // catalog service id.
      TCatalogObject catalog = new TCatalogObject();
      catalog.setType(TCatalogObjectType.CATALOG);
      // By setting the catalog version to the latest catalog version at this point,
      // it ensure impalads will always bump their versions, even in the case where
      // an object has been dropped.
      catalog.setCatalog_version(getCatalogVersion());
      catalog.setCatalog(new TCatalog(catalogServiceId_));
      resp.addToObjects(catalog);

      // The max version is the max catalog version of all items in the update.
      resp.setMax_catalog_version(getCatalogVersion());
      return resp;
    } finally {
      catalogLock_.readLock().unlock();
    }
  }

  /**
   * Returns all user defined functions (aggregate and scalar) in the specified database.
   * Functions are not returned in a defined order.
   */
  public List<Function> getFunctions(String dbName) throws DatabaseNotFoundException {
    Db db = getDb(dbName);
    if (db == null) {
      throw new DatabaseNotFoundException("Database does not exist: " + dbName);
    }

    // Contains map of overloaded function names to all functions matching that name.
    HashMap<String, List<Function>> dbFns = db.getAllFunctions();
    List<Function> fns = new ArrayList<Function>(dbFns.size());
    for (List<Function> fnOverloads: dbFns.values()) {
      for (Function fn: fnOverloads) {
        fns.add(fn);
      }
    }
    return fns;
  }

  /**
   * Checks if the Hive function 'fn' is Impala compatible. A function is Impala
   * compatible iff
   *
   * 1. The function is JAVA based,
   * 2. Has exactly one binary resource associated (We don't support loading
   *    dependencies yet) and
   * 3. The binary is of type JAR.
   *
   * Returns true if compatible and false otherwise. In case of incompatible
   * functions 'incompatMsg' has the reason for the incompatibility.
   * */
   public static boolean isFunctionCompatible(
       org.apache.hadoop.hive.metastore.api.Function fn, StringBuilder incompatMsg) {
    boolean isCompatible = true;
    if (fn.getFunctionType() != FunctionType.JAVA) {
      isCompatible = false;
      incompatMsg.append("Function type: " + fn.getFunctionType().name()
          + " is not supported. Only " + FunctionType.JAVA.name() + " functions "
          + "are supported.");
    } else if (fn.getResourceUrisSize() == 0) {
      isCompatible = false;
      incompatMsg.append("No executable binary resource (like a JAR file) is " +
          "associated with this function. To fix this, recreate the function by " +
          "specifying a 'location' in the function create statement.");
    } else if (fn.getResourceUrisSize() != 1) {
      isCompatible = false;
      List<String> resourceUris = Lists.newArrayList();
      for (ResourceUri resource: fn.getResourceUris()) {
        resourceUris.add(resource.getUri());
      }
      incompatMsg.append("Impala does not support multiple Jars for dependencies."
          + "(" + Joiner.on(",").join(resourceUris) + ") ");
    } else if (fn.getResourceUris().get(0).getResourceType() != ResourceType.JAR) {
      isCompatible = false;
      incompatMsg.append("Function binary type: " +
        fn.getResourceUris().get(0).getResourceType().name()
        + " is not supported. Only " + ResourceType.JAR.name()
        + " type is supported.");
    }
    return isCompatible;
  }

  /**
   * Returns a list of Impala Functions, one per compatible "evaluate" method in the UDF
   * class referred to by the given Java function. This method copies the UDF Jar
   * referenced by "function" to a temporary file in localLibraryPath_ and loads it
   * into the jvm. Then we scan all the methods in the class using reflection and extract
   * those methods and create corresponding Impala functions. Currently Impala supports
   * only "JAR" files for symbols and also a single Jar containing all the dependent
   * classes rather than a set of Jar files.
   */
  public static List<Function> extractFunctions(String db,
      org.apache.hadoop.hive.metastore.api.Function function)
      throws ImpalaRuntimeException{
    List<Function> result = Lists.newArrayList();
    List<String> addedSignatures = Lists.newArrayList();
    StringBuilder warnMessage = new StringBuilder();
    if (!isFunctionCompatible(function, warnMessage)) {
      LOG.warn("Skipping load of incompatible function: " +
          function.getFunctionName() + ". " + warnMessage.toString());
      return result;
    }
    String jarUri = function.getResourceUris().get(0).getUri();
    Class<?> udfClass = null;
    Path localJarPath = null;
    try {
      localJarPath = new Path(localLibraryPath_, UUID.randomUUID().toString() + ".jar");
      try {
        FileSystemUtil.copyToLocal(new Path(jarUri), localJarPath);
      } catch (IOException e) {
        String errorMsg = "Error loading Java function: " + db + "." +
            function.getFunctionName() + ". Couldn't copy " + jarUri +
            " to local path: " + localJarPath.toString();
        LOG.error(errorMsg, e);
        throw new ImpalaRuntimeException(errorMsg);
      }
      URL[] classLoaderUrls = new URL[] {new URL(localJarPath.toString())};
      URLClassLoader urlClassLoader = new URLClassLoader(classLoaderUrls);
      udfClass = urlClassLoader.loadClass(function.getClassName());
      // Check if the class is of UDF type. Currently we don't support other functions
      // TODO: Remove this once we support Java UDAF/UDTF
      if (FunctionUtils.getUDFClassType(udfClass) != FunctionUtils.UDFClassType.UDF) {
        LOG.warn("Ignoring load of incompatible Java function: " +
            function.getFunctionName() + " as " + FunctionUtils.getUDFClassType(udfClass)
            + " is not a supported type. Only UDFs are supported");
        return result;
      }
      // Load each method in the UDF class and create the corresponding Impala Function
      // object.
      for (Method m: udfClass.getMethods()) {
        if (!m.getName().equals(UdfExecutor.UDF_FUNCTION_NAME)) continue;
        Function fn = ScalarFunction.fromHiveFunction(db,
            function.getFunctionName(), function.getClassName(),
            m.getParameterTypes(), m.getReturnType(), jarUri);
        if (fn == null) {
          LOG.warn("Ignoring incompatible method: " + m.toString() + " during load of " +
             "Hive UDF:" + function.getFunctionName() + " from " + udfClass);
          continue;
        }
        if (!addedSignatures.contains(fn.signatureString())) {
          result.add(fn);
          addedSignatures.add(fn.signatureString());
        }
      }
    } catch (ClassNotFoundException c) {
      String errorMsg = "Error loading Java function: " + db + "." +
          function.getFunctionName() + ". Symbol class " + udfClass +
          "not found in Jar: " + jarUri;
      LOG.error(errorMsg);
      throw new ImpalaRuntimeException(errorMsg, c);
    } catch (Exception e) {
      LOG.error("Skipping function load: " + function.getFunctionName(), e);
      throw new ImpalaRuntimeException("Error extracting functions", e);
    } catch (LinkageError e) {
      String errorMsg = "Error resolving dependencies for Java function: " + db + "." +
          function.getFunctionName();
      LOG.error(errorMsg);
      throw new ImpalaRuntimeException(errorMsg, e);
    } finally {
      if (localJarPath != null) FileSystemUtil.deleteIfExists(localJarPath);
    }
    return result;
  }

 /**
   * Extracts Impala functions stored in metastore db parameters and adds them to
   * the catalog cache.
   */
  private void loadFunctionsFromDbParams(Db db,
      org.apache.hadoop.hive.metastore.api.Database msDb) {
    if (msDb == null || msDb.getParameters() == null) return;
    LOG.info("Loading native functions for database: " + db.getName());
    TCompactProtocol.Factory protocolFactory = new TCompactProtocol.Factory();
    for (String key: msDb.getParameters().keySet()) {
      if (!key.startsWith(Db.FUNCTION_INDEX_PREFIX)) continue;
      try {
        TFunction fn = new TFunction();
        JniUtil.deserializeThrift(protocolFactory, fn,
            Base64.decodeBase64(msDb.getParameters().get(key)));
        Function addFn = Function.fromThrift(fn);
        db.addFunction(addFn, false);
        addFn.setCatalogVersion(incrementAndGetCatalogVersion());
      } catch (ImpalaException e) {
        LOG.error("Encountered an error during function load: key=" + key
            + ",continuing", e);
      }
    }
    LOG.info("Loaded native functions for database: " + db.getName());
  }

  /**
   * Loads Java functions into the catalog. For each function in "functions",
   * we extract all Impala compatible evaluate() signatures and load them
   * as separate functions in the catalog.
   */
  private void loadJavaFunctions(Db db,
      List<org.apache.hadoop.hive.metastore.api.Function> functions) {
    Preconditions.checkNotNull(functions);
    LOG.info("Loading Java functions for database: " + db.getName());
    for (org.apache.hadoop.hive.metastore.api.Function function: functions) {
      try {
        for (Function fn: extractFunctions(db.getName(), function)) {
          db.addFunction(fn);
          fn.setCatalogVersion(incrementAndGetCatalogVersion());
        }
      } catch (Exception e) {
        LOG.error("Skipping function load: " + function.getFunctionName(), e);
      }
    }
    LOG.info("Loaded Java functions for database: " + db.getName());
  }

  /**
   * Invalidates the database 'db'. This method can have potential race
   * conditions with external changes to the Hive metastore and hence any
   * conflicting changes to the objects can manifest in the form of exceptions
   * from the HMS calls which are appropriately handled. Returns the invalidated
   * 'Db' object along with list of tables to be loaded by the TableLoadingMgr.
   * Returns null if the method encounters an exception during invalidation.
   */
  private Pair<Db, List<TTableName>> invalidateDb(
      MetaStoreClient msClient, String dbName, Db existingDb) {
    try {
      List<org.apache.hadoop.hive.metastore.api.Function> javaFns =
          Lists.newArrayList();
      for (String javaFn: msClient.getHiveClient().getFunctions(dbName, "*")) {
        javaFns.add(msClient.getHiveClient().getFunction(dbName, javaFn));
      }
      org.apache.hadoop.hive.metastore.api.Database msDb =
          msClient.getHiveClient().getDatabase(dbName);
      Db newDb = new Db(dbName, this, msDb);
      // existingDb is usually null when the Catalog loads for the first time.
      // In that case we needn't restore any transient functions.
      if (existingDb != null) {
        // Restore UDFs that aren't persisted. They are only cleaned up on
        // Catalog restart.
        for (Function fn: existingDb.getTransientFunctions()) {
          newDb.addFunction(fn);
          fn.setCatalogVersion(incrementAndGetCatalogVersion());
        }
      }
      // Reload native UDFs.
      loadFunctionsFromDbParams(newDb, msDb);
      // Reload Java UDFs from HMS.
      loadJavaFunctions(newDb, javaFns);
      newDb.setCatalogVersion(incrementAndGetCatalogVersion());

      List<TTableName> tblsToBackgroundLoad = Lists.newArrayList();
      for (String tableName: msClient.getHiveClient().getAllTables(dbName)) {
        Table incompleteTbl = IncompleteTable.createUninitializedTable(newDb, tableName);
        incompleteTbl.setCatalogVersion(incrementAndGetCatalogVersion());
        newDb.addTable(incompleteTbl);
        if (loadInBackground_) {
          tblsToBackgroundLoad.add(new TTableName(dbName, tableName.toLowerCase()));
        }
      }
      return Pair.create(newDb, tblsToBackgroundLoad);
    } catch (Exception e) {
      LOG.warn("Encountered an exception while invalidating database: " + dbName +
          ". Ignoring further load of this db.", e);
    }
    return null;
  }

  /**
   * Resets this catalog instance by clearing all cached table and database metadata.
   */
  public void reset() throws CatalogException {
    LOG.info("Invalidating all metadata.");

    // First update the policy metadata.
    if (sentryProxy_ != null) {
      // Sentry Service is enabled.
      try {
        // Update the authorization policy, waiting for the result to complete.
        sentryProxy_.refresh();
      } catch (Exception e) {
        throw new CatalogException("Error updating authorization policy: ", e);
      }
    }

    catalogLock_.writeLock().lock();
    try {
      // Not all Java UDFs are persisted to the metastore. The ones which aren't
      // should be restored once the catalog has been invalidated.
      Map<String, Db> oldDbCache = dbCache_.get();

      // Build a new DB cache, populate it, and replace the existing cache in one
      // step.
      ConcurrentHashMap<String, Db> newDbCache = new ConcurrentHashMap<String, Db>();
      List<TTableName> tblsToBackgroundLoad = Lists.newArrayList();
      try (MetaStoreClient msClient = getMetaStoreClient()) {
        for (String dbName: msClient.getHiveClient().getAllDatabases()) {
          dbName = dbName.toLowerCase();
          Db oldDb = oldDbCache.get(dbName);
          Pair<Db, List<TTableName>> invalidatedDb = invalidateDb(msClient,
              dbName, oldDb);
          if (invalidatedDb == null) continue;
          newDbCache.put(dbName, invalidatedDb.first);
          tblsToBackgroundLoad.addAll(invalidatedDb.second);
        }
      }
      dbCache_.set(newDbCache);
      // Submit tables for background loading.
      for (TTableName tblName: tblsToBackgroundLoad) {
        tableLoadingMgr_.backgroundLoad(tblName);
      }
    } catch (Exception e) {
      LOG.error(e);
      throw new CatalogException("Error initializing Catalog. Catalog may be empty.", e);
    } finally {
      catalogLock_.writeLock().unlock();
    }
    LOG.info("Invalidated all metadata.");
  }

  /**
   * Adds a database name to the metadata cache and returns the database's
   * new Db object. Used by CREATE DATABASE statements.
   */
  public Db addDb(String dbName, org.apache.hadoop.hive.metastore.api.Database msDb)
      throws ImpalaException {
    Db newDb = new Db(dbName, this, msDb);
    newDb.setCatalogVersion(incrementAndGetCatalogVersion());
    addDb(newDb);
    return newDb;
  }

  /**
   * Removes a database from the metadata cache and returns the removed database,
   * or null if the database did not exist in the cache.
   * Used by DROP DATABASE statements.
   */
  @Override
  public Db removeDb(String dbName) {
    Db removedDb = super.removeDb(dbName);
    if (removedDb != null) {
      removedDb.setCatalogVersion(incrementAndGetCatalogVersion());
    }
    return removedDb;
  }

  /**
   * Adds a table with the given name to the catalog and returns the new table,
   * loading the metadata if needed.
   */
  public Table addTable(String dbName, String tblName) {
    Db db = getDb(dbName);
    if (db == null) return null;
    Table incompleteTable = IncompleteTable.createUninitializedTable(db, tblName);
    incompleteTable.setCatalogVersion(incrementAndGetCatalogVersion());
    db.addTable(incompleteTable);
    return db.getTable(tblName);
  }

  /**
   * Gets the table with the given name, loading it if needed (if the existing catalog
   * object is not yet loaded). Returns the matching Table or null if no table with this
   * name exists in the catalog.
   * If the existing table is dropped or modified (indicated by the catalog version
   * changing) while the load is in progress, the loaded value will be discarded
   * and the current cached value will be returned. This may mean that a missing table
   * (not yet loaded table) will be returned.
   */
  public Table getOrLoadTable(String dbName, String tblName)
      throws CatalogException {
    TTableName tableName = new TTableName(dbName.toLowerCase(), tblName.toLowerCase());
    TableLoadingMgr.LoadRequest loadReq;

    long previousCatalogVersion;
    // Return the table if it is already loaded or submit a new load request.
    catalogLock_.readLock().lock();
    try {
      Table tbl = getTable(dbName, tblName);
      if (tbl == null || tbl.isLoaded()) return tbl;
      previousCatalogVersion = tbl.getCatalogVersion();
      loadReq = tableLoadingMgr_.loadAsync(tableName);
    } finally {
      catalogLock_.readLock().unlock();
    }
    Preconditions.checkNotNull(loadReq);
    try {
      // The table may have been dropped/modified while the load was in progress, so only
      // apply the update if the existing table hasn't changed.
      return replaceTableIfUnchanged(loadReq.get(), previousCatalogVersion);
    } finally {
      loadReq.close();
    }
  }

  /**
   * Replaces an existing Table with a new value if it exists and has not changed
   * (has the same catalog version as 'expectedCatalogVersion').
   */
  private Table replaceTableIfUnchanged(Table updatedTbl, long expectedCatalogVersion)
      throws DatabaseNotFoundException {
    catalogLock_.writeLock().lock();
    try {
      Db db = getDb(updatedTbl.getDb().getName());
      if (db == null) {
        throw new DatabaseNotFoundException(
            "Database does not exist: " + updatedTbl.getDb().getName());
      }

      Table existingTbl = db.getTable(updatedTbl.getName());
      // The existing table does not exist or has been modified. Instead of
      // adding the loaded value, return the existing table.
      if (existingTbl == null ||
          existingTbl.getCatalogVersion() != expectedCatalogVersion) return existingTbl;

      updatedTbl.setCatalogVersion(incrementAndGetCatalogVersion());
      db.addTable(updatedTbl);
      return updatedTbl;
    } finally {
      catalogLock_.writeLock().unlock();
    }
  }

  /**
   * Removes a table from the catalog and increments the catalog version.
   * Returns the removed Table, or null if the table or db does not exist.
   */
  public Table removeTable(String dbName, String tblName) {
    Db parentDb = getDb(dbName);
    if (parentDb == null) return null;

    Table removedTable = parentDb.removeTable(tblName);
    if (removedTable != null) {
      removedTable.setCatalogVersion(incrementAndGetCatalogVersion());
    }
    return removedTable;
  }

  /**
   * Removes a function from the catalog. Increments the catalog version and returns
   * the Function object that was removed. If the function did not exist, null will
   * be returned.
   */
  @Override
  public Function removeFunction(Function desc) {
    Function removedFn = super.removeFunction(desc);
    if (removedFn != null) {
      removedFn.setCatalogVersion(incrementAndGetCatalogVersion());
    }
    return removedFn;
  }

  /**
   * Adds a function from the catalog, incrementing the catalog version. Returns true if
   * the add was successful, false otherwise.
   */
  @Override
  public boolean addFunction(Function fn) {
    Db db = getDb(fn.getFunctionName().getDb());
    if (db == null) return false;
    if (db.addFunction(fn)) {
      fn.setCatalogVersion(incrementAndGetCatalogVersion());
      return true;
    }
    return false;
  }

  /**
   * Adds a data source to the catalog, incrementing the catalog version. Returns true
   * if the add was successful, false otherwise.
   */
  @Override
  public boolean addDataSource(DataSource dataSource) {
    if (dataSources_.add(dataSource)) {
      dataSource.setCatalogVersion(incrementAndGetCatalogVersion());
      return true;
    }
    return false;
  }

  @Override
  public DataSource removeDataSource(String dataSourceName) {
    DataSource dataSource = dataSources_.remove(dataSourceName);
    if (dataSource != null) {
      dataSource.setCatalogVersion(incrementAndGetCatalogVersion());
    }
    return dataSource;
  }

  /**
   * Returns the table parameter 'transient_lastDdlTime', or -1 if it's not set.
   * TODO: move this to a metastore helper class.
   */
  public static long getLastDdlTime(org.apache.hadoop.hive.metastore.api.Table msTbl) {
    Preconditions.checkNotNull(msTbl);
    Map<String, String> params = msTbl.getParameters();
    String lastDdlTimeStr = params.get("transient_lastDdlTime");
    if (lastDdlTimeStr != null) {
      try {
        return Long.parseLong(lastDdlTimeStr);
      } catch (NumberFormatException e) {}
    }
    return -1;
  }

  /**
   * Updates the cached lastDdlTime for the given table. The lastDdlTime is used during
   * the metadata refresh() operations to determine if there have been any external
   * (outside of Impala) modifications to the table.
   */
  public void updateLastDdlTime(TTableName tblName, long ddlTime) {
    Db db = getDb(tblName.getDb_name());
    if (db == null) return;
    Table tbl = db.getTable(tblName.getTable_name());
    if (tbl == null) return;
    tbl.updateLastDdlTime(ddlTime);
  }

  /**
   * Renames a table. Equivalent to an atomic drop + add of the table. Returns
   * the new Table object with an incremented catalog version or null if the
   * drop or add were unsuccessful. If null is returned, then the catalog cache
   * is in one of the following two states:
   * 1. Old table was not removed, and new table was not added
   * 2. Old table was removed, but new table was not added
   */
  public Table renameTable(TTableName oldTableName, TTableName newTableName)
      throws CatalogException {
    // Remove the old table name from the cache and add the new table.
    Db db = getDb(oldTableName.getDb_name());
    if (db == null) return null;
    Table oldTable = db.removeTable(oldTableName.getTable_name());
    if (oldTable == null) return null;
    return addTable(newTableName.getDb_name(), newTableName.getTable_name());
  }

  /**
   * Reloads metadata for table 'tbl' which must not be an IncompleteTable. Updates the
   * table metadata in-place by calling load() on the given table. Returns the
   * TCatalogObject representing 'tbl'. Applies proper synchronization to protect the
   * metadata load from concurrent table modifications and assigns a new catalog version.
   * Throws a CatalogException if there is an error loading table metadata.
   */
  public TCatalogObject reloadTable(Table tbl) throws CatalogException {
    LOG.info(String.format("Refreshing table metadata: %s", tbl.getFullName()));
    Preconditions.checkState(!(tbl instanceof IncompleteTable));
    String dbName = tbl.getDb().getName();
    String tblName = tbl.getName();

    if (!tryLockTable(tbl)) {
      throw new CatalogException(String.format("Error refreshing metadata for table " +
          "%s due to lock contention", tbl.getFullName()));
    }
    try {
      long newCatalogVersion = incrementAndGetCatalogVersion();
      catalogLock_.writeLock().unlock();
      try (MetaStoreClient msClient = getMetaStoreClient()) {
        org.apache.hadoop.hive.metastore.api.Table msTbl = null;
        try {
          msTbl = msClient.getHiveClient().getTable(dbName, tblName);
        } catch (Exception e) {
          throw new TableLoadingException("Error loading metadata for table: " +
              dbName + "." + tblName, e);
        }
        tbl.load(true, msClient.getHiveClient(), msTbl);
      }
      tbl.setCatalogVersion(newCatalogVersion);
      LOG.info(String.format("Refreshed table metadata: %s", tbl.getFullName()));
      return tbl.toTCatalogObject();
    } finally {
      Preconditions.checkState(!catalogLock_.isWriteLockedByCurrentThread());
      tbl.getLock().unlock();
    }
  }

  /**
   * Drops the partitions specified in 'partitionSet' from 'tbl'. Throws a
   * CatalogException if 'tbl' is not an HdfsTable. Returns the target table.
   */
  public Table dropPartitions(Table tbl, List<List<TPartitionKeyValue>> partitionSet)
      throws CatalogException {
    Preconditions.checkNotNull(tbl);
    Preconditions.checkNotNull(partitionSet);
    Preconditions.checkArgument(tbl.getLock().isHeldByCurrentThread());
    if (!(tbl instanceof HdfsTable)) {
      throw new CatalogException("Table " + tbl.getFullName() + " is not an Hdfs table");
    }
    HdfsTable hdfsTable = (HdfsTable) tbl;
    List<HdfsPartition> partitions =
        hdfsTable.getPartitionsFromPartitionSet(partitionSet);
    hdfsTable.dropPartitions(partitions);
    return hdfsTable;
  }

  /**
   * Adds a partition to its HdfsTable and returns the modified table.
   */
  public Table addPartition(HdfsPartition partition) throws CatalogException {
    Preconditions.checkNotNull(partition);
    HdfsTable hdfsTable = partition.getTable();
    hdfsTable.addPartition(partition);
    return hdfsTable;
  }

  /**
   * Invalidates the table in the catalog cache, potentially adding/removing the table
   * from the cache based on whether it exists in the Hive Metastore.
   * The invalidation logic is:
   * - If the table exists in the Metastore, add it to the catalog as an uninitialized
   *   IncompleteTable (replacing any existing entry). The table metadata will be
   *   loaded lazily, on the next access. If the parent database for this table does not
   *   yet exist in Impala's cache it will also be added.
   * - If the table does not exist in the Metastore, remove it from the catalog cache.
   * - If we are unable to determine whether the table exists in the Metastore (there was
   *   an exception thrown making the RPC), invalidate any existing Table by replacing
   *   it with an uninitialized IncompleteTable.
   * Returns the thrift representation of the added/updated/removed table, or null if
   * the table was not present in the catalog cache or the Metastore.
   * Sets tblWasRemoved to true if the table was absent from the Metastore and it was
   * removed from the catalog cache.
   * Sets dbWasAdded to true if both a new database and table were added to the catalog
   * cache.
   */
  public TCatalogObject invalidateTable(TTableName tableName,
      Reference<Boolean> tblWasRemoved, Reference<Boolean> dbWasAdded) {
    tblWasRemoved.setRef(false);
    dbWasAdded.setRef(false);
    String dbName = tableName.getDb_name();
    String tblName = tableName.getTable_name();
    LOG.info(String.format("Invalidating table metadata: %s.%s", dbName, tblName));

    // Stores whether the table exists in the metastore. Can have three states:
    // 1) true - Table exists in metastore.
    // 2) false - Table does not exist in metastore.
    // 3) unknown (null) - There was exception thrown by the metastore client.
    Boolean tableExistsInMetaStore;
    Db db = null;
    try (MetaStoreClient msClient = getMetaStoreClient()) {
      org.apache.hadoop.hive.metastore.api.Database msDb = null;
      try {
        tableExistsInMetaStore = msClient.getHiveClient().tableExists(dbName, tblName);
      } catch (UnknownDBException e) {
        // The parent database does not exist in the metastore. Treat this the same
        // as if the table does not exist.
        tableExistsInMetaStore = false;
      } catch (TException e) {
        LOG.error("Error executing tableExists() metastore call: " + tblName, e);
        tableExistsInMetaStore = null;
      }

      if (tableExistsInMetaStore != null && !tableExistsInMetaStore) {
        Table result = removeTable(dbName, tblName);
        if (result == null) return null;
        tblWasRemoved.setRef(true);
        result.getLock().lock();
        try {
          return result.toTCatalogObject();
        } finally {
          result.getLock().unlock();
        }
      }

      db = getDb(dbName);
      if ((db == null || !db.containsTable(tblName)) && tableExistsInMetaStore == null) {
        // The table does not exist in our cache AND it is unknown whether the
        // table exists in the Metastore. Do nothing.
        return null;
      } else if (db == null && tableExistsInMetaStore) {
        // The table exists in the Metastore, but our cache does not contain the parent
        // database. A new db will be added to the cache along with the new table. msDb
        // must be valid since tableExistsInMetaStore is true.
        try {
          msDb = msClient.getHiveClient().getDatabase(dbName);
          Preconditions.checkNotNull(msDb);
          db = new Db(dbName, this, msDb);
          db.setCatalogVersion(incrementAndGetCatalogVersion());
          addDb(db);
          dbWasAdded.setRef(true);
        } catch (TException e) {
          // The Metastore database cannot be get. Log the error and return.
          LOG.error("Error executing getDatabase() metastore call: " + dbName, e);
          return null;
        }
      }
    }

    // Add a new uninitialized table to the table cache, effectively invalidating
    // any existing entry. The metadata for the table will be loaded lazily, on the
    // on the next access to the table.
    Table newTable = IncompleteTable.createUninitializedTable(db, tblName);
    newTable.setCatalogVersion(incrementAndGetCatalogVersion());
    db.addTable(newTable);
    if (loadInBackground_) {
      tableLoadingMgr_.backgroundLoad(new TTableName(dbName.toLowerCase(),
          tblName.toLowerCase()));
    }
    if (dbWasAdded.getRef()) {
      // The database should always have a lower catalog version than the table because
      // it needs to be created before the table can be added.
      Preconditions.checkState(db.getCatalogVersion() < newTable.getCatalogVersion());
    }
    return newTable.toTCatalogObject();
  }

  /**
   * Adds a new role with the given name and grant groups to the AuthorizationPolicy.
   * If a role with the same name already exists it will be overwritten.
   */
  public Role addRole(String roleName, Set<String> grantGroups) {
    catalogLock_.writeLock().lock();
    try {
      Role role = new Role(roleName, grantGroups);
      role.setCatalogVersion(incrementAndGetCatalogVersion());
      authPolicy_.addRole(role);
      return role;
    } finally {
      catalogLock_.writeLock().unlock();
    }
  }

  /**
   * Removes the role with the given name from the AuthorizationPolicy. Returns the
   * removed role with an incremented catalog version, or null if no role with this name
   * exists.
   */
  public Role removeRole(String roleName) {
    catalogLock_.writeLock().lock();
    try {
      Role role = authPolicy_.removeRole(roleName);
      if (role == null) return null;
      role.setCatalogVersion(incrementAndGetCatalogVersion());
      return role;
    } finally {
      catalogLock_.writeLock().unlock();
    }
  }

  /**
   * Adds a grant group to the given role name and returns the modified Role with
   * an updated catalog version. If the role does not exist a CatalogException is thrown.
   */
  public Role addRoleGrantGroup(String roleName, String groupName)
      throws CatalogException {
    catalogLock_.writeLock().lock();
    try {
      Role role = authPolicy_.addGrantGroup(roleName, groupName);
      Preconditions.checkNotNull(role);
      role.setCatalogVersion(incrementAndGetCatalogVersion());
      return role;
    } finally {
      catalogLock_.writeLock().unlock();
    }
  }

  /**
   * Removes a grant group from the given role name and returns the modified Role with
   * an updated catalog version. If the role does not exist a CatalogException is thrown.
   */
  public Role removeRoleGrantGroup(String roleName, String groupName)
      throws CatalogException {
    catalogLock_.writeLock().lock();
    try {
      Role role = authPolicy_.removeGrantGroup(roleName, groupName);
      Preconditions.checkNotNull(role);
      role.setCatalogVersion(incrementAndGetCatalogVersion());
      return role;
    } finally {
      catalogLock_.writeLock().unlock();
    }
  }

  /**
   * Adds a privilege to the given role name. Returns the new RolePrivilege and
   * increments the catalog version. If the parent role does not exist a CatalogException
   * is thrown.
   */
  public RolePrivilege addRolePrivilege(String roleName, TPrivilege thriftPriv)
      throws CatalogException {
    catalogLock_.writeLock().lock();
    try {
      Role role = authPolicy_.getRole(roleName);
      if (role == null) throw new CatalogException("Role does not exist: " + roleName);
      RolePrivilege priv = RolePrivilege.fromThrift(thriftPriv);
      priv.setCatalogVersion(incrementAndGetCatalogVersion());
      authPolicy_.addPrivilege(priv);
      return priv;
    } finally {
      catalogLock_.writeLock().unlock();
    }
  }

  /**
   * Removes a RolePrivilege from the given role name. Returns the removed
   * RolePrivilege with an incremented catalog version or null if no matching privilege
   * was found. Throws a CatalogException if no role exists with this name.
   */
  public RolePrivilege removeRolePrivilege(String roleName, TPrivilege thriftPriv)
      throws CatalogException {
    catalogLock_.writeLock().lock();
    try {
      Role role = authPolicy_.getRole(roleName);
      if (role == null) throw new CatalogException("Role does not exist: " + roleName);
      RolePrivilege rolePrivilege =
          role.removePrivilege(thriftPriv.getPrivilege_name());
      if (rolePrivilege == null) return null;
      rolePrivilege.setCatalogVersion(incrementAndGetCatalogVersion());
      return rolePrivilege;
    } finally {
      catalogLock_.writeLock().unlock();
    }
  }

  /**
   * Gets a RolePrivilege from the given role name. Returns the privilege if it exists,
   * or null if no privilege matching the privilege spec exist.
   * Throws a CatalogException if the role does not exist.
   */
  public RolePrivilege getRolePrivilege(String roleName, TPrivilege privSpec)
      throws CatalogException {
    catalogLock_.readLock().lock();
    try {
      Role role = authPolicy_.getRole(roleName);
      if (role == null) throw new CatalogException("Role does not exist: " + roleName);
      return role.getPrivilege(privSpec.getPrivilege_name());
    } finally {
      catalogLock_.readLock().unlock();
    }
  }

  /**
   * Increments the current Catalog version and returns the new value.
   */
  public long incrementAndGetCatalogVersion() {
    catalogLock_.writeLock().lock();
    try {
      return ++catalogVersion_;
    } finally {
      catalogLock_.writeLock().unlock();
    }
  }

  /**
   * Returns the current Catalog version.
   */
  public long getCatalogVersion() {
    catalogLock_.readLock().lock();
    try {
      return catalogVersion_;
    } finally {
      catalogLock_.readLock().unlock();
    }
  }

  public ReentrantReadWriteLock getLock() { return catalogLock_; }

  public SentryProxy getSentryProxy() { return sentryProxy_; }
  public AuthorizationPolicy getAuthPolicy() { return authPolicy_; }

  /**
   * Reloads metadata for the partition defined by the partition spec
   * 'partitionSpec' in table 'tbl'. Returns the resulting table's TCatalogObject after
   * the partition metadata was reloaded.
   */
  public TCatalogObject reloadPartition(Table tbl, List<TPartitionKeyValue> partitionSpec)
      throws CatalogException {
    if (!tryLockTable(tbl)) {
      throw new CatalogException(String.format("Error reloading partition of table %s " +
          "due to lock contention", tbl.getFullName()));
    }
    try {
      long newCatalogVersion = incrementAndGetCatalogVersion();
      catalogLock_.writeLock().unlock();
      HdfsTable hdfsTable = (HdfsTable) tbl;
      HdfsPartition hdfsPartition = hdfsTable
          .getPartitionFromThriftPartitionSpec(partitionSpec);
      // Retrieve partition name from existing partition or construct it from
      // the partition spec
      String partitionName = hdfsPartition == null
          ? HdfsTable.constructPartitionName(partitionSpec)
          : hdfsPartition.getPartitionName();
      LOG.info(String.format("Refreshing partition metadata: %s %s",
          hdfsTable.getFullName(), partitionName));
      try (MetaStoreClient msClient = getMetaStoreClient()) {
        org.apache.hadoop.hive.metastore.api.Partition hmsPartition = null;
        try {
          hmsPartition = msClient.getHiveClient().getPartition(
              hdfsTable.getDb().getName(), hdfsTable.getName(), partitionName);
        } catch (NoSuchObjectException e) {
          // If partition does not exist in Hive Metastore, remove it from the
          // catalog
          if (hdfsPartition != null) {
            hdfsTable.dropPartition(partitionSpec);
            hdfsTable.setCatalogVersion(newCatalogVersion);
          }
          return hdfsTable.toTCatalogObject();
        } catch (Exception e) {
          throw new CatalogException("Error loading metadata for partition: "
              + hdfsTable.getFullName() + " " + partitionName, e);
        }
        hdfsTable.reloadPartition(hdfsPartition, hmsPartition);
      }
      hdfsTable.setCatalogVersion(newCatalogVersion);
      LOG.info(String.format("Refreshed partition metadata: %s %s",
          hdfsTable.getFullName(), partitionName));
      return hdfsTable.toTCatalogObject();
    } finally {
      Preconditions.checkState(!catalogLock_.isWriteLockedByCurrentThread());
      tbl.getLock().unlock();
    }
  }
}
