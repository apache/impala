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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.log4j.Logger;

import com.cloudera.impala.common.ImpalaException;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComputationException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MapMaker;
import com.google.common.base.Function;

/**
 * Interface to metadata stored in MetaStore instance.
 * Caches all db-, table- and column-related md during construction.
 *
 */
public class Catalog {
  public static final String DEFAULT_DB = "default";
  private static final Logger LOG = Logger.getLogger(Catalog.class);
  private static final int MAX_METASTORE_CLIENT_INIT_RETRIES = 5;
  private static final int MAX_METASTORE_RETRY_INTERVAL_IN_SECONDS = 5;
  private final boolean lazy;
  private boolean closed = false;
  private int nextTableId;

  // map from db name to DB
  private final LazyDbMap dbs;

  private final HiveMetaStoreClient msClient;

  // Tracks whether a Table/Db has all of its metadata loaded.
  enum MetadataLoadState {
    LOADED,
    UNINITIALIZED,
  }

  /**
   * Lazily loads database metadata on read (through 'get') and tracks the valid/known
   * database names.
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
    // Map of database name to Db metadata
    private final ConcurrentMap<String, Db> dbMetadataMap = new MapMaker()
        .makeComputingMap(
        new Function<String, Db>() {
          public Db apply(String dbName) {
            return loadDb(dbName.toLowerCase());
          }
        });

    // Map of database names to their metadata load state. It is only possible to load
    // metadata for databases that exist in this map.
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
     * - If the database already exists in the metadata map, its value will be returned.
     * - If the database is not present in the metadata map AND the database exists in the
     *   known database map the metadata will be loaded
     * - If the database is not present the database name map, null is returned.
     */
    public Db get(String dbName) {
      try {
        return dbMetadataMap.get(dbName);
      } catch (ComputationException e) {
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
        throw e;
      }
    }

    private Db loadDb(String dbName) {
      try {
        MetadataLoadState metadataState = dbNameMap.get(dbName);

        // This database doesn't exist in the database name cache. Throw an exception.
        if (metadataState == null) {
          throw new DatabaseNotFoundException("Database not found: " + dbName);
        }

        // We should never have a case where we make it here and the metadata is marked
        // as already loaded.
        Preconditions.checkState(metadataState != MetadataLoadState.LOADED);

        Db db = Db.loadDb(Catalog.this, msClient, dbName, lazy);
        dbNameMap.put(dbName, MetadataLoadState.LOADED);
        return db;
      } catch (DatabaseNotFoundException e) {
        // Convert to an unchecked exception
        throw new ComputationException(e);
      }
    }
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
    this(true);
  }

  /**
   * If lazy is true, tables are loaded on read, otherwise they are loaded eagerly in
   * the constructor.
   */
  public Catalog(boolean lazy) {
    this.nextTableId = 0;
    this.lazy = lazy;
    try {
      this.msClient = createHiveMetaStoreClient(new HiveConf(Catalog.class));
      this.dbs = new LazyDbMap(msClient.getAllDatabases());
      if (!lazy) {
        // Load all the metadata
        for (String dbName: dbs.getAllDbNames()) {
          dbs.get(dbName);
        }
      }
    } catch (MetaException e) {
      // turn into unchecked exception
      throw new UnsupportedOperationException(e);
    }
  }

  /**
   * Creates a HiveMetaStoreClient with the given configuration, retrying the operation
   * if MetaStore exceptions occur. A random sleep is injected between retries to help
   * reduce the likelihood of flooding the Meta Store with many requests at once.
   */
  static HiveMetaStoreClient createHiveMetaStoreClient(HiveConf conf)
      throws MetaException {
    // Ensure numbers are random across nodes.
    Random randomGen = new Random(UUID.randomUUID().hashCode());
    int maxRetries = MAX_METASTORE_CLIENT_INIT_RETRIES;
    for (int retryAttempt = 0; retryAttempt <= maxRetries; ++retryAttempt) {
      try {
        return new HiveMetaStoreClient(conf);
      } catch (MetaException e) {
        LOG.error("Error initializing Hive Meta Store client", e);
        if (retryAttempt == maxRetries) {
          throw e;
        }

        // Randomize the retry interval so the meta store isn't flooded with attempts.
        int retryInterval =
          randomGen.nextInt(MAX_METASTORE_RETRY_INTERVAL_IN_SECONDS) + 1;
        LOG.info(String.format("On retry attempt %d of %d. Sleeping %d seconds.",
            retryAttempt + 1, maxRetries, retryInterval));
        try {
          Thread.sleep(retryInterval * 1000);
        } catch (InterruptedException ie) {
          // Do nothing
        }
      }
    }
    // Should never make it to here. 
    throw new UnsupportedOperationException(
        "Unexpected error creating Hive Meta Store client");
  }

  /**
   * Releases the Hive Metastore Client resources. This method can be called
   * multiple times. Additional calls will be no-ops.
   */
  public void close() {
    if (this.msClient != null && !closed) {
      this.msClient.close();
      closed = true;
    }
  }

  public TableId getNextTableId() {
    return new TableId(nextTableId++);
  }

  /**
   * Case-insensitive lookup. Returns null if the database does not exist.
   */
  public Db getDb(String db) {
    Preconditions.checkState(db != null && !db.isEmpty(),
        "Null or empty database name given as argument to Catalog.getDb");
    return dbs.get(db);
  }

  public HiveMetaStoreClient getMetaStoreClient() {
    return msClient;
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
}
