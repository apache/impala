// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.catalog;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.log4j.Logger;

import com.google.common.collect.Maps;
import com.google.common.collect.Lists;

/**
 * Interface to metadata stored in MetaStore instance.
 * Caches all db-, table- and column-related md during construction.
 *
 */
public class Catalog {
  public static final String DEFAULT_DB = "default";
  private static final Logger LOG = Logger.getLogger(Catalog.class);

  private boolean closed = false;
  private int nextTableId;

  // map from db name to DB
  private final Map<String, Db> dbs;

  private final HiveMetaStoreClient msClient;

  public Catalog() {
    this(false);
  }

  /**
   * If lazy is true, tables are loaded on read, otherwise they are loaded eagerly in
   * the constructor.
   */
  public Catalog(boolean lazy) {
    this.nextTableId = 0;
    this.dbs = Maps.newHashMap();
    try {
      this.msClient = new HiveMetaStoreClient(new HiveConf(Catalog.class));
      List<String> msDbs = msClient.getAllDatabases();
      for (String dbName: msDbs) {
        Db db = Db.loadDb(this, msClient, dbName, lazy);
        dbs.put(dbName, db);
      }
    } catch (MetaException e) {
      // turn into unchecked exception
      throw new UnsupportedOperationException(e);
    }
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

  public Collection<Db> getDbs() {
    return dbs.values();
  }

  public TableId getNextTableId() {
    return new TableId(nextTableId++);
  }

  /**
   * Case-insensitive lookup. Null and empty string is mapped onto
   * Hive's default db.
   */
  public Db getDb(String db) {
    if (db == null || db.isEmpty()) {
      return dbs.get(DEFAULT_DB);
    } else {
      return dbs.get(db.toLowerCase());
    }
  }

  public HiveMetaStoreClient getMetaStoreClient() {
    return msClient;
  }

  /**
   * Implement Hive's pattern-matching getTables call. The only metacharacters
   * are '*' which matches any string of characters, and '|' which denotes
   * choice.  Doing the work here saves loading the tables from the metastore
   * (which Hive would do if we passed the call through to the metastore client).
   *
   * To deal with Impala not supporting sessions, we return all tables iff
   * dbName is null. Table names are returned fully-qualified, and we match on
   * the fully-qualified name (and thereby deviate from Hive a bit further).
   */
  public List<String> getTableNames(String dbName, String tablePattern) {
    List<String> matchingTables = Lists.newArrayList();
    List<Db> candidateDbs = Lists.newArrayList();

    if (dbName == null) {
      candidateDbs.addAll(dbs.values());
    } else {
      Db db = getDb(dbName);
      if (db == null) {
        return matchingTables;
      }
      candidateDbs.add(db);
    }
   
    List<String> patterns = Lists.newArrayList();
    // Hive ignores pretty much all metacharacters, so we have to escape them.
    final String metaCharacters = "+?.^()]\\/{}";
    final Pattern regex = Pattern.compile("([" + Pattern.quote(metaCharacters) + "])");
    if (tablePattern != null) {
      for (String pattern: Arrays.asList(tablePattern.split("\\|"))) {
        Matcher matcher = regex.matcher(pattern);
        pattern = matcher.replaceAll("\\\\$1").replace("*", ".*");
        patterns.add(pattern);
      }
    }

    for (Db db: candidateDbs) {
      for (String table: db.getAllTableNames()) {
        String qualifiedTableName = db.getName() + "." + table;
        if (tablePattern == null) {          
          matchingTables.add(qualifiedTableName);
        } else {
          for (String pattern: patterns) {
            // Empty string matches nothing in Hive's implementation
            if (!pattern.isEmpty() && qualifiedTableName.matches(pattern)) {
              matchingTables.add(qualifiedTableName);
            }
          }
        }
      }
    }

    Collections.sort(matchingTables);
    return matchingTables;
  }

  /**
   * Marks a table metadata as invalid, to be reloaded
   * the next time it is read.
   */
  public void invalidateTable(String tableName) throws TableNotFoundException {
    invalidateTable(DEFAULT_DB, tableName);
  }

  public static class TableNotFoundException extends Exception {
    private static final long serialVersionUID = -2203080667446640542L;

    public TableNotFoundException(String s) { super(s); }
  }

  public void invalidateTable(String dbName, String tableName)
      throws TableNotFoundException {
    Db db = getDb(dbName);
    // If no db known by that name, silently do nothing.
    if (db == null) {
      return;
    }

    db.invalidateTable(tableName);
  }
}
