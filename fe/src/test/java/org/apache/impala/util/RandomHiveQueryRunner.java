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

import com.google.common.base.Preconditions;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.impala.compat.MetastoreShim;
import org.apache.impala.testutil.HiveJdbcClientPool;
import org.apache.impala.testutil.HiveJdbcClientPool.HiveJdbcClient;
import org.apache.impala.testutil.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class can be used to run a lot of hive queries in parallel to generate stress on
 * Catalog. Its uses a simple Hive JDBC pool to submit queries in parallel which is much
 * faster than the python end-to-end tests which submit each hive query over a new
 * connection from Beeline shell. Each Hive query which is executed is idempotent. It
 * creates the necessary dependent metadata before actually executing. Currently it
 * supports metadata modification hive queries which include create/alter/drop of
 * database/table/partition and dynamic partition insert with overwrite
 */
public class RandomHiveQueryRunner {

  private final Random random_;
  private final String tblNamePrefix_;
  private final String dbNamePrefix_;
  // number of hive clients to concurrently execute random hive queries
  private final int numClients_;
  // number of queries per hive client
  private final int numQueriesPerClient_;
  // used to generate unique partition names for each client, also used to get
  // partition names to be dropped in case of drop partition queries
  private final AtomicInteger[] partitionIdGenerators_;
  private final ExecutorService executorService_;
  // Hive JDBC connection pool
  private final HiveJdbcClientPool hiveJdbcClientPool_;
  // Futures to keep track if any query errored out or not
  private ArrayList<Future<Void>> futures_;
  private final AtomicBoolean isStarted_ = new AtomicBoolean(false);
  private static final Logger LOG =
      LoggerFactory.getLogger(RandomHiveQueryRunner.class);
  // total weight of all the queryType defined. Used for calculating the bound for the
  // random number generator for determining the next random query type
  private int totalQueryWeight_;
  // maps a given number to a queryType using the weight distribution of the queryTypes
  private final RangeMap<Integer, QueryType> rangeMap = TreeRangeMap.create();

  private final List<QueryType> skippedQueryTypes;

  /**
   * Query type with weight. The weight of a QueryType determines the probability of its
   * occurrence by the Random Query runner. Higher the weight, more the probability of its
   * occurrence. This is needed to simulate the behavior in practice where not all the
   * query-types are equally likely to occur. Generally, partition level metadata updates
   * are most common, then table level and then database level. The weights used below are
   * not based off any real-world data but follow reasonable values based on what a
   * typical ETL workload looks like.
   */
  enum QueryType {
    CREATE_DB(5),
    DROP_DB(5),
    CREATE_TABLE(10),
    CREATE_TABLE_AS_SELECT(10),
    DROP_TABLE(10),
    ALTER_TABLE_ADD_COL(5),
    ALTER_TABLE_ADD_PROPERTY(15),
    ADD_PARTITION(25),
    DROP_PARTITION(25),
    DYN_PARTITION_INSERT(30),
    DYN_PARTITION_INSERT_OVERWRITE(20),
    INSERT_PARTITION(20),
    INSERT_OVERWRITE_PARTITION(15),
    INSERT_TABLE(25),
    INSERT_OVERWRITE_TABLE(20),
    // set config is not interesting since it does not change any metadata and it is
    // not randomly executed anyways. keeping the weight to 0
    SET_CONFIG(0);

    private final int weight_;

    QueryType(int weight) {
      Preconditions.checkArgument(weight >= 0);
      this.weight_ = weight;
    }
  }

  /**
   * Base class for all the TestHiveQueries. Each test hive query may have one or more
   * dependent queries needed for setting it up. For example, add partition query
   * depends on the table and the database to exist in order to succeed. Each query runs
   * the dependent queries before running the actual query
   *
   * All the TestHiveQueries must be idempotent to make sure there are no issues when
   * running them in a random order.
   */
  private static abstract class TestHiveQuery {

    private List<TestHiveQuery> dependentQueries_ = new ArrayList<>();

    protected abstract String getQuery();

    protected final QueryType queryType_;

    TestHiveQuery(QueryType queryType) {
      this.queryType_ = queryType;
    }

    private void runInternal(HiveJdbcClient client) throws SQLException {
      for (TestHiveQuery dependentQuery : dependentQueries_) {
        dependentQuery.runInternal(client);
      }
      client.executeSql(getQuery());
    }

    /**
     * Executes this query by using one of the Hive client from the given pool. All the
     * dependent queries along with this query will use the same hive client.
     */
    public void run(HiveJdbcClientPool pool) throws Exception {
      try (HiveJdbcClient hiveJdbcClient = pool.getClient()) {
        runInternal(hiveJdbcClient);
      } catch (Exception e) {
        LOG.error("Unexpected error received while running the hive query", e);
        throw e;
      }
    }

    /**
     * Adds to the list of dependent queries it is it not already present in it
     */
    protected void addDependentQuery(TestHiveQuery dependentQuery) {
      if (!dependentQueries_.contains(dependentQuery)) {
        dependentQueries_.add(dependentQuery);
      }
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("\n" + getQuery());
      for (int i = 0; i < dependentQueries_.size(); i++) {
        sb.append("\n   " + dependentQueries_.get(i).getQuery());
      }
      return sb.toString();
    }
  }

  /**
   * Create database [if not exists] query
   */
  private static class CreateDbQuery extends TestHiveQuery {

    private final String dbName_;
    private final boolean ifNotExists_;

    private CreateDbQuery(String dbName, boolean ifNotExists) {
      super(QueryType.CREATE_DB);
      dbName_ = Preconditions.checkNotNull(dbName);
      ifNotExists_ = ifNotExists;
    }

    static CreateDbQuery create(String dbName_) {
      CreateDbQuery query = new CreateDbQuery(dbName_, false);
      // if the user did not provide if not exists clause, the dependent query must
      // drop an existing db with the same name to make sure that this query succeeds
      query.addDependentQuery(new DropDbQuery(dbName_, true, true));
      return query;
    }

    @Override
    public String getQuery() {
      StringBuilder sb = new StringBuilder("create database ");
      if (ifNotExists_) { sb.append("if not exists "); }
      sb.append(dbName_);
      return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) { return true; }
      if (o == null || getClass() != o.getClass()) { return false; }
      CreateDbQuery that = (CreateDbQuery) o;
      return ifNotExists_ == that.ifNotExists_ &&
          Objects.equals(dbName_, that.dbName_);
    }

    @Override
    public int hashCode() {
      return Objects.hash(dbName_, ifNotExists_);
    }
  }

  /**
   * Drop database [if exists] [cascade] hive query
   */
  private static class DropDbQuery extends TestHiveQuery {

    private final String dbName_;
    private final boolean ifExists_;
    private final boolean cascade_;

    private DropDbQuery(String dbName, boolean ifExists, boolean cascade) {
      super(QueryType.DROP_DB);
      this.dbName_ = Preconditions.checkNotNull(dbName);
      this.ifExists_ = ifExists;
      this.cascade_ = cascade;
    }

    static DropDbQuery create(String dbName) {
      DropDbQuery query = new DropDbQuery(dbName, false, true);
      // add a dependent query to create the db if it does not
      // exist already to make sure that this query is successful
      query.addDependentQuery(new CreateDbQuery(dbName, true));
      return query;
    }

    @Override
    public String getQuery() {
      StringBuilder sb = new StringBuilder("drop database ");
      if (ifExists_) { sb.append("if exists "); }
      sb.append(dbName_);
      if (cascade_) { sb.append(" cascade"); }
      return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) { return true; }
      if (o == null || getClass() != o.getClass()) { return false; }
      DropDbQuery that = (DropDbQuery) o;
      return ifExists_ == that.ifExists_ &&
          cascade_ == that.cascade_ &&
          Objects.equals(dbName_, that.dbName_);
    }

    @Override
    public int hashCode() {
      return Objects.hash(dbName_, ifExists_, cascade_);
    }
  }

  /**
   * Creates a non-partitioned table using the following pattern using Hive Create table
   * <tablename> [if not exists]
   */
  private static class CreateTblQuery extends TestHiveQuery {

    private final String tblName_;
    private final String dbName_;
    private final String tblSpec_;
    private final boolean ifNotExists_;

    private CreateTblQuery(String dbName, String tblName, String tblSpec,
        boolean ifNotExists) {
      super(QueryType.CREATE_TABLE);
      this.dbName_ = Preconditions.checkNotNull(dbName);
      this.tblName_ = Preconditions.checkNotNull(tblName);
      this.tblSpec_ = Preconditions.checkNotNull(tblSpec);
      this.ifNotExists_ = ifNotExists;
    }

    static CreateTblQuery create(String dbName, String tblName, String tblSpec) {
      CreateTblQuery query = new CreateTblQuery(dbName, tblName, tblSpec, false);
      // in order for this query to succeed, the database must exists
      query.addDependentQuery(new CreateDbQuery(dbName, true));
      // if not exists clause is not provided, add a optional drop tbl if exists to
      // make sure that this query succeeds
      query.addDependentQuery(new DropTblQuery(dbName, tblName, true));
      return query;
    }

    @Override
    protected String getQuery() {
      StringBuilder sb = new StringBuilder("create table ");
      if (ifNotExists_) { sb.append("if not exists "); }
      sb.append(String.format("%s.%s ", dbName_, tblName_));
      sb.append("(" + tblSpec_ + ")");
      return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) { return true; }
      if (o == null || getClass() != o.getClass()) { return false; }
      CreateTblQuery that = (CreateTblQuery) o;
      return ifNotExists_ == that.ifNotExists_ &&
          tblName_.equals(that.tblName_) &&
          dbName_.equals(that.dbName_) &&
          tblSpec_.equals(that.tblSpec_);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tblName_, dbName_, tblSpec_, ifNotExists_);
    }
  }

  /**
   * Same as CreateTblQuery but creates a partitioned table instead. Random query running
   * provides a built-in way to generate valid partition names of the pattern
   * (part=<int>). If this built-in pattern is not used, callers must make sure that the
   * partitionSpec is valid for subsequent queries on this table
   */
  private static class CreatePartitionedTblQuery extends CreateTblQuery {

    private final String partitionSpec_;

    private CreatePartitionedTblQuery(String dbName, String tblName, String tblSpec,
        String partitionSpec, boolean ifNotExists) {
      super(dbName, tblName, tblSpec, ifNotExists);
      this.partitionSpec_ = partitionSpec;
    }

    static CreatePartitionedTblQuery create(String dbName, String tblName,
        String tblSpec, String partitionSpec) {
      CreatePartitionedTblQuery query = new CreatePartitionedTblQuery(dbName, tblName,
          tblSpec, partitionSpec, false);
      // in order for this query to succeed, the database must exist
      query.addDependentQuery(new CreateDbQuery(dbName, true));
      // if not exists clause is not provided, add a optional drop tbl if exists to
      // make sure that this query succeeds
      query.addDependentQuery(new DropTblQuery(dbName, tblName, true));
      return query;
    }

    @Override
    protected String getQuery() {
      return super.getQuery() + " partitioned by (" + partitionSpec_ + ")";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) { return true; }
      if (o == null || getClass() != o.getClass()) { return false; }
      if (!super.equals(o)) { return false; }
      CreatePartitionedTblQuery that = (CreatePartitionedTblQuery) o;
      return Objects.equals(partitionSpec_, that.partitionSpec_);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), partitionSpec_);
    }
  }

  private static class CreateTblAsSelect extends TestHiveQuery {

    private final String tblName_;
    private final String dbName_;
    private final String srcDbName_;
    private final String srcTblName_;
    private final boolean ifNotExists_;

    private CreateTblAsSelect(String dbName, String tblName,
        String srcDbName, String srcTblName,
        boolean ifNotExists) {
      super(QueryType.CREATE_TABLE_AS_SELECT);
      this.dbName_ = Preconditions.checkNotNull(dbName);
      this.tblName_ = Preconditions.checkNotNull(tblName);
      this.srcDbName_ = Preconditions.checkNotNull(srcDbName);
      this.srcTblName_ = Preconditions.checkNotNull(srcTblName);
      this.ifNotExists_ = ifNotExists;
    }

    static CreateTblAsSelect create(String dbName, String tblName, String srcDbName,
        String srcTblName) {
      CreateTblAsSelect query = new CreateTblAsSelect(dbName, tblName, srcDbName,
          srcTblName, false);
      // in order for this query to succeed, the database must exists
      query.addDependentQuery(new CreateDbQuery(dbName, true));
      // add a optional drop tbl if exists to
      // make sure that this query succeeds
      query.addDependentQuery(new DropTblQuery(dbName, tblName, true));
      return query;
    }

    @Override
    protected String getQuery() {
      StringBuilder sb = new StringBuilder("create table ");
      if (ifNotExists_) { sb.append("if not exists "); }
      sb.append(String.format("%s.%s ", dbName_, tblName_));
      sb.append(" like ");
      sb.append(String.format(" %s.%s ", srcDbName_, srcTblName_));
      return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) { return true; }
      if (o == null || getClass() != o.getClass()) { return false; }
      CreateTblAsSelect that = (CreateTblAsSelect) o;
      return ifNotExists_ == that.ifNotExists_ &&
          tblName_.equals(that.tblName_) &&
          dbName_.equals(that.dbName_) &&
          srcDbName_.equals(that.srcDbName_) &&
          srcTblName_.equals(that.srcTblName_);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tblName_, dbName_, srcDbName_, srcTblName_, ifNotExists_);
    }
  }

  /**
   * Drop table [if exists]
   */
  private static class DropTblQuery extends TestHiveQuery {

    private final String tblName_;
    private final String dbName_;
    private static final String dummyTblSpecForDepQueries_ = "c1 int";
    private final boolean ifExists_;

    private DropTblQuery(String dbName, String tblName, boolean ifExists) {
      super(QueryType.DROP_TABLE);
      this.dbName_ = Preconditions.checkNotNull(dbName);
      this.tblName_ = Preconditions.checkNotNull(tblName);
      this.ifExists_ = ifExists;
    }

    static DropTblQuery create(String dbName, String tblName) {
      DropTblQuery query = new DropTblQuery(dbName, tblName, false);
      query.addDependentQuery(new CreateDbQuery(dbName, true));
      query.addDependentQuery(new CreateTblQuery(dbName, tblName,
          dummyTblSpecForDepQueries_, true));
      return query;
    }

    @Override
    protected String getQuery() {
      StringBuilder sb = new StringBuilder("drop table ");
      if (ifExists_) { sb.append("if exists "); }
      sb.append(String.format("%s.%s ", dbName_, tblName_));
      return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) { return true; }
      if (o == null || getClass() != o.getClass()) { return false; }
      DropTblQuery that = (DropTblQuery) o;
      return ifExists_ == that.ifExists_ &&
          tblName_.equals(that.tblName_) &&
          dbName_.equals(that.dbName_) &&
          dummyTblSpecForDepQueries_.equals(that.dummyTblSpecForDepQueries_);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tblName_, dbName_, dummyTblSpecForDepQueries_, ifExists_);
    }
  }

  /**
   * Base class for all the alter table queries
   */
  private static abstract class AlterTblQuery extends TestHiveQuery {

    protected final String tblName_;
    protected final String dbName_;
    protected final String partitionSpec_;

    private AlterTblQuery(String dbName, String tblName, QueryType queryType) {
      super(queryType);
      this.dbName_ = Preconditions.checkNotNull(dbName);
      this.tblName_ = Preconditions.checkNotNull(tblName);
      this.partitionSpec_ = null;
      addDependentQuery(new CreateDbQuery(dbName_, true));
      addDependentQuery(new CreateTblQuery(dbName_, tblName_,
          getRandomColName(6) +
              " string", true));
    }

    private AlterTblQuery(String dbName, String tblName, String partitionSpec,
        QueryType queryType) {
      super(queryType);
      this.dbName_ = Preconditions.checkNotNull(dbName);
      this.tblName_ = Preconditions.checkNotNull(tblName);
      this.partitionSpec_ = partitionSpec;
    }

    static String getPartitionColFromSpec(String partitionSpec) {
      Preconditions.checkNotNull(partitionSpec);
      if (partitionSpec.contains("=")) {
        // we assume for simplicity that for this test purpose all partitioned tables
        // have only one partition key
        String[] parts = partitionSpec.split("=");
        return parts[0].trim() + " int";
      }
      return partitionSpec;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) { return true; }
      if (!(o instanceof AlterTblQuery)) { return false; }
      AlterTblQuery that = (AlterTblQuery) o;
      return tblName_.equals(that.tblName_) &&
          dbName_.equals(that.dbName_) &&
          Objects.equals(partitionSpec_, that.partitionSpec_);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tblName_, dbName_, partitionSpec_);
    }
  }

  /**
   * Alter table add col
   */
  private static class AlterTblAddColQuery extends AlterTblQuery {

    private final String colSpec_;

    private AlterTblAddColQuery(String dbName, String tblName, String colSpec) {
      super(dbName, tblName, QueryType.ALTER_TABLE_ADD_COL);
      this.colSpec_ = Preconditions.checkNotNull(colSpec);
    }

    static AlterTblAddColQuery create(String dbName, String tblName, String colSpec) {
      AlterTblAddColQuery query = new AlterTblAddColQuery(dbName, tblName, colSpec);
      query.addDependentQuery(new CreateDbQuery(dbName, true));
      query.addDependentQuery(new CreateTblQuery(dbName, tblName,
          getRandomColName(6) +
              " string", true));
      return query;
    }

    @Override
    protected String getQuery() {
      return String.format("alter table %s.%s add columns (%s)", dbName_, tblName_,
          colSpec_);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) { return true; }
      if (o == null || getClass() != o.getClass()) { return false; }
      if (!super.equals(o)) { return false; }
      AlterTblAddColQuery that = (AlterTblAddColQuery) o;
      return Objects.equals(colSpec_, that.colSpec_);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), colSpec_);
    }
  }

  private static class AlterTblAddProperty extends AlterTblQuery {

    private final String key_;
    private final String value_;

    private AlterTblAddProperty(String dbName, String tblName, String key, String val) {
      super(dbName, tblName, QueryType.ALTER_TABLE_ADD_PROPERTY);
      this.key_ = Preconditions.checkNotNull(key);
      this.value_ = Preconditions.checkNotNull(val);
    }

    static AlterTblAddProperty create(String dbName, String tblName, String key,
        String val) {
      AlterTblAddProperty query = new AlterTblAddProperty(dbName, tblName, key, val);
      query.addDependentQuery(new CreateDbQuery(dbName, true));
      query.addDependentQuery(new CreateTblQuery(dbName, tblName,
          getRandomColName(6) +
              " string", true));
      return query;
    }

    @Override
    protected String getQuery() {
      return String.format("alter table %s.%s set tblproperties ('%s'='%s')", dbName_,
          tblName_, key_, value_);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) { return true; }
      if (o == null || getClass() != o.getClass()) { return false; }
      if (!super.equals(o)) { return false; }
      AlterTblAddProperty that = (AlterTblAddProperty) o;
      return key_.equals(that.key_) &&
          value_.equals(that.value_);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), key_, value_);
    }
  }

  private static class AlterTableAddPartition extends AlterTblQuery {

    private final boolean ifNotExists_;

    private AlterTableAddPartition(String dbName, String tblName, String partitionSpec,
        boolean ifNotExists) {
      super(dbName, tblName, partitionSpec, QueryType.ADD_PARTITION);
      ifNotExists_ = ifNotExists;
    }

    static AlterTableAddPartition create(String dbName, String tblName,
        String partitionSpec) {
      Preconditions.checkNotNull(partitionSpec);
      AlterTableAddPartition alterTableAddPartition = new AlterTableAddPartition(dbName
          , tblName, partitionSpec, false);
      alterTableAddPartition.addDependentQuery(new CreateDbQuery(dbName, true));
      alterTableAddPartition.addDependentQuery(new CreatePartitionedTblQuery(dbName,
          tblName, getRandomColName(6) + " string",
          getPartitionColFromSpec(partitionSpec), true));
      alterTableAddPartition.addDependentQuery(new AlterTableDropPartition(dbName,
          tblName, partitionSpec, true));
      return alterTableAddPartition;
    }

    @Override
    protected String getQuery() {
      StringBuilder sb = new StringBuilder();
      sb.append(String.format("alter table %s.%s add ", dbName_, tblName_));
      if (ifNotExists_) { sb.append("if not exists "); }
      sb.append(String.format(" partition (%s)", partitionSpec_));
      return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) { return true; }
      if (o == null || getClass() != o.getClass()) { return false; }
      if (!super.equals(o)) { return false; }
      AlterTableAddPartition that = (AlterTableAddPartition) o;
      return ifNotExists_ == that.ifNotExists_;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), ifNotExists_);
    }
  }

  private static class AlterTableDropPartition extends AlterTblQuery {

    private final boolean ifExists_;

    private AlterTableDropPartition(String dbName, String tblName, String partitionSpec
        , boolean ifExists) {
      super(dbName, tblName, partitionSpec, QueryType.DROP_PARTITION);
      ifExists_ = ifExists;
    }

    static AlterTableDropPartition create(String dbName, String tblName,
        String partitionSpec) {
      Preconditions.checkNotNull(partitionSpec);
      AlterTableDropPartition query = new AlterTableDropPartition(dbName, tblName,
          partitionSpec, false);
      query.addDependentQuery(new CreateDbQuery(dbName, true));
      query.addDependentQuery(new CreatePartitionedTblQuery(dbName,
          tblName, getRandomColName(6) + " string",
          getPartitionColFromSpec(partitionSpec), true));
      query.addDependentQuery(new AlterTableAddPartition(dbName, tblName,
          partitionSpec, true));
      return query;
    }

    @Override
    protected String getQuery() {
      StringBuilder sb = new StringBuilder(String.format("alter table %s.%s drop ",
          dbName_, tblName_));
      if (ifExists_) { sb.append(" if exists "); }
      sb.append(String.format(" partition (%s)", partitionSpec_));
      return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) { return true; }
      if (o == null || getClass() != o.getClass()) { return false; }
      if (!super.equals(o)) { return false; }
      AlterTableDropPartition that = (AlterTableDropPartition) o;
      return ifExists_ == that.ifExists_;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), ifExists_);
    }
  }


  /**
   * Hive's set <key> = <value> statement needed as a precondition for some other queries
   * like dynamic partition inserts
   */
  private static class SetConfigStmt extends TestHiveQuery {

    private final String key_;
    private final String val_;

    private SetConfigStmt(String key, String value) {
      super(QueryType.SET_CONFIG);
      this.key_ = Preconditions.checkNotNull(key);
      this.val_ = Preconditions.checkNotNull(value);
    }

    @Override
    protected String getQuery() {
      return String.format("set %s = %s", key_, val_);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) { return true; }
      if (o == null || getClass() != o.getClass()) { return false; }
      SetConfigStmt that = (SetConfigStmt) o;
      return key_.equals(that.key_) &&
          val_.equals(that.val_);
    }

    @Override
    public int hashCode() {
      return Objects.hash(key_, val_);
    }
  }

  private abstract static class SourceTableBasedQuery extends TestHiveQuery {

    protected final String tblName_;
    protected final String dbName_;
    protected final String srcDbName_;
    protected final String srcTblName_;

    private SourceTableBasedQuery(String dbName, String tblName, String srcDbName,
        String srcTblName, QueryType queryType) {
      super(queryType);
      this.dbName_ = Preconditions.checkNotNull(dbName);
      this.tblName_ = Preconditions.checkNotNull(tblName);
      this.srcDbName_ = Preconditions.checkNotNull(srcDbName);
      this.srcTblName_ = Preconditions.checkNotNull(srcTblName);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) { return true; }
      if (o == null || getClass() != o.getClass()) { return false; }
      SourceTableBasedQuery that = (SourceTableBasedQuery) o;
      return tblName_.equals(that.tblName_) &&
          dbName_.equals(that.dbName_) &&
          srcDbName_.equals(that.srcDbName_) &&
          srcTblName_.equals(that.srcTblName_);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tblName_, dbName_, srcDbName_, srcTblName_);
    }
  }

  private static class InsertTblOrPartition extends SourceTableBasedQuery {

    private final boolean overwrite_;
    private final String partitionSpec_;
    private final int limit_;

    private InsertTblOrPartition(String dbName, String tblName, String srcDbName,
        String srcTblName, boolean overwrite, String partitionSpec, int limit) {
      super(dbName, tblName, srcDbName, srcTblName,
          getQueryType(overwrite, partitionSpec));
      this.overwrite_ = overwrite;
      // if partitionSpec is null its is insert into table query
      this.partitionSpec_ = partitionSpec;
      Preconditions.checkArgument(limit > 0);
      this.limit_ = limit;
    }

    private static QueryType getQueryType(boolean overwrite, String partitionSpec) {
      if (partitionSpec != null) {
        return overwrite ? QueryType.INSERT_OVERWRITE_PARTITION :
            QueryType.INSERT_PARTITION;
      } else {
        return overwrite ? QueryType.INSERT_OVERWRITE_TABLE :
            QueryType.INSERT_TABLE;
      }
    }

    static InsertTblOrPartition create(String dbName, String tblName, String srcDbName,
        String srcTblName, boolean overwrite, String partitionSpec, int limit) {
      InsertTblOrPartition query = new InsertTblOrPartition(dbName, tblName, srcDbName,
          srcTblName, overwrite, partitionSpec, limit);
      // in order for this query to succeed, the database must exists
      query.addDependentQuery(new CreateDbQuery(dbName, true));
      // add a optional drop tbl if exists to
      // make sure that this query succeeds
      query.addDependentQuery(new DropTblQuery(dbName, tblName, true));
      // makes sure that the table exists before you run the insert
      query.addDependentQuery(new CreateTblAsSelect(dbName, tblName, srcDbName,
          srcTblName, true));
      query.addDependentQuery(new SetConfigStmt("hive.exec.dynamic.partition.mode",
          "nonstrict"));
      query.addDependentQuery(new SetConfigStmt("hive.exec.max.dynamic.partitions",
          "10000"));
      query.addDependentQuery(new SetConfigStmt("hive.exec.max.dynamic.partitions"
          + ".pernode", "10000"));
      // in CDP builds where tez is the default execution engine on hive, running
      // many hive queries in parallel is slow because application master is not
      // released until the session is closed. This timeout value will close the
      // tez application if no new query is submitted even if the session is not
      // closed, thereby releasing resources faster
      if (MetastoreShim.getMajorVersion() >= 3) {
        query.addDependentQuery(
            new SetConfigStmt("tez.session.am.dag.submit.timeout.secs", "2"));
      }
      return query;
    }

    @Override
    protected String getQuery() {
      StringBuilder sb = new StringBuilder("insert ");
      if (overwrite_) {
        sb.append("overwrite table ");
      } else {
        sb.append("into table ");
      }
      sb.append(String.format("%s.%s ", dbName_, tblName_));
      if (partitionSpec_ != null) {
        sb.append(String.format("partition (%s) ", partitionSpec_));
      }
      sb.append(String.format("select * from %s.%s limit %s", srcDbName_, srcTblName_,
          limit_));
      return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) { return true; }
      if (o == null || getClass() != o.getClass()) { return false; }
      if (!super.equals(o)) { return false; }
      InsertTblOrPartition that = (InsertTblOrPartition) o;
      return overwrite_ == that.overwrite_ &&
          limit_ == that.limit_ &&
          Objects.equals(partitionSpec_, that.partitionSpec_);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), overwrite_, partitionSpec_, limit_);
    }
  }

  /**
   * Generates a insert [overwrite] table select * from source_table to dynamically add
   * the partitions based on the number of partitions from the source table
   */
  static class DynamicPartitionInsert extends SourceTableBasedQuery {

    private final String dyPartitionSpec_;
    private final boolean overwrite_;

    private DynamicPartitionInsert(String dbName, String tblName,
        String srcDbName, String srcTblName, String dyPartitionSpec, boolean overwrite) {
      super(dbName, tblName, srcDbName, srcTblName, overwrite ?
          QueryType.DYN_PARTITION_INSERT_OVERWRITE : QueryType.DYN_PARTITION_INSERT);
      this.dyPartitionSpec_ = Preconditions.checkNotNull(dyPartitionSpec);
      this.overwrite_ = overwrite;
    }

    static DynamicPartitionInsert create(String dbName, String tblName,
        String srcDbName, String srcTblName, String dyPartitionSpec, boolean overwrite) {
      DynamicPartitionInsert query = new DynamicPartitionInsert(dbName, tblName,
          srcDbName, srcTblName, dyPartitionSpec, overwrite);
      // in order for this query to succeed, the database must exists
      query.addDependentQuery(new CreateDbQuery(dbName, true));
      // add a optional drop tbl if exists to
      // make sure that this query succeeds
      query.addDependentQuery(new DropTblQuery(dbName, tblName, true));
      // makes sure that the table exists before you run the insert
      query.addDependentQuery(new CreateTblAsSelect(dbName, tblName, srcDbName,
          srcTblName, true));
      query.addDependentQuery(new SetConfigStmt("hive.exec.dynamic.partition.mode",
          "nonstrict"));
      query.addDependentQuery(new SetConfigStmt("hive.exec.max.dynamic.partitions",
          "10000"));
      query.addDependentQuery(new SetConfigStmt("hive.exec.max.dynamic.partitions"
          + ".pernode", "10000"));
      // in CDP builds where tez is the default execution engine on hive, running
      // many hive queries in parallel is slow because application master is not
      // released until the session is closed. This timeout value will close the
      // tez application if no new query is submitted even if the session is not
      // closed, thereby releasing resources faster
      if (MetastoreShim.getMajorVersion() >= 3) {
        query.addDependentQuery(
            new SetConfigStmt("tez.session.am.dag.submit.timeout.secs", "2"));
      }
      return query;
    }

    @Override
    protected String getQuery() {
      StringBuilder sb = new StringBuilder("insert");
      if (overwrite_) {
        sb.append(" overwrite table ");
      } else {
        sb.append(" into table ");
      }
      sb.append(String.format("%s.%s partition (%s)", dbName_, tblName_,
          dyPartitionSpec_));
      sb.append(" select * from ");
      sb.append(String.format("%s.%s", srcDbName_, srcTblName_));
      return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) { return true; }
      if (o == null || getClass() != o.getClass()) { return false; }
      if (!super.equals(o)) { return false; }
      DynamicPartitionInsert that = (DynamicPartitionInsert) o;
      return overwrite_ == that.overwrite_ &&
          Objects.equals(dyPartitionSpec_, that.dyPartitionSpec_);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), dyPartitionSpec_, overwrite_);
    }
  }

  public RandomHiveQueryRunner(Random random, String dbNamePrefix,
      String tblNamePrefix, int numClients, int numQueriesPerClient,
      List<QueryType> skippedQueryTypes)
      throws SQLException, ClassNotFoundException {
    this.random_ = Preconditions.checkNotNull(random);
    this.numClients_ = numClients;
    this.numQueriesPerClient_ = numQueriesPerClient;
    this.partitionIdGenerators_ = new AtomicInteger[numClients];
    for (int i = 0; i < numClients; i++) {
      partitionIdGenerators_[i] = new AtomicInteger(0);
    }
    this.dbNamePrefix_ = Preconditions.checkNotNull(dbNamePrefix);
    this.tblNamePrefix_ = Preconditions.checkNotNull(tblNamePrefix);
    this.executorService_ = Executors.newFixedThreadPool(numClients,
        new ThreadFactoryBuilder()
            .setNameFormat("hive-query-executor-%d")
            .build());
    hiveJdbcClientPool_ = HiveJdbcClientPool.create(numClients);
    totalQueryWeight_ = 0;
    this.skippedQueryTypes = skippedQueryTypes;
    for (QueryType qt : QueryType.values()) {
      rangeMap
          .put(Range.closedOpen(totalQueryWeight_, totalQueryWeight_ + qt.weight_), qt);
      totalQueryWeight_ += qt.weight_;
    }
  }

  private QueryType getNextQueryType() {
    return rangeMap.get(random_.nextInt(totalQueryWeight_));
  }

  /**
   * Gets the next random hive query according to the weight distribution provided to the
   * QueryTypes
   */
  private TestHiveQuery getNext(int clientId) {
    QueryType type = getNextQueryType();
    if (skippedQueryTypes != null && skippedQueryTypes.contains(type)) {
      LOG.info("Skipping this query type {}", type);
      return null;
    }
    String dbName = dbNamePrefix_ + clientId;
    String tblName = tblNamePrefix_ + clientId;
    switch (type) {
      case CREATE_DB:
        return CreateDbQuery.create(dbName);
      case DROP_DB:
        return DropDbQuery.create(dbName);
      case CREATE_TABLE:
        return CreateTblQuery.create(dbName, tblName, "c1 int, c2 string");
      case CREATE_TABLE_AS_SELECT:
        return CreateTblAsSelect.create(dbName, tblName, "functional", "alltypes");
      case DROP_TABLE:
        return DropTblQuery.create(dbName, tblName);
      case ALTER_TABLE_ADD_COL:
        return AlterTblAddColQuery.create(dbName, tblName, getRandomColName(6) +
            " string");
      case ALTER_TABLE_ADD_PROPERTY:
        return AlterTblAddProperty.create(dbName, tblName, getRandomColName(6),
            getRandomColName(6));
      case ADD_PARTITION:
        // for partitioned tables use a different tbl name so that it does not
        // conflict with non-partitioned tab create statements
        // get the next partition name from the partitionIdGenerator for this client
        tblName += "_part";
        return AlterTableAddPartition.create(dbName, tblName,
            "part=" + partitionIdGenerators_[clientId].getAndIncrement());
      case DROP_PARTITION:
        // for partitioned tables use a different tbl name so that it does not
        // conflict with non-partitioned tab create statements
        tblName += "_part";
        // it is possible that this drop partition is being called before any partition
        // was ever added on this table. Hence partitionIdGenerator for this table could
        // be 0 which cannot be used for the random.nextInt(bound). Check if the
        // lastPartitionId is 0 if yes, use a hard-coded bound of 1
        int lastPartitionId = partitionIdGenerators_[clientId].get();
        int bound = 1;
        if (lastPartitionId > 0) {
          bound = lastPartitionId;
        }
        return AlterTableDropPartition.create(dbName, tblName,
            "part=" + random_.nextInt(bound));
      case DYN_PARTITION_INSERT:
        // dynamic partition insert
        // use a different table name since the partition keys are different than the
        // built-in ones
        tblName += "_alltypes_part";
        return DynamicPartitionInsert.create(dbName, tblName, "functional",
            "alltypes",
            "year,month", false);
      case DYN_PARTITION_INSERT_OVERWRITE:
        // dynamic partition insert overwrite
        // use a different table name since the partition keys are different than the
        // built-in ones
        tblName += "_alltypes_part";
        return DynamicPartitionInsert.create(dbName, tblName, "functional", "alltypes",
            "year,month", true);
      case INSERT_PARTITION:
        // insert into partition
        tblName += "_alltypes_part";
        return InsertTblOrPartition.create(dbName, tblName, "functional", "alltypes",
            false,
            "year,month", 100);
      case INSERT_OVERWRITE_PARTITION:
        // insert overwrite into partition
        tblName += "_alltypes_part";
        return InsertTblOrPartition.create(dbName, tblName, "functional", "alltypes",
            true,
            "year,month", 100);
      case INSERT_TABLE:
        // insert into table
        tblName += "_alltypes_part";
        return InsertTblOrPartition.create(dbName, tblName, "functional",
            "alltypesnopart",
            false,
            null, 100);
      case INSERT_OVERWRITE_TABLE:
        // insert overwrite into table
        tblName += "_alltypes_part";
        return InsertTblOrPartition.create(dbName, tblName, "functional",
            "alltypesnopart",
            true,
            null, 100);
      default:
        throw new RuntimeException(String.format("Invalid statement type %s", type));
    }
  }

  public void start() throws Exception {
    if (!isStarted_.compareAndSet(false, true)) {
      throw new Exception("Random hive query generator is already started");
    }
    futures_ = new ArrayList<>();
    for (int i = 0; i < numClients_; i++) {
      futures_.add(executorService_.submit(() -> {
        final int clientId =
            Integer.parseInt(Thread.currentThread().getName().substring(
                "hive-query-executor-".length()));
        int queryNumber = 1;
        while (queryNumber <= numQueriesPerClient_) {
          TestHiveQuery query = getNext(clientId);
          if (query == null) {
            continue;
          }
          try {
            LOG.info("Client {} running hive query set {}: {}", clientId, queryNumber,
                query);
            query.run(hiveJdbcClientPool_);
            queryNumber++;
          } catch (Exception e) {
            throw new ExecutionException(String.format("Client %s errored out while "
                    + "executing query set "
                    + "%s %s or its dependent queries. Exception message is: %s",
                clientId, queryNumber, query, e.getMessage()), e);
          }
        }
        return null;
      }));
    }
    executorService_.shutdown();
  }

  /**
   * Checks if any of the queries failed during execution. The queryRunner must be in a
   * terminated state to make sure all the pending tasks are completed
   *
   * @throws Exception in case the query runner is interrupted or errors out
   */
  public void checkForErrors() throws ExecutionException {
    Preconditions.checkState(isStarted_.get());
    Preconditions.checkState(executorService_.isTerminated());
    try {
      for (Future<Void> result : futures_) {
        result.get();
      }
    } catch (InterruptedException e) {
      // ignored
    }
  }

  /**
   * Returns true if all the tasks have completed
   */
  public boolean isTerminated() {
    return executorService_.isTerminated();
  }

  /**
  * Returns a random column name for the queries
  */
  private static String getRandomColName(int size) {
    return "col_" + TestUtils.getRandomString(size);
  }

  /**
   * Terminates query runner. No gaurantees beyond best effort to terminate currently
   * running queries
   */
  public void shutdownNow() {
    try {
      executorService_.shutdownNow();
    } finally {
      if (hiveJdbcClientPool_ != null) { hiveJdbcClientPool_.close(); }
    }
  }
}
