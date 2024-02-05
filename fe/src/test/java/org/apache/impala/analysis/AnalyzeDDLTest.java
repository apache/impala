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

package org.apache.impala.analysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.impala.catalog.ArrayType;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.ColumnStats;
import org.apache.impala.catalog.DataSource;
import org.apache.impala.catalog.DataSourceTable;
import org.apache.impala.catalog.HdfsTable;
import org.apache.impala.catalog.PrimitiveType;
import org.apache.impala.catalog.ScalarType;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FileSystemUtil;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.Pair;
import org.apache.impala.common.PrintUtils;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.impala.thrift.TDescribeTableParams;
import org.apache.impala.thrift.TQueryOptions;
import org.apache.impala.util.MetaStoreUtil;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class AnalyzeDDLTest extends FrontendTestBase {

  @Test
  public void TestAlterTableAddDropPartition() throws CatalogException {
    String[] addDrop = {"add if not exists", "drop if exists"};
    for (String kw: addDrop) {
      // Add different partitions for different column types
      AnalyzesOk("alter table functional.alltypes " + kw +
          " partition(year=2050, month=10)");
      AnalyzesOk("alter table functional.alltypes " + kw +
          " partition(month=10, year=2050)");
      AnalyzesOk("alter table functional.insert_string_partitioned " + kw +
          " partition(s2='1234')");
      // Add/drop date partitions
      AnalyzesOk("alter table functional.date_tbl " + kw +
          " partition(date_part='1874-06-04')");
      AnalyzesOk("alter table functional.date_tbl " + kw +
          " partition(date_part=date '1874-06-04')");
      AnalyzesOk("alter table functional.date_tbl " + kw +
          " partition(date_part='1874-6-04')");
      AnalyzesOk("alter table functional.date_tbl " + kw +
          " partition(date_part=date '1874-6-04')");
      AnalyzesOk("alter table functional.date_tbl " + kw +
          " partition(date_part='1874-6-4')");
      AnalyzesOk("alter table functional.date_tbl " + kw +
          " partition(date_part=date '1874-6-4')");

      // Can't add/drop partitions to/from unpartitioned tables
      AnalysisError("alter table functional.alltypesnopart " + kw + " partition (i=1)",
          "Table is not partitioned: functional.alltypesnopart");
      AnalysisError("alter table functional_hbase.alltypesagg " + kw +
          " partition (i=1)", "Table is not partitioned: functional_hbase.alltypesagg");

      // Empty string partition keys
      AnalyzesOk("alter table functional.insert_string_partitioned " + kw +
          " partition(s2='')");
      // Arbitrary exprs as partition key values. Constant exprs are ok.
      AnalyzesOk("alter table functional.alltypes " + kw +
          " partition(year=-1, month=cast((10+5*4) as INT))");
      AnalyzesOk("alter table functional.date_tbl " + kw +
          " partition(date_part=cast('1874-6-4' as date))");

      // Table/Db does not exist
      AnalysisError("alter table db_does_not_exist.alltypes " + kw +
          " partition (i=1)", "Could not resolve table reference: " +
          "'db_does_not_exist.alltypes'");
      AnalysisError("alter table functional.table_does_not_exist " + kw +
          " partition (i=1)", "Could not resolve table reference: " +
          "'functional.table_does_not_exist'");

      // Cannot ALTER TABLE a view.
      AnalysisError("alter table functional.alltypes_view " + kw +
          " partition(year=2050, month=10)",
          "ALTER TABLE not allowed on a view: functional.alltypes_view");
      // Cannot ALTER TABLE to add or drop partition for data source tables.
      AnalysisError("alter table functional.alltypes_datasource " + kw +
          " partition(year=2050, month=10)",
          "ALTER TABLE " + kw.toUpperCase() + " PARTITION not allowed on a table " +
          "PRODUCED BY DATA SOURCE: functional.alltypes_datasource");
      AnalysisError("alter table functional.alltypes_jdbc_datasource " + kw +
          " partition(year=2050, month=10)",
          "ALTER TABLE " + kw.toUpperCase() + " PARTITION not allowed on a table " +
          "STORED BY JDBC: functional.alltypes_jdbc_datasource");

      // NULL partition keys
      AnalyzesOk("alter table functional.alltypes " + kw +
          " partition(year=NULL, month=1)");
      AnalyzesOk("alter table functional.alltypes " + kw +
          " partition(year=NULL, month=NULL)");
      AnalyzesOk("alter table functional.alltypes " + kw +
          " partition(year=ascii(null), month=ascii(NULL))");
      AnalyzesOk("alter table functional.date_tbl " + kw +
          " partition(date_part=NULL)");
      AnalyzesOk("alter table functional.date_tbl " + kw +
          " partition(date_part=cast(NULL as date))");
    }

    // Data types don't match
    AnalysisError("alter table functional.insert_string_partitioned add " +
        "partition(s2=1234)",
        "Value of partition spec (column=s2) has incompatible type: 'SMALLINT'. " +
        "Expected type: 'STRING'.");
    AnalysisError("alter table functional.insert_string_partitioned drop " +
        "partition(s2=1234)",
        "operands of type STRING and SMALLINT are not comparable: s2 = 1234");
    AnalysisError("alter table functional.date_tbl add partition (date_part=123)",
        "Value of partition spec (column=date_part) has incompatible type: 'TINYINT'. " +
        "Expected type: 'DATE'.");

    // Loss of precision
    AnalysisError(
        "alter table functional.alltypes add " +
        "partition(year=100000000000, month=10) ",
        "Partition key value may result in loss of precision.\nWould need to cast " +
        "'100000000000' to 'INT' for partition column: year");

    // Invalid date literal
    AnalysisError(
        "alter table functional.date_tbl add partition (date_part='1874-06-')",
        "Invalid date literal: '1874-06-'");
    AnalysisError(
        "alter table functional.date_tbl add partition (date_part=date '1874-06-')",
        "Invalid date literal: '1874-06-'");

    // Duplicate partition key name
    AnalysisError("alter table functional.alltypes add " +
        "partition(year=2050, year=2051)", "Duplicate partition key name: year");

    // Duplicate date partition key name. Date literals may have different variations.
    AnalysisError(
        "alter table functional.date_tbl add partition (date_part='0001-01-01')",
        "Partition spec already exists: (date_part=DATE '0001-01-01').");
    AnalysisError(
        "alter table functional.date_tbl add partition (date_part=DATE '0001-01-01')",
        "Partition spec already exists: (date_part=DATE '0001-01-01').");
    AnalysisError(
        "alter table functional.date_tbl add partition (date_part='0001-01-1')",
        "Partition spec already exists: (date_part=DATE '0001-01-01').");
    AnalysisError(
        "alter table functional.date_tbl add partition (date_part=date '0001-01-1')",
        "Partition spec already exists: (date_part=DATE '0001-01-01').");
    AnalysisError(
        "alter table functional.date_tbl add partition (date_part='0001-01-1')",
        "Partition spec already exists: (date_part=DATE '0001-01-01').");
    AnalysisError(
        "alter table functional.date_tbl add partition (date_part=date '0001-1-1')",
        "Partition spec already exists: (date_part=DATE '0001-01-01').");
    AnalysisError(
        "alter table functional.date_tbl add partition " +
        "(date_part=cast('0001-1-01' as date))",
        "Partition spec already exists: (date_part=CAST('0001-1-01' AS DATE)).");

    // Arbitrary exprs as partition key values. Non-constant exprs should fail.
    AnalysisError("alter table functional.alltypes add " +
        "partition(year=2050, month=int_col) ",
        "Non-constant expressions are not supported as static partition-key " +
        "values in 'month=int_col'.");
    AnalysisError("alter table functional.alltypes add " +
        "partition(year=cast(int_col as int), month=12) ",
        "Non-constant expressions are not supported as static partition-key " +
        "values in 'year=CAST(int_col AS INT)'.");

    // Not a partition column
    AnalysisError("alter table functional.alltypes drop " +
        "partition(year=2050, int_col=1)",
        "Partition exprs cannot contain non-partition column(s): int_col = 1.");

    // Arbitrary exprs as partition key values. Non-partition columns should fail.
    AnalysisError("alter table functional.alltypes drop " +
        "partition(year=2050, month=int_col) ",
        "Partition exprs cannot contain non-partition column(s): `month` = int_col.");
    AnalysisError("alter table functional.alltypes drop " +
        "partition(year=cast(int_col as int), month=12) ",
        "Partition exprs cannot contain non-partition column(s): " +
        "`year` = CAST(int_col AS INT).");

    // IF NOT EXISTS properly checks for partition existence
    AnalyzesOk("alter table functional.alltypes add " +
        "partition(year=2050, month=10)");
    AnalysisError("alter table functional.alltypes add " +
        "partition(year=2010, month=10)",
        "Partition spec already exists: (year=2010, month=10).");
    AnalyzesOk("alter table functional.alltypes add if not exists " +
        "partition(year=2010, month=10)");
    AnalyzesOk("alter table functional.alltypes add if not exists " +
        "partition(year=2010, month=10) location " +
        "'/test-warehouse/alltypes/year=2010/month=10'");

    // IF EXISTS properly checks for partition existence
    // with a fully specified partition.
    AnalyzesOk("alter table functional.alltypes drop " +
        "partition(year=2010, month=10)");
    AnalysisError("alter table functional.alltypes drop " +
        "partition(year=2050, month=10)",
        "No matching partition(s) found.");
    AnalyzesOk("alter table functional.alltypes drop if exists " +
        "partition(year=2050, month=10)");

    // NULL partition keys
    AnalysisError("alter table functional.alltypes drop " +
      "partition(year=NULL, month=1)",
      "No matching partition(s) found.");
    AnalysisError("alter table functional.alltypes drop " +
      "partition(year=NULL, month is NULL)",
      "No matching partition(s) found.");
    AnalysisError("alter table functional.date_tbl drop partition(date_part=NULL)",
      "No matching partition(s) found.");

    // Drop partition using predicates
    // IF EXISTS is added here
    AnalyzesOk("alter table functional.alltypes drop " +
        "partition(year<2011, month!=10)");
    AnalysisError("alter table functional.alltypes drop " +
        "partition(1=1, month=10)",
        "Invalid partition expr 1 = 1. " +
        "A partition spec may not contain constant predicates.");
    AnalyzesOk("alter table functional.alltypes drop " +
        "partition(year>1050, month=10)");
    AnalyzesOk("alter table functional.alltypes drop " +
        "partition(year>1050 and month=10)");
    AnalyzesOk("alter table functional.alltypes drop " +
        "partition(month=10)");
    AnalyzesOk("alter table functional.alltypes drop " +
        "partition(month+2000=year)");
    AnalyzesOk("alter table functional.alltypes drop " +
      "partition(year>9050, month=10)");
    AnalyzesOk("alter table functional.alltypes drop if exists " +
        "partition(year>9050, month=10)");
    AnalyzesOk("alter table functional.date_tbl drop if exists " +
        "partition(date_part > '1874-6-2')");
    AnalyzesOk("alter table functional.date_tbl drop if exists " +
        "partition(date_part > date '1874-6-02')");

    // Not a valid column
    AnalysisError("alter table functional.alltypes add " +
        "partition(year=2050, blah=1)",
        "Partition column 'blah' not found in table: functional.alltypes");
    AnalysisError("alter table functional.alltypes drop " +
        "partition(year=2050, blah=1)",
        "Could not resolve column/field reference: 'blah'");

    // Not a partition column
    AnalysisError("alter table functional.alltypes add " +
      "partition(year=2050, int_col=1) ",
      "Column 'int_col' is not a partition column in table: functional.alltypes");

    // Caching ops
    AnalyzesOk("alter table functional.alltypes add " +
        "partition(year=2050, month=10) cached in 'testPool'");
    AnalyzesOk("alter table functional.alltypes add " +
        "partition(year=2050, month=10) cached in 'testPool' with replication = 10");
    AnalyzesOk("alter table functional.alltypes add " +
        "partition(year=2050, month=10) uncached");
    AnalysisError("alter table functional.alltypes add " +
        "partition(year=2050, month=10) cached in 'badPool'",
        "The specified cache pool does not exist: badPool");
    AnalysisError("alter table functional.alltypes add " +
        "partition(year=2050, month=10) location " +
        "'file:///test-warehouse/alltypes/year=2010/month=10' cached in 'testPool'",
        "Location 'file:/test-warehouse/alltypes/year=2010/month=10' cannot be cached. " +
        "Please retry without caching: ALTER TABLE functional.alltypes ADD PARTITION " +
        "... UNCACHED");

    // Valid URIs.
    AnalyzesOk("alter table functional.alltypes add " +
        "partition(year=2050, month=10) location " +
        "'/test-warehouse/alltypes/year=2010/month=10'");
    AnalyzesOk("alter table functional.alltypes add " +
        "partition(year=2050, month=10) location " +
        "'hdfs://localhost:20500/test-warehouse/alltypes/year=2010/month=10'");
    AnalyzesOk("alter table functional.alltypes add " +
        "partition(year=2050, month=10) location " +
        "'s3a://bucket/test-warehouse/alltypes/year=2010/month=10'");
    AnalyzesOk("alter table functional.alltypes add " +
        "partition(year=2050, month=10) location " +
        "'file:///test-warehouse/alltypes/year=2010/month=10'");

    // Invalid URIs.
    AnalysisError("alter table functional.alltypes add " +
        "partition(year=2050, month=10) location " +
        "'foofs://bar/test-warehouse/alltypes/year=2010/month=10'",
        "No FileSystem for scheme: foofs");
    AnalysisError("alter table functional.alltypes add " +
        "partition(year=2050, month=10) location '  '",
        "URI path cannot be empty.");

    // Iceberg DROP PARTITION
    String partitioned =
        "alter table functional_parquet.iceberg_partitioned drop partition";
    String evolution =
        "alter table functional_parquet.iceberg_partition_evolution drop partition";
    String nonPartitioned =
        "alter table functional_parquet.iceberg_non_partitioned drop partition";

    AnalysisError(nonPartitioned + "(user = 'Alan')",
        "Table is not partitioned: functional_parquet.iceberg_non_partitioned");
    AnalysisError(partitioned + "(action = 'Foo')",
        "No matching partition(s) found");
    AnalysisError(partitioned + "(user = 'Alan')",
        "Partition exprs cannot contain non-partition column(s): `user`");
    AnalysisError(partitioned + "(user = 'Alan' or user = 'Lisa' and id > 10)",
        "Partition exprs cannot contain non-partition column(s): `user`");
    AnalysisError(partitioned + "(void(action) = 'click')",
        "VOID transform is not supported for partition selection");
    AnalysisError(partitioned + "(day(action) = 'Alan')",
        "Can't filter column 'action' with transform type: 'DAY'");
    AnalysisError(partitioned + "(action = action)",
        "Invalid partition filtering expression: action = action");
    AnalysisError(partitioned + "(action = action and action = 'click' "
            + "or hour(event_time) > '2020-01-01-01')",
        "Invalid partition filtering expression: "
            + "action = action AND action = 'click' OR HOUR(event_time) > 438289");
    AnalysisError(
        partitioned + "(action)", "Invalid partition filtering expression: action");
    AnalysisError(partitioned + "(2)", "Invalid partition filtering expression: 2");
    AnalysisError(partitioned + "(truncate(action))",
        "BUCKET and TRUNCATE partition transforms should have a parameter");
    AnalysisError(partitioned + "(truncate('string', action))",
        "Invalid transform parameter value: string");
    AnalysisError(partitioned + "(truncate(1, 2, action))",
        "Invalid partition predicate: truncate(1, 2, action)");
    AnalysisError(partitioned + " (action = 'click') purge",
        "Partition purge is not supported for Iceberg tables");

    AnalyzesOk(partitioned + "(hour(event_time) > '2020-01-01-01')");
    AnalyzesOk(partitioned + "(hour(event_time) < '2020-02-01-01')");
    AnalyzesOk(partitioned + "(hour(event_time) = '2020-01-01-9')");
    AnalyzesOk(partitioned + "(hour(event_time) = '2020-01-01-9', action = 'click')");
    AnalyzesOk(partitioned + "(action = 'click')");
    AnalyzesOk(partitioned + "(action = 'click' or action = 'download')");
    AnalyzesOk(partitioned + "(action in ('click', 'download'))");
    AnalyzesOk(partitioned + "(hour(event_time) in ('2020-01-01-9', '2020-01-01-1'))");
    AnalyzesOk(evolution + "(truncate(4,date_string_col,4) = '1231')");
    AnalyzesOk(evolution + "(month = 12)");
  }

  @Test
  public void TestAlterTableAddMultiplePartitions() {
    for (String cl: new String[]{"if not exists", ""}) {
      // Add multiple partitions.
      AnalyzesOk("alter table functional.alltypes add " + cl +
          " partition(year=2050, month=10)" +
          " partition(year=2050, month=11)" +
          " partition(year=2050, month=12)");
      // Add multiple DATE partitions with different formatting.
      AnalyzesOk("alter table functional.date_tbl add " + cl +
          " partition(date_part='1970-11-1')" +
          " partition(date_part='1971-2-01')" +
          " partition(date_part=DATE '1972-1-1')");

      // Duplicate partition specifications.
      AnalysisError("alter table functional.alltypes add " + cl +
          " partition(year=2050, month=10)" +
          " partition(year=2050, month=11)" +
          " partition(Month=10, YEAR=2050)",
          "Duplicate partition spec: (month=10, year=2050)");
      // Duplicate DATE partition specifications with different formatting.
      AnalysisError("alter table functional.date_tbl add " + cl +
          " partition(date_part='1970-1-1')" +
          " partition(date_part='1971-1-03')" +
          " partition(date_part='1970-01-01')",
          "Duplicate partition spec: (date_part=DATE '1970-01-01')");

      // Multiple partitions with locations and caching.
      AnalyzesOk("alter table functional.alltypes add " + cl +
          " partition(year=2050, month=10) location" +
          " '/test-warehouse/alltypes/y2050m10' cached in 'testPool'" +
          " partition(year=2050, month=11) location" +
          " 'hdfs://localhost:20500/test-warehouse/alltypes/y2050m11'" +
          " cached in 'testPool' with replication = 7" +
          " partition(year=2050, month=12) location" +
          " 'file:///test-warehouse/alltypes/y2050m12' uncached");
      // One of the partitions points to an invalid URI.
      AnalysisError("alter table functional.alltypes add " + cl +
          " partition(year=2050, month=10) location" +
          " '/test-warehouse/alltypes/y2050m10' cached in 'testPool'" +
          " partition(year=2050, month=11) location" +
          " 'hdfs://localhost:20500/test-warehouse/alltypes/y2050m11'" +
          " cached in 'testPool' with replication = 7" +
          " partition(year=2050, month=12) location" +
          " 'fil:///test-warehouse/alltypes/y2050m12' uncached",
          "No FileSystem for scheme: fil");
      // One of the partitions is cached in a non-existent pool.
      AnalysisError("alter table functional.alltypes add " + cl +
          " partition(year=2050, month=10) location" +
          " '/test-warehouse/alltypes/y2050m10' cached in 'testPool'" +
          " partition(year=2050, month=11) location" +
          " 'hdfs://localhost:20500/test-warehouse/alltypes/y2050m11'" +
          " cached in 'nonExistentTestPool' with replication = 7" +
          " partition(year=2050, month=12) location" +
          " 'file:///test-warehouse/alltypes/y2050m12' uncached",
          "The specified cache pool does not exist: nonExistentTestPool");
    }

    // If 'IF NOT EXISTS' is not used, ALTER TABLE ADD PARTITION cannot add a preexisting
    // partition to a table.
    AnalysisError("alter table functional.alltypes add partition(year=2050, month=1)" +
        "partition(year=2010, month=1) partition(year=2050, month=2)",
        "Partition spec already exists: (year=2010, month=1)");
  }

  @Test
  public void TestAlterTableAddColumn() {
    AnalyzesOk("alter table functional.alltypes add column new_col int");
    AnalyzesOk("alter table functional.alltypes add column NEW_COL int");
    AnalyzesOk("alter table functional.alltypes add column if not exists int_col int");
    AnalyzesOk("alter table functional.alltypes add column if not exists INT_COL int");
    // Valid unicode column name.
    AnalyzesOk("alter table functional.alltypes add column `???` int");

    // ALTER TABLE to add column for data source tables.
    AnalyzesOk("alter table functional.alltypes_datasource add column c1 string");
    AnalyzesOk("alter table functional.alltypes_jdbc_datasource add column c1 string");

    // Column name must be unique for add.
    AnalysisError("alter table functional.alltypes add column int_col int",
        "Column already exists: int_col");
    AnalysisError("alter table functional.alltypes add column INT_COL int",
        "Column already exists: int_col");
    // Add a column with same name as a partition column.
    AnalysisError("alter table functional.alltypes add column year int",
        "Column name conflicts with existing partition column: year");
    AnalysisError("alter table functional.alltypes add column if not exists year int",
        "Column name conflicts with existing partition column: year");
    AnalysisError("alter table functional.alltypes add column YEAR int",
        "Column name conflicts with existing partition column: year");
    AnalysisError("alter table functional.alltypes add column if not exists YEAR int",
        "Column name conflicts with existing partition column: year");

    // Table/Db does not exist.
    AnalysisError("alter table db_does_not_exist.alltypes add column i int",
        "Could not resolve table reference: 'db_does_not_exist.alltypes'");
    AnalysisError("alter table functional.table_does_not_exist add column i int",
        "Could not resolve table reference: 'functional.table_does_not_exist'");

    // Cannot ALTER TABLE a view.
    AnalysisError("alter table functional.alltypes_view add column c1 string",
        "ALTER TABLE not allowed on a view: functional.alltypes_view");
    // Cannot ALTER TABLE a nested collection.
    AnalysisError("alter table allcomplextypes.int_array_col add column c1 string",
        createAnalysisCtx("functional"),
        "ALTER TABLE not allowed on a nested collection: allcomplextypes.int_array_col");
    // Cannot ALTER TABLE to add column with unsupported data type for data source
    // table.
    AnalysisError("alter table functional.alltypes_jdbc_datasource add column i binary",
        "Tables stored by JDBC do not support the column type: BINARY");

    // Cannot ALTER TABLE ADD COLUMNS on an HBase table.
    AnalysisError("alter table functional_hbase.alltypes add column i int",
        "ALTER TABLE ADD COLUMNS not currently supported on HBase tables.");

    // Cannot ALTER ADD COLUMN primary key on Kudu table.
    AnalysisError("alter table functional_kudu.alltypes add column " +
        "new_col int primary key",
        "Cannot add a PRIMARY KEY using an ALTER TABLE ADD COLUMNS statement: " +
        "new_col INT PRIMARY KEY");
    AnalysisError("alter table functional_kudu.alltypes add column " +
        "new_col int non unique primary key",
        "Cannot add a NON UNIQUE PRIMARY KEY using an ALTER TABLE ADD COLUMNS " +
        "statement: new_col INT NON UNIQUE PRIMARY KEY");

    // A non-null column must have a default on Kudu table.
    AnalysisError("alter table functional_kudu.alltypes add column new_col int not null",
        "A new non-null column must have a default value: new_col INT NOT NULL");

    // Cannot ALTER ADD COLUMN complex type on Kudu table.
    AnalysisError("alter table functional_kudu.alltypes add column c struct<f1:int>",
        "Kudu tables do not support complex types: c STRUCT<f1:INT>");

    // A not null is a Kudu only option..
    AnalysisError("alter table functional.alltypes add column new_col int not null",
        "The specified column options are only supported in Kudu tables: " +
        "new_col INT NOT NULL");
  }

  @Test
  public void TestAlterTableAddColumns() {
    AnalyzesOk("alter table functional.alltypes add columns (new_col int)");
    AnalyzesOk("alter table functional.alltypes add columns (NEW_COL int)");
    AnalyzesOk("alter table functional.alltypes add columns (c1 string comment 'hi')");
    AnalyzesOk("alter table functional.alltypes add columns (c struct<f1:int>)");
    AnalyzesOk("alter table functional.alltypes add if not exists columns (int_col int)");
    AnalyzesOk("alter table functional.alltypes add if not exists columns (INT_COL int)");
    // Valid unicode column name.
    AnalyzesOk("alter table functional.alltypes add columns" +
        "(`시스???पताED` int)");

    // ALTER TABLE to add columns for data source tables.
    AnalyzesOk("alter table functional.alltypes_datasource add columns " +
        "(c1 string comment 'hi')");
    AnalyzesOk("alter table functional.alltypes_jdbc_datasource add columns " +
        "(c1 string comment 'hi')");

    // Column name must be unique for add.
    AnalysisError("alter table functional.alltypes add columns (int_col int)",
        "Column already exists: int_col");
    // Add a column with same name as a partition column.
    AnalysisError("alter table functional.alltypes add columns (year int)",
        "Column name conflicts with existing partition column: year");
    AnalysisError("alter table functional.alltypes add if not exists columns (year int)",
        "Column name conflicts with existing partition column: year");

    // Duplicate column names.
    AnalysisError("alter table functional.alltypes add columns (c1 int, c1 int)",
        "Duplicate column name: c1");
    AnalysisError("alter table functional.alltypes add columns (c1 int, C1 int)",
        "Duplicate column name: c1");
    AnalysisError("alter table functional.alltypes add if not exists columns " +
        "(c1 int, c1 int)", "Duplicate column name: c1");
    AnalysisError("alter table functional.alltypes add if not exists columns " +
        "(c1 int, C1 int)", "Duplicate column name: c1");

    // Table/Db does not exist.
    AnalysisError("alter table db_does_not_exist.alltypes add columns (i int)",
        "Could not resolve table reference: 'db_does_not_exist.alltypes'");
    AnalysisError("alter table functional.table_does_not_exist add columns (i int)",
        "Could not resolve table reference: 'functional.table_does_not_exist'");

    // Cannot ALTER TABLE a view.
    AnalysisError("alter table functional.alltypes_view " +
        "add columns (c1 string comment 'hi')",
        "ALTER TABLE not allowed on a view: functional.alltypes_view");
    // Cannot ALTER TABLE a nested collection.
    AnalysisError("alter table allcomplextypes.int_array_col " +
        "add columns (c1 string comment 'hi')",
        createAnalysisCtx("functional"),
        "ALTER TABLE not allowed on a nested collection: allcomplextypes.int_array_col");
    // Cannot ALTER TABLE to add columns with unsupported data type for data source
    // table.
    AnalysisError("alter table functional.alltypes_jdbc_datasource add columns " +
        "(c1 binary comment 'hi')",
        "Tables stored by JDBC do not support the column type: BINARY");

    // Cannot ALTER TABLE ADD COLUMNS on an HBase table.
    AnalysisError("alter table functional_hbase.alltypes add columns (i int)",
        "ALTER TABLE ADD COLUMNS not currently supported on HBase tables.");

    // Cannot ALTER ADD COLUMNS primary key on Kudu table.
    AnalysisError("alter table functional_kudu.alltypes add columns " +
        "(new_col int primary key)",
        "Cannot add a PRIMARY KEY using an ALTER TABLE ADD COLUMNS statement: " +
        "new_col INT PRIMARY KEY");
    AnalysisError("alter table functional_kudu.alltypes add columns " +
        "(new_col int non unique primary key)",
        "Cannot add a NON UNIQUE PRIMARY KEY using an ALTER TABLE ADD COLUMNS " +
        "statement: new_col INT NON UNIQUE PRIMARY KEY");

    // A non-null column must have a default on Kudu table.
    AnalysisError("alter table functional_kudu.alltypes add columns" +
        "(new_col int not null)",
        "A new non-null column must have a default value: new_col INT NOT NULL");

    // Cannot ALTER ADD COLUMN complex type on Kudu table.
    AnalysisError("alter table functional_kudu.alltypes add columns (c struct<f1:int>)",
        "Kudu tables do not support complex types: c STRUCT<f1:INT>");

    // A not null is a Kudu only option..
    AnalysisError("alter table functional.alltypes add columns(new_col int not null)",
        "The specified column options are only supported in Kudu tables: " +
        "new_col INT NOT NULL");
  }

  @Test
  public void TestAlterTableReplaceColumns() {
    AnalyzesOk("alter table functional.alltypes replace columns " +
        "(c1 int comment 'c', c2 int)");
    AnalyzesOk("alter table functional.alltypes replace columns " +
        "(C1 int comment 'c', C2 int)");
    AnalyzesOk("alter table functional.alltypes replace columns (c array<string>)");
    AnalyzesOk("alter table functional.alltypes replace columns" +
        "(`?최종हिंदी` int)");

    // Replace should not throw an error if the column already exists.
    AnalyzesOk("alter table functional.alltypes replace columns (int_col int)");
    AnalyzesOk("alter table functional.alltypes replace columns (INT_COL int)");
    // It is not possible to replace a partition column.
    AnalysisError("alter table functional.alltypes replace columns (year int)",
        "Column name conflicts with existing partition column: year");
    AnalysisError("alter table functional.alltypes replace columns (Year int)",
        "Column name conflicts with existing partition column: year");

    // Duplicate column names.
    AnalysisError("alter table functional.alltypes replace columns (c1 int, c1 int)",
        "Duplicate column name: c1");
    AnalysisError("alter table functional.alltypes replace columns (c1 int, C1 int)",
        "Duplicate column name: c1");

    // Table/Db does not exist
    AnalysisError("alter table db_does_not_exist.alltypes replace columns (i int)",
        "Could not resolve table reference: 'db_does_not_exist.alltypes'");
    AnalysisError("alter table functional.table_does_not_exist replace columns (i int)",
        "Could not resolve table reference: 'functional.table_does_not_exist'");

    // Cannot ALTER TABLE a view.
    AnalysisError("alter table functional.alltypes_view " +
            "replace columns (c1 string comment 'hi')",
        "ALTER TABLE not allowed on a view: functional.alltypes_view");
    // Cannot ALTER TABLE a nested collection.
    AnalysisError("alter table allcomplextypes.int_array_col " +
            "replace columns (c1 string comment 'hi')",
        createAnalysisCtx("functional"),
        "ALTER TABLE not allowed on a nested collection: allcomplextypes.int_array_col");
    // Cannot ALTER TABLE to replace columns for data source tables.
    AnalysisError("alter table functional.alltypes_datasource " +
            "replace columns (c1 string comment 'hi')",
        "ALTER TABLE REPLACE COLUMNS not allowed on a table PRODUCED BY DATA SOURCE: " +
            "functional.alltypes_datasource");
    AnalysisError("alter table functional.alltypes_jdbc_datasource " +
            "replace columns (c1 string comment 'hi')",
        "ALTER TABLE REPLACE COLUMNS not allowed on a table STORED BY JDBC: " +
            "functional.alltypes_jdbc_datasource");

    // Cannot ALTER TABLE REPLACE COLUMNS on a HBase table.
    AnalysisError("alter table functional_hbase.alltypes replace columns (i int)",
        "ALTER TABLE REPLACE COLUMNS not currently supported on HBase tables.");

    // Cannot ALTER TABLE REPLACE COLUMNS on a Kudu table.
    AnalysisError("alter table functional_kudu.alltypes replace columns (i int)",
        "ALTER TABLE REPLACE COLUMNS is not supported on Kudu tables.");
  }

  @Test
  public void TestAlterTableDropColumn() throws AnalysisException {
    AnalyzesOk("alter table functional.alltypes drop column int_col");

    // ALTER TABLE to drop column for data source tables.
    AnalyzesOk("alter table functional.alltypes_datasource drop column int_col");
    AnalyzesOk("alter table functional.alltypes_jdbc_datasource drop column int_col");

    AnalysisError("alter table functional.alltypes drop column no_col",
        "Column 'no_col' does not exist in table: functional.alltypes");

    AnalysisError("alter table functional.alltypes drop column year",
        "Cannot drop partition column: year");

    // Tables should always have at least 1 column
    AnalysisError("alter table functional_seq_snap.bad_seq_snap drop column field",
        "Cannot drop column 'field' from functional_seq_snap.bad_seq_snap. " +
        "Tables must contain at least 1 column.");

    // Table/Db does not exist
    AnalysisError("alter table db_does_not_exist.alltypes drop column col1",
        "Could not resolve table reference: 'db_does_not_exist.alltypes'");
    AnalysisError("alter table functional.table_does_not_exist drop column col1",
        "Could not resolve table reference: 'functional.table_does_not_exist'");

    // Cannot ALTER TABLE a view.
    AnalysisError("alter table functional.alltypes_view drop column int_col",
        "ALTER TABLE not allowed on a view: functional.alltypes_view");
    // Cannot ALTER TABLE a nested collection.
    AnalysisError("alter table allcomplextypes.int_array_col drop column int_col",
        createAnalysisCtx("functional"),
        "ALTER TABLE not allowed on a nested collection: allcomplextypes.int_array_col");

    // Cannot ALTER TABLE DROP COLUMN on an HBase table.
    AnalysisError("alter table functional_hbase.alltypes drop column int_col",
        "ALTER TABLE DROP COLUMN not currently supported on HBase tables.");
  }

  @Test
  public void TestAlterTableChangeColumn() throws AnalysisException {
    // Rename a column
    AnalyzesOk("alter table functional.alltypes change column int_col int_col2 int");
    // Rename and change the datatype
    AnalyzesOk("alter table functional.alltypes change column int_col c2 string");
    AnalyzesOk(
        "alter table functional.alltypes change column int_col c2 map<int, string>");
    // Change only the datatype
    AnalyzesOk("alter table functional.alltypes change column int_col int_col tinyint");
    // Add a comment to a column.
    AnalyzesOk("alter table functional.alltypes change int_col int_col int comment 'c'");
    AnalyzesOk("alter table functional.alltypes change column int_col `汉字` int");

    // ALTER TABLE to change column for data source tables.
    AnalyzesOk("alter table functional.alltypes_datasource " +
        "change column int_col int_col2 int");
    AnalyzesOk("alter table functional.alltypes_jdbc_datasource " +
        "change column int_col int_col2 int");
    AnalysisError("alter table functional.alltypes_jdbc_datasource " +
        "change column int_col bin_col binary",
        "Tables stored by JDBC do not support the column type: BINARY");

    AnalysisError("alter table functional.alltypes change column no_col c1 int",
        "Column 'no_col' does not exist in table: functional.alltypes");

    AnalysisError("alter table functional.alltypes change column year year int",
        "Cannot modify partition column: year");

    AnalysisError(
        "alter table functional.alltypes change column int_col Tinyint_col int",
        "Column already exists: tinyint_col");

    // Table/Db does not exist
    AnalysisError("alter table db_does_not_exist.alltypes change c1 c2 int",
        "Could not resolve table reference: 'db_does_not_exist.alltypes'");
    AnalysisError("alter table functional.table_does_not_exist change c1 c2 double",
        "Could not resolve table reference: 'functional.table_does_not_exist'");

    // Cannot ALTER TABLE a view.
    AnalysisError("alter table functional.alltypes_view " +
        "change column int_col int_col2 int",
        "ALTER TABLE not allowed on a view: functional.alltypes_view");
    // Cannot ALTER TABLE a nested collection.
    AnalysisError("alter table allcomplextypes.int_array_col " +
        "change column int_col int_col2 int",
        createAnalysisCtx("functional"),
        "ALTER TABLE not allowed on a nested collection: allcomplextypes.int_array_col");

    // Cannot ALTER TABLE CHANGE COLUMN on an HBase table.
    AnalysisError("alter table functional_hbase.alltypes CHANGE COLUMN int_col i int",
        "ALTER TABLE CHANGE/ALTER COLUMN not currently supported on HBase tables.");
  }

  @Test
  public void TestAlterTableSetRowFormat() throws AnalysisException {
    AnalyzesOk("alter table functional.alltypes set row format delimited " +
        "fields terminated by ' '");
    AnalyzesOk("alter table functional.alltypes partition (year=2010) set row format " +
        "delimited fields terminated by ' '");
    AnalyzesOk("alter table functional_seq.alltypes set row format delimited " +
        "fields terminated by ' '");
    AnalysisError("alter table functional.alltypesnopart PARTITION (month=1) " +
        "set row format delimited fields terminated by ' '",
        "Table is not partitioned: functional.alltypesnopart");
    String [] unsupportedFileFormatDbs =
      {"functional_parquet", "functional_rc", "functional_avro"};
    for (String format: unsupportedFileFormatDbs) {
      AnalysisError("alter table " + format + ".alltypes set row format delimited " +
          "fields terminated by ' '", "ALTER TABLE SET ROW FORMAT is only supported " +
          "on TEXT or SEQUENCE file formats");
    }
    AnalysisError("alter table functional_kudu.alltypes set row format delimited " +
        "fields terminated by ' '", "ALTER TABLE SET ROW FORMAT is only supported " +
        "on HDFS tables");
    AnalysisError("alter table functional.alltypesmixedformat partition(year=2009) " +
        "set row format delimited fields terminated by ' '",
        "ALTER TABLE SET ROW FORMAT is only supported on TEXT or SEQUENCE file formats");
    AnalyzesOk("alter table functional.alltypesmixedformat partition(year=2009,month=1) " +
        "set row format delimited fields terminated by ' '");
    // Cannot ALTER TABLE to set row format for data source tables.
    AnalysisError("alter table functional.alltypes_jdbc_datasource set row format " +
        "delimited fields terminated by ' '",
        "ALTER TABLE SET ROW FORMAT not allowed on a table STORED BY JDBC: " +
        "functional.alltypes_jdbc_datasource");
  }

  @Test
  public void TestAlterTableSet() throws AnalysisException {
    AnalyzesOk("alter table functional.alltypes set fileformat sequencefile");
    AnalyzesOk("alter table functional.alltypes set location '/a/b'");
    AnalyzesOk("alter table functional.alltypes set tblproperties('a'='1')");
    AnalyzesOk("alter table functional.alltypes set serdeproperties('a'='2')");
    AnalyzesOk("alter table functional.alltypes unset serdeproperties('a', 'b')");
    AnalyzesOk("alter table functional.alltypes unset serdeproperties" +
            " if exists('a', 'b')");
    AnalyzesOk("alter table functional.alltypes PARTITION (Year=2010, month=11) " +
               "set location '/a/b'");
    AnalyzesOk("alter table functional.alltypes PARTITION (month=11, year=2010) " +
               "set fileformat parquetfile");
    AnalyzesOk("alter table functional.alltypes PARTITION (month<=11, year=2010) " +
               "set fileformat parquetfile");
    AnalyzesOk("alter table functional.stringpartitionkey PARTITION " +
               "(string_col='partition1') set fileformat parquet");
    AnalyzesOk("alter table functional.stringpartitionkey PARTITION " +
               "(string_col='partition1') set location '/a/b/c'");
    AnalyzesOk("alter table functional.alltypes PARTITION (year=2010, month=11) " +
               "set tblproperties('a'='1')");
    AnalyzesOk("alter table functional.alltypes PARTITION (year<=2010, month=11) " +
               "set tblproperties('a'='1')");
    AnalyzesOk("alter table functional.alltypes PARTITION (year=2010, month=11) " +
            "unset tblproperties('a')");
    AnalyzesOk("alter table functional.alltypes PARTITION (year=2010, month=11) " +
            "unset tblproperties if exists ('a')");
    AnalyzesOk("alter table functional.alltypes PARTITION (year=2010, month=11) " +
               "set serdeproperties ('a'='2')");
    AnalyzesOk("alter table functional.alltypes PARTITION (year<=2010, month=11) " +
               "set serdeproperties ('a'='2')");

    // ALTER TABLE to set or unset tblproperties for data source tables.
    AnalyzesOk("alter table functional.alltypes_datasource set tblproperties ('a'='2')");
    AnalyzesOk("alter table functional.alltypes_jdbc_datasource set tblproperties " +
        "('a'='2')");
    AnalyzesOk("alter table functional.alltypes_datasource unset tblproperties " +
        "if exists ('a')");
    AnalyzesOk("alter table functional.alltypes_jdbc_datasource unset tblproperties " +
        "if exists ('a')");
    AnalysisError("alter table functional.alltypes_jdbc_datasource set " +
        "serdeproperties ('a'='2')",
        "ALTER TABLE SET SERDEPROPERTIES is not supported for DataSource table");
    AnalysisError("alter table functional.alltypes_jdbc_datasource PARTITION " +
        "(year=2010) set tblproperties ('a'='2')",
        "Table is not partitioned: functional.alltypes_jdbc_datasource");
    AnalysisError("alter table functional.alltypes_jdbc_datasource set tblproperties " +
        "('__IMPALA_DATA_SOURCE_NAME'='test')",
        "Changing the '__IMPALA_DATA_SOURCE_NAME' table property is not supported for " +
        "DataSource table.");
    AnalysisError("alter table functional.alltypes_jdbc_datasource unset " +
        "serdeproperties ('a')",
        "ALTER TABLE UNSET SERDEPROPERTIES is not supported for DataSource table");
    AnalysisError("alter table functional.alltypes_jdbc_datasource PARTITION " +
        "(year=2010) unset tblproperties ('a')",
        "Partition is not supported for DataSource table.");
    AnalysisError("alter table functional.alltypes_jdbc_datasource unset " +
        "tblproperties ('__IMPALA_DATA_SOURCE_NAME')",
        "Unsetting the '__IMPALA_DATA_SOURCE_NAME' table property is not supported " +
        "for DataSource table.");
    AnalysisError("alter table functional.alltypes_jdbc_datasource unset " +
        "tblproperties ('driver.url')",
        "Unsetting the 'driver.url' table property is not supported for JDBC " +
        "DataSource table.");

    AnalyzesOk("alter table functional.alltypes set tblproperties('sort.columns'='id')");
    AnalyzesOk("alter table functional.alltypes set tblproperties(" +
               "'sort.columns'='INT_COL,id')");
    AnalyzesOk("alter table functional.alltypes set tblproperties(" +
               "'sort.columns'='bool_col,int_col,id')");
    AnalyzesOk("alter table functional.alltypes set tblproperties('sort.columns'='')");
    AnalysisError("alter table functional.alltypes set tblproperties(" +
               "'sort.columns'='id,int_col,id')",
               "Duplicate column in SORT BY list: id");
    AnalysisError("alter table functional.alltypes set tblproperties(" +
               "'sort.columns'='ID, foo')",
               "Could not find SORT BY column 'foo' in table.");

    // Table is partitioned, but it has no matching partitions.
    AnalyzesOk("alter table functional.alltypesagg partition(year=2009, month=1) " +
        "set location 'hdfs://localhost:20500/test-warehouse/new_table'");
    AnalyzesOk("alter table functional.alltypesagg partition(year=2009, month=1) " +
        "set fileformat parquet");
    AnalyzesOk("alter table functional.alltypesagg partition(year=2009, month=1) " +
        "set tblproperties ('key'='value')");
    AnalyzesOk("alter table functional.alltypesagg partition(year=2009, month=1) " +
        "set serdeproperties ('key'='value')");
    AnalyzesOk("alter table functional.alltypesagg partition(year=2009, month=1) " +
        "set row format delimited fields terminated by '|'");

    {
      // Check that long_properties fail at the analysis layer
      String long_property_key = "";
      for (int i = 0; i < MetaStoreUtil.MAX_PROPERTY_KEY_LENGTH; ++i) {
        long_property_key += 'k';
      }
      String long_property_value = "";
      for (int i = 0; i < MetaStoreUtil.MAX_PROPERTY_VALUE_LENGTH; ++i) {
        long_property_value += 'v';
      }

      // At this point long_property_{key_value} are actually not quite long enough to
      // cause analysis to fail.

      AnalyzesOk("alter table functional.alltypes "
          + "set serdeproperties ('" + long_property_key + "'='" + long_property_value
          + "') ");

      AnalyzesOk("alter table functional.alltypes "
          + "set tblproperties ('" + long_property_key + "'='" + long_property_value
          + "') ");

      long_property_key += 'X';
      long_property_value += 'X';
      // Now that long_property_{key,value} are one character longer, they are too long
      // for the analyzer.

      AnalysisError("alter table functional.alltypes set "
              + "tblproperties ('" + long_property_key + "'='value')",
          "Property key length must be <= " + MetaStoreUtil.MAX_PROPERTY_KEY_LENGTH + ": "
              + (MetaStoreUtil.MAX_PROPERTY_KEY_LENGTH + 1));

      AnalysisError("alter table functional.alltypes unset "
              + "tblproperties ('" + long_property_key + "')",
              "Property key length must be <= " + MetaStoreUtil.MAX_PROPERTY_KEY_LENGTH
              + ": " + (MetaStoreUtil.MAX_PROPERTY_KEY_LENGTH + 1));

      AnalysisError("alter table functional.alltypes set "
              + "tblproperties ('key'='" + long_property_value + "')",
          "Property value length must be <= " + MetaStoreUtil.MAX_PROPERTY_VALUE_LENGTH
              + ": " + (MetaStoreUtil.MAX_PROPERTY_VALUE_LENGTH + 1));

      AnalysisError("alter table functional.alltypes set "
              + "serdeproperties ('" + long_property_key + "'='value')",
          "Property key length must be <= " + MetaStoreUtil.MAX_PROPERTY_KEY_LENGTH + ": "
              + (MetaStoreUtil.MAX_PROPERTY_KEY_LENGTH + 1));

      AnalysisError("alter table functional.alltypes unset "
              + "serdeproperties ('" + long_property_key + "')",
              "Property key length must be <= " + MetaStoreUtil.MAX_PROPERTY_KEY_LENGTH
              + ": " + (MetaStoreUtil.MAX_PROPERTY_KEY_LENGTH + 1));

      AnalysisError("alter table functional.alltypes set "
              + "serdeproperties ('key'='" + long_property_value + "')",
          "Property value length must be <= " + MetaStoreUtil.MAX_PROPERTY_VALUE_LENGTH
              + ": " + (MetaStoreUtil.MAX_PROPERTY_VALUE_LENGTH + 1));

      AnalysisError(
          "alter table functional.alltypes set tblproperties('storage_handler'='1')",
          "Changing the 'storage_handler' table property is not supported to protect " +
          "against metadata corruption.");

      AnalysisError(
          "alter table functional.alltypes unset tblproperties('storage_handler')",
          "Changing the 'storage_handler' table property is not supported to protect " +
          "against metadata corruption.");
    }

    // Arbitrary exprs as partition key values. Constant exprs are ok.
    AnalyzesOk("alter table functional.alltypes PARTITION " +
               "(year=cast(100*20+10 as INT), month=cast(2+9 as INT)) " +
               "set fileformat sequencefile");
    AnalyzesOk("alter table functional.alltypes PARTITION " +
               "(year=cast(100*20+10 as INT), month=cast(2+9 as INT)) " +
               "set location '/a/b'");

    // Arbitrary exprs as partition key values. One-partition-column-bound exprs are ok.
    AnalyzesOk("alter table functional.alltypes PARTITION " +
               "(Year*2=Year+2010, month=11) set fileformat sequencefile");

    // Arbitrary exprs as partition key values. Non-partition-column exprs.
    AnalysisError("alter table functional.alltypes PARTITION " +
                  "(int_col=3) set fileformat sequencefile",
                  "Partition exprs cannot contain non-partition column(s): int_col = 3.");

    // Partition expr matches more than one partition in set location statement.
    AnalysisError("alter table functional.alltypes PARTITION (year!=20) " +
                  "set location '/a/b'",
                  "Partition expr in set location statements can only match " +
                  "one partition. Too many matched partitions year=2009/month=1," +
                  "year=2009/month=2,year=2009/month=3");

    // Partition spec does not exist
    AnalysisError("alter table functional.alltypes PARTITION (year=2014, month=11) " +
                  "set location '/a/b'",
                  "No matching partition(s) found.");
    AnalysisError("alter table functional.alltypes PARTITION (year=2014, month=11) " +
                  "set tblproperties('a'='1')",
                  "No matching partition(s) found.");
    AnalysisError("alter table functional.alltypes PARTITION (month=11, year=2014) " +
                  "set fileformat sequencefile",
                  "No matching partition(s) found.");
    AnalysisError("alter table functional.alltypesnopart PARTITION (month=1) " +
                  "set fileformat sequencefile",
                  "Table is not partitioned: functional.alltypesnopart");
    AnalysisError("alter table functional.alltypesnopart PARTITION (month=1) " +
                  "set location '/a/b/c'",
                  "Table is not partitioned: functional.alltypesnopart");
    AnalysisError("alter table functional.stringpartitionkey PARTITION " +
                  "(string_col='partition2') set location '/a/b'",
                  "No matching partition(s) found.");
    AnalysisError("alter table functional.stringpartitionkey PARTITION " +
                  "(string_col='partition2') set fileformat sequencefile",
                  "No matching partition(s) found.");
    AnalysisError("alter table functional.alltypes PARTITION " +
                 "(year=cast(10*20+10 as INT), month=cast(5*3 as INT)) " +
                  "set location '/a/b'",
                  "No matching partition(s) found.");
    AnalysisError("alter table functional.alltypes PARTITION " +
                  "(year=cast(10*20+10 as INT), month=cast(5*3 as INT)) " +
                  "set fileformat sequencefile",
                  "No matching partition(s) found.");

    // Table/Db does not exist
    AnalysisError("alter table db_does_not_exist.alltypes set fileformat sequencefile",
        "Could not resolve table reference: 'db_does_not_exist.alltypes'");
    AnalysisError("alter table functional.table_does_not_exist set fileformat rcfile",
        "Could not resolve table reference: 'functional.table_does_not_exist'");
    AnalysisError("alter table db_does_not_exist.alltypes set location '/a/b'",
        "Could not resolve table reference: 'db_does_not_exist.alltypes'");
    AnalysisError("alter table functional.table_does_not_exist set location '/a/b'",
        "Could not resolve table reference: 'functional.table_does_not_exist'");
    AnalysisError("alter table functional.no_tbl partition(i=1) set location '/a/b'",
        "Could not resolve table reference: 'functional.no_tbl'");
    AnalysisError("alter table no_db.alltypes partition(i=1) set fileformat textfile",
        "Could not resolve table reference: 'no_db.alltypes'");

    // Valid location
    AnalyzesOk("alter table functional.alltypes set location " +
        "'hdfs://localhost:20500/test-warehouse/a/b'");
    AnalyzesOk("alter table functional.alltypes set location " +
        "'s3a://bucket/test-warehouse/a/b'");
    AnalyzesOk("alter table functional.alltypes set location " +
        "'file:///test-warehouse/a/b'");

    // Cannot ALTER TABLE to set location for data source tables.
    AnalysisError("alter table functional.alltypes_datasource set location " +
        "'file:///test-warehouse/a/b'",
        "ALTER TABLE SET LOCATION not allowed on a table PRODUCED BY DATA SOURCE: " +
        "functional.alltypes_datasource");
    AnalysisError("alter table functional.alltypes_jdbc_datasource set location " +
        "'file:///test-warehouse/a/b'",
        "ALTER TABLE SET LOCATION not allowed on a table STORED BY JDBC: " +
        "functional.alltypes_jdbc_datasource");

    // Invalid location
    AnalysisError("alter table functional.alltypes set location 'test/warehouse'",
        "URI path must be absolute: test/warehouse");
    AnalysisError("alter table functional.alltypes set location 'blah:///warehouse/'",
        "No FileSystem for scheme: blah");
    AnalysisError("alter table functional.alltypes set location ''",
        "URI path cannot be empty.");
    AnalysisError("alter table functional.alltypes set location '      '",
        "URI path cannot be empty.");

    // Cannot ALTER TABLE a view.
    AnalysisError("alter table functional.alltypes_view set fileformat sequencefile",
        "ALTER TABLE not allowed on a view: functional.alltypes_view");
    // Cannot ALTER TABLE a nested collection.
    AnalysisError("alter table allcomplextypes.int_array_col set fileformat sequencefile",
        createAnalysisCtx("functional"),
        "ALTER TABLE not allowed on a nested collection: allcomplextypes.int_array_col");
    // Cannot ALTER TABLE to set fileformat for data source tables.
    AnalysisError("alter table functional.alltypes_datasource set fileformat parquet",
        "ALTER TABLE SET FILEFORMAT not allowed on a table PRODUCED BY DATA SOURCE: " +
        "functional.alltypes_datasource");
    AnalysisError("alter table functional.alltypes_jdbc_datasource set fileformat " +
        "parquet",
        "ALTER TABLE SET FILEFORMAT not allowed on a table STORED BY JDBC: " +
        "functional.alltypes_jdbc_datasource");

    // Cannot ALTER TABLE SET on an HBase table.
    AnalysisError("alter table functional_hbase.alltypes set tblproperties('a'='b')",
        "ALTER TABLE SET not currently supported on HBase tables.");
  }

  @Test
  public void TestAlterTableSetCached() {
    // Positive cases
    AnalyzesOk("alter table functional.alltypesnopart set cached in 'testPool'");
    AnalyzesOk("alter table functional.alltypes set cached in 'testPool'");
    AnalyzesOk("alter table functional.alltypes partition(year=2010, month=12) " +
        "set cached in 'testPool'");
    AnalyzesOk("alter table functional.alltypes partition(year<=2010, month<=12) " +
        "set cached in 'testPool'");

    // Replication factor
    AnalyzesOk("alter table functional.alltypes set cached in 'testPool' " +
        "with replication = 10");
    AnalyzesOk("alter table functional.alltypes partition(year=2010, month=12) " +
        "set cached in 'testPool' with replication = 4");
    AnalysisError("alter table functional.alltypes set cached in 'testPool' " +
        "with replication = 0",
        "Cache replication factor must be between 0 and Short.MAX_VALUE");
    AnalysisError("alter table functional.alltypes set cached in 'testPool' " +
        "with replication = 90000",
        "Cache replication factor must be between 0 and Short.MAX_VALUE");

    // Attempt to alter a table that is not backed by HDFS.
    AnalysisError("alter table functional_hbase.alltypesnopart set cached in 'testPool'",
        "ALTER TABLE SET not currently supported on HBase tables.");
    AnalysisError("alter table functional.view_view set cached in 'testPool'",
        "ALTER TABLE not allowed on a view: functional.view_view");
    AnalysisError("alter table allcomplextypes.int_array_col set cached in 'testPool'",
        createAnalysisCtx("functional"),
        "ALTER TABLE not allowed on a nested collection: allcomplextypes.int_array_col");

    AnalysisError("alter table functional.alltypes set cached in 'badPool'",
        "The specified cache pool does not exist: badPool");
    AnalysisError("alter table functional.alltypes partition(year=2010, month=12) " +
        "set cached in 'badPool'", "The specified cache pool does not exist: badPool");

    // Attempt to uncache a table that is not cached. Should be a no-op.
    AnalyzesOk("alter table functional.alltypes set uncached");
    AnalyzesOk("alter table functional.alltypes partition(year=2010, month=12) " +
        "set uncached");

    // Attempt to cache a table that is already cached. Should be a no-op.
    AnalyzesOk("alter table functional.alltypestiny set cached in 'testPool'");
    AnalyzesOk("alter table functional.alltypestiny partition(year=2009, month=1) " +
        "set cached in 'testPool'");

    // Change location of a cached table/partition
    AnalysisError("alter table functional.alltypestiny set location '/tmp/tiny'",
        "Target table is cached, please uncache before changing the location using: " +
        "ALTER TABLE functional.alltypestiny SET UNCACHED");
    AnalysisError("alter table functional.alltypestiny partition (year=2009,month=1) " +
        "set location '/test-warehouse/new_location'",
        "Target partition is cached, please uncache before changing the location " +
        "using: ALTER TABLE functional.alltypestiny " +
        "PARTITION (`year` = 2009, `month` = 1) SET UNCACHED");

    // Table/db/partition do not exist
    AnalysisError("alter table baddb.alltypestiny set cached in 'testPool'",
        "Could not resolve table reference: 'baddb.alltypestiny'");
    AnalysisError("alter table functional.badtbl set cached in 'testPool'",
        "Could not resolve table reference: 'functional.badtbl'");
    AnalysisError("alter table functional.alltypestiny partition(year=9999, month=1) " +
        "set cached in 'testPool'",
        "No matching partition(s) found.");
    // Cannot ALTER TABLE to set cached for data source tables.
    AnalysisError("alter table functional.alltypes_jdbc_datasource set cached in " +
        "'testPool'",
        "ALTER TABLE SET CACHED not allowed on a table STORED BY JDBC: " +
        "functional.alltypes_jdbc_datasource");
  }

  @Test
  public void TestAlterTableSetColumnStats() {
    // Contains entries of the form 'statsKey'='statsValue' for every
    // stats key. A dummy value is used for 'statsValue'.
    List<String> testKeyValues = new ArrayList<>();
    for (ColumnStats.StatsKey statsKey: ColumnStats.StatsKey.values()) {
      testKeyValues.add(String.format("'%s'='10'", statsKey));
    }
    // Test updating all stats keys individually.
    for (String kv: testKeyValues) {
      AnalyzesOk(String.format(
          "alter table functional.alltypes set column stats string_col (%s)", kv));
      // Stats key is case-insensitive.
      AnalyzesOk(String.format(
          "alter table functional.alltypes set column stats string_col (%s)",
          kv.toLowerCase()));
      AnalyzesOk(String.format(
          "alter table functional.alltypes set column stats string_col (%s)",
          kv.toUpperCase()));
    }
    // Test updating all stats keys at once in a single statement.
    AnalyzesOk(String.format(
        "alter table functional.alltypes set column stats string_col (%s)",
        Joiner.on(",").join(testKeyValues)));
    // Test setting all stats keys to -1 (unknown).
    for (ColumnStats.StatsKey statsKey:  ColumnStats.StatsKey.values()) {
      AnalyzesOk(String.format(
          "alter table functional.alltypes set column stats string_col ('%s'='-1')",
          statsKey));
    }
    // Duplicate stats keys are valid. The last entry is used.
    AnalyzesOk("alter table functional.alltypes set column stats " +
        "int_col ('numDVs'='2','numDVs'='3')");

    // Test updating stats on all scalar types.
    for (Type t: Type.getSupportedTypes()) {
      if (t.isNull()) continue;
      Preconditions.checkState(t.isScalarType());
      String typeStr = t.getPrimitiveType().toString();
      if (t.getPrimitiveType() == PrimitiveType.CHAR ||
          t.getPrimitiveType() == PrimitiveType.VARCHAR) {
        typeStr += "(60)";
      }
      String tblName = "t_" + t.getPrimitiveType();
      addTestTable(String.format("create table %s (c %s)", tblName, typeStr));
      AnalyzesOk(String.format(
          "alter table %s set column stats c ('%s'='100','%s'='10')",
          tblName, ColumnStats.StatsKey.NUM_DISTINCT_VALUES,
          ColumnStats.StatsKey.NUM_NULLS));
      // Test setting stats values to -1 (unknown).
      AnalyzesOk(String.format(
          "alter table %s set column stats c ('%s'='-1','%s'='-1')",
          tblName, ColumnStats.StatsKey.NUM_DISTINCT_VALUES,
          ColumnStats.StatsKey.NUM_NULLS));
    }

    // Setting stats works on all table types.
    AnalyzesOk("alter table functional_hbase.alltypes set column stats " +
        "int_col ('numNulls'='2')");
    AnalyzesOk("alter table functional.alltypes_datasource set column stats " +
        "int_col ('numDVs'='2')");
    AnalyzesOk("alter table functional_kudu.testtbl set column stats " +
        "name ('numNulls'='2')");

    // Table does not exist.
    AnalysisError("alter table bad_tbl set column stats int_col ('numNulls'='2')",
        "Could not resolve table reference: 'bad_tbl'");
    // Column does not exist.
    AnalysisError(
        "alter table functional.alltypes set column stats bad_col ('numNulls'='2')",
        "Column 'bad_col' does not exist in table: functional.alltypes");

    // Cannot set column stats of a view.
    AnalysisError(
        "alter table functional.alltypes_view set column stats int_col ('numNulls'='2')",
        "ALTER TABLE not allowed on a view: functional.alltypes_view");
    // Cannot set column stats of a nested collection.
    AnalysisError(
        "alter table allcomplextypes.int_array_col " +
        "set column stats int_col ('numNulls'='2')",
        createAnalysisCtx("functional"),
        "ALTER TABLE not allowed on a nested collection: allcomplextypes.int_array_col");
    // Cannot set column stats of partition columns.
    AnalysisError(
        "alter table functional.alltypes set column stats month ('numDVs'='10')",
        "Updating the stats of a partition column is not allowed: month");
    // Cannot set the size of a fixed-length column.
    AnalysisError(
        "alter table functional.alltypes set column stats int_col ('maxSize'='10')",
        "Cannot update the 'maxSize' stats of column 'int_col' with type 'INT'.\n" +
        "Changing 'maxSize' is only allowed for variable-length columns.");
    AnalysisError(
        "alter table functional.alltypes set column stats int_col ('avgSize'='10')",
        "Cannot update the 'avgSize' stats of column 'int_col' with type 'INT'.\n" +
        "Changing 'avgSize' is only allowed for variable-length columns.");
    // Cannot set column stats of complex-typed columns.
    AnalysisError(
        "alter table functional.allcomplextypes set column stats int_array_col " +
        "('numNulls'='10')",
        "Statistics for column 'int_array_col' are not supported because " +
        "it has type 'ARRAY<INT>'.");
    AnalysisError(
        "alter table functional.allcomplextypes set column stats int_map_col " +
        "('numDVs'='10')",
        "Statistics for column 'int_map_col' are not supported because " +
        "it has type 'MAP<STRING,INT>'.");
    AnalysisError(
        "alter table functional.allcomplextypes set column stats int_struct_col " +
        "('numDVs'='10')",
        "Statistics for column 'int_struct_col' are not supported because " +
        "it has type 'STRUCT<f1:INT,f2:INT>'.");

    // Invalid stats key.
    AnalysisError(
        "alter table functional.alltypes set column stats int_col ('badKey'='10')",
        "Invalid column stats key: badKey");
    AnalysisError(
        "alter table functional.alltypes set column stats " +
        "int_col ('numDVs'='10',''='10')",
        "Invalid column stats key: ");
    // Invalid long stats values.
    AnalysisError(
        "alter table functional.alltypes set column stats int_col ('numDVs'='bad')",
        "Invalid stats value 'bad' for column stats key: numDVs");
    AnalysisError(
        "alter table functional.alltypes set column stats int_col ('numDVs'='-10')",
        "Invalid stats value '-10' for column stats key: numDVs");
    // Invalid float stats values.
    AnalysisError(
        "alter table functional.alltypes set column stats string_col ('avgSize'='bad')",
        "Invalid stats value 'bad' for column stats key: avgSize");
    AnalysisError(
        "alter table functional.alltypes set column stats string_col ('avgSize'='-1.5')",
        "Invalid stats value '-1.5' for column stats key: avgSize");
    AnalysisError(
        "alter table functional.alltypes set column stats string_col ('avgSize'='-0.5')",
        "Invalid stats value '-0.5' for column stats key: avgSize");
    AnalysisError(
        "alter table functional.alltypes set column stats string_col ('avgSize'='NaN')",
        "Invalid stats value 'NaN' for column stats key: avgSize");
    AnalysisError(
        "alter table functional.alltypes set column stats string_col ('avgSize'='inf')",
        "Invalid stats value 'inf' for column stats key: avgSize");
  }

  @Test
  public void TestAlterTableUnSetAvroProperties() {
    //Test unset tblproperties with avro.schema.url and avro.schema.literal
    for (String propertyType : Lists.newArrayList("tblproperties", "serdeproperties")) {
      AnalysisError(String.format("alter table functional.alltypes unset %s " +
          "('avro.schema.url')", propertyType),
          "Unsetting the 'avro.schema.url' table property is not supported for" +
          " Avro table.");
      AnalysisError(String.format("alter table functional.alltypes unset %s " +
          "('avro.schema.literal')", propertyType),
          "Unsetting the 'avro.schema.literal' table property is not supported for" +
          " Avro table.");
    }
  }

  @Test
  public void TestAlterTableSetAvroProperties() {
    // Test set tblproperties with avro.schema.url and avro.schema.literal
    // TODO: Include customized schema files

    for (String propertyType : Lists.newArrayList("tblproperties", "serdeproperties")) {
      // Valid url with valid schema
      AnalyzesOk(String.format("alter table functional.alltypes set %s" +
          "('avro.schema.url'=" +
          "'hdfs:///test-warehouse/avro_schemas/functional/alltypes.json')",
          propertyType));

      // Invalid schema URL
      AnalysisError(String.format("alter table functional.alltypes set %s " +
          "('avro.schema.url'='')", propertyType),
          "Invalid avro.schema.url: . Can not create a Path from an empty string");
      AnalysisError(String.format("alter table functional.alltypes set %s " +
          "('avro.schema.url'='hdfs://invalid*host/schema.avsc')", propertyType),
          "Failed to read Avro schema at: hdfs://invalid*host/schema.avsc. " +
          "Incomplete HDFS URI, no host: hdfs://invalid*host/schema.avsc");
      AnalysisError(String.format("alter table functional.alltypes set %s" +
          "('avro.schema.url'='schema.avsc')", propertyType),
          "Invalid avro.schema.url: schema.avsc. Path does not exist.");
      AnalysisError(String.format("alter table functional.alltypes set %s " +
          "('avro.schema.url'='foo://bar/schema.avsc')", propertyType),
          "Failed to read Avro schema at: foo://bar/schema.avsc. " +
          "No FileSystem for scheme: foo");

      // Valid schema literal
      AnalyzesOk(String.format("alter table functional.alltypes set %s" +
          "('avro.schema.literal'='{\"name\": \"my_record\", \"type\": \"record\", " +
          "\"fields\": [{\"name\": \"string1\", \"type\": \"string\"}] }')",
          propertyType));

      // Invalid schema
      AnalysisError(String.format("alter table functional.alltypes set %s " +
          "('avro.schema.literal'='{\"name\": \"my_record\", \"type\": \"record\", " +
          "\"fields\": {\"name\": \"string1\", \"type\": \"string\"}]}')", propertyType),
          "Error parsing Avro schema for table 'functional.alltypes': " +
          "org.codehaus.jackson.JsonParseException: Unexpected close marker ']': " +
          "expected '}'");
      AnalysisError(String.format("alter table functional.alltypes set %s " +
          "('avro.schema.literal'='')", propertyType),
          "Avro schema is null or empty: functional.alltypes");
      AnalysisError(String.format("alter table functional.alltypes set %s " +
          "('avro.schema.literal'='{\"name\": \"my_record\"}')", propertyType),
          "Error parsing Avro schema for table 'functional.alltypes': " +
          "No type: {\"name\":\"my_record\"}");
      AnalysisError(String.format("alter table functional.alltypes set %s " +
          "('avro.schema.literal'='{\"name\":\"my_record\", \"type\": \"record\"}')",
          propertyType), "Error parsing Avro schema for table 'functional.alltypes': " +
          "Record has no fields: {\"name\":\"my_record\",\"type\":\"record\"}");
      AnalysisError(String.format("alter table functional.alltypes set %s " +
          "('avro.schema.literal'='" +
          "{\"type\":\"record\", \"fields\":[ {\"name\":\"fff\",\"type\":\"int\"} ] }')",
          propertyType), "Error parsing Avro schema for table 'functional.alltypes': " +
          "No name in schema: {\"type\":\"record\",\"fields\":[{\"name\":\"fff\"," +
          "\"type\":\"int\"}]}");

      // Unsupported types
      // Union
      AnalysisError(String.format("alter table functional.alltypes set %s " +
          "('avro.schema.literal'='{\"name\": \"my_record\", \"type\": \"record\", " +
          "\"fields\": [{\"name\": \"string1\", \"type\": \"string\"}," +
          "{\"name\": \"union1\", \"type\": [\"float\", \"boolean\"]}]}')",
          propertyType), "Unsupported type 'union' of column 'union1'");

      // Check avro.schema.url and avro.schema.literal evaluation order,
      // skip checking url when literal is provided.
      AnalysisError(String.format("alter table functional.alltypes set %s " +
          "('avro.schema.literal'='{\"name\": \"my_record\", \"type\": \"record\", " +
          "\"fields\": {\"name\": \"string1\", \"type\": \"string\"}]}', " +
          "'avro.schema.url'='')", propertyType),
          "Error parsing Avro schema for table 'functional.alltypes': " +
          "org.codehaus.jackson.JsonParseException: Unexpected close marker ']': " +
          "expected '}'");
      // Url is invalid but ignored because literal is provided.
      AnalyzesOk(String.format("alter table functional.alltypes set %s " +
          "('avro.schema.literal'='{\"name\": \"my_record\", \"type\": \"record\", " +
          "\"fields\": [{\"name\": \"string1\", \"type\": \"string\"}] }', " +
          "'avro.schema.url'='')", propertyType));
      // Even though url is valid, literal has higher priority.
      AnalysisError(String.format("alter table functional.alltypes set %s " +
          "('avro.schema.literal'='', 'avro.schema.url'=" +
          "'hdfs:///test-warehouse/avro_schemas/functional/alltypes.json')",
          propertyType), "Avro schema is null or empty: functional.alltypes");
    }
  }

  @Test
  public void TestAlterTableRename() throws AnalysisException {
    AnalyzesOk("alter table functional.alltypes rename to new_alltypes");
    AnalyzesOk("alter table functional.alltypes rename to functional.new_alltypes");
    AnalysisError("alter table functional.alltypes rename to functional.alltypes",
        "Table already exists: functional.alltypes");
    AnalysisError("alter table functional.alltypes rename to functional.alltypesagg",
        "Table already exists: functional.alltypesagg");

    AnalysisError("alter table functional.table_does_not_exist rename to new_table",
        "Table does not exist: functional.table_does_not_exist");
    AnalysisError("alter table db_does_not_exist.alltypes rename to new_table",
        "Database does not exist: db_does_not_exist");

    // Invalid database/table name.
    AnalysisError("alter table functional.alltypes rename to `???`.new_table",
        "Invalid database name: ???");
    AnalysisError("alter table functional.alltypes rename to functional.`%^&`",
        "Invalid table/view name: %^&");

    AnalysisError(
        "alter table functional.alltypes rename to db_does_not_exist.new_table",
        "Database does not exist: db_does_not_exist");

    // Cannot ALTER TABLE a view.
    AnalysisError("alter table functional.alltypes_view rename to new_alltypes",
        "ALTER TABLE not allowed on a view: functional.alltypes_view");
    // Cannot ALTER TABLE a nested collection.
    AnalysisError("alter table allcomplextypes.int_array_col rename to new_alltypes",
        createAnalysisCtx("functional"),
        "Database does not exist: allcomplextypes");

    // It should be okay to rename an HBase table.
    AnalyzesOk("alter table functional_hbase.alltypes rename to new_alltypes");

    // It should be okay to rename data source tables.
    AnalyzesOk("alter table functional.alltypes_datasource rename to new_datasrc_tbl");
    AnalyzesOk("alter table functional.alltypes_jdbc_datasource rename to " +
        "new_jdbc_datasrc_tbl");
  }

  @Test
  public void TestAlterTableRecoverPartitions() throws CatalogException {
    AnalyzesOk("alter table functional.alltypes recover partitions");
    AnalysisError("alter table baddb.alltypes recover partitions",
        "Could not resolve table reference: 'baddb.alltypes'");
    AnalysisError("alter table functional.badtbl recover partitions",
        "Could not resolve table reference: 'functional.badtbl'");
    AnalysisError("alter table functional.alltypesnopart recover partitions",
        "Table is not partitioned: functional.alltypesnopart");
    AnalysisError("alter table functional.view_view recover partitions",
        "ALTER TABLE not allowed on a view: functional.view_view");
    AnalysisError("alter table allcomplextypes.int_array_col recover partitions",
        createAnalysisCtx("functional"),
        "ALTER TABLE not allowed on a nested collection: allcomplextypes.int_array_col");
    AnalysisError("alter table functional_hbase.alltypes recover partitions",
        "ALTER TABLE RECOVER PARTITIONS must target an HDFS table: " +
        "functional_hbase.alltypes");
  }

  @Test
  public void TestAlterTableSortBy() {
    AnalyzesOk("alter table functional.alltypes sort by (id)");
    AnalyzesOk("alter table functional.alltypes sort by (int_col,id)");
    AnalyzesOk("alter table functional.alltypes sort by (bool_col,int_col,id)");
    AnalyzesOk("alter table functional.alltypes sort by ()");
    AnalysisError("alter table functional.alltypes sort by (id,int_col,id)",
        "Duplicate column in SORT BY list: id");
    AnalysisError("alter table functional.alltypes sort by (id, foo)", "Could not find " +
        "SORT BY column 'foo' in table.");
    AnalysisError("alter table functional_hbase.alltypes sort by (id, foo)",
        "ALTER TABLE SORT BY not supported on HBase tables.");
    // Cannot ALTER TABLE to sort by for data source tables.
    AnalysisError("alter table functional.alltypes_jdbc_datasource sort by (id)",
        "ALTER TABLE SORT BY not allowed on a table STORED BY JDBC: " +
        "functional.alltypes_jdbc_datasource");
  }

  @Test
  public void TestAlterTableSortByZOrder() {
    AnalyzesOk("alter table functional.alltypes sort by zorder (int_col,id)");
    AnalyzesOk("alter table functional.alltypes sort by zorder (bool_col,int_col,id)");
    AnalyzesOk(
      "alter table functional.alltypes sort by zorder (timestamp_col, string_col)");
    AnalyzesOk("alter table functional.alltypes sort by zorder ()");
    AnalysisError("alter table functional.alltypes sort by zorder (id)",
        "SORT BY ZORDER with 1 column is equivalent to SORT BY. Please, use the " +
        "latter, if that was your intention.");
    AnalysisError("alter table functional.alltypes sort by zorder (id,int_col,id)",
        "Duplicate column in SORT BY list: id");
    AnalysisError("alter table functional.alltypes sort by zorder (id, foo)", "Could " +
        "not find SORT BY column 'foo' in table.");
    AnalysisError("alter table functional_hbase.alltypes sort by zorder (id, foo)",
        "ALTER TABLE SORT BY not supported on HBase tables.");
    // Cannot ALTER TABLE to sort by zorder for data source tables.
    AnalysisError("alter table functional.alltypes_jdbc_datasource sort by zorder (id)",
        "ALTER TABLE SORT BY not allowed on a table STORED BY JDBC: " +
        "functional.alltypes_jdbc_datasource");
  }

  @Test
  public void TestAlterBucketedTable() throws AnalysisException {
    AnalyzesOk("alter table functional.bucketed_table rename to bucketed_table_test");
    AnalyzesOk("drop table functional.bucketed_table");

    AnalysisError("alter table functional.bucketed_table add columns (a int)",
        "functional.bucketed_table is a bucketed table. " +
        "Only read operations are supported on such tables.");
    AnalysisError("alter table functional.bucketed_table change col1 default bigint",
        "functional.bucketed_table is a bucketed table. " +
        "Only read operations are supported on such tables.");
    AnalysisError("alter table functional.bucketed_table replace columns (a int)",
        "functional.bucketed_table is a bucketed table. " +
        "Only read operations are supported on such tables.");
    AnalysisError("alter table functional.bucketed_table drop col1",
        "functional.bucketed_table is a bucketed table. " +
        "Only read operations are supported on such tables.");
  }

  @Test
  public void TestAlterView() {
    // View-definition references a table.
    AnalyzesOk("alter view functional.alltypes_view as " +
        "select * from functional.alltypesagg");
    // View-definition references a view.
    AnalyzesOk("alter view functional.alltypes_view as " +
        "select * from functional.alltypes_view_sub");
    // Change column definitions.
    AnalyzesOk("alter view functional.alltypes_view (a, b) as " +
        "select int_col, string_col from functional.alltypes");
    // Change column definitions after renaming columns in select.
    AnalyzesOk("alter view functional.alltypes_view (a, b) as " +
        "select int_col x, string_col y from functional.alltypes");

    // View-definition resulting in Hive-style auto-generated column names.
    AnalyzesOk("alter view functional.alltypes_view as " +
        "select trim('abc'), 17 * 7");

    // Altering a view on a view is ok (alltypes_view is a view on alltypes).
    AnalyzesOk("alter view functional.alltypes_view (aaa, bbb) as " +
        "select * from functional.complex_view");

    // Altering a view with same column as existing one.
    AnalyzesOk("alter view functional.complex_view (abc, xyz) as " +
        "select year, month from functional.alltypes_view");

    // Alter view with joins and aggregates.
    AnalyzesOk("alter view functional.alltypes_view (cnt) as " +
        "select count(distinct x.int_col) from functional.alltypessmall x " +
        "inner join functional.alltypessmall y on (x.id = y.id) group by x.bigint_col");

    // Cannot ALTER VIEW a table.
    AnalysisError("alter view functional.alltypes as " +
        "select * from functional.alltypesagg",
        "ALTER VIEW not allowed on a table: functional.alltypes");
    AnalysisError("alter view functional_hbase.alltypesagg as " +
        "select * from functional.alltypesagg",
        "ALTER VIEW not allowed on a table: functional_hbase.alltypesagg");
    // Target database does not exist.
    AnalysisError("alter view baddb.alltypes_view as " +
        "select * from functional.alltypesagg",
        "Database does not exist: baddb");
    // Target view does not exist.
    AnalysisError("alter view functional.badview as " +
        "select * from functional.alltypesagg",
        "Table does not exist: functional.badview");
    // View-definition statement fails to analyze. Database does not exist.
    AnalysisError("alter view functional.alltypes_view as " +
        "select * from baddb.alltypesagg",
        "Could not resolve table reference: 'baddb.alltypesagg'");
    // View-definition statement fails to analyze. Table does not exist.
    AnalysisError("alter view functional.alltypes_view as " +
        "select * from functional.badtable",
        "Could not resolve table reference: 'functional.badtable'");
    // Duplicate column name.
    AnalysisError("alter view functional.alltypes_view as " +
        "select * from functional.alltypessmall a inner join " +
        "functional.alltypessmall b on a.id = b.id",
        "Duplicate column name: id");
    AnalyzesOk("alter view functional.alltypes_view as select 'abc' as `ㅛㅜ`");
    // Change the view definition to contain a subquery (IMPALA-1797)
    AnalyzesOk("alter view functional.alltypes_view as " +
        "select * from functional.alltypestiny where id in " +
        "(select id from functional.alltypessmall where int_col = 1)");
    // Mismatching number of columns in column definition and view-alteration statement.
    AnalysisError("alter view functional.alltypes_view (a) as " +
        "select int_col, string_col from functional.alltypes",
        "Column-definition list has fewer columns (1) than the " +
        "view-definition query statement returns (2).");
    AnalysisError("alter view functional.alltypes_view (a, b, c) as " +
        "select int_col from functional.alltypes",
        "Column-definition list has more columns (3) than the " +
        "view-definition query statement returns (1).");
    // Duplicate columns in the view-alteration statement.
    AnalysisError("alter view functional.alltypes_view as " +
        "select * from functional.alltypessmall a " +
        "inner join functional.alltypessmall b on a.id = b.id",
        "Duplicate column name: id");
    // Duplicate columns in the column definition.
    AnalysisError("alter view functional.alltypes_view (a, b, a) as " +
        "select int_col, int_col, int_col from functional.alltypes",
        "Duplicate column name: a");

    // Self-referncing view in view definition - SELECT.
    AnalysisError("alter view functional.alltypes_view as " +
        "select * from functional.alltypes_view",
        "Self-reference not allowed on view: functional.alltypes_view");
    AnalysisError("alter view functional.alltypes_view (a, b, c) as " +
        "select smallint_col, int_col, bigint_col from functional.alltypes_view",
        "Self-reference not allowed on view: functional.alltypes_view");
    // Self-referencing view in view definition - UNION.
    AnalysisError("alter view functional.alltypes_view as " +
        "select * from functional.alltypes union all " +
        "select * from functional.alltypes_view",
        "Self-reference not allowed on view: functional.alltypes_view");
    AnalysisError("alter view functional.alltypes_view as " +
        "select * from functional.alltypes union distinct " +
        "select * from functional.alltypes_view",
        "Self-reference not allowed on view: functional.alltypes_view");
    // Self-referencing view via a dependent view.
    AnalysisError("alter view functional.alltypes_view as " +
        "select * from functional.view_view",
        "Self-reference not allowed on view: functional.alltypes_view");
    AnalysisError("alter view functional.alltypes_view (a, b) as " +
        "select smallint_col, int_col from functional.view_view",
        "Self-reference not allowed on view: functional.alltypes_view");
    // Self-referencing view in where caluse.
    AnalysisError("alter view functional.alltypes_view as " +
        "select * from functional.alltypes " +
        "where id > (select sum(tinyint_col) from functional.alltypes_view)",
        "Self-reference not allowed on view: functional.alltypes_view");
    // Self-referencing view in with clause.
    AnalysisError("alter view functional.alltypes_view as " +
        "with temp_view(col_a, col_b, col_c) as " +
        "(select tinyint_col, int_col, bigint_col from functional.alltypes_view) " +
        "select * from temp_view",
        "Self-reference not allowed on view: functional.alltypes_view");
    AnalysisError("alter view functional.alltypes_view as " +
        "with temp_view(col_a, col_b, col_c) as " +
        "(select tinyint_col, int_col, bigint_col from functional.alltypes_view) " +
        "select * from functional.alltypes",
        "Self-reference not allowed on view: functional.alltypes_view");
    AnalysisError("alter view functional.alltypes_view as " +
        "with temp_view(col_a, col_b, col_c) as " +
        "(select tinyint_col, int_col, bigint_col from functional.alltypes_view) " +
        "select * from temp_view union all " +
        "select tinyint_col, int_col, bigint_col from functional.alltypes",
        "Self-reference not allowed on view: functional.alltypes_view");
    AnalysisError("alter view functional.alltypes_view as " +
        "with temp_view(col_a, col_b, col_c) as " +
        "(select tinyint_col, int_col, bigint_col from functional.alltypes_view) " +
        "select * from temp_view union distinct " +
        "select tinyint_col, int_col, bigint_col from functional.alltypes",
        "Self-reference not allowed on view: functional.alltypes_view");

    // IMPALA-7679: Inserting a null column type without an explicit type should
    // throw an error.
    AnalyzesOk("alter view functional.alltypes_view as " +
        "select cast(null as int) as new_col");
    AnalyzesOk("alter view functional.alltypes_view as " +
        "select cast(null as int) as null_col, 1 as one_col");
    AnalysisError("alter view functional.alltypes_view " +
        "as select null as new_col", "Unable to infer the column type for " +
        "column 'new_col'. Use cast() to explicitly specify the column type for " +
        "column 'new_col'.");
  }

  @Test
  public void TestAlterViewSetTblProperties() throws AnalysisException {
    AnalyzesOk("ALTER VIEW functional.alltypes_view SET TBLPROPERTIES " +
        " ('pro1' = 'test1', 'pro2' = 'test2')");
    AnalyzesOk("ALTER VIEW functional.alltypes_view UNSET TBLPROPERTIES " +
        " ('pro1', 'pro2')");

    AnalysisError("ALTER VIEW Foo.Bar SET TBLPROPERTIES " +
        " ('pro1' = 'test1', 'pro2' = 'test2')",
        "Database does not exist: Foo");
    AnalysisError("ALTER VIEW Foo.Bar UNSET TBLPROPERTIES ('pro1', 'pro2')",
        "Database does not exist: Foo");
    AnalysisError("alter view functional_orc_def.mv1_alltypes_jointbl " +
        "set TBLPROPERTIES('a' = 'a')",
        "ALTER VIEW not allowed on a materialized view: " +
        "functional_orc_def.mv1_alltypes_jointbl");
    AnalysisError("alter view functional.alltypes " +
        "set TBLPROPERTIES('a' = 'a')",
        "ALTER VIEW not allowed on a table: functional.alltypes");
    AnalysisError("alter view functional_orc_def.mv1_alltypes_jointbl " +
        "unset TBLPROPERTIES('a')",
        "ALTER VIEW not allowed on a materialized view: " +
        "functional_orc_def.mv1_alltypes_jointbl");
    AnalysisError("alter view functional.alltypes " +
        "unset TBLPROPERTIES('a')",
        "ALTER VIEW not allowed on a table: functional.alltypes");
  }

  @Test
  public void TestAlterViewRename() throws AnalysisException {
    AnalyzesOk("alter view functional.alltypes_view rename to new_view");
    AnalyzesOk("alter view functional.alltypes_view rename to functional.new_view");
    AnalysisError("alter view functional.alltypes_view rename to functional.alltypes",
        "Table already exists: functional.alltypes");
    AnalysisError("alter view functional.alltypes_view rename to functional.alltypesagg",
        "Table already exists: functional.alltypesagg");

    AnalysisError("alter view functional.view_does_not_exist rename to new_view",
        "Table does not exist: functional.view_does_not_exist");
    AnalysisError("alter view db_does_not_exist.alltypes_view rename to new_view",
        "Database does not exist: db_does_not_exist");

    AnalysisError("alter view functional.alltypes_view " +
        "rename to db_does_not_exist.new_view",
        "Database does not exist: db_does_not_exist");

    // Invalid database/table name.
    AnalysisError("alter view functional.alltypes_view rename to `???`.new_view",
        "Invalid database name: ???");
    AnalysisError("alter view functional.alltypes_view rename to functional.`%^&`",
        "Invalid table/view name: %^&");

    // Cannot ALTER VIEW a able.
    AnalysisError("alter view functional.alltypes rename to new_alltypes",
        "ALTER VIEW not allowed on a table: functional.alltypes");
  }

  @Test
  public void TestAlterTableAlterColumn() throws AnalysisException {
    AnalyzesOk("alter table functional_kudu.alltypes alter int_col set default 0");
    AnalyzesOk("alter table functional_kudu.alltypes alter int_col set " +
        "compression LZ4 encoding RLE");
    AnalyzesOk("alter table functional.alltypes alter int_col set comment 'a'");
    AnalyzesOk("alter table functional_kudu.alltypes alter int_col drop default");
    AnalyzesOk("alter table functional_kudu.alltypes alter int_col set comment 'a'");

    AnalysisError("alter table functional_kudu.alltypes alter id set default 0",
        "Cannot set default value for primary key column 'id'");
    AnalysisError("alter table functional_kudu.alltypes alter id drop default",
        "Cannot drop default value for primary key column 'id'");
    AnalysisError("alter table functional_kudu.alltypes alter int_col set default 'a'",
        "Default value 'a' (type: STRING) is not compatible with column 'int_col' " +
        "(type: INT)");
    AnalysisError("alter table functional_kudu.alltypes alter int_col set " +
        "encoding rle compression error", "Unsupported compression algorithm 'ERROR'");
    AnalysisError("alter table functional_kudu.alltypes alter int_col set primary key",
        "Altering a column to be a primary key is not supported.");
    AnalysisError("alter table functional_kudu.alltypes alter int_col set not null",
        "Altering the nullability of a column is not supported.");
    AnalysisError("alter table functional.alltypes alter int_col set compression lz4",
        "Unsupported column options for non-Kudu table: 'int_col INT COMPRESSION LZ4'");
    AnalysisError("alter table functional.alltypes alter int_col drop default",
        "Unsupported column option for non-Kudu table: DROP DEFAULT");
    AnalysisError("alter table functional.alltypes_datasource alter int_col drop " +
        "default", "Unsupported column option for non-Kudu table: DROP DEFAULT");
    AnalysisError("alter table functional.alltypes_jdbc_datasource alter int_col drop " +
        "default", "Unsupported column option for non-Kudu table: DROP DEFAULT");
  }

  ComputeStatsStmt checkComputeStatsStmt(String stmt) throws AnalysisException {
    return checkComputeStatsStmt(stmt, createAnalysisCtx());
  }

  ComputeStatsStmt checkComputeStatsStmt(String stmt, AnalysisContext ctx)
      throws AnalysisException {
    return checkComputeStatsStmt(stmt, ctx, null);
  }

  /**
   * Analyzes 'stmt' and checks that the table-level and column-level SQL that is used
   * to compute the stats is valid. Returns the analyzed statement.
   */
  ComputeStatsStmt checkComputeStatsStmt(String stmt, AnalysisContext ctx,
      String expectedWarning) throws AnalysisException {
    ParseNode parseNode = AnalyzesOk(stmt, ctx, expectedWarning);
    assertTrue(parseNode instanceof ComputeStatsStmt);
    ComputeStatsStmt parsedStmt = (ComputeStatsStmt)parseNode;
    AnalyzesOk(parsedStmt.getTblStatsQuery());
    String colsQuery = parsedStmt.getColStatsQuery();
    if (colsQuery != null) AnalyzesOk(colsQuery);
    return parsedStmt;
  }

  /**
   * In addition to the validation for checkComputeStatsStmt(String), checks that the
   * whitelisted columns match 'expColNames'.
   */
  void checkComputeStatsStmt(String stmt, List<String> expColNames)
      throws AnalysisException {
    ComputeStatsStmt parsedStmt = checkComputeStatsStmt(stmt);
    Set<Column> actCols = parsedStmt.getValidatedColumnWhitelist();
    if (expColNames == null) assertTrue("Expected no whitelist.", actCols == null);
    assertTrue("Expected whitelist.", actCols != null);
    Set<String> actColSet = new HashSet<>();
    for (Column col: actCols) actColSet.add(col.getName());
    Set<String> expColSet = Sets.newHashSet(expColNames);
    assertEquals(actColSet, expColSet);
  }

  @Test
  public void TestComputeStats() throws AnalysisException {
    // Analyze the stmt itself as well as the generated child queries.
    checkComputeStatsStmt("compute stats functional.alltypes");
    checkComputeStatsStmt("compute stats functional_hbase.alltypes");
    // Test that complex-typed columns are ignored.
    checkComputeStatsStmt("compute stats functional.allcomplextypes");
    // Test legal column restriction.
    checkComputeStatsStmt("compute stats functional.alltypes (int_col, double_col)",
        Lists.newArrayList("int_col", "double_col"));
    // Test legal column restriction with duplicate columns specified.
    checkComputeStatsStmt(
        "compute stats functional.alltypes (int_col, double_col, int_col)",
        Lists.newArrayList("int_col", "double_col"));
    // Test empty column restriction.
    checkComputeStatsStmt("compute stats functional.alltypes ()",
        new ArrayList<String>());
    // Test column restriction of a column that does not exist.
    AnalysisError("compute stats functional.alltypes(int_col, bogus_col, double_col)",
        "bogus_col not found in table:");
    // Test column restriction of a column with an unsupported type.
    AnalysisError("compute stats functional.allcomplextypes(id, map_map_col)",
        "COMPUTE STATS not supported for column");
    // Test column restriction of an Hdfs table partitioning column.
    AnalysisError("compute stats functional.stringpartitionkey(string_col)",
        "COMPUTE STATS not supported for partitioning");
    // Test column restriction of an HBase key column.
    checkComputeStatsStmt("compute stats functional_hbase.testtbl(id)",
        Lists.newArrayList("id"));

    // Cannot compute stats on a database.
    AnalysisError("compute stats tbl_does_not_exist",
        "Could not resolve table reference: 'tbl_does_not_exist'");
    // Cannot compute stats on a view.
    AnalysisError("compute stats functional.alltypes_view",
        "COMPUTE STATS not supported for view: functional.alltypes_view");

    AnalyzesOk("compute stats functional_avro_snap.alltypes");
    // Test mismatched column definitions and Avro schema (HIVE-6308, IMPALA-867).
    // See testdata/avro_schema_resolution/create_table.sql for the CREATE TABLE stmts.
    // Mismatched column type is ok because the conflict is resolved in favor of
    // the type in the column definition list in the CREATE TABLE.
    AnalyzesOk("compute stats functional_avro_snap.alltypes_type_mismatch");
    // Missing column definition is ok because the schema mismatch is resolved
    // in the CREATE TABLE.
    AnalyzesOk("compute stats functional_avro_snap.alltypes_missing_coldef");
    // Extra column definition is ok because the schema mismatch is resolved
    // in the CREATE TABLE.
    AnalyzesOk("compute stats functional_avro_snap.alltypes_extra_coldef");
    // No column definitions are ok.
    AnalyzesOk("compute stats functional_avro_snap.alltypes_no_coldef");
    // Mismatched column name (table was created by Hive).
    AnalysisError("compute stats functional_avro_snap.schema_resolution_test",
        "Cannot COMPUTE STATS on Avro table 'schema_resolution_test' because its " +
        "column definitions do not match those in the Avro schema.\nDefinition of " +
        "column 'col1' of type 'string' does not match the Avro-schema column " +
        "'boolean1' of type 'BOOLEAN' at position '0'.\nPlease re-create the table " +
        "with column definitions, e.g., using the result of 'SHOW CREATE TABLE'");

    // Test tablesample clause with extrapolation enabled/disabled. Replace/restore the
    // static backend config for this test to control stats extrapolation.
    TBackendGflags gflags = BackendConfig.INSTANCE.getBackendCfg();
    boolean origEnableStatsExtrapolation = gflags.isEnable_stats_extrapolation();
    try {
      // Setup for testing combinations of extrapolation config options.
      addTestDb("extrap_config", null);
      addTestTable("create table extrap_config.tbl_prop_unset (i int)");
      addTestTable("create table extrap_config.tbl_prop_false (i int) " +
          "tblproperties('impala.enable.stats.extrapolation'='false')");
      addTestTable("create table extrap_config.tbl_prop_true (i int) " +
          "tblproperties('impala.enable.stats.extrapolation'='true')");
      String stmt = "compute stats %s tablesample system (10)";
      String err = "COMPUTE STATS TABLESAMPLE requires stats extrapolation";

      // Test --enable_stats_extrapolation=false
      gflags.setEnable_stats_extrapolation(false);
      // Table property unset --> Extrapolation disabled
      AnalysisError(String.format(stmt, "extrap_config.tbl_prop_unset"), err);
      // Table property false --> Extrapolation disabled
      AnalysisError(String.format(stmt, "extrap_config.tbl_prop_false"), err);
      // Table property true --> Extrapolation enabled
      AnalyzesOk(String.format(stmt, "extrap_config.tbl_prop_true"));

      // Test --enable_stats_extrapolation=true
      gflags.setEnable_stats_extrapolation(true);
      // Table property unset --> Extrapolation enabled
      AnalyzesOk(String.format(stmt, "extrap_config.tbl_prop_unset"));
      // Table property false --> Extrapolation disabled
      AnalysisError(String.format(stmt, "extrap_config.tbl_prop_false"), err);
      // Table property true --> Extrapolation enabled
      AnalyzesOk(String.format(stmt, "extrap_config.tbl_prop_true"));

      // Test file formats.
      gflags.setEnable_stats_extrapolation(true);
      checkComputeStatsStmt("compute stats functional.alltypes tablesample system (10)");
      checkComputeStatsStmt(
          "compute stats functional.alltypes tablesample system (55) repeatable(1)");
      AnalysisError("compute stats functional.alltypes tablesample system (101)",
          "Invalid percent of bytes value '101'. " +
          "The percent of bytes to sample must be between 0 and 100.");
      AnalysisError("compute stats functional_kudu.alltypes tablesample system (1)",
          "TABLESAMPLE is only supported on HDFS tables.");
      AnalysisError("compute stats functional_hbase.alltypes tablesample system (2)",
          "TABLESAMPLE is only supported on HDFS tables.");
      AnalysisError(
          "compute stats functional.alltypes_datasource tablesample system (3)",
          "TABLESAMPLE is only supported on HDFS tables.");

      // Test file formats with columns whitelist.
      gflags.setEnable_stats_extrapolation(true);
      checkComputeStatsStmt(
          "compute stats functional.alltypes (int_col, double_col) tablesample " +
          "system (55) repeatable(1)",
          Lists.newArrayList("int_col", "double_col"));
      AnalysisError("compute stats functional.alltypes tablesample system (101)",
          "Invalid percent of bytes value '101'. " +
          "The percent of bytes to sample must be between 0 and 100.");
      AnalysisError("compute stats functional_kudu.alltypes tablesample system (1)",
          "TABLESAMPLE is only supported on HDFS tables.");
      AnalysisError("compute stats functional_hbase.alltypes tablesample system (2)",
          "TABLESAMPLE is only supported on HDFS tables.");
      AnalysisError(
          "compute stats functional.alltypes_datasource tablesample system (3)",
          "TABLESAMPLE is only supported on HDFS tables.");

      // Test different COMPUTE_STATS_MIN_SAMPLE_BYTES.
      TQueryOptions queryOpts = new TQueryOptions();

      // The default minimum sample size is greater than 'functional.alltypes'.
      // We expect TABLESAMPLE to be ignored.
      Preconditions.checkState(
          queryOpts.compute_stats_min_sample_size == 1024 * 1024 * 1024);
      ComputeStatsStmt noSamplingStmt = checkComputeStatsStmt(
          "compute stats functional.alltypes tablesample system (10) repeatable(1)",
          createAnalysisCtx(queryOpts),
          "Ignoring TABLESAMPLE because the effective sampling rate is 100%");
      Assert.assertTrue(noSamplingStmt.getEffectiveSamplingPerc() == 1.0);
      String tblStatsQuery = noSamplingStmt.getTblStatsQuery().toUpperCase();
      Assert.assertTrue(!tblStatsQuery.contains("TABLESAMPLE"));
      Assert.assertTrue(!tblStatsQuery.contains("SAMPLED_NDV"));
      String colStatsQuery = noSamplingStmt.getColStatsQuery().toUpperCase();
      Assert.assertTrue(!colStatsQuery.contains("TABLESAMPLE"));
      Assert.assertTrue(!colStatsQuery.contains("SAMPLED_NDV"));

      // No minimum sample bytes.
      queryOpts.setCompute_stats_min_sample_size(0);
      checkComputeStatsStmt("compute stats functional.alltypes tablesample system (10)",
          createAnalysisCtx(queryOpts));
      checkComputeStatsStmt(
          "compute stats functional.alltypes tablesample system (55) repeatable(1)",
          createAnalysisCtx(queryOpts));

      // Sample is adjusted based on the minimum sample bytes.
      // Assumes that functional.alltypes has 24 files of roughly 20KB each.
      // The baseline statement with no sampling minimum should select exactly one file
      // and have an effective sampling rate of ~0.04 (1/24).
      queryOpts.setCompute_stats_min_sample_size(0);
      ComputeStatsStmt baselineStmt = checkComputeStatsStmt(
          "compute stats functional.alltypes tablesample system (1) repeatable(1)",
          createAnalysisCtx(queryOpts));
      // Approximate validation of effective sampling rate.
      Assert.assertTrue(baselineStmt.getEffectiveSamplingPerc() > 0.03);
      Assert.assertTrue(baselineStmt.getEffectiveSamplingPerc() < 0.05);
      // The adjusted statement with a 100KB minimum should select ~5 files and have
      // an effective sampling rate of ~0.21 (5/24).
      queryOpts.setCompute_stats_min_sample_size(100 * 1024);
      ComputeStatsStmt adjustedStmt = checkComputeStatsStmt(
          "compute stats functional.alltypes tablesample system (1) repeatable(1)",
          createAnalysisCtx(queryOpts));
      // Approximate validation to avoid flakiness due to sampling and file size
      // changes. Expect a sample between 4 and 6 of the 24 total files.
      Assert.assertTrue(adjustedStmt.getEffectiveSamplingPerc() >= 4.0 / 24);
      Assert.assertTrue(adjustedStmt.getEffectiveSamplingPerc() <= 6.0 / 24);
    } finally {
      gflags.setEnable_stats_extrapolation(origEnableStatsExtrapolation);
    }
  }

  @Test
  public void TestComputeIncrementalStats() throws AnalysisException {
    checkComputeStatsStmt("compute incremental stats functional.alltypes");
    checkComputeStatsStmt(
        "compute incremental stats functional.alltypes partition(year=2010, month=10)");
    checkComputeStatsStmt(
        "compute incremental stats functional.alltypes partition(year<=2010)");

    AnalysisError(
        "compute incremental stats functional.alltypes partition(year=9999, month=10)",
        "No matching partition(s) found.");
    AnalysisError(
        "compute incremental stats functional.alltypes partition(year=2010, month)",
        "Partition expr requires return type 'BOOLEAN'. Actual type is 'INT'.");

    // Test that NULL partitions generates a valid query
    checkComputeStatsStmt("compute incremental stats functional.alltypesagg " +
        "partition(year=2010, month=1, day is NULL)");

    AnalysisError("compute incremental stats functional_hbase.alltypes " +
        "partition(year=2010, month=1)", "COMPUTE INCREMENTAL ... PARTITION not " +
        "supported for non-HDFS table functional_hbase.alltypes");

    AnalysisError("compute incremental stats functional.view_view",
        "COMPUTE STATS not supported for view: functional.view_view");

    checkComputeStatsStmt("compute incremental stats functional.alltypes " +
        "(tinyint_col, smallint_col)");
    checkComputeStatsStmt(
        "compute incremental stats functional.alltypes partition(year=2010, month=10)" +
        "(tinyint_col, smallint_col)");
    checkComputeStatsStmt(
        "compute incremental stats functional.alltypes partition(year<=2010)" +
        "(tinyint_col, smallint_col)");

    long bytes = BackendConfig.INSTANCE.getBackendCfg().getInc_stats_size_limit_bytes();
    // functional.alltypes has 24 partitions and 13 columns.
    long statsBytes = 24 * 13 * HdfsTable.STATS_SIZE_PER_COLUMN_BYTES - 1;
    // Change the value of inc_stats_size_limit_bytes to a small value for unit test
    BackendConfig.INSTANCE.getBackendCfg().setInc_stats_size_limit_bytes(statsBytes);

    // Calculate all partitions, the Incremental stats size exceeds the limit
    AnalysisError("compute incremental stats functional.alltypes",
        "Incremental stats size estimate exceeds "
        + PrintUtils.printBytes(statsBytes)
        + ". Please try COMPUTE STATS instead.");

    // All partitions of functional.alltypes have no incremental statistics,
    // so if only one partition is calculated,
    // "Incremental stats size estimate exceeds " should not be reported
    checkComputeStatsStmt(
        "compute incremental stats functional.alltypes partition(year=2010, month=10)");

    //functional.alltypes has 13 columns.
    statsBytes = 12 * 13 * HdfsTable.STATS_SIZE_PER_COLUMN_BYTES - 1;
    BackendConfig.INSTANCE.getBackendCfg().setInc_stats_size_limit_bytes(statsBytes);

    // Calculate 12 partitions, the Incremental stats size exceeds the limit
    AnalysisError("compute incremental stats functional.alltypes partition(year=2009)",
        "Incremental stats size estimate exceeds "
        + PrintUtils.printBytes(statsBytes)
        + ". Please try COMPUTE STATS instead.");

    // Calculate 11 partitions,
    checkComputeStatsStmt(
        "compute incremental stats functional.alltypes partition(year=2009, month<12)");

    BackendConfig.INSTANCE.getBackendCfg().setInc_stats_size_limit_bytes(bytes);
  }


  @Test
  public void TestDropIncrementalStats() throws AnalysisException {
    AnalyzesOk(
        "drop incremental stats functional.alltypes partition(year=2010, month=10)");
    AnalyzesOk(
        "drop incremental stats functional.alltypes partition(year<=2010, month=10)");
    AnalysisError(
        "drop incremental stats functional.alltypes partition(year=9999, month=10)",
        "No matching partition(s) found.");
  }


  @Test
  public void TestDropStats() throws AnalysisException {
    AnalyzesOk("drop stats functional.alltypes");

    // Table does not exist
    AnalysisError("drop stats tbl_does_not_exist",
        "Could not resolve table reference: 'tbl_does_not_exist'");
    // Database does not exist
    AnalysisError("drop stats no_db.no_tbl",
        "Could not resolve table reference: 'no_db.no_tbl'");

    AnalysisError("drop stats functional.alltypes partition(year=2010, month=10)",
        "Syntax error");
    AnalysisError("drop stats functional.alltypes partition(year, month)",
        "Syntax error");
  }

  @Test
  public void TestDrop() throws AnalysisException {
    AnalyzesOk("drop database functional");
    AnalyzesOk("drop database functional cascade");
    AnalyzesOk("drop database functional restrict");
    AnalyzesOk("drop table functional.alltypes");
    AnalyzesOk("drop view functional.alltypes_view");

    // If the database does not exist, and the user hasn't specified "IF EXISTS",
    // an analysis error should be thrown
    AnalysisError("drop database db_does_not_exist",
        "Database does not exist: db_does_not_exist");
    AnalysisError("drop database db_does_not_exist cascade",
        "Database does not exist: db_does_not_exist");
    AnalysisError("drop database db_does_not_exist restrict",
        "Database does not exist: db_does_not_exist");
    AnalysisError("drop table db_does_not_exist.alltypes",
        "Database does not exist: db_does_not_exist");
    AnalysisError("drop view db_does_not_exist.alltypes_view",
        "Database does not exist: db_does_not_exist");
    // Invalid name reports non-existence instead of invalidity.
    AnalysisError("drop database `???`",
        "Database does not exist: ???");
    AnalysisError("drop database `???` cascade",
        "Database does not exist: ???");
    AnalysisError("drop database `???` restrict",
        "Database does not exist: ???");
    AnalysisError("drop table functional.`%^&`",
        "Table does not exist: functional.%^&");
    AnalysisError("drop view functional.`@#$%`",
        "Table does not exist: functional.@#$%");

    // If the database exist but the table doesn't, and the user hasn't specified
    // "IF EXISTS", an analysis error should be thrown
    AnalysisError("drop table functional.badtable",
        "Table does not exist: functional.badtable");
    AnalysisError("drop view functional.badview",
        "Table does not exist: functional.badview");

    // No error is thrown if the user specifies IF EXISTS
    AnalyzesOk("drop database if exists db_does_not_exist");
    AnalyzesOk("drop database if exists db_does_not_exist cascade");
    AnalyzesOk("drop database if exists db_does_not_exist restrict");

    // No error is thrown if the database does not exist
    AnalyzesOk("drop table if exists db_does_not_exist.alltypes");
    AnalyzesOk("drop view if exists db_does_not_exist.alltypes");
    // No error is thrown if the database table does not exist and IF EXISTS
    // is true
    AnalyzesOk("drop table if exists functional.notbl");
    AnalyzesOk("drop view if exists functional.notbl");

    // Cannot drop a view with DROP TABLE.
    AnalysisError("drop table functional.alltypes_view",
        "DROP TABLE not allowed on a view: functional.alltypes_view");
    // Cannot drop a table with DROP VIEW.
    AnalysisError("drop view functional.alltypes",
        "DROP VIEW not allowed on a table: functional.alltypes");

    // No analysis error for tables that can't be loaded.
    AnalyzesOk("drop table functional.unsupported_binary_partition");
  }

  @Test
  public void TestTruncate() throws AnalysisException {
    AnalyzesOk("truncate table functional.alltypes");
    AnalyzesOk("truncate table if exists functional.alltypes");
    AnalyzesOk("truncate functional.alltypes");
    AnalyzesOk("truncate if exists functional.alltypes");

    // If the database does not exist, an analysis error should be thrown
    AnalysisError("truncate table db_does_not_exist.alltypes",
        "Database does not exist: db_does_not_exist");

    // If the database does not exist, IF EXISTS would run ok
    AnalyzesOk("truncate table if exists db_does_not_exist.alltypes");

    // Invalid name reports non-existence instead of invalidity.
    AnalysisError("truncate table functional.`%^&`",
        "Table does not exist: functional.%^&");

    // If the database exists but the table doesn't, an analysis error should be thrown.
    AnalysisError("truncate table functional.badtable",
        "Table does not exist: functional.badtable");

    // If the database exists but the table doesn't, IF EXISTS would run ok
    AnalyzesOk("truncate if exists functional.badtable");

    // Cannot truncate a non hdfs table.
    AnalysisError("truncate table functional.alltypes_view",
        "TRUNCATE TABLE not supported on non-HDFS table: functional.alltypes_view");
  }

  @Test
  public void TestCreateDataSource() {
    final String DATA_SOURCE_NAME = "TestDataSource1";
    final DataSource DATA_SOURCE = new DataSource(DATA_SOURCE_NAME, "/foo.jar",
        "foo.Bar", "V1");
    catalog_.addDataSource(DATA_SOURCE);
    AnalyzesOk("CREATE DATA SOURCE IF NOT EXISTS " + DATA_SOURCE_NAME +
        " LOCATION '/foo.jar' CLASS 'foo.Bar' API_VERSION 'V1'");
    AnalyzesOk("CREATE DATA SOURCE IF NOT EXISTS " + DATA_SOURCE_NAME.toLowerCase() +
        " LOCATION '/foo.jar' CLASS 'foo.Bar' API_VERSION 'V1'");
    AnalyzesOk("CREATE DATA SOURCE IF NOT EXISTS " + DATA_SOURCE_NAME +
        " LOCATION 'hdfs://localhost:20500/foo.jar' CLASS 'foo.Bar' API_VERSION 'V1'");
    AnalyzesOk("CREATE DATA SOURCE foo LOCATION '/' CLASS '' API_VERSION 'v1'");
    AnalyzesOk("CREATE DATA SOURCE foo LOCATION '/foo.jar' CLASS 'com.bar.Foo' " +
        "API_VERSION 'V1'");
    AnalyzesOk("CREATE DATA SOURCE foo LOCATION '/FOO.jar' CLASS 'COM.BAR.FOO' " +
        "API_VERSION 'v1'");
    AnalyzesOk("CREATE DATA SOURCE foo LOCATION \"/foo.jar\" CLASS \"com.bar.Foo\" " +
        "API_VERSION \"V1\"");
    AnalyzesOk("CREATE DATA SOURCE foo LOCATION '/x/foo@hi_^!#.jar' " +
        "CLASS 'com.bar.Foo' API_VERSION 'V1'");
    AnalyzesOk("CREATE DATA SOURCE foo LOCATION 'hdfs://localhost:20500/a/b/foo.jar' " +
        "CLASS 'com.bar.Foo' API_VERSION 'V1'");
    AnalyzesOk("CREATE DATA SOURCE foo LOCATION 's3a://bucket/a/b/foo.jar' " +
        "CLASS 'com.bar.Foo' API_VERSION 'V1'");
    AnalyzesOk("CREATE DATA SOURCE foo CLASS 'com.bar.Foo' API_VERSION 'V1'");

    AnalysisError("CREATE DATA SOURCE foo LOCATION 'blah://localhost:20500/foo.jar' " +
        "CLASS 'com.bar.Foo' API_VERSION 'V1'",
        "No FileSystem for scheme: blah");
    AnalysisError("CREATE DATA SOURCE " + DATA_SOURCE_NAME + " LOCATION '/foo.jar' " +
        "CLASS 'foo.Bar' API_VERSION 'V1'",
        "Data source already exists: " + DATA_SOURCE_NAME.toLowerCase());
    AnalysisError("CREATE DATA SOURCE foo LOCATION '/foo.jar' " +
        "CLASS 'foo.Bar' API_VERSION 'V2'", "Invalid API version: 'V2'");
  }

  @Test
  public void TestCreateDb() throws AnalysisException {
    AnalyzesOk("create database some_new_database");
    AnalysisError("create database functional", "Database already exists: functional");
    AnalyzesOk("create database if not exists functional");
    // Invalid database name,
    AnalysisError("create database `%^&`", "Invalid database name: %^&");

    // Valid URIs.
    AnalyzesOk("create database new_db location " +
        "'/test-warehouse/new_db'");
    AnalyzesOk("create database new_db location " +
        "'hdfs://localhost:50200/test-warehouse/new_db'");
    AnalyzesOk("create database new_db location " +
        "'s3a://bucket/test-warehouse/new_db'");
    // Invalid URI.
    AnalysisError("create database new_db location " +
        "'blah://bucket/test-warehouse/new_db'",
        "No FileSystem for scheme: blah");

    // URIs for managedlocation.
    AnalyzesOk("create database new_db managedlocation '/test-warehouse/new_db'");
    AnalyzesOk("create database new_db location " +
        "'/test-warehouse/new_db' managedlocation " +
        "'/test-warehouse/new_db'");
    AnalysisError("create database new_db managedlocation " +
        "'blah://bucket/test-warehouse/new_db'",
        "No FileSystem for scheme: blah");
  }

  @Test
  public void TestCreateTableLikeFile() throws AnalysisException {
    // check that we analyze all of the CREATE TABLE options
    AnalyzesOk("create table if not exists newtbl_DNE like parquet "
        + "'/test-warehouse/schemas/alltypestiny.parquet'");
    AnalyzesOk("create table newtbl_DNE like parquet "
        + "'/test-warehouse/schemas/zipcode_incomes.parquet'");
    AnalyzesOk("create table default.newtbl_DNE like parquet "
        + "'/test-warehouse/schemas/zipcode_incomes.parquet'");
    AnalyzesOk("create table newtbl_DNE like parquet "
        + "'/test-warehouse/schemas/zipcode_incomes.parquet' stored as parquet");
    AnalyzesOk("create external table newtbl_DNE like parquet "
        + "'/test-warehouse/schemas/zipcode_incomes.parquet' sort by (id,zip) "
        + "stored as parquet");
    AnalyzesOk("create external table newtbl_DNE like parquet "
        + "'/test-warehouse/schemas/decimal.parquet' sort by zorder (d32, d11) "
        + "stored as parquet");
    AnalyzesOk("create table newtbl_DNE like parquet "
        + "'/test-warehouse/schemas/zipcode_incomes.parquet' sort by (id,zip)");
    AnalyzesOk("create table newtbl_DNE like parquet "
        + "'/test-warehouse/schemas/decimal.parquet' sort by zorder (d32, d11)");
    AnalyzesOk("create table if not exists functional.zipcode_incomes like parquet "
        + "'/test-warehouse/schemas/zipcode_incomes.parquet'");
    AnalyzesOk("create table if not exists newtbl_DNE like parquet "
        + "'/test-warehouse/schemas/zipcode_incomes.parquet'");
    AnalyzesOk("create table if not exists newtbl_DNE like parquet "
        + "'/test-warehouse/schemas/decimal.parquet'");
    AnalyzesOk("create table if not exists newtbl_DNE like parquet'"
        + " /test-warehouse/schemas/enum/enum.parquet'");

    // check we error in the same situations as standard create table
    AnalysisError("create table functional.zipcode_incomes like parquet "
        + "'/test-warehouse/schemas/zipcode_incomes.parquet'",
        "Table already exists: functional.zipcode_incomes");
    AnalysisError("create table database_DNE.newtbl_DNE like parquet "
        + "'/test-warehouse/schemas/zipcode_incomes.parquet'",
        "Database does not exist: database_DNE");

    // check invalid paths
    AnalysisError("create table if not exists functional.zipcode_incomes like parquet " +
        "'/test-warehouse'",
        "Cannot infer schema, path is not a file: hdfs://localhost:20500/test-warehouse");
    AnalysisError("create table newtbl_DNE like parquet 'foobar'",
        "URI path must be absolute: foobar");
    AnalysisError("create table newtbl_DNE like parquet '/not/a/file/path'",
        "Cannot infer schema, path does not exist: " +
        "hdfs://localhost:20500/not/a/file/path");
    AnalysisError("create table if not exists functional.zipcode_incomes like parquet " +
        "'file:///tmp/foobar'",
        "Cannot infer schema, path does not exist: file:/tmp/foobar");

    // check valid paths with bad file contents
    AnalysisError("create table database_DNE.newtbl_DNE like parquet " +
        "'/test-warehouse/zipcode_incomes_rc/000000_0'",
        "File is not a parquet file: " +
        "hdfs://localhost:20500/test-warehouse/zipcode_incomes_rc/000000_0");

    // this is a decimal file without annotations
    AnalysisError("create table if not exists functional.zipcode_incomes like parquet " +
        "'/test-warehouse/schemas/malformed_decimal_tiny.parquet'",
        "Unsupported parquet type FIXED_LEN_BYTE_ARRAY for field c1");

    // Invalid file format
    AnalysisError("create table newtbl_kudu like parquet " +
        "'/test-warehouse/schemas/alltypestiny.parquet' stored as kudu",
        "CREATE TABLE LIKE FILE statement is not supported for Kudu tables.");
    AnalysisError("create table newtbl_jdbc like parquet " +
        "'/test-warehouse/schemas/alltypestiny.parquet' stored as JDBC",
        "CREATE TABLE LIKE FILE statement is not supported for JDBC tables.");
  }

  @Test
  public void TestCreateTableLikeFileOrc() throws AnalysisException {
    Assume.assumeTrue(
        "Skipping this test; CREATE TABLE LIKE ORC is only supported when running " +
            "against Hive-3 or greater", TestUtils.getHiveMajorVersion() >= 3);

    AnalysisError("create table database_DNE.newtbl_DNE like ORC " +
        "'/test-warehouse/schemas/alltypestiny.parquet'",
        "Failed to open file as an ORC file: org.apache.orc.FileFormatException: " +
        "Malformed ORC file " +
        "hdfs://localhost:20500/test-warehouse/schemas/alltypestiny.parquet" +
        ". Invalid postscript.");

    // Inferring primitive and complex types
    AnalyzesOk("create table if not exists newtbl_DNE like orc " +
        "'/test-warehouse/schemas/alltypes_non_acid.orc'");
    AnalyzesOk("create table if not exists newtbl_DNE like orc " +
        "'/test-warehouse/complextypestbl_non_transactional_orc_def/nullable.orc'");

    // check invalid paths
    AnalysisError("create table if not exists functional.zipcode_incomes like ORC " +
        "'/test-warehouse'",
        "Cannot infer schema, path is not a file: hdfs://localhost:20500/test-warehouse");
    AnalysisError("create table newtbl_DNE like ORC 'foobar'",
        "URI path must be absolute: foobar");
    AnalysisError("create table newtbl_DNE like ORC '/not/a/file/path'",
        "Cannot infer schema, path does not exist: " +
        "hdfs://localhost:20500/not/a/file/path");
    AnalysisError("create table if not exists functional.zipcode_incomes like ORC " +
        "'file:///tmp/foobar'",
        "Cannot infer schema, path does not exist: file:/tmp/foobar");
  }

  @Test
  public void TestCreateTableLikeFileOrcWithHive2() throws AnalysisException {
    // Testing if error is thrown when trying to create table like orc file with Hive-2.
    Assume.assumeTrue(TestUtils.getHiveMajorVersion() < 3);

    // Inferring primitive and complex types
    AnalysisError("create table if not exists newtbl_DNE like orc " +
        "'/test-warehouse/alltypestiny_orc_def/year=2009/month=1/000000_0'",
        "Creating table like ORC file is unsupported for Hive with version < 3");
  }

  @Test
  public void TestCreateTableAsSelect() throws AnalysisException {
    // Constant select.
    AnalyzesOk("create table newtbl as select 1+2, 'abc'");

    // Select from partitioned and unpartitioned tables using different
    // queries.
    AnalyzesOk("create table newtbl stored as textfile " +
        "as select * from functional.jointbl");
    AnalyzesOk("create table newtbl stored as parquetfile " +
        "as select * from functional.alltypes");
    AnalyzesOk("create table newtbl stored as parquet " +
        "as select * from functional.alltypes");
    AnalyzesOk("create table newtbl as select int_col from functional.alltypes");

    AnalyzesOk("create table functional.newtbl " +
        "as select count(*) as CNT from functional.alltypes");
    AnalyzesOk("create table functional.tbl as select a.* from functional.alltypes a " +
        "join functional.alltypes b on (a.int_col=b.int_col) limit 1000");
    // CTAS with a select query that requires expression rewrite (IMPALA-6307)
    AnalyzesOk("create table functional.ctas_tbl partitioned by (year) as " +
        "with tmp as (select a.timestamp_col, a.year from functional.alltypes a " +
        "left join functional.alltypes b " +
        "on b.timestamp_col between a.timestamp_col and a.timestamp_col) " +
        "select a.timestamp_col, a.year from tmp a");

    // Caching operations
    AnalyzesOk("create table functional.newtbl cached in 'testPool'" +
        " as select count(*) as CNT from functional.alltypes");
    AnalyzesOk("create table functional.newtbl uncached" +
        " as select count(*) as CNT from functional.alltypes");

    // Table already exists with and without IF NOT EXISTS
    AnalysisError("create table functional.alltypes as select 1",
        "Table already exists: functional.alltypes");
    AnalyzesOk("create table if not exists functional.alltypes as select 1");

    // Database does not exist
    AnalysisError("create table db_does_not_exist.new_table as select 1",
        "Database does not exist: db_does_not_exist");

    // Analysis errors in the SELECT statement
    AnalysisError("create table newtbl as select * from tbl_does_not_exist",
        "Could not resolve table reference: 'tbl_does_not_exist'");
    AnalysisError("create table newtbl as select 1 as c1, 2 as c1",
        "Duplicate column name: c1");

    // Unsupported file formats
    AnalysisError("create table foo stored as sequencefile as select 1",
        "CREATE TABLE AS SELECT does not support the (SEQUENCEFILE) file format. " +
         "Supported formats are: (PARQUET, TEXTFILE, KUDU, ICEBERG)");
    AnalysisError("create table foo stored as RCFILE as select 1",
        "CREATE TABLE AS SELECT does not support the (RCFILE) file format. " +
         "Supported formats are: (PARQUET, TEXTFILE, KUDU, ICEBERG)");

    // CTAS with a WITH clause and inline view (IMPALA-1100)
    AnalyzesOk("create table test_with as with with_1 as (select 1 as int_col from " +
        "functional.alltypes as t1 right join (select 1 as int_col from " +
        "functional.alltypestiny as t1) as t2 on t2.int_col = t1.int_col) " +
        "select * from with_1 limit 10");

    // CTAS with a correlated inline view.
    AnalyzesOk("create table test as select id, item " +
        "from functional.allcomplextypes b, (select item from b.int_array_col) v1");
    // Correlated inline view in WITH clause.
    AnalyzesOk("create table test as " +
        "with w as (select id, item from functional.allcomplextypes b, " +
        "(select item from b.int_array_col) v1) select * from w");
    // CTAS with illegal correlated inline views.
    AnalysisError("create table test as select id, item " +
        "from functional.allcomplextypes b, " +
        "(select item from b.int_array_col, functional.alltypes) v1",
        "Nested query is illegal because it contains a table reference " +
        "'b.int_array_col' correlated with an outer block as well as an " +
        "uncorrelated one 'functional.alltypes':\n" +
        "SELECT item FROM b.int_array_col, functional.alltypes");
    AnalysisError("create table test as " +
        "with w as (select id, item from functional.allcomplextypes b, " +
        "(select item from b.int_array_col, functional.alltypes) v1) select * from w",
        "Nested query is illegal because it contains a table reference " +
        "'b.int_array_col' correlated with an outer block as well as an " +
        "uncorrelated one 'functional.alltypes':\n" +
        "SELECT item FROM b.int_array_col, functional.alltypes");

    // CTAS into partitioned table.
    AnalyzesOk("create table p partitioned by (int_col) as " +
        "select double_col, int_col from functional.alltypes");
    AnalyzesOk("create table p partitioned by (int_col) as " +
        "select sum(double_col), int_col from functional.alltypes group by int_col");
    // At least one non-partition column must be specified.
    AnalysisError("create table p partitioned by (int_col, tinyint_col) as " +
        "select int_col, tinyint_col from functional.alltypes",
        "Number of partition columns (2) must be smaller than the number of columns in " +
        "the select statement (2).");
    // Order of the columns is important and not automatically corrected.
    AnalysisError("create table p partitioned by (int_col) as " +
        "select double_col, int_col, tinyint_col from functional.alltypes",
        "Partition column name mismatch: int_col != tinyint_col");
    AnalysisError("create table p partitioned by (tinyint_col, int_col) as " +
        "select double_col, int_col, tinyint_col from functional.alltypes",
        "Partition column name mismatch: tinyint_col != int_col");

    // CTAS into managed Kudu tables
    AnalyzesOk("create table t primary key (id) partition by hash (id) partitions 3" +
        " stored as kudu as select id, bool_col, tinyint_col, smallint_col, int_col, " +
        "bigint_col, float_col, double_col, date_string_col, string_col " +
        "from functional.alltypestiny");
    AnalyzesOk("create table t non unique primary key (id) partition by hash (id) " +
        "partitions 3 stored as kudu as select id, bool_col, tinyint_col, " +
        "smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, " +
        "string_col from functional.alltypestiny");
    AnalyzesOk("create table t primary key (id) partition by range (id) " +
        "(partition values < 10, partition 20 <= values < 30, partition value = 50) " +
        "stored as kudu as select id, bool_col, tinyint_col, smallint_col, int_col, " +
        "bigint_col, float_col, double_col, date_string_col, string_col " +
        "from functional.alltypestiny");
    AnalyzesOk("create table t primary key (id) partition by hash (id) partitions 3, "+
        "range (id) (partition values < 10, partition 10 <= values < 20, " +
        "partition value = 30) stored as kudu as select id, bool_col, tinyint_col, " +
        "smallint_col, int_col, bigint_col, float_col, double_col, date_string_col, " +
        "string_col from functional.alltypestiny");
    // Creating unpartitioned table results in a warning.
    AnalyzesOk("create table t primary key(id) stored as kudu as select id, bool_col " +
        "from functional.alltypestiny",
        "Unpartitioned Kudu tables are inefficient for large data sizes.");
    // IMPALA-5796: CTAS into a Kudu table with expr rewriting.
    AnalyzesOk("create table t primary key(id) stored as kudu as select id, bool_col " +
        "from functional.alltypestiny where id between 0 and 10");
    // CTAS with a select query that requires expression rewrite (IMPALA-6307)
    AnalyzesOk("create table t primary key(year) stored as kudu as " +
        "with tmp as (select a.timestamp_col, a.year from functional.alltypes a " +
        "left join functional.alltypes b " +
        "on b.timestamp_col between a.timestamp_col and a.timestamp_col) " +
        "select a.timestamp_col, a.year from tmp a");
    // CTAS into Kudu with decimal type
    AnalyzesOk("create table t primary key (id) partition by hash partitions 3" +
        " stored as kudu as select c1 as id from functional.decimal_tiny");

    // CTAS into Kudu with varchar type
    AnalyzesOk("create table t primary key (vc) partition by hash partitions 3" +
        " stored as kudu as select vc from functional.chars_tiny");

    // CTAS in an external Kudu table
    AnalysisError("create external table t stored as kudu " +
        "tblproperties('kudu.table_name'='t') as select id, int_col from " +
        "functional.alltypestiny", "CREATE TABLE AS SELECT is not supported for " +
        "external Kudu tables.");

    // CTAS into Kudu tables with unsupported types
    AnalysisError("create table t primary key (cs) partition by hash partitions 3" +
        " stored as kudu as select cs from functional.chars_tiny",
        "Cannot create table 't': Type CHAR(5) is not supported in Kudu");
    AnalysisError("create table t primary key (id) partition by hash partitions 3" +
        " stored as kudu as select id, m from functional.complextypes_fileformat",
        "Cannot create table 't': Type MAP<STRING,BIGINT> is not supported in Kudu");
    AnalysisError("create table t primary key (id) partition by hash partitions 3" +
        " stored as kudu as select id, a from functional.complextypes_fileformat",
        "Cannot create table 't': Type ARRAY<INT> is not supported in Kudu");

    // IMPALA-6454: CTAS into Kudu tables with primary key specified in upper case.
    AnalyzesOk("create table part_kudu_tbl primary key(INT_COL, SMALLINT_COL, ID)" +
        " partition by hash(INT_COL, SMALLINT_COL, ID) PARTITIONS 2" +
        " stored as kudu as SELECT INT_COL, SMALLINT_COL, ID, BIGINT_COL," +
        " DATE_STRING_COL, STRING_COL, TIMESTAMP_COL, YEAR, MONTH FROM " +
        " functional.alltypes");
    AnalyzesOk("create table part_kudu_tbl non unique primary key(INT_COL, " +
        "SMALLINT_COL, ID) partition by hash(INT_COL, SMALLINT_COL, ID) " +
        "PARTITIONS 2 stored as kudu as SELECT INT_COL, SMALLINT_COL, ID, " +
        "BIGINT_COL, DATE_STRING_COL, STRING_COL, TIMESTAMP_COL, YEAR, " +
        "MONTH FROM functional.alltypes");
    AnalyzesOk("create table part_kudu_tbl non unique primary key(INT_COL, " +
        "SMALLINT_COL, ID, BIGINT_COL, DATE_STRING_COL, STRING_COL, TIMESTAMP_COL, " +
        "YEAR, MONTH) partition by hash(INT_COL, SMALLINT_COL, ID) " +
        "PARTITIONS 2 stored as kudu as SELECT INT_COL, SMALLINT_COL, ID, " +
        "BIGINT_COL, DATE_STRING_COL, STRING_COL, TIMESTAMP_COL, YEAR, " +
        "MONTH FROM functional.alltypes");
    AnalyzesOk("create table no_part_kudu_tbl non unique primary key(INT_COL, " +
        "SMALLINT_COL, ID) stored as kudu as SELECT INT_COL, SMALLINT_COL, ID, " +
        "BIGINT_COL, DATE_STRING_COL, STRING_COL, TIMESTAMP_COL, YEAR, " +
        "MONTH FROM functional.alltypes");

    // CTAS is not supported for JDBC tables.
    AnalysisError("create table t stored as JDBC as select id, bool_col, tinyint_col " +
        "from functional.alltypestiny",
        "CREATE TABLE AS SELECT does not support the (JDBC) file format. " +
        "Supported formats are: (PARQUET, TEXTFILE, KUDU, ICEBERG)");

    // IMPALA-7679: Inserting a null column type without an explicit type should
    // throw an error.
    AnalyzesOk("create table t as select cast(null as int) as new_col");
    AnalyzesOk("create table t as select cast(null as int) as null_col, 1 as one_col");
    AnalysisError("create table t as select null as new_col",
        "Unable to infer the column type for column 'new_col'. Use cast() to " +
        "explicitly specify the column type for column 'new_col'.");

    // IMPALA-11268: Allow STORED BY as well for storage engines
    AnalyzesOk("create table t primary key (id) " +
        " stored by kudu as select id, bool_col, tinyint_col, smallint_col, " +
        "int_col, bigint_col, float_col, double_col, date_string_col, string_col " +
        "from functional.alltypestiny");
    AnalyzesOk("create table t primary key (id) not enforced " +
        "stored by iceberg as select id, bool_col, int_col, float_col, double_col, " +
        "date_string_col, string_col from functional.alltypestiny");

    // IMPALA-9822 Row Format Delimited is valid only for Text Files
    String[] fileFormats = {"TEXTFILE", "PARQUET", "ICEBERG"};
    for (int i = 0; i < fileFormats.length; ++i) {
      String format = fileFormats[i];
      for (String rowFormat : ImmutableList.of(
               "FIELDS TERMINATED BY ','", "LINES TERMINATED BY ','", "ESCAPED BY ','")) {
        String stmt = String.format(
            "create table new_table row format delimited %s stored as %s as select *"
                + " from functional.child_table", rowFormat, format);
        if (i == 0) {
          // No warrnings for TEXT tables
          AnalyzesOkWithoutWarnings(stmt);
        } else {
          AnalyzesOk(stmt, "'ROW FORMAT DELIMITED " + rowFormat + "' is ignored.");
        }
      }
    }
  }

  @Test
  public void TestCreateTableAsSelectWithHints() throws AnalysisException {
    // Test if CTAS hints are analyzed correctly and that conflicting hints
    // result in error.
    // The tests here are minimal, because other tests already cover this logic:
    // - ParserTests#TestPlanHints tests if hints are set correctly during parsing.
    // - AnalyzeStmtsTest#TestInsertHints tests the analyzes of insert hints, which
    //   is the same as the analyzes of CTAS hints.
    for (String[] hintStyle: hintStyles_) {
      String prefix = hintStyle[0];
      String suffix = hintStyle[1];
      // Test plan hints for partitioned Hdfs tables.
      AnalyzesOk(String.format("create %sshuffle%s table t " +
          "partitioned by (year, month) as select * from functional.alltypes",
          prefix, suffix));
      // Warn on unrecognized hints.
      AnalyzesOk(String.format("create %sbadhint%s table t " +
          "partitioned by (year, month) as select * from functional.alltypes",
          prefix, suffix),
          "INSERT hint not recognized: badhint");
      // Conflicting plan hints.
      AnalysisError(String.format("create %sshuffle,noshuffle%s table t " +
          "partitioned by (year, month) as " +
          "select * from functional.alltypes", prefix, suffix),
          "Conflicting INSERT hints: shuffle and noshuffle");
    }

    // Test default hints using query option.
    AnalysisContext insertCtx = createAnalysisCtx();
    // Test default hints for partitioned Hdfs tables.
    insertCtx.getQueryOptions().setDefault_hints_insert_statement("SHUFFLE");
    AnalyzesOk("create table t partitioned by (year, month) " +
        "as select * from functional.alltypes",
        insertCtx);
    // Warn on unrecognized hints.
    insertCtx.getQueryOptions().setDefault_hints_insert_statement("badhint");
    AnalyzesOk("create table t partitioned by (year, month) " +
        "as select * from functional.alltypes",
        insertCtx,
        "INSERT hint not recognized: badhint");
    // Conflicting plan hints.
    insertCtx.getQueryOptions().setDefault_hints_insert_statement("shuffle:noshuFFLe");
    AnalysisError("create table t partitioned by (year, month) " +
        "as select * from functional.alltypes",
        insertCtx,
        "Conflicting INSERT hints: shuffle and noshuffle");
  }

  @Test
  public void TestCreateTableLike() throws AnalysisException {
    AnalyzesOk("create table if not exists functional.new_tbl like functional.alltypes");
    AnalyzesOk("create table functional.like_view like functional.view_view");
    AnalyzesOk(
        "create table if not exists functional.alltypes like functional.alltypes");
    AnalysisError("create table functional.alltypes like functional.alltypes",
        "Table already exists: functional.alltypes");
    AnalysisError("create table functional.new_table like functional.tbl_does_not_exist",
        "Table does not exist: functional.tbl_does_not_exist");
    AnalysisError("create table functional.new_table like db_does_not_exist.alltypes",
        "Database does not exist: db_does_not_exist");
    // Invalid database name.
    AnalysisError("create table `???`.new_table like functional.alltypes",
        "Invalid database name: ???");
    // Invalid table/view name.
    AnalysisError("create table functional.`^&*` like functional.alltypes",
        "Invalid table/view name: ^&*");
    // Invalid source database/table name reports non-existence instead of invalidity.
    AnalysisError("create table functional.foo like `???`.alltypes",
        "Database does not exist: ???");
    AnalysisError("create table functional.foo like functional.`%^&`",
        "Table does not exist: functional.%^&");
    // Valid URI values.
    AnalyzesOk("create table tbl like functional.alltypes location " +
        "'/test-warehouse/new_table'");
    AnalyzesOk("create table tbl like functional.alltypes location " +
        "'hdfs://localhost:20500/test-warehouse/new_table'");
    // 'file' scheme does not take an authority, so file:/// is equivalent to file://
    // and file:/.
    AnalyzesOk("create table tbl like functional.alltypes location " +
        "'file:///test-warehouse/new_table'");
    AnalyzesOk("create table tbl like functional.alltypes location " +
        "'file://test-warehouse/new_table'");
    AnalyzesOk("create table tbl like functional.alltypes location " +
        "'file:/test-warehouse/new_table'");
    AnalyzesOk("create table tbl like functional.alltypes location " +
        "'s3a://bucket/test-warehouse/new_table'");
    // Invalid URI values.
    AnalysisError("create table tbl like functional.alltypes location " +
        "'foofs://test-warehouse/new_table'",
        "No FileSystem for scheme: foofs");
    AnalysisError("create table functional.baz like functional.alltypes location '  '",
        "URI path cannot be empty.");

    // CREATE TABLE LIKE is only implements cloning between Kudu tables (see IMPALA-4052)
    AnalysisError("create table kudu_tbl like functional.alltypestiny stored as kudu",
        "functional.alltypestiny cannot be cloned into a KUDU table: " +
        "CREATE TABLE LIKE is not supported between Kudu tables and non-Kudu tables.");
    AnalysisError(
        "create table kudu_to_parquet like functional_kudu.alltypes stored as parquet",
        "functional_kudu.alltypes cannot be cloned into a PARQUET table: CREATE "
            + "TABLE LIKE is not supported between Kudu tables and non-Kudu tables.");
    AnalysisError("create table kudu_decimal_tbl_clone sort by (d1, d2) like "
            + "functional_kudu.decimal_tbl",
        "functional_kudu.decimal_tbl cannot be cloned "
            + "because SORT BY is not supported for Kudu tables.");
    AnalysisError(
        "create table alltypestiny_clone sort by (d1, d2) like functional.alltypestiny " +
        "stored as kudu", "functional.alltypestiny cannot be cloned into a KUDU table: " +
        "CREATE TABLE LIKE is not supported between Kudu tables and non-Kudu tables.");
    // Kudu tables with range partitions cannot be cloned
    AnalysisError("create table kudu_jointbl_clone like functional_kudu.jointbl",
        "CREATE TABLE LIKE is not supported for Kudu tables having range partitions.");

    // CREATE TABLE LIKE is not supported for JDBC tables.
    AnalysisError("create table jdbc_tbl like functional.alltypestiny stored as JDBC",
        "CREATE TABLE LIKE is not supported for JDBC tables.");

    // Test sort columns.
    AnalyzesOk("create table tbl sort by (int_col,id) like functional.alltypes");
    AnalysisError("create table tbl sort by (int_col,foo) like functional.alltypes",
        "Could not find SORT BY column 'foo' in table.");

    // Test sort by zorder columns.
    AnalyzesOk("create table tbl sort by zorder (int_col,id) like functional.alltypes");
    AnalyzesOk("create table tbl sort by zorder (float_col,id) like functional.alltypes");
    AnalyzesOk("create table tbl sort by zorder (double_col,id) like " +
        "functional.alltypes");
    AnalyzesOk("create table tbl sort by zorder (string_col,timestamp_col) like " +
        "functional.alltypes");
    AnalysisError("create table tbl sort by zorder (int_col,foo) like " +
        "functional.alltypes", "Could not find SORT BY column 'foo' in table.");
  }

  @Test
  public void TestCreateTable() throws AnalysisException {
    AnalyzesOk("create table functional.new_table (i int)");
    AnalyzesOk("create table if not exists functional.alltypes (i int)");
    AnalysisError("create table functional.alltypes",
        "Table already exists: functional.alltypes");
    AnalysisError("create table functional.alltypes (i int)",
        "Table already exists: functional.alltypes");
    AnalyzesOk("create table functional.new_table (i int) row format delimited fields " +
        "terminated by '|'");

    AnalyzesOk("create table new_table (i int) PARTITIONED BY (d decimal)");
    AnalyzesOk("create table new_table (i int) PARTITIONED BY (d decimal(3,1))");
    AnalyzesOk("create table new_table(d1 decimal, d2 decimal(10), d3 decimal(5, 2))");
    AnalysisError("create table new_table (i int) PARTITIONED BY (d decimal(40,1))",
        "Decimal precision must be <= 38: 40");

    AnalyzesOk("create table new_table(s1 varchar(1), s2 varchar(32672), " +
        "s3 varchar(65535))");
    AnalysisError("create table new_table(s1 varchar(0))",
        "Varchar size must be > 0: 0");
    AnalysisError("create table new_table(s1 varchar(65536))",
        "Varchar size must be <= 65535: 65536");
    AnalysisError("create table new_table(s1 char(0))",
        "Char size must be > 0: 0");
    AnalysisError("create table new_table(s1 Char(256))",
        "Char size must be <= 255: 256");
    AnalyzesOk("create table new_table (i int) PARTITIONED BY (s varchar(3))");
    AnalyzesOk("create table functional.new_table (c char(250))");
    AnalyzesOk("create table new_table (i int) PARTITIONED BY (c char(3))");

     // Primary key and foreign key specification.
    AnalyzesOk("create table foo(id int, year int, primary key (id))");
    AnalyzesOk("create table foo(id int, year int, primary key (id, year))");
    AnalyzesOk("create table foo(id int, year int, primary key (id, year) disable "
        + "novalidate rely)");
    AnalysisError("create table foo(id int, year int, primary key (id, year) enable"
        + " novalidate rely)", "ENABLE feature is not supported yet.");
    AnalysisError("create table foo(id int, year int, primary key (id, year) disable"
        + " validate rely)", "VALIDATE feature is not supported yet.");
    AnalysisError("create table pk(id int, primary key(year))", "PRIMARY KEY column "
        + "'year' does not exist in the table");

    // Foreign key test needs a valid primary key table to pass.
    addTestDb("test_pk_fk", "Test DB for PK/FK tests");
    AnalysisContext ctx = createAnalysisCtx("test_pk_fk");
    AnalysisError("create table foo(seq int, id int, year int, a int, foreign key "
        + "(id, year) references functional.parent_table(id, year) enable novalidate "
        + "rely)", ctx,"ENABLE feature is not supported yet.");
    AnalysisError("create table foo(seq int, id int, year int, a int, foreign key "
        + "(id, year) references functional.parent_table(id, year) disable validate "
        + "rely)", ctx,"VALIDATE feature is not supported yet.");
    AnalyzesOk("create table foo(seq int, id int, year int, a int, "
        + "foreign key(id, year) references functional.parent_table(id, year) "
        + "DISABLE NOVALIDATE RELY)", ctx);
    AnalyzesOk("create table foo(seq int, id int, year int, a int, "
        + "foreign key(id, year) references functional.parent_table(id, year)"
        + " disable novalidate rely)", ctx);
    AnalyzesOk("create table foo(seq int, id int, year int, a int, "
            + "foreign key(id, year) references functional.parent_table(id, year))", ctx);
    AnalysisError("create table fk(id int, year string, foreign key(year) "
        + "references pk2(year))", ctx, "Parent table not found: test_pk_fk.pk2");
    AnalyzesOk("create table fk(id int, year string, foreign key(id, year) references"
        + " functional.parent_table(id, year))", ctx);
    AnalysisError("create table fk(id int, year string, foreign key(id, year) "
        + "references pk(year))", ctx, "The number of foreign key columns should be same"
        + " as the number of parent key columns.");
    AnalysisError("create table fk(id int, foreign key(id) references "
            + "functional.parent_table(foo))", ctx, "Parent column not found: foo");
    AnalysisError("create table fk(id int, foreign key(id) references "
        + "functional.alltypes(int_col))", ctx, "Parent column int_col is not part of "
        + "primary key.");

    {
      // Check that long_properties fail at the analysis layer
      String long_property_key = "";
      for (int i = 0; i < MetaStoreUtil.MAX_PROPERTY_KEY_LENGTH; ++i) {
        long_property_key += 'k';
      }
      String long_property_value = "";
      for (int i = 0; i < MetaStoreUtil.MAX_PROPERTY_VALUE_LENGTH; ++i) {
        long_property_value += 'v';
      }

      // At this point long_property_{key_value} are actually not quite long enough to
      // cause analysis to fail.

      AnalyzesOk("create table new_table (i int) "
          + "with serdeproperties ('" + long_property_key + "'='" + long_property_value
          + "') "
          + "tblproperties ('" + long_property_key + "'='" + long_property_value + "')");

      long_property_key += 'X';
      long_property_value += 'X';
      // Now that long_property_{key,value} are one character longer, they are too long
      // for the analyzer.

      AnalysisError("create table new_table (i int) "
              + "tblproperties ('" + long_property_key + "'='value')",
          "Property key length must be <= " + MetaStoreUtil.MAX_PROPERTY_KEY_LENGTH + ": "
              + (MetaStoreUtil.MAX_PROPERTY_KEY_LENGTH + 1));

      AnalysisError("create table new_table (i int) "
              + "tblproperties ('key'='" + long_property_value + "')",
          "Property value length must be <= " + MetaStoreUtil.MAX_PROPERTY_VALUE_LENGTH
              + ": " + (MetaStoreUtil.MAX_PROPERTY_VALUE_LENGTH + 1));

      AnalysisError("create table new_table (i int) "
              + "with serdeproperties ('" + long_property_key + "'='value')",
          "Serde property key length must be <= " + MetaStoreUtil.MAX_PROPERTY_KEY_LENGTH
              + ": " + (MetaStoreUtil.MAX_PROPERTY_KEY_LENGTH + 1));

      AnalysisError("create table new_table (i int) "
              + "with serdeproperties ('key'='" + long_property_value + "')",
          "Serde property value length must be <= "
              + MetaStoreUtil.MAX_PROPERTY_VALUE_LENGTH + ": "
              + (MetaStoreUtil.MAX_PROPERTY_VALUE_LENGTH + 1));
    }

    // Supported file formats. Exclude Avro since it is tested separately.
    String[] fileFormats = {
        "TEXTFILE", "SEQUENCEFILE", "PARQUET", "PARQUETFILE", "RCFILE", "JSONFILE"};
    String[] fileFormatsStr = {
        "TEXT", "SEQUENCE_FILE", "PARQUET", "PARQUET", "RC_FILE", "JSON"};
    int formatIndx = 0;
    for (String format: fileFormats) {
      for (String create: ImmutableList.of("create table", "create external table")) {
        AnalyzesOk(String.format("%s new_table (i int) " +
            "partitioned by (d decimal) comment 'c' stored as %s", create, format));
        // No column definitions.
        AnalysisError(String.format("%s new_table " +
            "partitioned by (d decimal) comment 'c' stored as %s", create, format),
            "Table requires at least 1 column");
      }
      AnalysisError(String.format("create table t (i int primary key) stored as %s",
          format), String.format("Unsupported column options for file format " +
              "'%s': 'i INT PRIMARY KEY'", fileFormatsStr[formatIndx]));
      formatIndx++;
    }

    for (formatIndx = 0; formatIndx < fileFormats.length; formatIndx++) {
      for (String rowFormat : ImmutableList.of(
               "FIELDS TERMINATED BY ','", "LINES TERMINATED BY ','", "ESCAPED BY ','")) {
        String stmt = String.format(
            "create table new_table (i int) row format delimited %s stored as %s",
            rowFormat, fileFormats[formatIndx]);
        if (formatIndx < 2) {
          // No warrnings for TEXT and SEQUENCE tables
          AnalyzesOkWithoutWarnings(stmt);
        } else {
          AnalyzesOk(stmt, "'ROW FORMAT DELIMITED " + rowFormat + "' is ignored");
        }
      }
    }

    // Note: Backslashes need to be escaped twice - once for Java and once for Impala.
    // For example, if this were a real query the value '\' would be stored in the
    // metastore for the ESCAPED BY field.
    AnalyzesOk("create table functional.new_table (i int) row format delimited fields " +
        "terminated by '\\t' escaped by '\\\\' lines terminated by '\\n'");
    AnalyzesOk("create table functional.new_table (i int) row format delimited fields " +
        "terminated by '\\001' escaped by '\\002' lines terminated by '\\n'");
    AnalyzesOk("create table functional.new_table (i int) row format delimited fields " +
        "terminated by '-2' escaped by '-3' lines terminated by '\\n'");
    AnalyzesOk("create table functional.new_table (i int) row format delimited fields " +
        "terminated by '-128' escaped by '127' lines terminated by '40'");

    AnalysisError("create table functional.new_table (i int) row format delimited " +
        "fields terminated by '-2' escaped by '128' lines terminated by '\\n'",
        "ESCAPED BY values and LINE/FIELD terminators must be specified as a single " +
        "character or as a decimal value in the range [-128:127]: 128");
    AnalysisError("create table functional.new_table (i int) row format delimited " +
        "fields terminated by '-2' escaped by '127' lines terminated by '255'",
        "ESCAPED BY values and LINE/FIELD terminators must be specified as a single " +
        "character or as a decimal value in the range [-128:127]: 255");
    AnalysisError("create table functional.new_table (i int) row format delimited " +
        "fields terminated by '-129' escaped by '127' lines terminated by '\\n'",
        "ESCAPED BY values and LINE/FIELD terminators must be specified as a single " +
        "character or as a decimal value in the range [-128:127]: -129");
    AnalysisError("create table functional.new_table (i int) row format delimited " +
        "fields terminated by '||' escaped by '\\\\' lines terminated by '\\n'",
        "ESCAPED BY values and LINE/FIELD terminators must be specified as a single " +
        "character or as a decimal value in the range [-128:127]: ||");

    // IMPALA-2251: it should not be possible to create text tables with the same
    // delimiter character used for multiple purposes.
    AnalysisError("create table functional.broken_text_table (c int) " +
        "row format delimited fields terminated by '\001' lines terminated by '\001'",
        "Field delimiter and line delimiter have same value: byte 1");
    AnalysisError("create table functional.broken_text_table (c int) " +
         "row format delimited lines terminated by '\001'",
        "Field delimiter and line delimiter have same value: byte 1");
    AnalysisError("create table functional.broken_text_table (c int) " +
        "row format delimited fields terminated by '\012'",
        "Field delimiter and line delimiter have same value: byte 10");
    AnalyzesOk("create table functional.broken_text_table (c int) " +
        "row format delimited escaped by '\001'",
        "Field delimiter and escape character have same value: byte 1. " +
        "Escape character will be ignored");
    AnalyzesOk("create table functional.broken_text_table (c int) " +
        "row format delimited escaped by 'x' lines terminated by 'x'",
        "Line delimiter and escape character have same value: byte 120. " +
        "Escape character will be ignored");

    AnalysisError("create table db_does_not_exist.new_table (i int)",
        "Database does not exist: db_does_not_exist");
    AnalysisError("create table new_table (i int, I string)",
        "Duplicate column name: i");
    AnalysisError("create table new_table (c1 double, col2 int, c1 double, c4 string)",
        "Duplicate column name: c1");
    AnalysisError("create table new_table (i int, s string) PARTITIONED BY (i int)",
        "Duplicate column name: i");
    AnalysisError("create table new_table (i int) PARTITIONED BY (C int, c2 int, c int)",
        "Duplicate column name: c");

    // Unsupported partition-column types.
    AnalysisError("create table new_table (i int) PARTITIONED BY (t timestamp)",
        "Type 'TIMESTAMP' is not supported as partition-column type in column: t");
    AnalysisError("create table new_table (i int) PARTITIONED BY (t binary)",
        "Type 'BINARY' is not supported as partition-column type in column: t");

    // Caching ops
    AnalyzesOk("create table cached_tbl(i int) partitioned by(j int) " +
        "cached in 'testPool'");
    AnalyzesOk("create table cached_tbl(i int) partitioned by(j int) uncached");
    AnalyzesOk("create table cached_tbl(i int) partitioned by(j int) " +
        "location '/test-warehouse/' cached in 'testPool'");
    AnalyzesOk("create table cached_tbl(i int) partitioned by(j int) " +
        "location '/test-warehouse/' uncached");
    AnalysisError("create table cached_tbl(i int) location " +
        "'file:///test-warehouse/cache_tbl' cached in 'testPool'",
        "Location 'file:/test-warehouse/cache_tbl' cannot be cached. " +
        "Please retry without caching: CREATE TABLE ... UNCACHED");

    // Invalid database name.
    AnalysisError("create table `???`.new_table (x int) PARTITIONED BY (y int)",
        "Invalid database name: ???");
    // Invalid table/view name.
    AnalysisError("create table functional.`^&*` (x int) PARTITIONED BY (y int)",
        "Invalid table/view name: ^&*");
    // Valid unicode column names.
    AnalyzesOk("create table new_table (`???` int) PARTITIONED BY (i int)");
    AnalyzesOk("create table new_table (i int) PARTITIONED BY (`^&*` int)");
    // Test HMS constraint on comment length.
    AnalyzesOk(String.format("create table t (i int comment '%s')",
        StringUtils.repeat("c", MetaStoreUtil.CREATE_MAX_COMMENT_LENGTH)));
    AnalysisError(String.format("create table t (i int comment '%s')",
        StringUtils.repeat("c", MetaStoreUtil.CREATE_MAX_COMMENT_LENGTH + 1)),
        "Comment of column 'i' exceeds maximum length of 256 characters:");

    // Valid URI values.
    AnalyzesOk("create table tbl (i int) location '/test-warehouse/new_table'");
    AnalyzesOk("create table tbl (i int) location " +
        "'hdfs://localhost:20500/test-warehouse/new_table'");
    AnalyzesOk("create table tbl (i int) location " +
        "'file:///test-warehouse/new_table'");
    AnalyzesOk("create table tbl (i int) location " +
        "'s3a://bucket/test-warehouse/new_table'");
    AnalyzesOk("ALTER TABLE functional_seq_snap.alltypes SET LOCATION " +
        "'file://test-warehouse/new_table'");

    // Invalid URI values.
    AnalysisError("create table functional.foo (x int) location " +
        "'foofs://test-warehouse/new_table'",
        "No FileSystem for scheme: foofs");
    AnalysisError("create table functional.foo (x int) location " +
        "'  '", "URI path cannot be empty.");
    AnalysisError("ALTER TABLE functional_seq_snap.alltypes SET LOCATION " +
        "'foofs://test-warehouse/new_table'",
        "No FileSystem for scheme: foofs");
    AnalysisError("ALTER TABLE functional_seq_snap.alltypes SET LOCATION " +
        "'  '", "URI path cannot be empty.");

    // Create JDBC tables
    AnalyzesOk("CREATE EXTERNAL TABLE Foo (i int) STORED BY JDBC " +
        "TBLPROPERTIES ('database.type'='a', 'jdbc.url'='b', " +
        "'jdbc.driver'='c', 'driver.url'='d', 'dbcp.username'='e', " +
        "'dbcp.password'='f', 'table'='g')");
    AnalysisError("CREATE TABLE Foo (i int) STORED BY JDBC " +
        "TBLPROPERTIES ('database.type'='a', 'jdbc.url'='b', " +
        "'jdbc.driver'='c', 'driver.url'='d', 'dbcp.username'='e', " +
        "'dbcp.password'='f', 'table'='g')",
        "JDBC table must be created as external table");
    AnalysisError("CREATE EXTERNAL TABLE Foo STORED BY JDBC " +
        "TBLPROPERTIES ('database.type'='a', 'jdbc.url'='b', " +
        "'jdbc.driver'='c', 'driver.url'='d', 'dbcp.username'='e', " +
        "'dbcp.password'='f', 'table'='g')",
        "Table requires at least 1 column");
    AnalysisError("CREATE EXTERNAL TABLE Foo (i int) STORED BY JDBC " +
        "TBLPROPERTIES ('a'='b')",
        "Cannot create table 'Foo': Required JDBC config 'database.type' is not " +
        "present in table properties.");
    AnalysisError("CREATE EXTERNAL TABLE Foo (i int) STORED BY JDBC " +
        "CACHED IN 'testPool'",
        "A JDBC table cannot be cached in HDFS.");
    AnalysisError("CREATE EXTERNAL TABLE Foo (i int) STORED BY JDBC LOCATION " +
        "'/test-warehouse/new_table'", "LOCATION cannot be specified for a JDBC table.");
    AnalysisError("CREATE EXTERNAL TABLE Foo (i int) PARTITIONED BY (d decimal) " +
        "STORED BY JDBC ",
        "PARTITIONED BY cannot be used in a JDBC table.");

    // Create table PRODUCED BY DATA SOURCE
    final String DATA_SOURCE_NAME = "TestDataSource1";
    catalog_.addDataSource(new DataSource(DATA_SOURCE_NAME, "/foo.jar",
        "foo.Bar", "V1"));
    AnalyzesOk("CREATE TABLE DataSrcTable1 (x int) PRODUCED BY DATA SOURCE " +
        DATA_SOURCE_NAME);
    AnalyzesOk("CREATE TABLE DataSrcTable1 (x int) PRODUCED BY DATA SOURCE " +
        DATA_SOURCE_NAME.toLowerCase());
    AnalyzesOk("CREATE TABLE DataSrcTable1 (x int) PRODUCED BY DATA SOURCE " +
        DATA_SOURCE_NAME + "(\"\")");
    AnalyzesOk("CREATE TABLE DataSrcTable1 (a tinyint, b smallint, c int, d bigint, " +
        "e float, f double, g boolean, h string) PRODUCED BY DATA SOURCE " +
        DATA_SOURCE_NAME);
    AnalysisError("CREATE TABLE DataSrcTable1 (x int) PRODUCED BY DATA SOURCE " +
        "not_a_data_src(\"\")", "Data source does not exist");
    for (Type t: Type.getSupportedTypes()) {
      PrimitiveType type = t.getPrimitiveType();
      if (DataSourceTable.isSupportedPrimitiveType(type) || t.isNull()) continue;
      String typeSpec = type.name();
      if (type == PrimitiveType.CHAR || type == PrimitiveType.DECIMAL ||
          type == PrimitiveType.VARCHAR) {
        typeSpec += "(10)";
      }
      AnalysisError("CREATE TABLE DataSrcTable1 (x " + typeSpec + ") PRODUCED " +
          "BY DATA SOURCE " + DATA_SOURCE_NAME,
          "Tables produced by an external data source do not support the column type: " +
          type.name());
    }

    // Tables with sort columns
    AnalyzesOk("create table functional.new_table (i int, j int) sort by (i)");
    AnalyzesOk("create table functional.new_table (i int, j int) sort by (i, j)");
    AnalyzesOk("create table functional.new_table (i int, j int) sort by (j, i)");

    // Tables with sort columns, using Z-ordering
    AnalysisError("create table functional.new_table (i int, j int) sort by zorder (i)",
        "SORT BY ZORDER with 1 column is equivalent to SORT BY. Please, use the " +
        "latter, if that was your intention.");
    AnalyzesOk("create table functional.new_table (i int, j int) sort by zorder (i, j)");
    AnalyzesOk("create table functional.new_table (i int, j int) sort by zorder (j, i)");

    // 'sort.columns' property not supported in table definition.
    AnalysisError("create table Foo (i int) sort by (i) " +
        "tblproperties ('sort.columns'='i')", "Table definition must not contain the " +
        "sort.columns table property. Use SORT BY (...) instead.");

    // 'sort.order' property not supported in table definition.
    AnalysisError("create table Foo (i int, j int) sort by zorder (i, j) " +
        "tblproperties ('sort.order'='ZORDER')", "Table definition must not " +
        "contain the sort.order table property. Use SORT BY ZORDER (...) instead.");

    // Column in sort by list must exist.
    AnalysisError("create table functional.new_table (i int) sort by (j)", "Could not " +
        "find SORT BY column 'j' in table.");

    // Column in sort by zorder list must exist.
    AnalysisError("create table functional.new_table (i int) sort by zorder (j)",
        "Could not find SORT BY column 'j' in table.");

    // Partitioned HDFS table
    AnalyzesOk("create table functional.new_table (i int) PARTITIONED BY (d decimal)" +
        "SORT BY (i)");
    AnalyzesOk("create table functional.new_table (i int, j int) PARTITIONED BY " +
        "(d decimal) sort by zorder (i, j)");
    // Column in sort by list must not be a Hdfs partition column.
    AnalysisError("create table functional.new_table (i int) PARTITIONED BY (d decimal)" +
        "SORT BY (d)", "SORT BY column list must not contain partition column: 'd'");
    // Column in sort by list must not be a Hdfs partition column.
    AnalysisError("create table functional.new_table (i int) PARTITIONED BY (d decimal)" +
        "sort by zorder (i, d)", "SORT BY column list must not contain partition " +
        "column: 'd'");
  }

  @Test
  public void TestCreateBucketedTable() throws AnalysisException {
    AnalyzesOk("CREATE TABLE functional.bucket (i int COMMENT 'hello', s string) " +
        "CLUSTERED BY(i) INTO 24 BUCKETS");
    AnalyzesOk("CREATE TABLE functional.bucket (i int COMMENT 'hello', s string) " +
        "CLUSTERED BY(i) SORT BY (s) INTO 24 BUCKETS");

    // Bucketed table not supported for Kudu, ICEBERG and JDBC table
    AnalysisError("CREATE TABLE functional.bucket (i int COMMENT 'hello', s string) " +
        "CLUSTERED BY (i) INTO 24 BUCKETS STORED BY KUDU", "CLUSTERED BY not " +
        "support fileformat: 'KUDU'");
    AnalysisError("CREATE TABLE functional.bucket (i int COMMENT 'hello', s string) " +
        "CLUSTERED BY (i) INTO 24 BUCKETS STORED BY ICEBERG",
        "CLUSTERED BY not support fileformat: 'ICEBERG'");
    AnalysisError("CREATE TABLE functional.bucket (i int COMMENT 'hello', s string) " +
        "CLUSTERED BY (i) INTO 24 BUCKETS STORED BY JDBC",
        "CLUSTERED BY not support fileformat: 'JDBC'");
    // Bucketed columns must not contain partition column and don't duplicate
    AnalysisError("CREATE TABLE functional.bucket (i int COMMENT 'hello', s string) " +
        "PARTITIONED BY(dt string) CLUSTERED BY (dt) INTO 24 BUCKETS",
        "CLUSTERED BY column list must not contain partition column: 'dt'");
    AnalysisError("CREATE TABLE functional.bucket (i int COMMENT 'hello', s string) " +
        "CLUSTERED BY (i, i) INTO 24 BUCKETS",
        "Duplicate column in CLUSTERED BY list: i");
    AnalysisError("CREATE TABLE functional.bucket (i int COMMENT 'hello', s string) " +
        "CLUSTERED BY (a) INTO 24 BUCKETS",
        "Could not find CLUSTERED BY column 'a' in table.");
    AnalysisError("CREATE TABLE functional.bucket (i int COMMENT 'hello', s string) " +
        "CLUSTERED BY (i) INTO 0 BUCKETS",
        "Bucket's number must be greater than 0.");
    AnalysisError("CREATE TABLE functional.bucket (i int COMMENT 'hello', s string) " +
        "CLUSTERED BY () INTO 12 BUCKETS",
        "Bucket columns must be not null.");
  }

  @Test
  public void TestCreateAvroTest() {
    String alltypesSchemaLoc =
        "hdfs:///test-warehouse/avro_schemas/functional/alltypes.json";

    // Analysis of Avro schemas. Column definitions match the Avro schema exactly.
    // Note: Avro does not have a tinyint and smallint type.
    AnalyzesOk(String.format(
        "create table foo_avro (id int, bool_col boolean, tinyint_col int, " +
        "smallint_col int, int_col int, bigint_col bigint, float_col float," +
        "double_col double, date_string_col string, string_col string, " +
        "timestamp_col timestamp) with serdeproperties ('avro.schema.url'='%s')" +
        "stored as avro", alltypesSchemaLoc));
    AnalyzesOk(String.format(
        "create table foo_avro (id int, bool_col boolean, tinyint_col int, " +
        "smallint_col int, int_col int, bigint_col bigint, float_col float," +
        "double_col double, date_string_col string, string_col string, " +
        "timestamp_col timestamp) stored as avro tblproperties ('avro.schema.url'='%s')",
        alltypesSchemaLoc));
    AnalyzesOk("create table foo_avro (string1 string) stored as avro tblproperties " +
        "('avro.schema.literal'='{\"name\": \"my_record\", \"type\": \"record\", " +
        "\"fields\": [{\"name\": \"string1\", \"type\": \"string\"}]}')");

    // No column definitions.
    AnalyzesOk(String.format(
        "create table foo_avro with serdeproperties ('avro.schema.url'='%s')" +
        "stored as avro", alltypesSchemaLoc));
    AnalyzesOk(String.format(
        "create table foo_avro stored as avro tblproperties ('avro.schema.url'='%s')",
        alltypesSchemaLoc));
    AnalyzesOk("create table foo_avro stored as avro tblproperties " +
        "('avro.schema.literal'='{\"name\": \"my_record\", \"type\": \"record\", " +
        "\"fields\": [{\"name\": \"string1\", \"type\": \"string\"}]}')");

    // Analysis of Avro schemas. Column definitions do not match Avro schema.
    AnalyzesOk(String.format(
        "create table foo_avro (id int) with serdeproperties ('avro.schema.url'='%s')" +
        "stored as avro", alltypesSchemaLoc),
        "Ignoring column definitions in favor of Avro schema.\n" +
        "The Avro schema has 11 column(s) but 1 column definition(s) were given.");
    AnalyzesOk(String.format(
        "create table foo_avro (bool_col boolean, string_col string) " +
        "stored as avro tblproperties ('avro.schema.url'='%s')",
        alltypesSchemaLoc),
        "Ignoring column definitions in favor of Avro schema.\n" +
        "The Avro schema has 11 column(s) but 2 column definition(s) were given.");
    AnalyzesOk("create table foo_avro (string1 string) stored as avro tblproperties " +
        "('avro.schema.literal'='{\"name\": \"my_record\", \"type\": \"record\", " +
        "\"fields\": [{\"name\": \"string1\", \"type\": \"string\"}," +
        "{\"name\": \"string2\", \"type\": \"string\"}]}')",
        "Ignoring column definitions in favor of Avro schema.\n" +
        "The Avro schema has 2 column(s) but 1 column definition(s) were given.");
    // Mismatched name.
    AnalyzesOk(String.format(
        "create table foo_avro (id int, bool_col boolean, tinyint_col int, " +
        "smallint_col int, bad_int_col int, bigint_col bigint, float_col float," +
        "double_col double, date_string_col string, string_col string, " +
        "timestamp_col timestamp) with serdeproperties ('avro.schema.url'='%s')" +
        "stored as avro", alltypesSchemaLoc),
        "Resolved the following name and/or type inconsistencies between the column " +
        "definitions and the Avro schema.\n" +
        "Column definition at position 4:  bad_int_col INT\n" +
        "Avro schema column at position 4: int_col INT\n" +
        "Resolution at position 4: int_col INT\n" +
        "Column definition at position 10:  timestamp_col TIMESTAMP\n" +
        "Avro schema column at position 10: timestamp_col STRING\n" +
        "Resolution at position 10: timestamp_col STRING");
    // Mismatched type.
    AnalyzesOk(String.format(
        "create table foo_avro (id int, bool_col boolean, tinyint_col int, " +
        "smallint_col int, int_col int, bigint_col bigint, float_col float," +
        "double_col bigint, date_string_col string, string_col string, " +
        "timestamp_col timestamp) stored as avro tblproperties ('avro.schema.url'='%s')",
        alltypesSchemaLoc),
        "Resolved the following name and/or type inconsistencies between the column " +
        "definitions and the Avro schema.\n" +
        "Column definition at position 7:  double_col BIGINT\n" +
        "Avro schema column at position 7: double_col DOUBLE\n" +
        "Resolution at position 7: double_col DOUBLE\n" +
        "Column definition at position 10:  timestamp_col TIMESTAMP\n" +
        "Avro schema column at position 10: timestamp_col STRING\n" +
        "Resolution at position 10: timestamp_col STRING");

    // Avro schema is inferred from column definitions.
    AnalyzesOk("create table foo_avro (c1 tinyint, c2 smallint, c3 int, c4 bigint, " +
        "c5 float, c6 double, c7 timestamp, c8 string, c9 char(10), c10 varchar(20)," +
        "c11 decimal(10, 5), c12 struct<f1:int,f2:string>, c13 array<int>," +
        "c14 map<string,string>) stored as avro");
    AnalyzesOk("create table foo_avro (c1 tinyint, c2 smallint, c3 int, c4 bigint, " +
        "c5 float, c6 double, c7 timestamp, c8 string, c9 char(10), c10 varchar(20)," +
        "c11 decimal(10, 5), c12 struct<f1:int,f2:string>, c13 array<int>," +
        "c14 map<string,string>) partitioned by (year int, month int) stored as avro");
    // Neither Avro schema nor column definitions.
    AnalysisError("create table foo_avro stored as avro tblproperties ('a'='b')",
        "An Avro table requires column definitions or an Avro schema.");

    // Invalid schema URL
    AnalysisError("create table foo_avro (i int) stored as avro tblproperties " +
        "('avro.schema.url'='')",
        "Invalid avro.schema.url: . Can not create a Path from an empty string");
    AnalysisError("create table foo_avro (i int) stored as avro tblproperties " +
        "('avro.schema.url'='schema.avsc')",
        "Invalid avro.schema.url: schema.avsc. Path does not exist.");
    AnalysisError("create table foo_avro (i int) stored as avro tblproperties " +
        "('avro.schema.url'='hdfs://invalid*host/schema.avsc')",
        "Failed to read Avro schema at: hdfs://invalid*host/schema.avsc. " +
        "Incomplete HDFS URI, no host: hdfs://invalid*host/schema.avsc");
    AnalysisError("create table foo_avro (i int) stored as avro tblproperties " +
        "('avro.schema.url'='foo://bar/schema.avsc')",
        "Failed to read Avro schema at: foo://bar/schema.avsc. " +
        "No FileSystem for scheme: foo");

    // Decimal parsing
    AnalyzesOk("create table foo_avro (i int) stored as avro tblproperties " +
        "('avro.schema.literal'='{\"name\": \"my_record\", \"type\": \"record\", " +
        "\"fields\": [{\"name\":\"value\",\"type\":{\"type\":\"bytes\", " +
        "\"logicalType\":\"decimal\",\"precision\":5,\"scale\":2}}]}')");
    // Scale not required (defaults to zero).
    AnalyzesOk("create table foo_avro (i int) stored as avro tblproperties " +
        "('avro.schema.literal'='{\"name\": \"my_record\", \"type\": \"record\", " +
        "\"fields\": [{\"name\":\"value\",\"type\":{\"type\":\"bytes\", " +
        "\"logicalType\":\"decimal\",\"precision\":5}}]}')");
    // Precision is always required
    AnalysisError("create table foo_avro (i int) stored as avro tblproperties " +
        "('avro.schema.literal'='{\"name\": \"my_record\", \"type\": \"record\", " +
        "\"fields\": [{\"name\":\"value\",\"type\":{\"type\":\"bytes\", " +
        "\"logicalType\":\"decimal\",\"scale\":5}}]}')",
        "Error parsing Avro schema for table 'default.foo_avro': " +
        "No 'precision' property specified for 'decimal' logicalType");
    // Precision/Scale must be positive integers
    AnalysisError("create table foo_avro (i int) stored as avro tblproperties " +
        "('avro.schema.literal'='{\"name\": \"my_record\", \"type\": \"record\", " +
        "\"fields\": [{\"name\":\"value\",\"type\":{\"type\":\"bytes\", " +
        "\"logicalType\":\"decimal\",\"scale\":5, \"precision\":-20}}]}')",
        "Error parsing Avro schema for table 'default.foo_avro': " +
        "Invalid decimal 'precision' property value: -20");
    AnalysisError("create table foo_avro (i int) stored as avro tblproperties " +
        "('avro.schema.literal'='{\"name\": \"my_record\", \"type\": \"record\", " +
        "\"fields\": [{\"name\":\"value\",\"type\":{\"type\":\"bytes\", " +
        "\"logicalType\":\"decimal\",\"scale\":-1, \"precision\":20}}]}')",
        "Error parsing Avro schema for table 'default.foo_avro': " +
        "Invalid decimal 'scale' property value: -1");

    // Invalid schema (bad JSON - missing opening bracket for "field" array)
    AnalysisError("create table foo_avro (i int) stored as avro tblproperties " +
        "('avro.schema.literal'='{\"name\": \"my_record\", \"type\": \"record\", " +
        "\"fields\": {\"name\": \"string1\", \"type\": \"string\"}]}')",
        "Error parsing Avro schema for table 'default.foo_avro': " +
        "org.codehaus.jackson.JsonParseException: Unexpected close marker ']': "+
        "expected '}'");

    // Map/Array types in Avro schema.
    AnalyzesOk("create table foo_avro (i int) stored as avro tblproperties " +
        "('avro.schema.literal'='{\"name\": \"my_record\", \"type\": \"record\", " +
        "\"fields\": [{\"name\": \"string1\", \"type\": \"string\"}," +
        "{\"name\": \"list1\", \"type\": {\"type\":\"array\", \"items\": \"int\"}}]}')");
    AnalyzesOk("create table foo_avro (i int) stored as avro tblproperties " +
        "('avro.schema.literal'='{\"name\": \"my_record\", \"type\": \"record\", " +
        "\"fields\": [{\"name\": \"string1\", \"type\": \"string\"}," +
        "{\"name\": \"map1\", \"type\": {\"type\":\"map\", \"values\": \"int\"}}]}')");

    // Union is not supported
    AnalysisError("create table foo_avro (i int) stored as avro tblproperties " +
        "('avro.schema.literal'='{\"name\": \"my_record\", \"type\": \"record\", " +
        "\"fields\": [{\"name\": \"string1\", \"type\": \"string\"}," +
        "{\"name\": \"union1\", \"type\": [\"float\", \"boolean\"]}]}')",
        "Unsupported type 'union' of column 'union1'");

    // TODO: Add COLLECTION ITEMS TERMINATED BY and MAP KEYS TERMINATED BY clauses.
    // Test struct complex type.
    AnalyzesOk("create table functional.new_table (" +
        "a struct<f1: int, f2: string, f3: timestamp, f4: boolean>, " +
        "b struct<f1: struct<f11: int>, f2: struct<f21: struct<f22: string>>>, " +
        "c struct<f1: map<int, string>, f2: array<bigint>>," +
        "d struct<f1: struct<f11: map<int, string>, f12: array<bigint>>>)");
    // Test array complex type.
    AnalyzesOk("create table functional.new_table (" +
        "a array<int>, b array<timestamp>, c array<string>, d array<boolean>, " +
        "e array<array<int>>, f array<array<array<string>>>, " +
        "g array<struct<f1: int, f2: string>>, " +
        "h array<map<string,int>>)");
    // Test map complex type.
    AnalyzesOk("create table functional.new_table (" +
        "a map<string, int>, b map<timestamp, boolean>, c map<bigint, float>, " +
        "d array<array<int>>, e array<array<array<string>>>, " +
        "f array<struct<f1: int, f2: string>>," +
        "g array<map<string,int>>)");
    // Cannot partition by a complex column.
    AnalysisError("create table functional.new_table (i int) " +
        "partitioned by (x array<int>)",
        "Type 'ARRAY<INT>' is not supported as partition-column type in column: x");
    AnalysisError("create table functional.new_table (i int) " +
        "partitioned by (x map<int,int>)",
        "Type 'MAP<INT,INT>' is not supported as partition-column type in column: x");
    AnalysisError("create table functional.new_table (i int) " +
        "partitioned by (x struct<f1:int>)",
        "Type 'STRUCT<f1:INT>' is not supported as partition-column type in column: x");

    // Kudu specific clauses used in an Avro table.
    AnalysisError("create table functional.new_table (i int) " +
        "partition by hash(i) partitions 3 stored as avro",
        "Only Kudu tables can use the PARTITION BY clause.");
    AnalysisError("create table functional.new_table (i int primary key) " +
        "stored as avro", "Unsupported column options for file format 'AVRO': " +
        "'i INT PRIMARY KEY'");
  }

  @Test
  public void TestCreateView() throws AnalysisException {
    AnalyzesOk(
        "create view foo_new as select int_col, string_col from functional.alltypes");
    AnalyzesOk("create view functional.foo as select * from functional.alltypes");
    AnalyzesOk("create view if not exists foo as select * from functional.alltypes");
    AnalyzesOk("create view foo (a, b) as select int_col, string_col " +
        "from functional.alltypes");
    AnalyzesOk("create view functional.foo (a, b) as select int_col x, double_col y " +
        "from functional.alltypes");

    // Creating a view on a view is ok (alltypes_view is a view on alltypes).
    AnalyzesOk("create view foo as select * from functional.alltypes_view");
    AnalyzesOk("create view foo (aaa, bbb) as select * from functional.complex_view");

    // Create a view resulting in Hive-style auto-generated column names.
    AnalyzesOk("create view foo as select trim('abc'), 17 * 7");

    // Creating a view on an HBase table is ok.
    AnalyzesOk("create view foo as select * from functional_hbase.alltypesagg");

    // Complex view definition with joins and aggregates.
    AnalyzesOk("create view foo (cnt) as " +
        "select count(distinct x.int_col) from functional.alltypessmall x " +
        "inner join functional.alltypessmall y on (x.id = y.id) group by x.bigint_col");

    // Test different query-statement types as view definition.
    AnalyzesOk("create view foo (a, b) as values(1, 'a'), (2, 'b')");
    AnalyzesOk("create view foo (a, b) as select 1, 'a' union all select 2, 'b'");

    // View with a subquery
    AnalyzesOk("create view test_view_with_subquery as " +
        "select * from functional.alltypestiny t where exists " +
        "(select * from functional.alltypessmall s where s.id = t.id)");

    // Mismatching number of columns in column definition and view-definition statement.
    AnalysisError("create view foo (a) as select int_col, string_col " +
        "from functional.alltypes",
        "Column-definition list has fewer columns (1) than the " +
        "view-definition query statement returns (2).");
    AnalysisError("create view foo (a, b, c) as select int_col " +
        "from functional.alltypes",
        "Column-definition list has more columns (3) than the " +
        "view-definition query statement returns (1).");
    // Duplicate columns in the view-definition statement.
    AnalysisError("create view foo as select * from functional.alltypessmall a " +
        "inner join functional.alltypessmall b on a.id = b.id",
        "Duplicate column name: id");
    // Duplicate columns in the column definition.
    AnalysisError("create view foo (a, b, a) as select int_col, int_col, int_col " +
        "from functional.alltypes",
        "Duplicate column name: a");

    // Invalid database/view/column names.
    AnalysisError("create view `???`.new_view as select 1, 2, 3",
        "Invalid database name: ???");
    AnalysisError("create view `^%&` as select 1, 2, 3",
        "Invalid table/view name: ^%&");
    AnalyzesOk("create view foo as select 1 as `???`");
    AnalyzesOk("create view foo(`%^&`) as select 1");

    // Table/view already exists.
    AnalysisError("create view functional.alltypes as " +
        "select * from functional.alltypessmall ",
        "View already exists: functional.alltypes");
    // Target database does not exist.
    AnalysisError("create view wrongdb.test as " +
        "select * from functional.alltypessmall ",
        "Database does not exist: wrongdb");
    // Source database does not exist,
    AnalysisError("create view foo as " +
        "select * from wrongdb.alltypessmall ",
        "Could not resolve table reference: 'wrongdb.alltypessmall'");
    // Source table does not exist,
    AnalysisError("create view foo as " +
        "select * from wrongdb.alltypessmall ",
        "Could not resolve table reference: 'wrongdb.alltypessmall'");
    // Analysis error in view-definition statement.
    AnalysisError("create view foo as " +
        "select int_col from functional.alltypessmall union all " +
        "select string_col from functional.alltypes",
        "Incompatible return types 'INT' and 'STRING' of exprs " +
        "'int_col' and 'string_col'.");

    AnalyzesOk("create view functional.foo (a) as " +
        "select int_array_col " +
        "from functional.allcomplextypes");
    AnalyzesOk("create view functional.foo (a) as " +
        "select int_map_col " +
        "from functional.allcomplextypes");
    // It's allowed to do the same with struct as it is supported in the select list.
    AnalysisContext ctx = createAnalysisCtx();
    AnalyzesOk("create view functional.foo (a) as " +
        "select tiny_struct from functional_orc_def.complextypes_structs", ctx);

    // IMPALA-7679: Inserting a null column type without an explicit type should
    // throw an error.
    AnalyzesOk("create view v as select cast(null as int) as new_col");
    AnalyzesOk("create view v as select cast(null as int) as null_col, 1 as one_col");
    AnalysisError("create view v as select null as new_col",
        "Unable to infer the column type for column 'new_col'. Use cast() to " +
            "explicitly specify the column type for column 'new_col'.");

    AnalyzesOk("create view v_tblproperties TBLPROPERTIES ('a' = 'a')" +
        "as select cast(null as int) as new_col");
    AnalyzesOk("create view v_tblproperties TBLPROPERTIES ('a' = 'a', 'b' = 'b')" +
        "as select cast(null as int) as new_col");
    AnalysisError("create view functional.view_view TBLPROPERTIES ('a' = 'a')" +
        "as select cast(null as int) as new_col",
        "View already exists: functional.view_view");
  }

  @Test
  public void TestUdf() throws AnalysisException {
    final String symbol =
        "'_Z8IdentityPN10impala_udf15FunctionContextERKNS_10BooleanValE'";
    final String udfSuffix = " LOCATION '/test-warehouse/libTestUdfs.so' " +
        "SYMBOL=" + symbol;
    final String udfSuffixIr = " LOCATION '/test-warehouse/test-udfs.ll' " +
        "SYMBOL=" + symbol;
    final String hdfsPath = "hdfs://localhost:20500/test-warehouse/libTestUdfs.so";
    final String javaFnSuffix = " LOCATION '/test-warehouse/impala-hive-udfs.jar' " +
        "SYMBOL='org.apache.impala.TestUdf'";

    AnalyzesOk("create function foo() RETURNS int" + udfSuffix);
    AnalyzesOk("create function foo(int, int, string) RETURNS int" + udfSuffix);
    AnalyzesOk("create function foo" + javaFnSuffix);
    AnalyzesOk("create function foo(INT) returns INT" + javaFnSuffix);

    // Try some fully qualified function names
    AnalyzesOk("create function functional.B() RETURNS int" + udfSuffix);
    AnalyzesOk("create function functional.B1() RETURNS int" + udfSuffix);
    AnalyzesOk("create function functional.`B1C`() RETURNS int" + udfSuffix);

    // Name with underscore
    AnalyzesOk("create function A_B() RETURNS int" + udfSuffix);

    // Locations for all the udfs types.
    AnalyzesOk("create function foo() RETURNS int LOCATION " +
        "'/test-warehouse/libTestUdfs.so' " +
        "SYMBOL='_Z8IdentityPN10impala_udf15FunctionContextERKNS_10BooleanValE'");
    AnalysisError("create function foo() RETURNS int LOCATION " +
        "'/test-warehouse/libTestUdfs.ll' " +
        "SYMBOL='_Z8IdentityPN10impala_udf15FunctionContextERKNS_10BooleanValE'",
        "Could not load binary: /test-warehouse/libTestUdfs.ll");
    AnalyzesOk("create function foo() RETURNS int LOCATION " +
        "'/test-warehouse/test-udfs.ll' " +
        "SYMBOL='_Z8IdentityPN10impala_udf15FunctionContextERKNS_10BooleanValE'");
    AnalyzesOk("create function foo(int) RETURNS int LOCATION " +
        "'/test-warehouse/test-udfs.ll' SYMBOL='Identity'");

    AnalyzesOk("create function foo() RETURNS int LOCATION " +
        "'/test-warehouse/libTestUdfs.SO' " +
        "SYMBOL='_Z8IdentityPN10impala_udf15FunctionContextERKNS_10BooleanValE'");
    AnalyzesOk("create function foo() RETURNS int LOCATION " +
        "'/test-warehouse/hive-exec.jar' SYMBOL='a'");

    // Test Java UDFs for unsupported types
    AnalysisError("create function foo() RETURNS timestamp LOCATION " +
        "'/test-warehouse/hive-exec.jar' SYMBOL='a'",
        "Type TIMESTAMP is not supported for Java UDFs.");
    AnalysisError("create function foo(timestamp) RETURNS int LOCATION '/a.jar'",
        "Type TIMESTAMP is not supported for Java UDFs.");
    AnalysisError("create function foo() RETURNS date LOCATION " +
        "'/test-warehouse/hive-exec.jar' SYMBOL='a'",
        "Type DATE is not supported for Java UDFs.");
    AnalysisError("create function foo(date) RETURNS int LOCATION '/a.jar'",
        "Type DATE is not supported for Java UDFs.");
    AnalysisError("create function foo() RETURNS decimal LOCATION '/a.jar'",
        "Type DECIMAL(9,0) is not supported for Java UDFs.");
    AnalysisError("create function foo(Decimal) RETURNS int LOCATION '/a.jar'",
        "Type DECIMAL(9,0) is not supported for Java UDFs.");
    AnalysisError("create function foo(char(5)) RETURNS int LOCATION '/a.jar'",
        "Type CHAR(5) is not supported for Java UDFs.");
    AnalysisError("create function foo(varchar(5)) RETURNS int LOCATION '/a.jar'",
        "Type VARCHAR(5) is not supported for Java UDFs.");
    AnalysisError("create function foo() RETURNS CHAR(5) LOCATION '/a.jar'",
        "Type CHAR(5) is not supported for Java UDFs.");
    AnalysisError("create function foo() RETURNS VARCHAR(5) LOCATION '/a.jar'",
        "Type VARCHAR(5) is not supported for Java UDFs.");
    AnalysisError("create function foo() RETURNS CHAR(5)" + udfSuffix,
        "UDFs that use CHAR are not yet supported.");
    AnalysisError("create function foo() RETURNS VARCHAR(5)" + udfSuffix,
        "UDFs that use VARCHAR are not yet supported.");
    AnalysisError("create function foo(CHAR(5)) RETURNS int" + udfSuffix,
        "UDFs that use CHAR are not yet supported.");
    AnalysisError("create function foo(VARCHAR(5)) RETURNS int" + udfSuffix,
        "UDFs that use VARCHAR are not yet supported.");

    AnalyzesOk("create function foo() RETURNS decimal" + udfSuffix);
    AnalyzesOk("create function foo() RETURNS decimal(38,10)" + udfSuffix);
    AnalyzesOk("create function foo(Decimal, decimal(10, 2)) RETURNS int" + udfSuffix);
    AnalysisError("create function foo() RETURNS decimal(100)" + udfSuffix,
        "Decimal precision must be <= 38: 100");
    AnalysisError("create function foo(Decimal(2, 3)) RETURNS int" + udfSuffix,
        "Decimal scale (3) must be <= precision (2)");

    // Varargs
    AnalyzesOk("create function foo(INT...) RETURNS int" + udfSuffix);

    // Prepare/Close functions
    AnalyzesOk("create function foo() returns int" + udfSuffix
        + " prepare_fn='ValidateOpenPrepare'" + " close_fn='ValidateOpenClose'");
    AnalyzesOk("create function foo() returns int" + udfSuffixIr
        + " prepare_fn='ValidateOpenPrepare'" + " close_fn='ValidateOpenClose'");
    AnalyzesOk("create function foo() returns int" + udfSuffixIr
        + " prepare_fn='_Z19ValidateOpenPreparePN10impala_udf15FunctionContextENS0_18FunctionStateScopeE'"
        + " close_fn='_Z17ValidateOpenClosePN10impala_udf15FunctionContextENS0_18FunctionStateScopeE'");
    AnalysisError("create function foo() returns int" + udfSuffix + " prepare_fn=''",
        "Could not find symbol ''");
    AnalysisError("create function foo() returns int" + udfSuffix + " close_fn=''",
        "Could not find symbol ''");
    AnalysisError("create function foo() returns int" + udfSuffix +
        " prepare_fn='FakePrepare'",
        "Could not find function FakePrepare(impala_udf::FunctionContext*, "+
        "impala_udf::FunctionContext::FunctionStateScope) in: ");

    // Try to create a function with the same name as a builtin
    AnalyzesOk("create function sin(double) RETURNS double" + udfSuffix);
    AnalyzesOk("create function sin() RETURNS double" + udfSuffix);
    // Try to create a function in the system database.
    AnalysisError("create function _impala_builtins.sin(double) returns double" +
        udfSuffix, "Cannot modify system database.");
    AnalysisError("create function sin(double) returns double" + udfSuffix,
        createAnalysisCtx("_impala_builtins"), "Cannot modify system database.");
    AnalysisError("create function _impala_builtins.f(double) returns double" +
        udfSuffix, "Cannot modify system database.");

    // Try to drop a function with the same name as builtin.
    addTestFunction("sin", Lists.newArrayList(Type.DOUBLE), false);
    // default.sin(double) function exists.
    AnalyzesOk("drop function sin(double)");
    // default.cos(double) does not exist.
    AnalysisError("drop function cos(double)", "Function does not exist: cos(DOUBLE)");
    AnalysisError("drop function _impala_builtins.sin(double)",
        "Cannot modify system database.");

    // Try to select a function with the same name as builtin.
    // This will call _impala_builtins.sin(1).
    AnalyzesOk("select sin(1)");
    AnalyzesOk("select _impala_builtins.sin(1)");
    AnalyzesOk("select default.sin(1)");
    AnalysisError("select functional.sin(1)", "functional.sin() unknown");

    // Try to create with a bad location
    AnalysisError("create function foo() RETURNS int LOCATION 'bad-location' SYMBOL='c'",
        "URI path must be absolute: bad-location");
    AnalysisError("create function foo LOCATION 'bad-location' SYMBOL='c'",
        "URI path must be absolute: bad-location");
    AnalysisError("create function foo() RETURNS int LOCATION " +
        "'blah://localhost:50200/bad-location' SYMBOL='c'",
        "No FileSystem for scheme: blah");
    AnalysisError("create function foo LOCATION " +
        "'blah://localhost:50200/bad-location' SYMBOL='c'",
        "No FileSystem for scheme: blah");
    AnalysisError("create function foo() RETURNS int LOCATION " +
        "'file:///foo.jar' SYMBOL='c'",
        "Could not load binary: file:///foo.jar");
    AnalysisError("create function foo LOCATION " +
        "'file:///foo.jar' SYMBOL='c'",
        "Could not load binary: file:///foo.jar");

    // Try creating udfs with unknown extensions
    AnalysisError("create function foo() RETURNS int LOCATION '/binary' SYMBOL='a'",
        "Unknown binary type: '/binary'. Binary must end in .jar, .so or .ll");
    AnalysisError("create function foo() RETURNS int LOCATION '/binary.a' SYMBOL='a'",
        "Unknown binary type: '/binary.a'. Binary must end in .jar, .so or .ll");
    AnalysisError("create function foo() RETURNS int LOCATION '/binary.so.' SYMBOL='a'",
        "Unknown binary type: '/binary.so.'. Binary must end in .jar, .so or .ll");

    // Try with missing symbol
    AnalysisError("create function foo() RETURNS int LOCATION '/binary.so'",
        "Argument 'SYMBOL' must be set.");

    // Try with symbols missing in binary and symbols
    AnalysisError("create function foo() RETURNS int LOCATION '/blah.so' " +
        "SYMBOL='ab'", "Could not load binary: /blah.so");
    AnalysisError("create function foo() RETURNS int LOCATION '/binary.JAR' SYMBOL='a'",
        "Could not load binary: /binary.JAR");
    AnalysisError("create function foo LOCATION '/binary.JAR' SYMBOL='a'",
        "Could not load binary: /binary.JAR");
    AnalysisError("create function foo() RETURNS int " +
        "LOCATION '/test-warehouse/libTestUdfs.so' " +
        "SYMBOL='b'", "Could not find function b() in: " + hdfsPath);
    AnalysisError("create function foo() RETURNS int " +
        "LOCATION '/test-warehouse/libTestUdfs.so' " +
        "SYMBOL=''", "Could not find symbol ''");
    AnalysisError("create function foo() RETURNS int " +
        "LOCATION '/test-warehouse/libTestUdfs.so' " +
        "SYMBOL='_ZAB'",
        "Could not find symbol '_ZAB' in: " + hdfsPath);

    // Infer the fully mangled symbol from the signature
    AnalyzesOk("create function foo() RETURNS int " +
        "LOCATION '/test-warehouse/libTestUdfs.so' " + "SYMBOL='NoArgs'");
    // We can't get the return type so any of those will match
    AnalyzesOk("create function foo() RETURNS double " +
        "LOCATION '/test-warehouse/libTestUdfs.so' " + "SYMBOL='NoArgs'");
    // The part the user specifies is case sensitive
    AnalysisError("create function foo() RETURNS int " +
        "LOCATION '/test-warehouse/libTestUdfs.so' " + "SYMBOL='noArgs'",
        "Could not find function noArgs() in: " + hdfsPath);
    // Types no longer match
    AnalysisError("create function foo(int) RETURNS int " +
        "LOCATION '/test-warehouse/libTestUdfs.so' " + "SYMBOL='NoArgs'",
        "Could not find function NoArgs(INT) in: " + hdfsPath);

    // Check we can match identity for all types
    AnalyzesOk("create function identity(boolean) RETURNS int " +
        "LOCATION '/test-warehouse/libTestUdfs.so' " + "SYMBOL='Identity'");
    AnalyzesOk("create function identity(tinyint) RETURNS int " +
        "LOCATION '/test-warehouse/libTestUdfs.so' " + "SYMBOL='Identity'");
    AnalyzesOk("create function identity(smallint) RETURNS int " +
        "LOCATION '/test-warehouse/libTestUdfs.so' " + "SYMBOL='Identity'");
    AnalyzesOk("create function identity(int) RETURNS int " +
        "LOCATION '/test-warehouse/libTestUdfs.so' " + "SYMBOL='Identity'");
    AnalyzesOk("create function identity(bigint) RETURNS int " +
        "LOCATION '/test-warehouse/libTestUdfs.so' " + "SYMBOL='Identity'");
    AnalyzesOk("create function identity(float) RETURNS int " +
        "LOCATION '/test-warehouse/libTestUdfs.so' " + "SYMBOL='Identity'");
    AnalyzesOk("create function identity(double) RETURNS int " +
        "LOCATION '/test-warehouse/libTestUdfs.so' " + "SYMBOL='Identity'");
    AnalyzesOk("create function identity(string) RETURNS int " +
        "LOCATION '/test-warehouse/libTestUdfs.so' " + "SYMBOL='Identity'");
    AnalyzesOk("create function all_types_fn(string, boolean, tinyint, " +
        "smallint, int, bigint, float, double, decimal, date, binary) returns int " +
        "location '/test-warehouse/libTestUdfs.so' symbol='AllTypes'");

    // Try creating functions with illegal function names.
    AnalysisError("create function 123A() RETURNS int" + udfSuffix,
        "Function cannot start with a digit: 123a");
    AnalysisError("create function A.`1A`() RETURNS int" + udfSuffix,
        "Function cannot start with a digit: 1a");
    AnalysisError("create function A.`ABC-D`() RETURNS int" + udfSuffix,
        "Function names must be all alphanumeric or underscore. Invalid name: abc-d");
    AnalysisError("create function baddb.f() RETURNS int" + udfSuffix,
        "Database does not exist: baddb");
    AnalysisError("create function a.b.c() RETURNS int" + udfSuffix,
        "Invalid function name: 'a.b.c'. Expected [dbname].funcname.");
    AnalysisError("create function a.b.c.d(smallint) RETURNS int" + udfSuffix,
        "Invalid function name: 'a.b.c.d'. Expected [dbname].funcname.");

    // Try creating functions with unsupported return/arg types.
    AnalysisError("create function f() RETURNS array<int>" + udfSuffix,
        "Type 'ARRAY<INT>' is not supported in UDFs/UDAs.");
    AnalysisError("create function f(map<string,int>) RETURNS int" + udfSuffix,
        "Type 'MAP<STRING,INT>' is not supported in UDFs/UDAs.");
    AnalysisError("create function f() RETURNS struct<f:int>" + udfSuffix,
        "Type 'STRUCT<f:INT>' is not supported in UDFs/UDAs.");

    // Try dropping functions.
    AnalyzesOk("drop function if exists foo()");
    AnalysisError("drop function foo()", "Function does not exist: foo()");
    AnalyzesOk("drop function if exists a.foo()");
    AnalysisError("drop function a.foo()", "Database does not exist: a");
    AnalyzesOk("drop function if exists foo()");
    AnalyzesOk("drop function if exists foo");
    AnalyzesOk("drop function if exists foo(int...)");
    AnalyzesOk("drop function if exists foo(double, int...)");

    // Add functions default.TestFn(), default.TestFn(double), default.TestFn(String...),
    addTestFunction("TestFn", new ArrayList<ScalarType>(), false);
    addTestFunction("TestFn", Lists.newArrayList(Type.DOUBLE), false);
    addTestFunction("TestFn", Lists.newArrayList(Type.STRING), true);

    AnalysisError("create function TestFn() RETURNS INT " + udfSuffix,
        "Function already exists: testfn()");
    AnalysisError("create function TestFn(double) RETURNS INT " + udfSuffix,
        "Function already exists: testfn(DOUBLE)");
    // Persistent Java UDF name clashes with existing NATIVE function and fails if
    // 'if not exists' is specified.
    AnalysisError("create function TestFn" + javaFnSuffix,
        "Function already exists: testfn()");
    AnalyzesOk("create function if not exists TestFn" + javaFnSuffix);

    // Fn(Double) and Fn(Double...) should be a conflict.
    AnalysisError("create function TestFn(double...) RETURNS INT" + udfSuffix,
        "Function already exists: testfn(DOUBLE)");
    AnalysisError("create function TestFn(double) RETURNS INT" + udfSuffix,
        "Function already exists: testfn(DOUBLE)");

    // Add default.TestFn(int, int)
    addTestFunction("TestFn", Lists.newArrayList(Type.INT, Type.INT), false);
    AnalyzesOk("drop function TestFn(int, int)");
    AnalysisError("drop function TestFn(int, int, int)",
        "Function does not exist: testfn(INT, INT, INT)");

    // Fn(String...) was already added.
    AnalysisError("create function TestFn(String) RETURNS INT" + udfSuffix,
        "Function already exists: testfn(STRING...)");
    AnalysisError("create function TestFn(String...) RETURNS INT" + udfSuffix,
        "Function already exists: testfn(STRING...)");
    AnalysisError("create function TestFn(String, String) RETURNS INT" + udfSuffix,
        "Function already exists: testfn(STRING...)");
    AnalyzesOk("create function TestFn(String, String, Int) RETURNS INT" + udfSuffix);

    // Check function overloading.
    AnalyzesOk("create function TestFn(int) RETURNS INT " + udfSuffix);

    // Create a function with the same signature in a different db
    AnalyzesOk("create function functional.TestFn() RETURNS INT " + udfSuffix);

    AnalyzesOk("drop function TestFn()");
    AnalyzesOk("drop function TestFn(double)");
    AnalyzesOk("drop function TestFn(string...)");
    AnalyzesOk("drop function if exists functional.TestFn");
    AnalysisError("drop function TestFn(double...)",
        "Function does not exist: testfn(DOUBLE...)");
    AnalysisError("drop function TestFn(int)", "Function does not exist: testfn(INT)");
    AnalysisError(
        "drop function functional.TestFn()", "Function does not exist: testfn()");
    AnalysisError("drop function functional.TestFn", "Function does not exist: testfn");

    AnalysisError("create function f() returns int " + udfSuffix +
        "init_fn='a'", "Optional argument 'INIT_FN' should not be set");
    AnalysisError("create function f() returns int " + udfSuffix +
        "serialize_fn='a'", "Optional argument 'SERIALIZE_FN' should not be set");
    AnalysisError("create function f() returns int " + udfSuffix +
        "merge_fn='a'", "Optional argument 'MERGE_FN' should not be set");
    AnalysisError("create function f() returns int " + udfSuffix +
        "finalize_fn='a'", "Optional argument 'FINALIZE_FN' should not be set");
  }

  @Test
  public void TestUda() throws AnalysisException {
    final String loc = " LOCATION '/test-warehouse/libTestUdas.so' ";
    final String hdfsLoc = "hdfs://localhost:20500/test-warehouse/libTestUdas.so";
    AnalyzesOk("create aggregate function foo(int) RETURNS int" + loc +
        "UPDATE_FN='AggUpdate'");
    AnalyzesOk("create aggregate function foo(int) RETURNS int" + loc +
        "UPDATE_FN='AggUpdate' INIT_FN='AggInit'");
    AnalyzesOk("create aggregate function foo(int) RETURNS int" + loc +
        "UPDATE_FN='AggUpdate' INIT_FN='AggInit' MERGE_FN='AggMerge'");
    AnalysisError("create aggregate function foo(int) RETURNS int" + loc +
        "UPDATE_FN='AggUpdate' INIT_FN='AGgInit'",
        "Could not find function AGgInit() returns INT in: " + hdfsLoc);
    AnalyzesOk("create aggregate function foo(int, int) RETURNS int" + loc +
        "UPDATE_FN='AggUpdate'");
    AnalyzesOk("create aggregate function foo(string, double) RETURNS string" + loc +
        "UPDATE_FN='AggUpdate'");

    // TODO: remove these when the BE can execute them
    AnalysisError("create aggregate function foo(int...) RETURNS int" + loc,
        "UDAs with varargs are not yet supported.");
    AnalysisError("create aggregate function "
        + "foo(int, int, int, int, int, int, int , int, int) "
        + "RETURNS int" + loc,
        "UDAs with more than 8 arguments are not yet supported.");

    // Check that CHAR and VARCHAR are not valid UDA argument or return types
    String symbols =
        " UPDATE_FN='_Z9AggUpdatePN10impala_udf15FunctionContextERKNS_6IntValEPS2_' " +
        "INIT_FN='_Z7AggInitPN10impala_udf15FunctionContextEPNS_6IntValE' " +
        "MERGE_FN='_Z8AggMergePN10impala_udf15FunctionContextERKNS_6IntValEPS2_'";
    AnalysisError("create aggregate function foo(CHAR(5)) RETURNS int" + loc + symbols,
        "UDAs with CHAR arguments are not yet supported.");
    AnalysisError("create aggregate function foo(VARCHAR(5)) RETURNS int" + loc + symbols,
        "UDAs with VARCHAR arguments are not yet supported.");
    AnalysisError("create aggregate function foo(int) RETURNS CHAR(5)" + loc + symbols,
        "UDAs with CHAR return type are not yet supported.");
    AnalysisError("create aggregate function foo(int) RETURNS VARCHAR(5)" + loc + symbols,
        "UDAs with VARCHAR return type are not yet supported.");

    // Specify the complete symbol. If the user does this, we can't guess the
    // other function names.
    // TODO: think about these error messages more. Perhaps they can be made
    // more actionable.
    AnalysisError("create aggregate function foo(int) RETURNS int" + loc +
        "UPDATE_FN='_Z9AggUpdatePN10impala_udf15FunctionContextERKNS_6IntValEPS2_'",
        "Could not infer symbol for init() function.");
    AnalysisError("create aggregate function foo(int) RETURNS int" + loc +
        "UPDATE_FN='_Z9AggUpdatePN10impala_udf15FunctionContextERKNS_6IntValEPS2_' " +
        "INIT_FN='_Z7AggInitPN10impala_udf15FunctionContextEPNS_6IntValE'",
        "Could not infer symbol for merge() function.");
    AnalyzesOk("create aggregate function foo(int) RETURNS int" + loc +
        "UPDATE_FN='_Z9AggUpdatePN10impala_udf15FunctionContextERKNS_6IntValEPS2_' " +
        "INIT_FN='_Z7AggInitPN10impala_udf15FunctionContextEPNS_6IntValE' " +
        "MERGE_FN='_Z8AggMergePN10impala_udf15FunctionContextERKNS_6IntValEPS2_'");

    // Try with intermediate type
    AnalyzesOk("create aggregate function foo(int) RETURNS int " +
        "INTERMEDIATE int" + loc + "UPDATE_FN='AggUpdate'");
    AnalyzesOk("create aggregate function foo(int) RETURNS int " +
        "INTERMEDIATE CHAR(10)" + loc + "UPDATE_FN='AggIntermediateUpdate'");
    AnalysisError("create aggregate function foo(int) RETURNS int " +
        "INTERMEDIATE CHAR(10)" + loc + "UPDATE_FN='AggIntermediate' " +
        "INIT_FN='AggIntermediateInit' MERGE_FN='AggIntermediateMerge'" ,
        "Finalize() is required for this UDA.");
    AnalyzesOk("create aggregate function foo(int) RETURNS int " +
        "INTERMEDIATE CHAR(10)" + loc + "UPDATE_FN='AggIntermediate' " +
        "INIT_FN='AggIntermediateInit' MERGE_FN='AggIntermediateMerge' " +
        "FINALIZE_FN='AggIntermediateFinalize'");

    // Udf only arguments must not be set.
    AnalysisError("create aggregate function foo(int) RETURNS int" + loc + "SYMBOL='Bad'",
        "Optional argument 'SYMBOL' should not be set.");

    // Invalid char(0) type.
    AnalysisError("create aggregate function foo(int) RETURNS int " +
        "INTERMEDIATE CHAR(0) LOCATION '/foo.so' UPDATE_FN='b'",
        "Char size must be > 0: 0");
    AnalysisError("create aggregate function foo() RETURNS int" + loc,
        "UDAs must take at least one argument.");
    AnalysisError("create aggregate function foo(int) RETURNS int LOCATION " +
        "'/foo.jar' UPDATE_FN='b'", "Java UDAs are not supported.");

    // Try creating functions with unsupported return/arg types.
    AnalysisError("create aggregate function foo(string, double) RETURNS array<int> " +
        loc + "UPDATE_FN='AggUpdate'",
        "Type 'ARRAY<INT>' is not supported in UDFs/UDAs.");
    AnalysisError("create aggregate function foo(map<string,int>) RETURNS int " +
        loc + "UPDATE_FN='AggUpdate'",
        "Type 'MAP<STRING,INT>' is not supported in UDFs/UDAs.");
    AnalysisError("create aggregate function foo(int) RETURNS struct<f:int> " +
        loc + "UPDATE_FN='AggUpdate'",
        "Type 'STRUCT<f:INT>' is not supported in UDFs/UDAs.");
    AnalysisError("create aggregate function foo(int) RETURNS int " +
        "INTERMEDIATE fixed_uda_intermediate(10) " + loc + " UPDATE_FN='foo'",
        "Syntax error in line 1");

    // Test missing .ll file. TODO: reenable when we can run IR UDAs
    AnalysisError("create aggregate function foo(int) RETURNS int LOCATION " +
        "'/foo.ll' UPDATE_FN='Fn'", "IR UDAs are not yet supported.");
    //AnalysisError("create aggregate function foo(int) RETURNS int LOCATION " +
    //    "'/foo.ll' UPDATE_FN='Fn'", "Could not load binary: /foo.ll");
    //AnalysisError("create aggregate function foo(int) RETURNS int LOCATION " +
    //    "'/foo.ll' UPDATE_FN='_ZABCD'", "Could not load binary: /foo.ll");

    // Test cases where the UPDATE_FN doesn't contain "Update" in which case the user has
    // to explicitly specify the other functions.
    AnalysisError("create aggregate function foo(string, double) RETURNS string" + loc +
        "UPDATE_FN='Agg'", "Could not infer symbol for init() function.");
    AnalysisError("create aggregate function foo(string, double) RETURNS string" + loc +
        "UPDATE_FN='Agg' INIT_FN='AggInit'",
        "Could not infer symbol for merge() function.");
    AnalyzesOk("create aggregate function foo(string, double) RETURNS string" + loc +
        "UPDATE_FN='Agg' INIT_FN='AggInit' MERGE_FN='AggMerge'");
    AnalyzesOk("create aggregate function foo(string, double) RETURNS string" + loc +
        "UPDATE_FN='Agg' INIT_FN='AggInit' MERGE_FN='AggMerge' " +
        "SERIALIZE_FN='AggSerialize'");
    AnalyzesOk("create aggregate function foo(string, double) RETURNS string" + loc +
        "UPDATE_FN='Agg' INIT_FN='AggInit' MERGE_FN='AggMerge' " +
        "SERIALIZE_FN='AggSerialize' FINALIZE_FN='AggFinalize'");

    // Serialize and Finalize have the same signature, make sure that's possible.
    AnalyzesOk("create aggregate function foo(string, double) RETURNS string" + loc +
        "UPDATE_FN='AggUpdate' SERIALIZE_FN='AggSerialize' FINALIZE_FN='AggSerialize'");
    AnalyzesOk("create aggregate function foo(string, double) RETURNS string" + loc +
        "UPDATE_FN='AggUpdate' SERIALIZE_FN='AggFinalize' FINALIZE_FN='AggFinalize'");

    // If you don't specify the full symbol, we look for it in the binary. This should
    // prevent mismatched names by accident.
    AnalysisError("create aggregate function foo(string, double) RETURNS string" + loc +
        "UPDATE_FN='AggUpdate' INIT_FN='AggSerialize'",
        "Could not find function AggSerialize() returns STRING in: " + hdfsLoc);
    // If you specify a mangled name, we just check it exists.
    // TODO: we should be able to validate better. This is almost certainly going
    // to crash everything.
    AnalyzesOk("create aggregate function foo(string, double) RETURNS string" + loc +
        "UPDATE_FN='AggUpdate' "+
        "INIT_FN='_Z12AggSerializePN10impala_udf15FunctionContextERKNS_6IntValE'");
    AnalysisError("create aggregate function foo(string, double) RETURNS string" + loc +
        "UPDATE_FN='AggUpdate' INIT_FN='_ZAggSerialize'",
        "Could not find symbol '_ZAggSerialize' in: " + hdfsLoc);

    // Tests for checking the symbol exists
    AnalysisError("create aggregate function foo(string, double) RETURNS string" + loc +
        "UPDATE_FN='Agg2Update'",
        "Could not find function Agg2Init() returns STRING in: " + hdfsLoc);
    AnalysisError("create aggregate function foo(string, double) RETURNS string" + loc +
        "UPDATE_FN='Agg2Update' INIT_FN='AggInit'",
        "Could not find function Agg2Merge(STRING) returns STRING in: " + hdfsLoc);
    AnalyzesOk("create aggregate function foo(string, double) RETURNS string" + loc +
        "UPDATE_FN='Agg2Update' INIT_FN='AggInit' MERGE_FN='AggMerge'");
    AnalysisError("create aggregate function foo(string, double) RETURNS string" + loc +
        "UPDATE_FN='Agg2Update' INIT_FN='AggInit' MERGE_FN='BadFn'",
        "Could not find function BadFn(STRING) returns STRING in: " + hdfsLoc);
    AnalysisError("create aggregate function foo(string, double) RETURNS string" + loc +
        "UPDATE_FN='Agg2Update' INIT_FN='AggInit' MERGE_FN='AggMerge' "+
        "FINALIZE_FN='not there'",
        "Could not find function not there(STRING) in: " + hdfsLoc);
  }

  /**
   * Wraps the given typeDefs in a CREATE TABLE stmt and runs AnalyzesOk().
   * Also tests that the type is analyzes correctly in ARRAY, MAP, and STRUCT types.
   */
  private void TypeDefsAnalyzeOk(String... typeDefs) {
    for (String typeDefStr: typeDefs) {
      ParseNode stmt = AnalyzesOk(String.format("CREATE TABLE t (i %s)", typeDefStr));
      AnalyzesOk(String.format("CREATE TABLE t (i ARRAY<%s>)", typeDefStr));
      AnalyzesOk(String.format("CREATE TABLE t (i STRUCT<f:%s>)", typeDefStr));

      Preconditions.checkState(stmt instanceof CreateTableStmt);
      CreateTableStmt createTableStmt = (CreateTableStmt) stmt;
      Type t = createTableStmt.getColumnDefs().get(0).getType();
      // If the given type is complex, don't use it as a map key.
      if (t.isComplexType()) {
        AnalyzesOk(String.format(
            "CREATE TABLE t (i MAP<int, %s>)", typeDefStr, typeDefStr));
      } else {
        AnalyzesOk(String.format(
            "CREATE TABLE t (i MAP<%s, %s>)", typeDefStr, typeDefStr));
      }
    }
  }

  /**
   * Wraps the given typeDef in a CREATE TABLE stmt and runs AnalyzesOk().
   * Returns the analyzed type.
   */
  private Type TypeDefAnalyzeOk(String typeDef) {
    ParseNode stmt = AnalyzesOk(String.format("CREATE TABLE t (i %s)", typeDef));
    CreateTableStmt createTableStmt = (CreateTableStmt) stmt;
    return createTableStmt.getColumnDefs().get(0).getType();
  }

  /**
   * Wraps the given typeDefs in a CREATE TABLE stmt and asserts that the type def
   * failed to analyze with the given error message.
   */
  private void TypeDefAnalysisError(String typeDef, String expectedError) {
    AnalysisError(String.format("CREATE TABLE t (i %s)", typeDef), expectedError);
  }

  @Test
  public void TestTypes() {
    // Test primitive types.
    TypeDefsAnalyzeOk("BOOLEAN");
    TypeDefsAnalyzeOk("TINYINT");
    TypeDefsAnalyzeOk("SMALLINT");
    TypeDefsAnalyzeOk("INT", "INTEGER");
    TypeDefsAnalyzeOk("BIGINT");
    TypeDefsAnalyzeOk("FLOAT");
    TypeDefsAnalyzeOk("DOUBLE", "REAL");
    TypeDefsAnalyzeOk("STRING");
    TypeDefsAnalyzeOk("CHAR(1)", "CHAR(20)");
    TypeDefsAnalyzeOk("DECIMAL");
    TypeDefsAnalyzeOk("TIMESTAMP");
    TypeDefsAnalyzeOk("DATE");
    TypeDefsAnalyzeOk("BINARY");

    // Test decimal.
    TypeDefsAnalyzeOk("DECIMAL");
    TypeDefsAnalyzeOk("DECIMAL(1)");
    TypeDefsAnalyzeOk("DECIMAL(12, 7)");
    TypeDefsAnalyzeOk("DECIMAL(38)");
    TypeDefsAnalyzeOk("DECIMAL(38, 1)");
    TypeDefsAnalyzeOk("DECIMAL(38, 38)");

    TypeDefAnalysisError("DECIMAL(1, 10)",
        "Decimal scale (10) must be <= precision (1)");
    TypeDefAnalysisError("DECIMAL(0, 0)",
        "Decimal precision must be > 0: 0");
    TypeDefAnalysisError("DECIMAL(39, 0)",
        "Decimal precision must be <= 38");

    // Test unsupported types
    for (ScalarType t: Type.getUnsupportedTypes()) {
      TypeDefAnalysisError(t.toSql(),
          String.format("Unsupported data type: %s", t.toSql()));
    }

    // Test complex types.
    TypeDefsAnalyzeOk("ARRAY<BIGINT>");
    TypeDefsAnalyzeOk("MAP<TINYINT, DOUBLE>");
    TypeDefsAnalyzeOk("STRUCT<f:TINYINT>");
    TypeDefsAnalyzeOk("STRUCT<a:TINYINT, b:BIGINT, c:DOUBLE>");
    TypeDefsAnalyzeOk("STRUCT<a:TINYINT COMMENT 'x', b:BIGINT, c:DOUBLE COMMENT 'y'>");

    // Map keys can't be complex types.
    TypeDefAnalysisError("map<array<int>, int>",
        "Map type cannot have a complex-typed key: MAP<ARRAY<INT>,INT>");
    // Duplicate struct-field name.
    TypeDefAnalysisError("STRUCT<f1: int, f2: string, f1: float>",
        "Duplicate field name 'f1' in struct 'STRUCT<f1:INT,f2:STRING,f1:FLOAT>'");
    // Invalid struct-field name.
    TypeDefAnalysisError("STRUCT<`???`: int>",
        "Invalid struct field name: ???");

    // Test maximum nesting depth with all complex types.
    for (String prefix: Arrays.asList("struct<f1:int,f2:", "array<", "map<string,")) {
      String middle = "int";
      String suffix = ">";
      // Test type with exactly the max nesting depth.
      String maxTypeDef = genTypeSql(Type.MAX_NESTING_DEPTH, prefix, middle, suffix);
      Type maxType = TypeDefAnalyzeOk(maxTypeDef);
      Assert.assertFalse(maxType.exceedsMaxNestingDepth());
      // Test type with exactly one level above the max nesting depth.
      String oneAboveMaxDef =
          genTypeSql(Type.MAX_NESTING_DEPTH + 1, prefix, middle, suffix);
      TypeDefAnalysisError(oneAboveMaxDef, "Type exceeds the maximum nesting depth");
      // Test type with very deep nesting to test we do not hit a stack overflow.
      String veryDeepDef =
          genTypeSql(Type.MAX_NESTING_DEPTH * 100, prefix, middle, suffix);
      TypeDefAnalysisError(veryDeepDef, "Type exceeds the maximum nesting depth");
    }
  }

  /**
   * Generates a string with the following pattern:
   * <prefix>*<middle><suffix>*
   * with exactly depth-1 repetitions of prefix and suffix
   */
  private String genTypeSql(int depth, String prefix, String middle, String suffix) {
    return StringUtils.repeat(prefix, depth - 1) +
        middle + StringUtils.repeat(suffix, depth - 1);
  }

  @Test
  public void TestUseDb() throws AnalysisException {
    AnalyzesOk("use functional");
    AnalysisError("use db_does_not_exist", "Database does not exist: db_does_not_exist");
  }

  @Test
  public void TestUseStatement() {
    Assert.assertTrue(AnalyzesOk("USE functional") instanceof UseStmt);
  }

  @Test
  public void TestDescribeDb() throws AnalysisException {
    addTestDb("test_analyse_desc_db", null);
    AnalyzesOk("describe database test_analyse_desc_db");
    AnalyzesOk("describe database extended test_analyse_desc_db");
    AnalyzesOk("describe database formatted test_analyse_desc_db");

    AnalysisError("describe database db_does_not_exist",
        "Database does not exist: db_does_not_exist");
    AnalysisError("describe database extended db_does_not_exist",
        "Database does not exist: db_does_not_exist");
    AnalysisError("describe database formatted db_does_not_exist",
        "Database does not exist: db_does_not_exist");
  }

  @Test
  public void TestDescribe() throws AnalysisException {
    AnalyzesOk("describe formatted functional.alltypes");
    AnalyzesOk("describe functional.alltypes");
    AnalysisError("describe formatted nodb.alltypes",
        "Could not resolve path: 'nodb.alltypes'");
    AnalysisError("describe functional.notbl",
        "Could not resolve path: 'functional.notbl'");

    // Complex typed fields.
    AnalyzesOk("describe functional_parquet.allcomplextypes.int_array_col");
    AnalyzesOk("describe functional_parquet.allcomplextypes.map_array_col");
    AnalyzesOk("describe functional_parquet.allcomplextypes.map_map_col");
    AnalyzesOk("describe functional_parquet.allcomplextypes.map_map_col.value");
    AnalyzesOk("describe functional_parquet.allcomplextypes.complex_struct_col");
    AnalyzesOk("describe functional_parquet.allcomplextypes.complex_struct_col.f3");
    AnalysisError("describe formatted functional_parquet.allcomplextypes.int_array_col",
        "DESCRIBE FORMATTED|EXTENDED must refer to a table");
    AnalysisError("describe functional_parquet.allcomplextypes.id",
        "Cannot describe path 'functional_parquet.allcomplextypes.id' targeting " +
        "scalar type: INT");
    AnalysisError("describe functional_parquet.allcomplextypes.nonexistent",
        "Could not resolve path: 'functional_parquet.allcomplextypes.nonexistent'");

    // Handling of ambiguous paths.
    addTestDb("ambig", null);
    addTestTable("create table ambig.ambig (ambig struct<ambig:array<int>>)");
    // Single element path can only be resolved as <table>.
    DescribeTableStmt describe = (DescribeTableStmt)AnalyzesOk("describe ambig",
        createAnalysisCtx("ambig"));
    TDescribeTableParams tdesc = describe.toThrift();
    Assert.assertTrue(tdesc.isSetTable_name());
    Assert.assertEquals("ambig", tdesc.table_name.getDb_name());
    Assert.assertEquals("ambig", tdesc.table_name.getTable_name(), "ambig");
    Assert.assertFalse(tdesc.isSetResult_struct());

    // Path could be resolved as either <db>.<table> or <table>.<complex field>
    AnalysisError("describe ambig.ambig", createAnalysisCtx("ambig"),
        "Path is ambiguous: 'ambig.ambig'");
    // Path could be resolved as either <db>.<table>.<field> or <table>.<field>.<field>
    AnalysisError("describe ambig.ambig.ambig", createAnalysisCtx("ambig"),
        "Path is ambiguous: 'ambig.ambig.ambig'");
    // 4 element path can only be resolved to nested array.
    describe = (DescribeTableStmt) AnalyzesOk(
        "describe ambig.ambig.ambig.ambig", createAnalysisCtx("ambig"));
    tdesc = describe.toThrift();
    Type expectedType =
        org.apache.impala.analysis.Path.getTypeAsStruct(new ArrayType(Type.INT));
    Assert.assertTrue(tdesc.isSetResult_struct());
    Assert.assertEquals(expectedType, Type.fromThrift(tdesc.getResult_struct()));
  }

  @Test
  public void TestShow() throws AnalysisException {
    AnalyzesOk("show databases");
    AnalyzesOk("show databases like '*pattern'");

    AnalyzesOk("show data sources");
    AnalyzesOk("show data sources like '*pattern'");

    AnalyzesOk("show tables");
    AnalyzesOk("show tables like '*pattern'");

    AnalyzesOk("show tables in functional");
    AnalyzesOk("show tables in functional like '*pattern'");

    AnalyzesOk("show metadata tables in functional_parquet.iceberg_query_metadata");
    AnalyzesOk(
        "show metadata tables in functional_parquet.iceberg_query_metadata like 'e*|f*'");

    AnalysisError("show metadata tables in functional_parquet.alltypes",
        "The SHOW METADATA TABLES statement is only valid for Iceberg tables: " +
        "'functional_parquet.alltypes' is not an Iceberg table.");

    for (String fnType: new String[]{"", "aggregate", "analytic"}) {
      AnalyzesOk(String.format("show %s functions", fnType));
      AnalyzesOk(String.format("show %s functions like '*pattern'", fnType));
      AnalyzesOk(String.format("show %s functions in functional", fnType));
      AnalyzesOk(String.format(
          "show %s functions in functional like '*pattern'", fnType));
    }
    // Database doesn't exist.
    AnalysisError("show functions in baddb", "Database does not exist: baddb");
    AnalysisError("show functions in baddb like '*pattern'",
        "Database does not exist: baddb");
  }

  @Test
  public void TestShowFiles() throws AnalysisException {
    // Test empty table
    AnalyzesOk(String.format("show files in functional.emptytable"));

    String[] partitions = new String[] {
        "",
        "partition(month=10, year=2010)",
        "partition(month>10, year<2011, year>2008)"};
    for (String partition: partitions) {
      AnalyzesOk(String.format("show files in functional.alltypes %s", partition));
      // Database/table doesn't exist.
      AnalysisError(String.format("show files in baddb.alltypes %s", partition),
          "Could not resolve table reference: 'baddb.alltypes'");
      AnalysisError(String.format("show files in functional.badtbl %s", partition),
          "Could not resolve table reference: 'functional.badtbl'");
      // Cannot show files on a non hdfs table.
      AnalysisError(String.format("show files in functional.alltypes_view %s",
          partition),
          "SHOW FILES not applicable to a non hdfs table: functional.alltypes_view");
      AnalysisError(String.format("show files in allcomplextypes.int_array_col %s",
          partition), createAnalysisCtx("functional"),
          "SHOW FILES not applicable to a non hdfs table: allcomplextypes.int_array_col");
    }

    // Not a partition column.
    AnalysisError("show files in functional.alltypes partition(year=2010,int_col=1)",
        "Partition exprs cannot contain non-partition column(s): int_col = 1.");
    // Not a valid column.
    AnalysisError("show files in functional.alltypes partition(year=2010,day=1)",
        "Could not resolve column/field reference: 'day'");
    // Table is not partitioned.
    AnalysisError("show files in functional.tinyinttable partition(int_col=1)",
        "Table is not partitioned: functional.tinyinttable");
    // Partition spec does not exist
    AnalysisError("show files in functional.alltypes partition(year=2010,month=NULL)",
        "No matching partition(s) found.");
  }

  @Test
  public void TestShowStats() throws AnalysisException {
    String[] statsQuals = new String[] {"table", "column"};
    for (String qual : statsQuals) {
      AnalyzesOk(String.format("show %s stats functional.alltypes", qual));
      // Database/table doesn't exist.
      AnalysisError(String.format("show %s stats baddb.alltypes", qual),
          "Database does not exist: baddb");
      AnalysisError(String.format("show %s stats functional.badtbl", qual),
          "Table does not exist: functional.badtbl");
      // Cannot show stats on a view.
      AnalysisError(String.format("show %s stats functional.alltypes_view", qual),
          String.format("SHOW %s STATS not applicable to a view: " +
              "functional.alltypes_view", qual.toUpperCase()));
    }
  }

  @Test
  public void TestShowPartitions() throws AnalysisException {
    AnalyzesOk("show partitions functional.alltypes");
    AnalysisError("show partitions baddb.alltypes",
        "Database does not exist: baddb");
    AnalysisError("show partitions functional.badtbl",
        "Table does not exist: functional.badtbl");
    AnalysisError("show partitions functional.alltypesnopart",
        "Table is not partitioned: functional.alltypesnopart");
    AnalysisError("show partitions functional.view_view",
        "SHOW PARTITIONS not applicable to a view: functional.view_view");
    AnalysisError("show partitions functional_hbase.alltypes",
        "SHOW PARTITIONS must target an HDFS or Kudu table: functional_hbase.alltypes");
  }

  @Test
  public void TestShowRangePartitions() throws AnalysisException {
    AnalyzesOk("show range partitions functional_kudu.dimtbl");
    AnalysisError("show range partitions baddb.alltypes",
        "Database does not exist: baddb");
    AnalysisError("show range partitions functional.badtbl",
        "Table does not exist: functional.badtbl");
    AnalysisError("show range partitions functional.alltypes",
        "SHOW RANGE PARTITIONS must target a Kudu table: functional.alltypes");
    AnalysisError("show range partitions functional.alltypesnopart",
        "SHOW RANGE PARTITIONS must target a Kudu table: functional.alltypes");
    AnalysisError("show range partitions functional_kudu.alltypes",
        "SHOW RANGE PARTITIONS requested but table does not have range partitions: " +
        "functional_kudu.alltypes");
    AnalysisError("show range partitions functional.view_view",
        "SHOW RANGE PARTITIONS not applicable to a view: functional.view_view");
    AnalysisError("show range partitions functional_hbase.alltypes",
        "SHOW RANGE PARTITIONS must target a Kudu table: functional_hbase.alltypes");
  }

  @Test
  public void TestShowCreateFunction() throws AnalysisException {
    addTestFunction("TestFn", Lists.newArrayList(Type.INT, Type.INT), false);
    AnalyzesOk("show create function TestFn");
    addTestUda("AggFn", Type.INT, Type.INT);
    AnalyzesOk("show create aggregate function AggFn");

    // Verify there is differentiation between UDF and UDA.
    AnalysisError("show create aggregate function default.TestFn",
        "Function testfn() does not exist in database default");
    AnalysisError("show create function default.AggFn",
        "Function aggfn() does not exist in database default");
    AnalysisError("show create function default.foobar",
        "Function foobar() does not exist in database default");
    AnalysisError("show create function foobar.fn",
        "Database does not exist: foobar");
  }

  /**
   * Validate if location path analysis issues proper warnings when directory
   * permissions/existence checks fail.
   */
  @Test
  public void TestPermissionValidation() throws AnalysisException {
    String location = "/test-warehouse/.tmp_" + UUID.randomUUID().toString();
    Path parentPath = FileSystemUtil.createFullyQualifiedPath(new Path(location));
    FileSystem fs = null;
    try {
      fs = parentPath.getFileSystem(FileSystemUtil.getConfiguration());

      // Test location doesn't exist
      AnalyzesOk(String.format("create table new_table (col INT) location '%s/new_table'",
          location),
          String.format("Path '%s' cannot be reached: Path does not exist.",
              parentPath));

      // Test localtion path with trailing slash.
      AnalyzesOk(String.format("create table new_table (col INT) location " +
          "'%s/new_table/'", location),
          String.format("Path '%s' cannot be reached: Path does not exist.",
              parentPath));

      AnalyzesOk(String.format("create table new_table location '%s/new_table' " +
          "as select 1, 1", location),
          String.format("Path '%s' cannot be reached: Path does not exist.",
              parentPath));

      AnalyzesOk(String.format("create table new_table like functional.alltypes " +
          "location '%s/new_table'", location),
          String.format("Path '%s' cannot be reached: Path does not exist.",
              parentPath));

      AnalyzesOk(String.format("create database new_db location '%s/new_db'",
          location),
          String.format("Path '%s' cannot be reached: Path does not exist.",
              parentPath));

      fs.mkdirs(parentPath);
      // Create a test data file for load data test
      FSDataOutputStream out =
          fs.create(new Path(parentPath, "test_loaddata/testdata.txt"));
      out.close();

      fs.setPermission(parentPath,
          new FsPermission(FsAction.NONE, FsAction.NONE, FsAction.NONE));

      // Test location exists but Impala doesn't have sufficient permission
      AnalyzesOk(String.format("create data Source serverlog location " +
          "'%s/foo.jar' class 'foo.Bar' API_VERSION 'V1'", location),
          String.format("Impala does not have READ access to path '%s'", parentPath));

      AnalyzesOk(String.format("create external table new_table (col INT) location " +
          "'%s/new_table'", location),
          String.format("Impala does not have READ_WRITE access to path '%s'",
              parentPath));

      AnalyzesOk(String.format("alter table functional.insert_string_partitioned " +
          "add partition (s2='hello') location '%s/new_partition'", location),
          String.format("Impala does not have READ_WRITE access to path '%s'",
              parentPath));

      AnalyzesOk(String.format("alter table functional.stringpartitionkey " +
          "partition(string_col = 'partition1') set location '%s/new_part_loc'", location),
          String.format("Impala does not have READ_WRITE access to path '%s'",
              parentPath));

      // Test location exists and Impala does have sufficient permission
      fs.setPermission(parentPath,
          new FsPermission(FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE));

      AnalyzesOk(String.format("create external table new_table (col INT) location " +
          "'%s/new_table'", location));
    } catch (IOException e) {
      throw new AnalysisException(e.getMessage(), e);
    } finally {
      // Clean up
      try {
        if (fs != null && fs.exists(parentPath)) {
          fs.delete(parentPath, true);
        }
      } catch (IOException e) {
        // Ignore
      }
    }
  }

  @Test
  public void TestCommentOnDatabase() {
    AnalyzesOk("comment on database functional is 'comment'");
    AnalyzesOk("comment on database functional is ''");
    AnalyzesOk("comment on database functional is null");
    AnalysisError("comment on database doesntexist is 'comment'",
        "Database does not exist: doesntexist");
    AnalysisError(String.format("comment on database functional is '%s'",
        buildLongComment()), "Comment exceeds maximum length of 256 characters. " +
        "The given comment has 261 characters.");
  }

  @Test
  public void TestCommentOnTable() {
    for (Pair<String, AnalysisContext> pair : new Pair[]{
        new Pair<>("functional.alltypes", createAnalysisCtx()),
        new Pair<>("alltypes", createAnalysisCtx("functional"))}) {
      AnalyzesOk(String.format("comment on table %s is 'comment'", pair.first),
          pair.second);
      AnalyzesOk(String.format("comment on table %s is ''", pair.first), pair.second);
      AnalyzesOk(String.format("comment on table %s is null", pair.first), pair.second);
    }
    AnalysisError("comment on table doesntexist is 'comment'",
        "Could not resolve table reference: 'default.doesntexist'");
    AnalysisError("comment on table functional.alltypes_view is 'comment'",
        "COMMENT ON TABLE not allowed on a view: functional.alltypes_view");
    AnalysisError(String.format("comment on table functional.alltypes is '%s'",
        buildLongComment()), "Comment exceeds maximum length of 256 characters. " +
        "The given comment has 261 characters.");
  }

  @Test
  public void TestCommentOnView() {
    for (Pair<String, AnalysisContext> pair : new Pair[]{
        new Pair<>("functional.alltypes_view", createAnalysisCtx()),
        new Pair<>("alltypes_view", createAnalysisCtx("functional"))}) {
      AnalyzesOk(String.format("comment on view %s is 'comment'", pair.first),
          pair.second);
      AnalyzesOk(String.format("comment on view %s is ''", pair.first), pair.second);
      AnalyzesOk(String.format("comment on view %s is null", pair.first), pair.second);
    }
    AnalysisError("comment on view doesntexist is 'comment'",
        "Could not resolve table reference: 'default.doesntexist'");
    AnalysisError("comment on view functional.alltypes is 'comment'",
        "COMMENT ON VIEW not allowed on a table: functional.alltypes");
    AnalysisError(String.format("comment on view functional.alltypes_view is '%s'",
        buildLongComment()), "Comment exceeds maximum length of 256 characters. " +
        "The given comment has 261 characters.");
  }

  @Test
  public void TestCommentOnColumn() {
    for (Pair<String, AnalysisContext> pair : new Pair[]{
        new Pair<>("functional.alltypes.id", createAnalysisCtx()),
        new Pair<>("alltypes.id", createAnalysisCtx("functional")),
        new Pair<>("functional.alltypes_view.id", createAnalysisCtx()),
        new Pair<>("alltypes_view.id", createAnalysisCtx("functional")),
        new Pair<>("functional_kudu.alltypes.id", createAnalysisCtx())}) {
      AnalyzesOk(String.format("comment on column %s is 'comment'", pair.first),
          pair.second);
      AnalyzesOk(String.format("comment on column %s is ''", pair.first), pair.second);
      AnalyzesOk(String.format("comment on column %s is null", pair.first), pair.second);
    }
    AnalysisError("comment on column functional.alltypes.doesntexist is 'comment'",
        "Column 'doesntexist' does not exist in table: functional.alltypes");
    AnalysisError(String.format("comment on column functional.alltypes.id is '%s'",
        buildLongComment()), "Comment exceeds maximum length of 256 characters. " +
        "The given comment has 261 characters.");
  }

  private static String buildLongComment() {
    StringBuilder comment = new StringBuilder();
    for (int i = 0; i < MetaStoreUtil.CREATE_MAX_COMMENT_LENGTH + 5; i++) {
      comment.append("a");
    }
    return comment.toString();
  }

  @Test
  public void TestAlterDatabaseSetOwner() {
    String[] ownerTypes = new String[]{"user", "role"};
    for (String ownerType : ownerTypes) {
      AnalyzesOk(String.format("alter database functional set owner %s foo", ownerType));
      AnalysisError(String.format("alter database doesntexist set owner %s foo",
          ownerType), "Database does not exist: doesntexist");
      AnalysisError(String.format("alter database functional set owner %s %s",
          ownerType, buildLongOwnerName()), "Owner name exceeds maximum length of 128 " +
          "characters. The given owner name has 133 characters.");
    }
  }

  @Test
  public void TestAlterTableSetOwner() {
    String[] ownerTypes = new String[]{"user", "role"};
    for (String ownerType : ownerTypes) {
      AnalyzesOk(String.format("alter table functional.alltypes set owner %s foo",
          ownerType));
      AnalysisError(String.format("alter table nodb.alltypes set owner %s foo",
          ownerType), "Could not resolve table reference: 'nodb.alltypes'");
      AnalysisError(String.format("alter table functional.notbl set owner %s foo",
          ownerType), "Could not resolve table reference: 'functional.notbl'");
      AnalysisError(String.format("alter table functional.alltypes set owner %s %s",
          ownerType, buildLongOwnerName()), "Owner name exceeds maximum length of 128 " +
          "characters. The given owner name has 133 characters.");
      AnalysisError(String.format("alter table functional.alltypes_view " +
          "set owner %s foo", ownerType), "ALTER TABLE not allowed on a view: " +
          "functional.alltypes_view");
    }
  }

  @Test
  public void TestAlterViewSetOwner() {
    String[] ownerTypes = new String[]{"user", "role"};
    for (String ownerType : ownerTypes) {
      AnalyzesOk(String.format("alter view functional.alltypes_view set owner %s foo",
          ownerType));
      AnalysisError(String.format("alter view nodb.alltypes set owner %s foo",
          ownerType), "Could not resolve table reference: 'nodb.alltypes'");
      AnalysisError(String.format("alter view functional.notbl set owner %s foo",
          ownerType), "Could not resolve table reference: 'functional.notbl'");
      AnalysisError(String.format("alter view functional.alltypes_view set owner %s %s",
          ownerType, buildLongOwnerName()), "Owner name exceeds maximum length of 128 " +
          "characters. The given owner name has 133 characters.");
      AnalysisError(String.format("alter view functional.alltypes " +
          "set owner %s foo", ownerType), "ALTER VIEW not allowed on a table: " +
          "functional.alltypes");
    }
  }

  @Test
  public void TestAlterExecuteExpireSnapshots() {
    AnalyzesOk("alter table functional_parquet.iceberg_partitioned execute " +
        "expire_snapshots(now() - interval 20 years);");
    AnalyzesOk("alter table functional_parquet.iceberg_partitioned execute " +
        "expire_snapshots('2022-01-04 10:00:00');");

    // Negative tests
    AnalysisError("alter table functional_parquet.iceberg_partitioned execute " +
        "unsupported_operation(123456789);", "'unsupported_operation' is not supported " +
        "by ALTER TABLE <table> EXECUTE. Supported operations are: " +
        "EXPIRE_SNAPSHOTS(<expression>), ROLLBACK(<expression>)");
    AnalysisError("alter table functional_parquet.iceberg_partitioned execute " +
        "expire_snapshots(now(), 3);", "EXPIRE_SNAPSHOTS(<expression>) must have one " +
        "parameter: expire_snapshots(now(), 3)");
    AnalysisError("alter table functional_parquet.iceberg_partitioned execute " +
        "expire_snapshots(id);", "EXPIRE_SNAPSHOTS(<expression>) must be a constant " +
        "expression: expire_snapshots(id)");
    AnalysisError("alter table functional_parquet.iceberg_partitioned execute " +
        "expire_snapshots(42);", "EXPIRE_SNAPSHOTS(<expression>) must be a timestamp " +
        "type but is 'TINYINT': 42");
    AnalysisError("alter table functional_parquet.iceberg_partitioned execute " +
        "expire_snapshots('2021-02-32 15:52:45');", "Invalid TIMESTAMP expression has" +
        " been given to EXPIRE_SNAPSHOTS(<expression>)");
  }

  @Test
  public void TestAlterExecuteRollback() {
    AnalyzesOk("alter table functional_parquet.iceberg_partitioned execute " +
        "rollback('2022-01-04 10:00:00');");
    AnalyzesOk("alter table functional_parquet.iceberg_partitioned execute " +
        "rollback(123456);");
    // Timestamp can be an expression.
    AnalyzesOk("alter table functional_parquet.iceberg_partitioned execute " +
        "rollback(cast('2021-08-09 15:52:45' as timestamp) - interval 2 days + " +
        "interval 3 hours);");

    // Negative tests
    AnalysisError("alter table nodb.alltypes execute " +
        "rollback('2022-01-04 10:00:00');",
       "Could not resolve table reference: 'nodb.alltypes'");
    AnalysisError("alter table functional.alltypes execute " +
        "rollback('2022-01-04 10:00:00');",
       "ALTER TABLE EXECUTE ROLLBACK is only supported for Iceberg tables: " +
           "functional.alltypes");
    AnalysisError("alter table functional_parquet.iceberg_partitioned execute " +
        "rollback(id);", "EXECUTE ROLLBACK(<expression>): " +
        "<expression> must be a constant expression: EXECUTE rollback(id)");
    AnalysisError("alter table functional_parquet.iceberg_partitioned execute " +
        "rollback(3.14);", "EXECUTE ROLLBACK(<expression>): <expression> " +
        "must be an integer type or a timestamp, but is 'DECIMAL(3,2)': " +
        "EXECUTE rollback(3.14)");
    AnalysisError("alter table functional_parquet.iceberg_partitioned execute " +
        "rollback('2021-02-32 15:52:45');", "An invalid TIMESTAMP expression has been " +
        "given to EXECUTE ROLLBACK(<expression>): the expression " +
        "'2021-02-32 15:52:45' cannot be converted to a TIMESTAMP");
    AnalysisError("alter table functional_parquet.iceberg_partitioned execute " +
        "rollback('the beginning');", "An invalid TIMESTAMP expression has been " +
        "given to EXECUTE ROLLBACK(<expression>): the expression " +
        "'the beginning' cannot be converted to a TIMESTAMP");
    AnalysisError("alter table functional_parquet.iceberg_partitioned execute " +
        "rollback(1111,2222);",
        "EXECUTE ROLLBACK(<expression>): must have one parameter");
    AnalysisError("alter table functional_parquet.iceberg_partitioned execute " +
        "rollback('1111');", "An invalid TIMESTAMP expression has been " +
        "given to EXECUTE ROLLBACK(<expression>): the expression " +
        "'1111' cannot be converted to a TIMESTAMP");
  }

  private static String buildLongOwnerName() {
    StringBuilder comment = new StringBuilder();
    for (int i = 0; i < MetaStoreUtil.MAX_OWNER_LENGTH + 5; i++) {
      comment.append("a");
    }
    return comment.toString();
  }
}
