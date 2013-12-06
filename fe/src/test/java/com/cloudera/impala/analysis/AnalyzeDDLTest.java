// Copyright (c) 2012 Cloudera, Inc. All rights reserved.
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

package com.cloudera.impala.analysis;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;

import junit.framework.Assert;

import org.junit.Test;

import com.cloudera.impala.catalog.CatalogException;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.common.AnalysisException;
import com.google.common.collect.Lists;

public class AnalyzeDDLTest extends AnalyzerTest {

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

      // Can't add/drop partitions to/from unpartitioned tables
      AnalysisError("alter table functional.alltypesnopart " + kw + " partition (i=1)",
          "Table is not partitioned: functional.alltypesnopart");
      AnalysisError("alter table functional_hbase.alltypesagg " + kw +
          " partition (i=1)", "Table is not partitioned: functional_hbase.alltypesagg");

      // Duplicate partition key name
      AnalysisError("alter table functional.alltypes " + kw +
          " partition(year=2050, year=2051)", "Duplicate partition key name: year");
      // Not a partition column
      AnalysisError("alter table functional.alltypes " + kw +
          " partition(year=2050, int_col=1)",
          "Column 'int_col' is not a partition column in table: functional.alltypes");

      // NULL partition keys
      AnalyzesOk("alter table functional.alltypes " + kw +
          " partition(year=NULL, month=1)");
      AnalyzesOk("alter table functional.alltypes " + kw +
          " partition(year=NULL, month=NULL)");
      AnalyzesOk("alter table functional.alltypes " + kw +
          " partition(year=ascii(null), month=ascii(NULL))");
      // Empty string partition keys
      AnalyzesOk("alter table functional.insert_string_partitioned " + kw +
          " partition(s2='')");
      // Arbitrary exprs as partition key values. Constant exprs are ok.
      AnalyzesOk("alter table functional.alltypes " + kw +
          " partition(year=-1, month=cast((10+5*4) as INT))");

      // Arbitrary exprs as partition key values. Non-constant exprs should fail.
      AnalysisError("alter table functional.alltypes " + kw +
          " partition(year=2050, month=int_col)",
          "Non-constant expressions are not supported as static partition-key values " +
          "in 'month=int_col'.");
      AnalysisError("alter table functional.alltypes " + kw +
          " partition(year=cast(int_col as int), month=12)",
          "Non-constant expressions are not supported as static partition-key values " +
          "in 'year=CAST(int_col AS INT)'.");

      // Not a valid column
      AnalysisError("alter table functional.alltypes " + kw +
          " partition(year=2050, blah=1)",
          "Partition column 'blah' not found in table: functional.alltypes");

      // Data types don't match
      AnalysisError(
          "alter table functional.insert_string_partitioned " + kw +
          " partition(s2=1234)",
          "Target table not compatible.\nIncompatible types 'STRING' and 'SMALLINT' " +
          "in column 's2'");

      // Loss of precision
      AnalysisError(
          "alter table functional.alltypes " + kw +
          " partition(year=100000000000, month=10)",
          "Partition key value may result in loss of precision.\nWould need to cast" +
          " '100000000000' to 'INT' for partition column: year");


      // Table/Db does not exist
      AnalysisError("alter table db_does_not_exist.alltypes " + kw +
          " partition (i=1)", "Database does not exist: db_does_not_exist");
      AnalysisError("alter table functional.table_does_not_exist " + kw +
          " partition (i=1)", "Table does not exist: functional.table_does_not_exist");

      // Cannot ALTER TABLE a view.
      AnalysisError("alter table functional.alltypes_view " + kw +
          " partition(year=2050, month=10)",
          "ALTER TABLE not allowed on a view: functional.alltypes_view");
    }

    // IF NOT EXISTS properly checks for partition existence
    AnalyzesOk("alter table functional.alltypes add " +
          "partition(year=2050, month=10)");
    AnalysisError("alter table functional.alltypes add " +
          "partition(year=2010, month=10)",
          "Partition spec already exists: (year=2010, month=10).");
    AnalyzesOk("alter table functional.alltypes add if not exists " +
          " partition(year=2010, month=10)");
    AnalyzesOk("alter table functional.alltypes add if not exists " +
        " partition(year=2010, month=10) location " +
        "'/test-warehouse/alltypes/year=2010/month=10'");

    // IF EXISTS properly checks for partition existence
    AnalyzesOk("alter table functional.alltypes drop " +
          "partition(year=2010, month=10)");
    AnalysisError("alter table functional.alltypes drop " +
          "partition(year=2050, month=10)",
          "Partition spec does not exist: (year=2050, month=10).");
    AnalyzesOk("alter table functional.alltypes drop if exists " +
          "partition(year=2050, month=10)");
  }

  @Test
  public void TestAlterTableAddReplaceColumns() throws AnalysisException {
    AnalyzesOk("alter table functional.alltypes add columns (new_col int)");
    AnalyzesOk("alter table functional.alltypes add columns (c1 string comment 'hi')");
    AnalyzesOk(
        "alter table functional.alltypes replace columns (c1 int comment 'c', c2 int)");

    // Column name must be unique for add
    AnalysisError("alter table functional.alltypes add columns (int_col int)",
        "Column already exists: int_col");
    // Add a column with same name as a partition column
    AnalysisError("alter table functional.alltypes add columns (year int)",
        "Column name conflicts with existing partition column: year");
    // Invalid column name.
    AnalysisError("alter table functional.alltypes add columns (`???` int)",
        "Invalid column name: ???");
    AnalysisError("alter table functional.alltypes replace columns (`???` int)",
        "Invalid column name: ???");

    // Replace should not throw an error if the column already exists
    AnalyzesOk("alter table functional.alltypes replace columns (int_col int)");
    // It is not possible to replace a partition column
    AnalysisError("alter table functional.alltypes replace columns (Year int)",
        "Column name conflicts with existing partition column: year");

    // Duplicate column names
    AnalysisError("alter table functional.alltypes add columns (c1 int, c1 int)",
        "Duplicate column name: c1");
    AnalysisError("alter table functional.alltypes replace columns (c1 int, C1 int)",
        "Duplicate column name: c1");

    // Table/Db does not exist
    AnalysisError("alter table db_does_not_exist.alltypes add columns (i int)",
        "Database does not exist: db_does_not_exist");
    AnalysisError("alter table functional.table_does_not_exist add columns (i int)",
        "Table does not exist: functional.table_does_not_exist");

    // Cannot ALTER TABLE a view.
    AnalysisError("alter table functional.alltypes_view " +
        "add columns (c1 string comment 'hi')",
        "ALTER TABLE not allowed on a view: functional.alltypes_view");

    // Cannot ALTER TABLE ADD/REPLACE COLUMNS on an HBase table.
    AnalysisError("alter table functional_hbase.alltypes add columns (i int)",
        "ALTER TABLE ADD|REPLACE COLUMNS not currently supported on HBase tables.");
  }

  @Test
  public void TestAlterTableDropColumn() throws AnalysisException {
    AnalyzesOk("alter table functional.alltypes drop column int_col");

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
        "Database does not exist: db_does_not_exist");
    AnalysisError("alter table functional.table_does_not_exist drop column col1",
        "Table does not exist: functional.table_does_not_exist");

    // Cannot ALTER TABLE a view.
    AnalysisError("alter table functional.alltypes_view drop column int_col",
        "ALTER TABLE not allowed on a view: functional.alltypes_view");

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
    // Change only the datatype
    AnalyzesOk("alter table functional.alltypes change column int_col int_col tinyint");
    // Add a comment to a column.
    AnalyzesOk("alter table functional.alltypes change int_col int_col int comment 'c'");

    AnalysisError("alter table functional.alltypes change column no_col c1 int",
        "Column 'no_col' does not exist in table: functional.alltypes");

    AnalysisError("alter table functional.alltypes change column year year int",
        "Cannot modify partition column: year");

    AnalysisError("alter table functional.alltypes change column int_col Tinyint_col int",
        "Column already exists: Tinyint_col");

    // Invalid column name.
    AnalysisError("alter table functional.alltypes change column int_col `???` int",
        "Invalid column name: ???");

    // Table/Db does not exist
    AnalysisError("alter table db_does_not_exist.alltypes change c1 c2 int",
        "Database does not exist: db_does_not_exist");
    AnalysisError("alter table functional.table_does_not_exist change c1 c2 double",
        "Table does not exist: functional.table_does_not_exist");

    // Cannot ALTER TABLE a view.
    AnalysisError("alter table functional.alltypes_view " +
        "change column int_col int_col2 int",
        "ALTER TABLE not allowed on a view: functional.alltypes_view");

    // Cannot ALTER TABLE CHANGE COLUMN on an HBase table.
    AnalysisError("alter table functional_hbase.alltypes CHANGE COLUMN int_col i int",
        "ALTER TABLE CHANGE COLUMN not currently supported on HBase tables.");
  }

  @Test
  public void TestAlterTableSet() throws AnalysisException {
    AnalyzesOk("alter table functional.alltypes set fileformat sequencefile");
    AnalyzesOk("alter table functional.alltypes set location '/a/b'");
    AnalyzesOk("alter table functional.alltypes set tblproperties('a'='1')");
    AnalyzesOk("alter table functional.alltypes set serdeproperties('a'='2')");
    AnalyzesOk("alter table functional.alltypes PARTITION (Year=2010, month=11) " +
               "set location '/a/b'");
    AnalyzesOk("alter table functional.alltypes PARTITION (month=11, year=2010) " +
               "set fileformat parquetfile");
    AnalyzesOk("alter table functional.stringpartitionkey PARTITION " +
               "(string_col='partition1') set fileformat parquet");
    AnalyzesOk("alter table functional.stringpartitionkey PARTITION " +
               "(string_col='PaRtiTion1') set location '/a/b/c'");
    AnalyzesOk("alter table functional.alltypes PARTITION (year=2010, month=11) " +
               "set tblproperties('a'='1')");
    AnalyzesOk("alter table functional.alltypes PARTITION (year=2010, month=11) " +
               "set serdeproperties ('a'='2')");
    // Arbitrary exprs as partition key values. Constant exprs are ok.
    AnalyzesOk("alter table functional.alltypes PARTITION " +
               "(year=cast(100*20+10 as INT), month=cast(2+9 as INT)) " +
               "set fileformat sequencefile");
    AnalyzesOk("alter table functional.alltypes PARTITION " +
               "(year=cast(100*20+10 as INT), month=cast(2+9 as INT)) " +
               "set location '/a/b'");
    // Arbitrary exprs as partition key values. Non-constant exprs should fail.
    AnalysisError("alter table functional.alltypes PARTITION " +
                  "(Year=2050, month=int_col) set fileformat sequencefile",
                  "Non-constant expressions are not supported as static partition-key " +
                  "values in 'month=int_col'.");
    AnalysisError("alter table functional.alltypes PARTITION " +
                  "(Year=2050, month=int_col) set location '/a/b'",
                  "Non-constant expressions are not supported as static partition-key " +
                  "values in 'month=int_col'.");

    // Partition spec does not exist
    AnalysisError("alter table functional.alltypes PARTITION (year=2014, month=11) " +
                  "set location '/a/b'",
                  "Partition spec does not exist: (year=2014, month=11)");
    AnalysisError("alter table functional.alltypes PARTITION (year=2014, month=11) " +
                  "set tblproperties('a'='1')",
                  "Partition spec does not exist: (year=2014, month=11)");
    AnalysisError("alter table functional.alltypes PARTITION (year=2010) " +
                  "set tblproperties('a'='1')",
                  "Items in partition spec must exactly match the partition columns " +
                  "in the table definition: functional.alltypes (1 vs 2)");
    AnalysisError("alter table functional.alltypes PARTITION (year=2010, year=2010) " +
                  "set location '/a/b'",
                  "Duplicate partition key name: year");
    AnalysisError("alter table functional.alltypes PARTITION (month=11, year=2014) " +
                  "set fileformat sequencefile",
                  "Partition spec does not exist: (month=11, year=2014)");
    AnalysisError("alter table functional.alltypesnopart PARTITION (month=1) " +
                  "set fileformat sequencefile",
                  "Table is not partitioned: functional.alltypesnopart");
    AnalysisError("alter table functional.alltypesnopart PARTITION (month=1) " +
                  "set location '/a/b/c'",
                  "Table is not partitioned: functional.alltypesnopart");
    AnalysisError("alter table functional.stringpartitionkey PARTITION " +
                  "(string_col='partition2') set location '/a/b'",
                  "Partition spec does not exist: (string_col='partition2')");
    AnalysisError("alter table functional.stringpartitionkey PARTITION " +
                  "(string_col='partition2') set fileformat sequencefile",
                  "Partition spec does not exist: (string_col='partition2')");
    AnalysisError("alter table functional.alltypes PARTITION " +
                 "(year=cast(10*20+10 as INT), month=cast(5*3 as INT)) " +
                  "set location '/a/b'",
                  "Partition spec does not exist: " +
                  "(year=CAST(10 * 20 + 10 AS INT), month=CAST(5 * 3 AS INT))");
    AnalysisError("alter table functional.alltypes PARTITION " +
                  "(year=cast(10*20+10 as INT), month=cast(5*3 as INT)) " +
                  "set fileformat sequencefile",
                  "Partition spec does not exist: " +
                  "(year=CAST(10 * 20 + 10 AS INT), month=CAST(5 * 3 AS INT))");

    // Table/Db does not exist
    AnalysisError("alter table db_does_not_exist.alltypes set fileformat sequencefile",
        "Database does not exist: db_does_not_exist");
    AnalysisError("alter table functional.table_does_not_exist set fileformat rcfile",
        "Table does not exist: functional.table_does_not_exist");
    AnalysisError("alter table db_does_not_exist.alltypes set location '/a/b'",
        "Database does not exist: db_does_not_exist");
    AnalysisError("alter table functional.table_does_not_exist set location '/a/b'",
        "Table does not exist: functional.table_does_not_exist");
    AnalysisError("alter table functional.no_tbl partition(i=1) set location '/a/b'",
        "Table does not exist: functional.no_tbl");
    AnalysisError("alter table no_db.alltypes partition(i=1) set fileformat textfile",
        "Database does not exist: no_db");

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

    // Cannot ALTER TABLE SET on an HBase table.
    AnalysisError("alter table functional_hbase.alltypes set tblproperties('a'='b')",
        "ALTER TABLE SET not currently supported on HBase tables.");
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

    AnalysisError("alter table functional.alltypes rename to db_does_not_exist.new_table",
        "Database does not exist: db_does_not_exist");

    // Cannot ALTER TABLE a view.
    AnalysisError("alter table functional.alltypes_view rename to new_alltypes",
        "ALTER TABLE not allowed on a view: functional.alltypes_view");

    // It should be okay to rename an HBase table.
    AnalyzesOk("alter table functional_hbase.alltypes rename to new_alltypes");
  }

  @Test
  public void TestAlterView() {
    // View-definition references a table.
    AnalyzesOk("alter view functional.alltypes_view as " +
        "select * from functional.alltypesagg");
    // View-definition references a view.
    AnalyzesOk("alter view functional.alltypes_view as " +
        "select * from functional.alltypes_view");

    // View-definition resulting in Hive-style auto-generated column names.
    AnalyzesOk("alter view functional.alltypes_view as " +
        "select trim('abc'), 17 * 7");

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
        "Database does not exist: baddb");
    // View-definition statement fails to analyze. Table does not exist.
    AnalysisError("alter view functional.alltypes_view as " +
        "select * from functional.badtable",
        "Table does not exist: functional.badtable");
    // Duplicate column name.
    AnalysisError("alter view functional.alltypes_view as " +
        "select * from functional.alltypessmall a inner join " +
        "functional.alltypessmall b on a.id = b.id",
        "Duplicate column name: id");
    // Invalid column name.
    AnalysisError("alter view functional.alltypes_view as select 'abc' as `???`",
        "Invalid column name: ???");
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
  public void TestComputeStats() throws AnalysisException {
    // Analyze the stmt itself as well as the generated child queries.
    ParseNode parseNode = AnalyzesOk("compute stats functional.alltypes");
    assertTrue(parseNode instanceof ComputeStatsStmt);
    ComputeStatsStmt stmt = (ComputeStatsStmt) parseNode;
    AnalyzesOk(stmt.getTblStatsQuery());
    AnalyzesOk(stmt.getColStatsQuery());

    parseNode = AnalyzesOk("compute stats functional_hbase.alltypes");
    assertTrue(parseNode instanceof ComputeStatsStmt);
    stmt = (ComputeStatsStmt) parseNode;
    AnalyzesOk(stmt.getTblStatsQuery());
    AnalyzesOk(stmt.getColStatsQuery());

    // Cannot compute stats on a database.
    AnalysisError("compute stats tbl_does_not_exist",
        "Table does not exist: default.tbl_does_not_exist");
    // Cannot compute stats on a view.
    AnalysisError("compute stats functional.alltypes_view",
        "COMPUTE STATS not allowed on a view: functional.alltypes_view");
  }

  @Test
  public void TestDrop() throws AnalysisException {
    AnalyzesOk("drop database functional");
    AnalyzesOk("drop table functional.alltypes");
    AnalyzesOk("drop view functional.alltypes_view");

    // If the database does not exist, and the user hasn't specified "IF EXISTS",
    // an analysis error should be thrown
    AnalysisError("drop database db_does_not_exist",
        "Database does not exist: db_does_not_exist");
    AnalysisError("drop table db_does_not_exist.alltypes",
        "Database does not exist: db_does_not_exist");
    AnalysisError("drop view db_does_not_exist.alltypes_view",
        "Database does not exist: db_does_not_exist");
    // Invalid name reports non-existence instead of invalidity.
    AnalysisError("drop database `???`",
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
  }

  @Test
  public void TestCreateDb() throws AnalysisException {
    AnalyzesOk("create database some_new_database");
    AnalysisError("create database functional", "Database already exists: functional");
    AnalyzesOk("create database if not exists functional");
    // Invalid database name,
    AnalysisError("create database `%^&`", "Invalid database name: %^&");
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

    // Table already exists with and without IF NOT EXISTS
    AnalysisError("create table functional.alltypes as select 1",
        "Table already exists: functional.alltypes");
    AnalyzesOk("create table if not exists functional.alltypes as select 1");

    // Database does not exist
    AnalysisError("create table db_does_not_exist.new_table as select 1",
        "Database does not exist: db_does_not_exist");

    // Analysis errors in the SELECT statement
    AnalysisError("create table newtbl as select * from tbl_does_not_exist",
        "Table does not exist: default.tbl_does_not_exist");
    AnalysisError("create table newtbl as select 1 as c1, 2 as c1",
        "Duplicate column name: c1");

    // Unsupported file formats
    AnalysisError("create table foo stored as sequencefile as select 1",
        "CREATE TABLE AS SELECT does not support (SEQUENCEFILE) file format. " +
         "Supported formats are: (PARQUET, TEXTFILE)");
    AnalysisError("create table foo stored as RCFILE as select 1",
        "CREATE TABLE AS SELECT does not support (RCFILE) file format. " +
         "Supported formats are: (PARQUET, TEXTFILE)");
  }

  @Test
  public void TestCreateTable() throws AnalysisException {
    AnalyzesOk("create table functional.new_table (i int)");
    AnalyzesOk("create table if not exists functional.alltypes (i int)");
    AnalyzesOk("create table if not exists functional.new_tbl like functional.alltypes");
    AnalyzesOk("create table if not exists functional.alltypes like functional.alltypes");
    AnalysisError("create table functional.alltypes like functional.alltypes",
        "Table already exists: functional.alltypes");
    AnalysisError("create table functional.new_table like functional.tbl_does_not_exist",
        "Table does not exist: functional.tbl_does_not_exist");
    AnalysisError("create table functional.new_table like db_does_not_exist.alltypes",
        "Database does not exist: db_does_not_exist");
    AnalysisError("create table functional.alltypes (i int)",
        "Table already exists: functional.alltypes");
    AnalyzesOk("create table functional.new_table (i int) row format delimited fields " +
        "terminated by '|'");

    // Note: Backslashes need to be escaped twice - once for Java and once for Impala.
    // For example, if this were a real query the value '\' would be stored in the
    // metastore for the ESCAPED BY field.
    AnalyzesOk("create table functional.new_table (i int) row format delimited fields " +
        "terminated by '\\t' escaped by '\\\\' lines terminated by '\\n'");

    AnalysisError("create table functional.new_table (i int) row format delimited " +
        "fields terminated by '||' escaped by '\\\\' lines terminated by '\\n'",
        "ESCAPED BY values and LINE/FIELD terminators must have length of 1: ||");

    AnalysisError("create table db_does_not_exist.new_table (i int)",
        "Database does not exist: db_does_not_exist");
    AnalysisError("create table new_table (i int, I string)",
        "Duplicate column name: I");
    AnalysisError("create table new_table (c1 double, col2 int, c1 double, c4 string)",
        "Duplicate column name: c1");
    AnalysisError("create table new_table (i int, s string) PARTITIONED BY (i int)",
        "Duplicate column name: i");
    AnalysisError("create table new_table (i int) PARTITIONED BY (C int, c2 int, c int)",
        "Duplicate column name: c");

    // Unsupported partition-column types.
    AnalysisError("create table new_table (i int) PARTITIONED BY (t timestamp)",
        "Type 'TIMESTAMP' is not supported as partition-column type in column: t");
    AnalysisError("create table new_table (i int) PARTITIONED BY (d date)",
        "Type 'DATE' is not supported as partition-column type in column: d");
    AnalysisError("create table new_table (i int) PARTITIONED BY (d datetime)",
        "Type 'DATETIME' is not supported as partition-column type in column: d");

    // Analysis of Avro schemas
    AnalyzesOk("create table foo (i int) with serdeproperties ('avro.schema.url'=" +
        "'hdfs://schema.avsc') stored as avro");
    AnalyzesOk("create table foo (i int) stored as avro tblproperties " +
        "('avro.schema.url'='hdfs://schema.avsc')");
    AnalyzesOk("create table foo (i int) stored as avro tblproperties " +
        "('avro.schema.literal'='{\"name\": \"my_record\"}')");
    AnalysisError("create table foo (i int) stored as avro",
        "No Avro schema provided for table: default.foo");
    AnalysisError("create table foo (i int) stored as avro tblproperties ('a'='b')",
        "No Avro schema provided for table: default.foo");
    AnalysisError("create table foo (i int) stored as avro tblproperties " +
        "('avro.schema.url'='schema.avsc')", "avro.schema.url must be of form " +
        "\"http://path/to/schema/file\" or \"hdfs://namenode:port/path/to/schema/file" +
        "\", got schema.avsc");

    // Invalid database name.
    AnalysisError("create table `???`.new_table (x int) PARTITIONED BY (y int)",
        "Invalid database name: ???");
    // Invalid table/view name.
    AnalysisError("create table functional.`^&*` (x int) PARTITIONED BY (y int)",
        "Invalid table/view name: ^&*");
    // Invalid column names.
    AnalysisError("create table new_table (`???` int) PARTITIONED BY (i int)",
        "Invalid column name: ???");
    AnalysisError("create table new_table (i int) PARTITIONED BY (`^&*` int)",
        "Invalid column name: ^&*");
    // Invalid source database/table name reports non-existence instead of invalidity.
    AnalysisError("create table functional.foo like `???`.alltypes",
        "Database does not exist: ???");
    AnalysisError("create table functional.foo like functional.`%^&`",
        "Table does not exist: functional.%^&");

    // Invalid URI values.
    AnalysisError("ALTER TABLE functional_seq_snap.alltypes SET LOCATION " +
        "'file://test-warehouse/new_table'", "URI location " +
        "'file://test-warehouse/new_table' must point to an HDFS file system.");
    AnalysisError("ALTER TABLE functional_seq_snap.alltypes SET LOCATION " +
        "'  '", "URI path cannot be empty.");
  }

  @Test
  public void TestCreateView() throws AnalysisException {
    AnalyzesOk("create view foo as select int_col, string_col from functional.alltypes");
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
    AnalysisError("create view foo as select 1 as `???`",
        "Invalid column name: ???");
    AnalysisError("create view foo(`%^&`) as select 1",
        "Invalid column name: %^&");

    // Table/view already exists.
    AnalysisError("create view functional.alltypes as " +
        "select * from functional.alltypessmall ",
        "Table already exists: functional.alltypes");
    // Target database does not exist.
    AnalysisError("create view wrongdb.test as " +
        "select * from functional.alltypessmall ",
        "Database does not exist: wrongdb");
    // Source database does not exist,
    AnalysisError("create view foo as " +
        "select * from wrongdb.alltypessmall ",
        "Database does not exist: wrongdb");
    // Source table does not exist,
    AnalysisError("create view foo as " +
        "select * from wrongdb.alltypessmall ",
        "Database does not exist: wrongdb");
    // Analysis error in view-definition statement.
    AnalysisError("create view foo as " +
        "select int_col from functional.alltypessmall union all " +
        "select string_col from functional.alltypes",
        "Incompatible return types 'INT' and 'STRING' of exprs " +
        "'int_col' and 'string_col'.");
  }

  @Test
  public void TestUdf() throws AnalysisException {
    final String symbol =
        "'_Z8IdentityPN10impala_udf15FunctionContextERKNS_10BooleanValE'";
    final String udfSuffix = " LOCATION '/test-warehouse/libTestUdfs.so' " +
        "SYMBOL=" + symbol;

    AnalyzesOk("create function foo() RETURNS int" + udfSuffix);
    AnalyzesOk("create function foo(int, int, string) RETURNS int" + udfSuffix);

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
    AnalyzesOk("create function foo() RETURNS int LOCATION '/binary.JAR' SYMBOL='a'");

    // Varargs
    AnalyzesOk("create function foo(INT...) RETURNS int" + udfSuffix);

    // Try to create a function with the same name as a builtin
    AnalysisError("create function sin(double) RETURNS double" + udfSuffix,
        "Function cannot have the same name as a builtin: sin");
    AnalysisError("create function sin() RETURNS double" + udfSuffix,
        "Function cannot have the same name as a builtin: sin");

    // Try to create with a bad location
    AnalysisError("create function foo() RETURNS int LOCATION 'bad-location' SYMBOL='c'",
        "URI path must be absolute: bad-location");

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
    AnalysisError("create function foo() RETURNS int " +
        "LOCATION '/test-warehouse/libTestUdfs.so' " +
        "SYMBOL='b'", "Could not find symbol 'b' in: /test-warehouse/libTestUdfs.so");
    AnalysisError("create function foo() RETURNS int " +
        "LOCATION '/test-warehouse/libTestUdfs.so' " +
        "SYMBOL=''", "Could not find symbol '' in: /test-warehouse/libTestUdfs.so");
    AnalysisError("create function foo() RETURNS int " +
        "LOCATION '/test-warehouse/libTestUdfs.so' " +
        "SYMBOL='_ZAB'",
        "Could not find symbol '_ZAB' in: /test-warehouse/libTestUdfs.so");

    // Infer the fully mangled symbol from the signature
    AnalyzesOk("create function foo() RETURNS int " +
        "LOCATION '/test-warehouse/libTestUdfs.so' " + "SYMBOL='NoArgs'");
    // We can't get the return type so any of those will match
    AnalyzesOk("create function foo() RETURNS double " +
        "LOCATION '/test-warehouse/libTestUdfs.so' " + "SYMBOL='NoArgs'");
    // The part the user specifies is case sensitive
    AnalysisError("create function foo() RETURNS int " +
        "LOCATION '/test-warehouse/libTestUdfs.so' " + "SYMBOL='noArgs'",
        "Could not find symbol 'noArgs' in: /test-warehouse/libTestUdfs.so");
    // Types no longer match
    AnalysisError("create function foo(int) RETURNS int " +
        "LOCATION '/test-warehouse/libTestUdfs.so' " + "SYMBOL='NoArgs'",
        "Could not find symbol 'NoArgs' in: /test-warehouse/libTestUdfs.so");

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
        "smallint, int, bigint, float, double) returns int " +
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

    // Try dropping functions.
    AnalyzesOk("drop function if exists foo()");
    AnalysisError("drop function foo()", "Function does not exist: foo()");
    AnalyzesOk("drop function if exists a.foo()");
    AnalysisError("drop function a.foo()", "Database does not exist: a");
    AnalyzesOk("drop function if exists foo()");
    AnalyzesOk("drop function if exists foo(int...)");
    AnalyzesOk("drop function if exists foo(double, int...)");

    // Add functions default.TestFn(), default.TestFn(double), default.TestFn(String...),
    addTestFunction("TestFn", new ArrayList<PrimitiveType>(), false);
    addTestFunction("TestFn", Lists.newArrayList(PrimitiveType.DOUBLE), false);
    addTestFunction("TestFn", Lists.newArrayList(PrimitiveType.STRING), true);

    AnalysisError("create function TestFn() RETURNS INT " + udfSuffix,
        "Function already exists: testfn()");
    AnalysisError("create function TestFn(double) RETURNS INT " + udfSuffix,
        "Function already exists: testfn(DOUBLE)");

    // Fn(Double) and Fn(Double...) should be a conflict.
    AnalysisError("create function TestFn(double...) RETURNS INT" + udfSuffix,
        "Function already exists: testfn(DOUBLE)");
    AnalysisError("create function TestFn(double) RETURNS INT" + udfSuffix,
        "Function already exists: testfn(DOUBLE)");

    // Add default.TestFn(int, int)
    addTestFunction("TestFn",
        Lists.newArrayList(PrimitiveType.INT, PrimitiveType.INT), false);
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
    AnalysisError("drop function TestFn(double...)",
        "Function does not exist: testfn(DOUBLE...)");
    AnalysisError("drop function TestFn(int)", "Function does not exist: testfn(INT)");
    AnalysisError(
        "drop function functional.TestFn()", "Function does not exist: testfn()");

    addTestFunction("udf_test", "TestFn", new ArrayList<PrimitiveType>(), false);
    AnalysisError(
        "drop database udf_test", "Cannot drop non-empty database: udf_test");

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
    AnalyzesOk("create aggregate function foo(int) RETURNS int" + loc +
        "UPDATE_FN='AggUpdate'");
    AnalyzesOk("create aggregate function foo(int) RETURNS int" + loc +
        "UPDATE_FN='AggUpdate' INIT_FN='AggInit'");
    AnalyzesOk("create aggregate function foo(int) RETURNS int" + loc +
        "UPDATE_FN='AggUpdate' INIT_FN='AggInit' MERGE_FN='AggMerge'");
    AnalysisError("create aggregate function foo(int) RETURNS int" + loc +
        "UPDATE_FN='AggUpdate' INIT_FN='AGgInit'",
        "Could not find symbol 'AGgInit' in: /test-warehouse/libTestUdas.so");
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
    // TODO: this is currently not supported. Remove these tests and re-enable
    // the commented out ones when we do.
    AnalyzesOk("create aggregate function foo(int) RETURNS int " +
        "INTERMEDIATE int" + loc + "UPDATE_FN='AggUpdate'");
    AnalysisError("create aggregate function foo(int) RETURNS int " +
        "INTERMEDIATE double" + loc + "UPDATE_FN='AggUpdate'",
        "UDAs with an intermediate type, DOUBLE, that is different from the " +
        "return type, INT, are currently not supported.");
    AnalysisError("create aggregate function foo(int) RETURNS int " +
        "INTERMEDIATE char(10)" + loc + "UPDATE_FN='AggUpdate'",
        "UDAs with an intermediate type, CHAR(10), that is different from the " +
        "return type, INT, are currently not supported.");
    //AnalyzesOk("create aggregate function foo(int) RETURNS int " +
    //    "INTERMEDIATE CHAR(10)" + loc + "UPDATE_FN='AggUpdate'");
    //AnalysisError("create aggregate function foo(int) RETURNS int " +
    //    "INTERMEDIATE CHAR(10)" + loc + "UPDATE_FN='Agg' INIT_FN='AggInit' " +
    //    "MERGE_FN='AggMerge'" ,
    //    "Finalize() is required for this UDA.");
    //AnalyzesOk("create aggregate function foo(int) RETURNS int " +
    //    "INTERMEDIATE CHAR(10)" + loc + "UPDATE_FN='Agg' INIT_FN='AggInit' " +
    //    "MERGE_FN='AggMerge' FINALIZE_FN='AggFinalize'");

    // Udf only arguments must not be set.
    AnalysisError("create aggregate function foo(int) RETURNS int" + loc + "SYMBOL='Bad'",
        "Optional argument 'SYMBOL' should not be set.");

    // Invalid char(0) type.
    AnalysisError("create aggregate function foo(int) RETURNS int " +
        "INTERMEDIATE CHAR(0) LOCATION '/foo.so' UPDATE_FN='b'",
        "Array size must be > 0. Size was set to: 0.");
    AnalysisError("create aggregate function foo() RETURNS int" + loc,
        "UDAs must take at least one argument.");
    AnalysisError("create aggregate function foo(int) RETURNS int LOCATION " +
        "'/foo.jar' UPDATE_FN='b'", "Java UDAs are not supported.");

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
        "Could not find symbol 'AggSerialize' in: /test-warehouse/libTestUdas.so");
    // If you specify a mangled name, we just check it exists.
    // TODO: we should be able to validate better. This is almost certainly going
    // to crash everything.
    AnalyzesOk("create aggregate function foo(string, double) RETURNS string" + loc +
        "UPDATE_FN='AggUpdate' "+
        "INIT_FN='_Z12AggSerializePN10impala_udf15FunctionContextERKNS_6IntValE'");
    AnalysisError("create aggregate function foo(string, double) RETURNS string" + loc +
        "UPDATE_FN='AggUpdate' INIT_FN='_ZAggSerialize'",
        "Could not find symbol '_ZAggSerialize' in: /test-warehouse/libTestUdas.so");

    // Tests for checking the symbol exists
    AnalysisError("create aggregate function foo(string, double) RETURNS string" + loc +
        "UPDATE_FN='Agg2Update'",
        "Could not find symbol 'Agg2Init' in: /test-warehouse/libTestUdas.so");
    AnalysisError("create aggregate function foo(string, double) RETURNS string" + loc +
        "UPDATE_FN='Agg2Update' INIT_FN='AggInit'",
        "Could not find symbol 'Agg2Merge' in: /test-warehouse/libTestUdas.so");
    AnalyzesOk("create aggregate function foo(string, double) RETURNS string" + loc +
        "UPDATE_FN='Agg2Update' INIT_FN='AggInit' MERGE_FN='AggMerge'");
    AnalysisError("create aggregate function foo(string, double) RETURNS string" + loc +
        "UPDATE_FN='Agg2Update' INIT_FN='AggInit' MERGE_FN='BadFn'",
        "Could not find symbol 'BadFn' in: /test-warehouse/libTestUdas.so");
    AnalysisError("create aggregate function foo(string, double) RETURNS string" + loc +
        "UPDATE_FN='Agg2Update' INIT_FN='AggInit' MERGE_FN='AggMerge' "+
            "FINALIZE_FN='not there'",
        "Could not find symbol 'not there' in: /test-warehouse/libTestUdas.so");
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
  public void TestDescribe() throws AnalysisException {
    AnalyzesOk("describe formatted functional.alltypes");
    AnalyzesOk("describe functional.alltypes");
    AnalysisError("describe formatted nodb.alltypes",
        "Database does not exist: nodb");
    AnalysisError("describe functional.notbl",
        "Table does not exist: functional.notbl");
  }

  @Test
  public void TestShow() throws AnalysisException {
    AnalyzesOk("show databases");
    AnalyzesOk("show databases like '*pattern'");

    AnalyzesOk("show tables");
    AnalyzesOk("show tables like '*pattern'");

    AnalyzesOk("show functions");
    AnalyzesOk("show functions like '*pattern'");
    AnalyzesOk("show functions in functional");
    AnalyzesOk("show functions in functional like '*pattern'");
    // Database doesn't exist.
    AnalysisError("show functions in baddb", "Database does not exist: baddb");
    AnalysisError("show functions in baddb like '*pattern'",
        "Database does not exist: baddb");
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
}
