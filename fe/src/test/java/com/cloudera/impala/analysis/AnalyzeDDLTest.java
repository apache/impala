// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.analysis;

import junit.framework.Assert;

import org.junit.Test;

import com.cloudera.impala.common.AnalysisException;

public class AnalyzeDDLTest extends AnalyzerTest {

  @Test
  public void TestAlterTableAddDropPartition() throws AnalysisException {
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
      AnalysisError("alter table functional.hbasealltypesagg " + kw +
          " partition (i=1)", "Table is not partitioned: functional.hbasealltypesagg");

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
          " partition (i=1)", "Unknown database: db_does_not_exist");
      AnalysisError("alter table functional.table_does_not_exist " + kw +
          " partition (i=1)", "Unknown table: functional.table_does_not_exist");
    }

    // IF NOT EXISTS properly checks for partition existence
    AnalyzesOk("alter table functional.alltypes add " +
          "partition(year=2050, month=10)");
    AnalysisError("alter table functional.alltypes add " +
          "partition(year=2010, month=10)",
          "Partition spec already exists: (year=2010, month=10).");
    AnalyzesOk("alter table functional.alltypes add if not exists " +
          " partition(year=2010, month=10)");

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
        "Unknown database: db_does_not_exist");
    AnalysisError("alter table functional.table_does_not_exist add columns (i int)",
        "Unknown table: functional.table_does_not_exist");
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
        "Unknown database: db_does_not_exist");
    AnalysisError("alter table functional.table_does_not_exist drop column col1",
        "Unknown table: functional.table_does_not_exist");
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

    // Table/Db does not exist
    AnalysisError("alter table db_does_not_exist.alltypes change c1 c2 int",
        "Unknown database: db_does_not_exist");
    AnalysisError("alter table functional.table_does_not_exist change c1 c2 double",
        "Unknown table: functional.table_does_not_exist");
  }

  @Test
  public void TestAlterTableSet() throws AnalysisException {
    AnalyzesOk("alter table functional.alltypes set fileformat sequencefile");
    AnalyzesOk("alter table functional.alltypes set location '/a/b'");
    AnalyzesOk("alter table functional.alltypes PARTITION (Year=2010, month=11) " +
               "set location '/a/b'");
    AnalyzesOk("alter table functional.alltypes PARTITION (month=11, year=2010) " +
               "set fileformat parquetfile");
    AnalyzesOk("alter table functional.stringpartitionkey PARTITION " +
               "(string_col='partition1') set fileformat parquetfile");
    AnalyzesOk("alter table functional.stringpartitionkey PARTITION " +
               "(string_col='PaRtiTion1') set location '/a/b/c'");
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
                  "No matching partition spec found: (year=2014, month=11)");
    AnalysisError("alter table functional.alltypes PARTITION (year=2010, year=2010) " +
                  "set location '/a/b'",
                  "No matching partition spec found: (year=2010, year=2010)");
    AnalysisError("alter table functional.alltypes PARTITION (month=11, year=2014) " +
                  "set fileformat sequencefile",
                  "No matching partition spec found: (month=11, year=2014)");
    AnalysisError("alter table functional.alltypesnopart PARTITION (month=1) " +
                  "set fileformat sequencefile",
                  "Table is not partitioned: functional.alltypesnopart");
    AnalysisError("alter table functional.alltypesnopart PARTITION (month=1) " +
                  "set location '/a/b/c'",
                  "Table is not partitioned: functional.alltypesnopart");
    AnalysisError("alter table functional.stringpartitionkey PARTITION " +
                  "(string_col='partition2') set location '/a/b'",
                  "No matching partition spec found: (string_col='partition2')");
    AnalysisError("alter table functional.stringpartitionkey PARTITION " +
                  "(string_col='partition2') set fileformat sequencefile",
                  "No matching partition spec found: (string_col='partition2')");
    AnalysisError("alter table functional.alltypes PARTITION " +
                 "(year=cast(10*20+10 as INT), month=cast(5*3 as INT)) " +
                  "set location '/a/b'",
                  "No matching partition spec found: " +
                  "(year=CAST(10 * 20 + 10 AS INT), month=CAST(5 * 3 AS INT))");
    AnalysisError("alter table functional.alltypes PARTITION " +
                  "(year=cast(10*20+10 as INT), month=cast(5*3 as INT)) " +
                  "set fileformat sequencefile",
                  "No matching partition spec found: " +
                  "(year=CAST(10 * 20 + 10 AS INT), month=CAST(5 * 3 AS INT))");

    // Table/Db does not exist
    AnalysisError("alter table db_does_not_exist.alltypes set fileformat sequencefile",
        "Unknown database: db_does_not_exist");
    AnalysisError("alter table functional.table_does_not_exist set fileformat rcfile",
        "Unknown table: functional.table_does_not_exist");
    AnalysisError("alter table db_does_not_exist.alltypes set location '/a/b'",
        "Unknown database: db_does_not_exist");
    AnalysisError("alter table functional.table_does_not_exist set location '/a/b'",
        "Unknown table: functional.table_does_not_exist");
    AnalysisError("alter table functional.no_tbl partition(i=1) set location '/a/b'",
        "Unknown table: functional.no_tbl");
    AnalysisError("alter table no_db.alltypes partition(i=1) set fileformat textfile",
        "Unknown database: no_db");
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
        "Unknown table: functional.table_does_not_exist");
    AnalysisError("alter table db_does_not_exist.alltypes rename to new_table",
        "Unknown database: db_does_not_exist");

    AnalysisError("alter table functional.alltypes rename to db_does_not_exist.new_table",
        "Unknown database: db_does_not_exist");
  }

  @Test
  public void TestDrop() throws AnalysisException {
    AnalyzesOk("drop database functional");
    AnalyzesOk("drop table functional.alltypes");

    // If the database does not exist, and the user hasn't specified "IF EXISTS",
    // an analysis error should be thrown.
    AnalysisError("drop database db_does_not_exist",
        "Unknown database: db_does_not_exist");
    AnalysisError("drop table db_does_not_exist.alltypes",
        "Unknown database: db_does_not_exist");

    // No error is thrown if the user specifies IF EXISTS
    AnalyzesOk("drop database if exists db_does_not_exist");
    AnalyzesOk("drop table if exists db_does_not_exist.alltypes");
  }

  @Test
  public void TestCreateDb() throws AnalysisException {
    AnalyzesOk("create database some_new_database");
    AnalysisError("create database functional", "Database already exists: functional");
    AnalyzesOk("create database if not exists functional");
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
        "Source table does not exist: functional.tbl_does_not_exist");
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
}
