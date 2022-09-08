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

import org.apache.impala.catalog.KuduTable;
import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.testutil.TestUtils;
import org.apache.kudu.ColumnSchema.CompressionAlgorithm;
import org.apache.kudu.ColumnSchema.Encoding;
import org.junit.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Tests on DDL analysis for Kudu tables.
 */
public class AnalyzeKuduDDLTest extends FrontendTestBase {

  /**
   * This is wrapper around super.AnalyzesOk method. The additional boolean is used to
   * add tblproperties for synchronized table
   */
  public ParseNode AnalyzesOk(String stmt, String errorStr, boolean isExternalPurgeTbl) {
    return super
        .AnalyzesOk(appendSynchronizedTblProps(stmt, isExternalPurgeTbl), errorStr);
  }

  /**
   * Wrapper around super.AnalyzesOk with additional boolean for adding synchronized
   * table properties
   */
  public ParseNode AnalyzesOk(String stmt, boolean isExternalPurgeTbl) {
    return super.AnalyzesOk(appendSynchronizedTblProps(stmt, isExternalPurgeTbl));
  }

  private String appendSynchronizedTblProps(String stmt, boolean append) {
    if (!append) { return stmt; }

    stmt = stmt.replace("create table", "create external table");
    if (!stmt.contains("tblproperties")) {
      stmt += " tblproperties ('external.table.purge'='true')";
    } else {
      stmt = stmt.replaceAll("tblproperties\\s*\\(",
          "tblproperties ('external.table.purge'='true', ");
    }
    return stmt;
  }

  public void AnalysisError(String stmt, String expectedError,
      boolean isExternalPurgeTbl) {
    super.AnalysisError(appendSynchronizedTblProps(stmt, isExternalPurgeTbl),
        expectedError);
  }

  private void testDDlsOnKuduTable(boolean isExternalPurgeTbl) {
    // Test primary keys and partition by clauses
    AnalyzesOk("create table tab (x int primary key) partition by hash(x) " +
        "partitions 8 stored as kudu", isExternalPurgeTbl);
    AnalyzesOk("create table tab (x int, primary key(x)) partition by hash(x) " +
        "partitions 8 stored as kudu", isExternalPurgeTbl);
    AnalyzesOk("create table tab (x int, y int, primary key (x, y)) " +
        "partition by hash(x, y) partitions 8 stored as kudu", isExternalPurgeTbl);
    AnalyzesOk("create table tab (x int, y int, primary key (x)) " +
        "partition by hash(x) partitions 8 stored as kudu", isExternalPurgeTbl);
    AnalyzesOk("create table tab (x int, y int, primary key(x, y)) " +
        "partition by hash(y) partitions 8 stored as kudu", isExternalPurgeTbl);
    AnalyzesOk("create table tab (x timestamp, y timestamp, primary key(x)) " +
        "partition by hash(x) partitions 8 stored as kudu", isExternalPurgeTbl);
    // Test non unique primary key
    AnalyzesOk("create table tab (x int non unique primary key) partition by hash(x) " +
        "partitions 8 stored as kudu", isExternalPurgeTbl);
    AnalyzesOk("create table tab (x int, non unique primary key(x)) " +
        "partition by hash(x) partitions 8 stored as kudu", isExternalPurgeTbl);
    AnalyzesOk("create table tab (x int, y int, non unique primary key (x, y)) " +
        "partition by hash(x, y) partitions 8 stored as kudu", isExternalPurgeTbl);
    AnalyzesOk("create table tab (x int, y int, non unique primary key (x)) " +
        "partition by range (partition values < 10, partition 10 <= values < 30, " +
        "partition 30 <= values) stored as kudu tblproperties(" +
        "'kudu.num_tablet_replicas' = '3')", isExternalPurgeTbl);
    AnalyzesOk("create table tab (x int, y int, non unique primary key(x, y)) " +
        "stored as kudu", isExternalPurgeTbl);
    AnalyzesOk("create table tab (x timestamp, y timestamp, non unique primary key(x))" +
        " partition by hash(x) partitions 8 stored as kudu", isExternalPurgeTbl);
    // Promote all partition columns as non unique primary key columns if primary keys
    // are not declared, but partition columns must be the first columns in the table.
    AnalyzesOk("create table tab (x int, y int) partition by hash(x) partitions 8 " +
        "stored as kudu", isExternalPurgeTbl);
    AnalyzesOk("create table tab (x int, y int) partition by hash(x, y) partitions 8 " +
        "stored as kudu", isExternalPurgeTbl);
    AnalysisError("create table tab (x int, y int) partition by hash(y) partitions 8 " +
        "stored as kudu", "Specify primary key or non unique primary key for the Kudu " +
        "table, or create partitions with the beginning columns of the table.",
        isExternalPurgeTbl);

    AnalyzesOk("create table tab (x int, y string, primary key (x)) partition by " +
        "hash (x) partitions 3, range (x) (partition values < 1, partition " +
        "1 <= values < 10, partition 10 <= values < 20, partition value = 30) " +
        "stored as kudu", isExternalPurgeTbl);
    AnalyzesOk("create table tab (x int, y int, primary key (x, y)) partition by " +
        "range (x, y) (partition value = (2001, 1), partition value = (2002, 1), " +
        "partition value = (2003, 2)) stored as kudu", isExternalPurgeTbl);
    // Non-literal boundary values in range partitions
    AnalyzesOk("create table tab (x int, y int, primary key (x)) partition by " +
        "range (x) (partition values < 1 + 1, partition (1+3) + 2 < values < 10, " +
        "partition factorial(4) < values < factorial(5), " +
        "partition value = factorial(6)) stored as kudu", isExternalPurgeTbl);
    AnalyzesOk("create table tab (x int, y int, primary key(x, y)) partition by " +
        "range(x, y) (partition value = (1+1, 2+2), partition value = ((1+1+1)+1, 10), " +
        "partition value = (cast (30 as int), factorial(5))) stored as kudu",
        isExternalPurgeTbl);
    AnalysisError("create table tab (x int primary key) partition by range (x) " +
        "(partition values < x + 1) stored as kudu", "Only constant values are allowed " +
        "for range-partition bounds: x + 1", isExternalPurgeTbl);
    AnalysisError("create table tab (x int primary key) partition by range (x) " +
        "(partition values <= isnull(null, null)) stored as kudu", "Range partition " +
        "values cannot be NULL. Range partition: 'PARTITION VALUES <= " +
        "isnull(NULL, NULL)'", isExternalPurgeTbl);
    AnalysisError("create table tab (x int primary key) partition by range (x) " +
        "(partition values <= (select count(*) from functional.alltypestiny)) " +
        "stored as kudu", "Only constant values are allowed for range-partition " +
        "bounds: (SELECT count(*) FROM functional.alltypestiny)", isExternalPurgeTbl);
    // Multilevel partitioning. Data is split into 3 buckets based on 'x' and each
    // bucket is partitioned into 4 tablets based on the range partitions of 'y'.
    AnalyzesOk("create table tab (x int, y string, primary key(x, y)) " +
        "partition by hash(x) partitions 3, range(y) " +
        "(partition values < 'aa', partition 'aa' <= values < 'bb', " +
        "partition 'bb' <= values < 'cc', partition 'cc' <= values) " +
        "stored as kudu", isExternalPurgeTbl);
    // Swapped RANGE and HASH
    AnalyzesOk("create table tab (x int, y string, primary key(x, y)) " +
        "partition by range(y) (partition values < 'aa', partition 'aa' " +
        "<= values < 'bb', partition 'bb' <= values < 'cc', partition 'cc' <= values), " +
        "hash(x) partitions 3 stored as kudu", isExternalPurgeTbl);
    // RANGE followed by multiple HASH
    AnalyzesOk("create table tab (x int, y string, primary key(x, y)) " +
        "partition by range(y) (partition values < 'aa', partition 'aa' " +
        "<= values < 'bb', partition 'bb' <= values < 'cc', partition 'cc' <= values), " +
        "hash(x) partitions 3, hash(y) partitions 2 stored as kudu", isExternalPurgeTbl);
    // Key column in upper case
    AnalyzesOk("create table tab (x int, y int, primary key (X)) " +
        "partition by hash (x) partitions 8 stored as kudu", isExternalPurgeTbl);
    AnalyzesOk("create table tab (x int, y int, non unique primary key (X)) " +
        "partition by hash (x) partitions 8 stored as kudu", isExternalPurgeTbl);
    // Flexible Partitioning
    AnalyzesOk("create table tab (a int, b int, c int, d int, primary key (a, b, c))" +
        "partition by hash (a, b) partitions 8, hash(c) partitions 2 stored as " +
        "kudu", isExternalPurgeTbl);
    // No columns specified in the PARTITION BY HASH clause
    AnalyzesOk("create table tab (a int primary key, b int, c int, d int) " +
        "partition by hash partitions 8 stored as kudu", isExternalPurgeTbl);
    // Distribute range data types are picked up during analysis and forwarded to Kudu.
    // Column names in distribute params should also be case-insensitive.
    AnalyzesOk("create table tab (a int, b int, c int, d int, primary key(a, b, c, d))" +
        "partition by hash (a, B, c) partitions 8, " +
        "range (A) (partition values < 1, partition 1 <= values < 2, " +
        "partition 2 <= values < 3, partition 3 <= values < 4, partition 4 <= values) " +
        "stored as kudu", isExternalPurgeTbl);
    // Allowing range partitioning on a subset of the primary keys
    AnalyzesOk("create table tab (id int, name string, valf float, vali bigint, " +
        "primary key (id, name)) partition by range (name) " +
        "(partition 'aa' < values <= 'bb') stored as kudu", isExternalPurgeTbl);
    // Null values in range partition values
    AnalysisError("create table tab (id int, name string, primary key(id, name)) " +
        "partition by hash (id) partitions 3, range (name) " +
        "(partition value = null, partition value = 1) stored as kudu",
        "Range partition values cannot be NULL. Range partition: 'PARTITION " +
        "VALUE = NULL'", isExternalPurgeTbl);
    // Primary key specified in tblproperties
    AnalysisError(String.format("create table tab (x int) partition by hash (x) " +
        "partitions 8 stored as kudu tblproperties ('%s' = 'x')",
        KuduTable.KEY_KEY_COLUMNS), "PRIMARY KEY must be used instead of the table " +
        "property", isExternalPurgeTbl);
    // Primary key column that doesn't exist
    AnalysisError("create table tab (x int, y int, primary key (z)) " +
        "partition by hash (x) partitions 8 stored as kudu",
        "PRIMARY KEY column 'z' does not exist in the table", isExternalPurgeTbl);
    AnalysisError("create table tab (x int, y int, non unique primary key (z)) " +
        "partition by hash (x) partitions 8 stored as kudu",
        "NON UNIQUE PRIMARY KEY column 'z' does not exist in the table",
        isExternalPurgeTbl);
    // Invalid composite primary key
    AnalysisError("create table tab (x int primary key, primary key(x)) stored " +
        "as kudu", "Multiple PRIMARY KEYS specified. Composite PRIMARY KEY can " +
        "be specified using the PRIMARY KEY (col1, col2, ...) syntax at the end " +
        "of the column definition.", isExternalPurgeTbl);
    AnalysisError("create table tab (x int primary key, y int primary key) stored " +
        "as kudu", "Multiple PRIMARY KEYS specified. Composite PRIMARY KEY can " +
        "be specified using the PRIMARY KEY (col1, col2, ...) syntax at the end " +
        "of the column definition.", isExternalPurgeTbl);
    // Invalid composite non unique primary key
    AnalysisError("create table tab (x int non unique primary key, " +
        "non unique primary key(x)) stored as kudu",
        "Multiple NON UNIQUE PRIMARY KEYS specified. Composite NON UNIQUE PRIMARY KEY " +
        "can be specified using the NON UNIQUE PRIMARY KEY (col1, col2, ...) syntax at " +
        "the end of the column definition.", isExternalPurgeTbl);
    AnalysisError("create table tab (x int non unique primary key, " +
        "y int non unique primary key) stored as kudu",
        "Multiple NON UNIQUE PRIMARY KEYS specified. Composite NON UNIQUE PRIMARY KEY " +
        "can be specified using the NON UNIQUE PRIMARY KEY (col1, col2, ...) syntax at " +
        "the end of the column definition.", isExternalPurgeTbl);
    AnalysisError("create table tab (x int non unique primary key, " +
        "y int primary key) stored as kudu",
        "Multiple NON UNIQUE PRIMARY KEYS specified. Composite NON UNIQUE PRIMARY KEY " +
        "can be specified using the NON UNIQUE PRIMARY KEY (col1, col2, ...) syntax at " +
        "the end of the column definition.", isExternalPurgeTbl);
    AnalysisError("create table tab (x int primary key, y int non unique primary key) " +
        "stored as kudu", "Multiple PRIMARY KEYS specified. Composite PRIMARY KEY " +
        "can be specified using the PRIMARY KEY (col1, col2, ...) syntax at " +
        "the end of the column definition.", isExternalPurgeTbl);
    // Specifying the same primary key column multiple times
    AnalysisError("create table tab (x int, primary key (x, x)) partition by hash (x) " +
        "partitions 8 stored as kudu",
        "Column 'x' is listed multiple times as a PRIMARY KEY.", isExternalPurgeTbl);
    AnalysisError("create table tab (x int, non unique primary key (x, x)) " +
        "partition by hash (x) partitions 8 stored as kudu",
        "Column 'x' is listed multiple times as a NON UNIQUE PRIMARY KEY.",
        isExternalPurgeTbl);
    // Number of range partition boundary values should be equal to the number of range
    // columns.
    AnalysisError("create table tab (a int, b int, c int, d int, primary key(a, b, c)) " +
        "partition by range(a) (partition value = (1, 2), " +
        "partition value = 3, partition value = 4) stored as kudu",
        "Number of specified range partition values is different than the number of " +
        "partitioning columns: (2 vs 1). Range partition: 'PARTITION VALUE = (1, 2)'",
        isExternalPurgeTbl);
    // Key ranges must match the column types.
    AnalysisError("create table tab (a int, b int, c int, d int, primary key(a, b, c)) " +
        "partition by hash (a, b, c) partitions 8, range (a) " +
        "(partition value = 1, partition value = 'abc', partition 3 <= values) " +
        "stored as kudu", "Range partition value 'abc' (type: STRING) is not type " +
        "compatible with partitioning column 'a' (type: INT).", isExternalPurgeTbl);
    AnalysisError("create table tab (a tinyint primary key) partition by range (a) " +
        "(partition value = 128) stored as kudu", "Range partition value 128 " +
        "(type: SMALLINT) is not type compatible with partitioning column 'a' " +
        "(type: TINYINT)", isExternalPurgeTbl);
    AnalysisError("create table tab (a smallint primary key) partition by range (a) " +
        "(partition value = 32768) stored as kudu", "Range partition value 32768 " +
        "(type: INT) is not type compatible with partitioning column 'a' " +
        "(type: SMALLINT)", isExternalPurgeTbl);
    AnalysisError("create table tab (a int primary key) partition by range (a) " +
        "(partition value = 2147483648) stored as kudu", "Range partition value " +
        "2147483648 (type: BIGINT) is not type compatible with partitioning column 'a' " +
        "(type: INT)", isExternalPurgeTbl);
    AnalysisError("create table tab (a bigint primary key) partition by range (a) " +
        "(partition value = 9223372036854775808) stored as kudu", "Range partition " +
        "value 9223372036854775808 (type: DECIMAL(19,0)) is not type compatible with " +
        "partitioning column 'a' (type: BIGINT)", isExternalPurgeTbl);
    // Test implicit casting/folding of partition values.
    AnalyzesOk("create table tab (a int primary key) partition by range (a) " +
        "(partition value = false, partition value = true) stored as kudu",
        isExternalPurgeTbl);
    // Non-key column used in PARTITION BY
    AnalysisError("create table tab (a int, b string, c bigint, primary key (a)) " +
        "partition by range (b) (partition value = 'abc') stored as kudu",
        "Column 'b' in 'RANGE (b) (PARTITION VALUE = 'abc')' is not a key column. " +
        "Only key columns can be used in PARTITION BY.", isExternalPurgeTbl);
    // No float range partition values
    AnalysisError("create table tab (a int, b int, c int, d int, primary key (a, b, c))" +
        "partition by hash (a, b, c) partitions 8, " +
        "range (a) (partition value = 1.2, partition value = 2) stored as kudu",
        "Range partition value 1.2 (type: DECIMAL(2,1)) is not type compatible with " +
        "partitioning column 'a' (type: INT).", isExternalPurgeTbl);
    // Non-existing column used in PARTITION BY
    AnalysisError("create table tab (a int, b int, primary key (a, b)) " +
        "partition by range(unknown_column) (partition value = 'abc') stored as kudu",
        "Column 'unknown_column' in 'RANGE (unknown_column) " +
        "(PARTITION VALUE = 'abc')' is not a key column. Only key columns can be used " +
        "in PARTITION BY", isExternalPurgeTbl);
    // Kudu num_tablet_replicas is specified in tblproperties
    String kuduMasters = catalog_.getDefaultKuduMasterHosts();
    AnalyzesOk(String.format("create table tab (x int primary key) partition by " +
        "hash (x) partitions 8 stored as kudu tblproperties " +
        "('kudu.num_tablet_replicas'='1', 'kudu.master_addresses' = '%s')",
        kuduMasters), isExternalPurgeTbl);
    // Kudu table name is specified in tblproperties resulting in an error
    AnalysisError("create table tab (x int primary key) partition by hash (x) " +
        "partitions 8 stored as kudu tblproperties ('kudu.table_name'='tab')",
        "Not allowed to set 'kudu.table_name' manually for synchronized Kudu tables.",
        isExternalPurgeTbl);
    // No port is specified in kudu master address
    AnalyzesOk(String.format("create table tdata_no_port (id int primary key, " +
        "name string, valf float, vali bigint) partition by range(id) " +
        "(partition values <= 10, partition 10 < values <= 30, " +
        "partition 30 < values) stored as kudu tblproperties" +
        "('kudu.master_addresses' = '%s')", kuduMasters), isExternalPurgeTbl);
    // Not using the STORED AS KUDU syntax to specify a Kudu table
    AnalysisError("create table tab (x int) tblproperties (" +
        "'storage_handler'='org.apache.hadoop.hive.kudu.KuduStorageHandler')",
        CreateTableStmt.KUDU_STORAGE_HANDLER_ERROR_MESSAGE, isExternalPurgeTbl);
    // Creating unpartitioned table results in a warning.
    AnalyzesOk("create table tab (x int primary key) stored as kudu tblproperties (" +
        "'storage_handler'='org.apache.hadoop.hive.kudu.KuduStorageHandler')",
        "Unpartitioned Kudu tables are inefficient for large data sizes.",
        isExternalPurgeTbl);
    // Invalid value for number of replicas
    AnalysisError("create table t (x int primary key) stored as kudu tblproperties (" +
        "'kudu.num_tablet_replicas'='1.1')",
        "Table property 'kudu.num_tablet_replicas' must be an integer.",
        isExternalPurgeTbl);
    // Don't allow caching
    AnalysisError("create table tab (x int primary key) stored as kudu cached in " +
        "'testPool'", "A Kudu table cannot be cached in HDFS.", isExternalPurgeTbl);
    // LOCATION cannot be used with Kudu tables
    AnalysisError("create table tab (a int primary key) partition by hash (a) " +
        "partitions 3 stored as kudu location '/test-warehouse/'",
        "LOCATION cannot be specified for a Kudu table.", isExternalPurgeTbl);
    // Creating unpartitioned table results in a warning.
    AnalyzesOk("create table tab (a int, primary key (a)) stored as kudu",
        "Unpartitioned Kudu tables are inefficient for large data sizes.",
        isExternalPurgeTbl);
    AnalysisError("create table tab (a int) stored as kudu",
        "A primary key is required for a Kudu table.", isExternalPurgeTbl);
    // Using ROW FORMAT with a Kudu table
    AnalysisError("create table tab (x int primary key) " +
        "row format delimited escaped by 'X' stored as kudu",
        "ROW FORMAT cannot be specified for file format KUDU.", isExternalPurgeTbl);
    // Using PARTITIONED BY with a Kudu table
    AnalysisError("create table tab (x int primary key) " +
        "partitioned by (y int) stored as kudu", "PARTITIONED BY cannot be used " +
        "in Kudu tables.", isExternalPurgeTbl);
    // Multi-column range partitions
    AnalyzesOk("create table tab (a bigint, b tinyint, c double, primary key(a, b)) " +
        "partition by range(a, b) (partition (0, 0) < values <= (1, 1)) stored as kudu",
        isExternalPurgeTbl);
    AnalysisError("create table tab (a bigint, b tinyint, c double, primary key(a, b)) " +
        "partition by range(a, b) (partition values <= (1, 'b')) stored as kudu",
        "Range partition value 'b' (type: STRING) is not type compatible with " +
        "partitioning column 'b' (type: TINYINT)", isExternalPurgeTbl);
    AnalysisError("create table tab (a bigint, b tinyint, c double, primary key(a, b)) " +
        "partition by range(a, b) (partition 0 < values <= 1) stored as kudu",
        "Number of specified range partition values is different than the number of " +
        "partitioning columns: (1 vs 2). Range partition: 'PARTITION 0 < VALUES <= 1'",
        isExternalPurgeTbl);


    // Test unsupported Kudu types
    List<String> unsupportedTypes = Lists.newArrayList("CHAR(20)",
        "STRUCT<f1:INT,f2:STRING>", "ARRAY<INT>", "MAP<STRING,STRING>");
    for (String t: unsupportedTypes) {
      String expectedError = String.format(
          "Cannot create table 'tab': Type %s is not supported in Kudu", t);

      // Unsupported type is PK and partition col
      String stmt = String.format("create table tab (x %s primary key) " +
          "partition by hash(x) partitions 3 stored as kudu", t);
      AnalysisError(stmt, expectedError, isExternalPurgeTbl);

      // Unsupported type is not PK/partition col
      stmt = String.format("create table tab (x int primary key, y %s) " +
          "partition by hash(x) partitions 3 stored as kudu", t);
      AnalysisError(stmt, expectedError, isExternalPurgeTbl);
    }

    // Test column options
    String[] nullability = {"not null", "null", ""};
    String[] defaultVal = {"default 10", ""};
    String[] blockSize = {"block_size 4096", ""};
    for (Encoding enc: Encoding.values()) {
      for (CompressionAlgorithm comp: CompressionAlgorithm.values()) {
        for (String nul: nullability) {
          for (String def: defaultVal) {
            for (String block: blockSize) {
              // Test analysis for a non-key column
              AnalyzesOk(String.format("create table tab (x int primary key " +
                  "not null encoding %s compression %s %s %s, y int encoding %s " +
                  "compression %s %s %s %s) partition by hash (x) " +
                  "partitions 3 stored as kudu", enc, comp, def, block, enc,
                  comp, def, nul, block), isExternalPurgeTbl);

              // For a key column
              String createTblStr = String.format("create table tab (x int primary key " +
                  "%s encoding %s compression %s %s %s) partition by hash (x) " +
                  "partitions 3 stored as kudu", nul, enc, comp, def, block);
              if (nul.equals("null")) {
                AnalysisError(createTblStr, "PRIMARY KEY columns cannot be nullable",
                    isExternalPurgeTbl);
              } else {
                AnalyzesOk(createTblStr, isExternalPurgeTbl);
              }
              createTblStr = String.format("create table tab (x int non unique primary " +
                  "key %s encoding %s compression %s %s %s) partition by hash (x) " +
                  "partitions 3 stored as kudu", nul, enc, comp, def, block);
              if (nul.equals("null")) {
                AnalysisError(
                    createTblStr, "NON UNIQUE PRIMARY KEY columns cannot be nullable",
                    isExternalPurgeTbl);
              } else {
                AnalyzesOk(createTblStr, isExternalPurgeTbl);
              }
            }
          }
        }
      }
    }
    // Use NULL as default values
    AnalyzesOk("create table tab (x int primary key, i1 tinyint default null, " +
        "i2 smallint default null, i3 int default null, i4 bigint default null, " +
        "vals string default null, valf float default null, vald double default null, " +
        "valb boolean default null, valdec decimal(10, 5) default null, " +
        "valdate date default null, valvc varchar(10) default null) " +
        "partition by hash (x) partitions 3 stored as kudu", isExternalPurgeTbl);
    // Use NULL as a default value on a non-nullable column
    AnalysisError("create table tab (x int primary key, y int not null default null) " +
        "partition by hash (x) partitions 3 stored as kudu", "Default value of NULL " +
        "not allowed on non-nullable column: 'y'", isExternalPurgeTbl);
    // Primary key specified using the PRIMARY KEY clause
    AnalyzesOk("create table tab (x int not null encoding plain_encoding " +
        "compression snappy block_size 1, y int null encoding rle compression lz4 " +
        "default 1, primary key(x)) partition by hash (x) partitions 3 " +
        "stored as kudu", isExternalPurgeTbl);
    AnalyzesOk("create table tab (x int not null encoding plain_encoding " +
        "compression snappy block_size 1, y int null encoding rle compression lz4 " +
        "default 1, non unique primary key(x)) partition by hash (x) partitions 3 " +
        "stored as kudu", isExternalPurgeTbl);
    // Primary keys can't be null
    AnalysisError("create table tab (x int primary key null, y int not null) " +
        "partition by hash (x) partitions 3 stored as kudu", "PRIMARY KEY columns " +
        "cannot be nullable: x INT PRIMARY KEY NULL", isExternalPurgeTbl);
    AnalysisError("create table tab (x int not null, y int null, primary key (x, y)) " +
        "partition by hash (x) partitions 3 stored as kudu", "PRIMARY KEY columns " +
        "cannot be nullable: y INT NULL", isExternalPurgeTbl);
    AnalysisError("create table tab (x int non unique primary key null, " +
        "y int not null) partition by hash (x) partitions 3 stored as kudu",
        "NON UNIQUE PRIMARY KEY columns cannot be nullable: " +
        "x INT NON UNIQUE PRIMARY KEY NULL", isExternalPurgeTbl);
    AnalysisError("create table tab (x int not null, y int null, " +
        "non unique primary key (x, y)) partition by hash (x) partitions 3 " +
        "stored as kudu", "NON UNIQUE PRIMARY KEY columns cannot be nullable: " +
        "y INT NULL", isExternalPurgeTbl);
    // Unsupported encoding value
    AnalysisError("create table tab (x int primary key, y int encoding invalid_enc) " +
        "partition by hash (x) partitions 3 stored as kudu", "Unsupported encoding " +
        "value 'INVALID_ENC'. Supported encoding values are: " +
        Joiner.on(", ").join(Encoding.values()), isExternalPurgeTbl);
    // Unsupported compression algorithm
    AnalysisError("create table tab (x int primary key, y int compression " +
        "invalid_comp) partition by hash (x) partitions 3 stored as kudu",
        "Unsupported compression algorithm 'INVALID_COMP'. Supported compression " +
        "algorithms are: " + Joiner.on(", ").join(CompressionAlgorithm.values()),
        isExternalPurgeTbl);
    // Default values
    AnalyzesOk("create table tab (i1 tinyint default 1, i2 smallint default 10, " +
        "i3 int default 100, i4 bigint default 1000, vals string default 'test', " +
        "valf float default cast(1.2 as float), vald double default " +
        "cast(3.1452 as double), valb boolean default true, " +
        "valdec decimal(10, 5) default 3.14159, " +
        "valdate date default date '1970-01-01', " +
        "valvc varchar(10) default cast('test' as varchar(10)), " +
        "primary key (i1, i2, i3, i4, vals)) partition by hash (i1) partitions 3 " +
        "stored as kudu", isExternalPurgeTbl);
    AnalyzesOk("create table tab (i int primary key default 1+1+1) " +
        "partition by hash (i) partitions 3 stored as kudu", isExternalPurgeTbl);
    AnalyzesOk("create table tab (i int primary key default factorial(5)) " +
        "partition by hash (i) partitions 3 stored as kudu", isExternalPurgeTbl);
    AnalyzesOk("create table tab (i int primary key, x int null default " +
        "isnull(null, null)) partition by hash (i) partitions 3 stored as kudu",
        isExternalPurgeTbl);
    // Invalid default values
    AnalysisError("create table tab (i int primary key default 'string_val') " +
        "partition by hash (i) partitions 3 stored as kudu", "Default value " +
        "'string_val' (type: STRING) is not compatible with column 'i' (type: INT).",
        isExternalPurgeTbl);
    AnalysisError("create table tab (i int primary key, x int default 1.1) " +
        "partition by hash (i) partitions 3 stored as kudu",
        "Default value 1.1 (type: DECIMAL(2,1)) is not compatible with column " +
        "'x' (type: INT).", isExternalPurgeTbl);
    AnalysisError("create table tab (i tinyint primary key default 128) " +
        "partition by hash (i) partitions 3 stored as kudu", "Default value " +
        "128 (type: SMALLINT) is not compatible with column 'i' (type: TINYINT).",
        isExternalPurgeTbl);
    AnalysisError("create table tab (i int primary key default isnull(null, null)) " +
        "partition by hash (i) partitions 3 stored as kudu", "Default value of " +
        "NULL not allowed on non-nullable column: 'i'", isExternalPurgeTbl);
    AnalysisError("create table tab (i int primary key, x int not null " +
        "default isnull(null, null)) partition by hash (i) partitions 3 " +
        "stored as kudu", "Default value of NULL not allowed on non-nullable column: " +
        "'x'", isExternalPurgeTbl);
    // Invalid block_size values
    AnalysisError("create table tab (i int primary key block_size 1.1) " +
        "partition by hash (i) partitions 3 stored as kudu", "Invalid value " +
        "for BLOCK_SIZE: 1.1. A positive INTEGER value is expected.",
        isExternalPurgeTbl);
    AnalysisError("create table tab (i int primary key block_size 'val') " +
        "partition by hash (i) partitions 3 stored as kudu", "Invalid value " +
        "for BLOCK_SIZE: 'val'. A positive INTEGER value is expected.",
        isExternalPurgeTbl);

    // Sort columns are not supported for Kudu tables.
    AnalysisError("create table tab (i int, x int primary key) partition by hash(x) " +
        "partitions 8 sort by(i) stored as kudu", "SORT BY is not supported for Kudu " +
        "tables.", isExternalPurgeTbl);

    // Z-Order sorted columns are not supported for Kudu tables.
    AnalysisError("create table tab (i int, x int primary key) partition by hash(x) " +
        "partitions 8 sort by zorder(i) stored as kudu", "SORT BY is not " +
        "supported for Kudu tables.", isExternalPurgeTbl);

    // Range partitions with TIMESTAMP
    AnalyzesOk("create table ts_ranges (ts timestamp primary key) " +
        "partition by range (partition cast('2009-01-01 00:00:00' as timestamp) " +
        "<= VALUES < '2009-01-02 00:00:00') stored as kudu", isExternalPurgeTbl);
    AnalyzesOk("create table ts_ranges (ts timestamp primary key) " +
        "partition by range (partition value = cast('2009-01-01 00:00:00' as timestamp" +
        ")) stored as kudu", isExternalPurgeTbl);
    AnalyzesOk("create table ts_ranges (ts timestamp primary key) " +
        "partition by range (partition value = '2009-01-01 00:00:00') " +
        "stored as kudu", isExternalPurgeTbl);
    AnalyzesOk("create table ts_ranges (id int, ts timestamp, primary key(id, ts))" +
        "partition by range (partition value = (9, cast('2009-01-01 00:00:00' as " +
        "timestamp))) stored as kudu", isExternalPurgeTbl);
    AnalyzesOk("create table ts_ranges (id int, ts timestamp, primary key(id, ts))" +
        "partition by range (partition value = (9, '2009-01-01 00:00:00')) " +
        "stored as kudu", isExternalPurgeTbl);
    AnalysisError("create table ts_ranges (ts timestamp primary key, i int)" +
        "partition by range (partition '2009-01-01 00:00:00' <= VALUES < " +
        "'NOT A TIMESTAMP') stored as kudu",
        "Range partition value 'NOT A TIMESTAMP' cannot be cast to target TIMESTAMP " +
        "partitioning column.", isExternalPurgeTbl);
    AnalysisError("create table ts_ranges (ts timestamp primary key, i int)" +
        "partition by range (partition 100 <= VALUES < 200) stored as kudu",
        "Range partition value 100 (type: TINYINT) is not type " +
        "compatible with partitioning column 'ts' (type: TIMESTAMP).",
        isExternalPurgeTbl);

    // TIMESTAMP columns with default values
    AnalyzesOk("create table tdefault (id int primary key, ts timestamp default now())" +
        "partition by hash(id) partitions 3 stored as kudu", isExternalPurgeTbl);
    AnalyzesOk("create table tdefault (id int primary key, ts timestamp default " +
        "unix_micros_to_utc_timestamp(1230768000000000)) partition by hash(id) " +
        "partitions 3 stored as kudu", isExternalPurgeTbl);
    AnalyzesOk("create table tdefault (id int primary key, " +
        "ts timestamp not null default '2009-01-01 00:00:00') " +
        "partition by hash(id) partitions 3 stored as kudu", isExternalPurgeTbl);
    AnalyzesOk("create table tdefault (id int primary key, " +
        "ts timestamp not null default cast('2009-01-01 00:00:00' as timestamp)) " +
        "partition by hash(id) partitions 3 stored as kudu", isExternalPurgeTbl);
    AnalysisError("create table tdefault (id int primary key, ts timestamp " +
        "default null) partition by hash(id) partitions 3 stored as kudu",
        "NULL cannot be cast to a TIMESTAMP literal.", isExternalPurgeTbl);
    AnalysisError("create table tdefault (id int primary key, " +
        "ts timestamp not null default cast('00:00:00' as timestamp)) " +
        "partition by hash(id) partitions 3 stored as kudu",
        "Default value of NULL not allowed on non-nullable column:", isExternalPurgeTbl);
    AnalysisError("create table tdefault (id int primary key, " +
        "ts timestamp not null default '2009-1 foo') " +
        "partition by hash(id) partitions 3 stored as kudu",
        "String '2009-1 foo' cannot be cast to a TIMESTAMP literal.",
        isExternalPurgeTbl);

    // Test column comments.
    AnalyzesOk("create table tab (x int comment 'x', y int comment 'y', " +
        "primary key (x, y)) stored as kudu", isExternalPurgeTbl);

    // Managed table is not allowed to set table property 'kudu.table_id'.
    AnalysisError("create table tab (x int primary key) partition by hash(x) " +
        "partitions 8 stored as kudu tblproperties ('kudu.table_id'='123456')",
        String.format("Table property %s should not be specified when " +
            "creating a Kudu table.", KuduTable.KEY_TABLE_ID), isExternalPurgeTbl);

    // Kudu master address needs to be valid.
    AnalysisError("create table tab (x int primary key) partition by " +
        "hash (x) partitions 8 stored as kudu tblproperties " +
        "('kudu.master_addresses' = 'foo')",
        "Cannot analyze Kudu table 'tab': Error determining if Kudu's integration " +
        "with the Hive Metastore is enabled", isExternalPurgeTbl);
  }

  @Test
  public void TestCreateManagedKuduTable() {
    testDDlsOnKuduTable(false);
  }

  @Test
  public void TestCreateSynchronizedKuduTable() {
    testDDlsOnKuduTable(true);
  }

  @Test
  public void TestCreateExternalKuduTable() {
    final String kuduMasters = catalog_.getDefaultKuduMasterHosts();
    AnalyzesOk("create external table t stored as kudu " +
        "tblproperties('kudu.table_name'='t')");
    // Use all allowed optional table props.
    AnalyzesOk(String.format("create external table t stored as kudu tblproperties (" +
        "'kudu.table_name'='tab', 'kudu.master_addresses' = '%s', " +
        "'storage_handler'='org.apache.hadoop.hive.kudu.KuduStorageHandler')",
        kuduMasters));
    // Kudu table should be specified using the STORED AS KUDU syntax.
    AnalysisError("create external table t tblproperties (" +
        "'storage_handler'='com.cloudera.kudu.hive.KuduStorageHandler'," +
        "'kudu.table_name'='t')",
        CreateTableStmt.KUDU_STORAGE_HANDLER_ERROR_MESSAGE);
    AnalysisError("create external table t tblproperties (" +
        "'storage_handler'='org.apache.hadoop.hive.kudu.KuduStorageHandler'," +
        "'kudu.table_name'='t')",
        CreateTableStmt.KUDU_STORAGE_HANDLER_ERROR_MESSAGE);
    // Columns should not be specified in an external Kudu table
    AnalysisError("create external table t (x int) stored as kudu " +
        "tblproperties('kudu.table_name'='t')",
        "Columns cannot be specified with an external Kudu table.");
    // Primary keys cannot be specified in an external Kudu table
    AnalysisError("create external table t (x int primary key) stored as kudu " +
        "tblproperties('kudu.table_name'='t')", "Primary keys cannot be specified " +
        "for an external Kudu table");
    // Invalid syntax for specifying a Kudu table
    AnalysisError("create external table t (x int) stored as parquet tblproperties (" +
        "'storage_handler'='com.cloudera.kudu.hive.KuduStorageHandler'," +
        "'kudu.table_name'='t')", CreateTableStmt.KUDU_STORAGE_HANDLER_ERROR_MESSAGE);
    AnalysisError("create external table t (x int) stored as parquet tblproperties (" +
        "'storage_handler'='org.apache.hadoop.hive.kudu.KuduStorageHandler'," +
        "'kudu.table_name'='t')", CreateTableStmt.KUDU_STORAGE_HANDLER_ERROR_MESSAGE);
    AnalysisError("create external table t stored as kudu tblproperties (" +
        "'storage_handler'='foo', 'kudu.table_name'='t')",
        "Invalid storage handler specified for Kudu table: foo");
    // Cannot specify the number of replicas for external Kudu tables
    AnalysisError("create external table tab (x int) stored as kudu " +
        "tblproperties ('kudu.num_tablet_replicas' = '1', " +
        "'kudu.table_name'='tab')",
        "Table property 'kudu.num_tablet_replicas' cannot be used with an external " +
        "Kudu table.");
    // Don't allow caching
    AnalysisError("create external table t stored as kudu cached in 'testPool' " +
        "tblproperties('kudu.table_name'='t')",
        "A Kudu table cannot be cached in HDFS.");
    // LOCATION cannot be used for a Kudu table
    AnalysisError("create external table t stored as kudu " +
        "location '/test-warehouse' tblproperties('kudu.table_name'='t')",
        "LOCATION cannot be specified for a Kudu table.");
    // External table is not allowed to set table property 'kudu.table_id'.
    AnalysisError("create external table t stored as kudu tblproperties " +
        "('kudu.table_name'='t', 'kudu.table_id'='123456')",
        String.format("Table property %s should not be specified when creating " +
            "a Kudu table.", KuduTable.KEY_TABLE_ID));
    // External table is not allowed to set table property 'kudu.table_name'
    AnalysisError("create external table t stored as kudu tblproperties " +
        "('external.table.purge'='true', 'kudu.table_name'='t')",
        "Not allowed to set 'kudu.table_name' manually for synchronized Kudu tables");
    // trying to create the legacy external table syntax with external.table.purge
    // property should error out
    AnalysisError("create external table t stored as kudu tblproperties " +
            "('external.table.purge'='true')",
        "A primary key is required for a Kudu table.");
  }

  @Test
  public void TestAlterKuduTable() {
    // ALTER TABLE ADD/DROP range partitions
    String[] addDrop = {"add if not exists", "add", "drop if exists", "drop"};
    for (String kw: addDrop) {
      AnalyzesOk(String.format("alter table functional_kudu.testtbl %s range " +
          "partition 10 <= values < 20", kw));
      AnalyzesOk(String.format("alter table functional_kudu.testtbl %s range " +
          "partition value = 30", kw));
      AnalyzesOk(String.format("alter table functional_kudu.testtbl %s range " +
          "partition values < 100", kw));
      AnalyzesOk(String.format("alter table functional_kudu.testtbl %s range " +
          "partition 10 <= values", kw));
      AnalyzesOk(String.format("alter table functional_kudu.testtbl %s range " +
          "partition 1+1 <= values <= factorial(3)", kw));
      AnalyzesOk(String.format("alter table functional_kudu.jointbl %s range " +
          "partition (0, '0') < values", kw));
      AnalyzesOk(String.format("alter table functional_kudu.jointbl %s range " +
          "partition values <= (1, '1')", kw));
      AnalyzesOk(String.format("alter table functional_kudu.jointbl %s range " +
          "partition (0, '0') <= values < (1, '1')", kw));
      AnalyzesOk(String.format("alter table functional_kudu.jointbl %s range " +
          "partition value = (-1, 'a')", kw));
      AnalysisError(String.format("alter table functional.alltypes %s range " +
          "partition 10 < values < 20", kw), "Table functional.alltypes does not " +
          "support range partitions: RANGE PARTITION 10 < VALUES < 20");
      AnalysisError(String.format("alter table functional_kudu.testtbl %s range " +
          "partition values < isnull(null, null)", kw), "Range partition values " +
          "cannot be NULL. Range partition: 'PARTITION VALUES < isnull(NULL, NULL)'");
      AnalysisError(String.format("alter table functional_kudu.jointbl %s range " +
          "partition (0) < values", kw),
          "Number of specified range partition values is different than the number of " +
          "partitioning columns: (1 vs 2). Range partition: 'PARTITION (0) < VALUES'");
      AnalysisError(String.format("alter table functional_kudu.jointbl %s range " +
          "partition values < (0, 0)", kw),
          "Range partition value 0 (type: TINYINT) is not type compatible with " +
          "partitioning column 'test_name' (type: STRING).");
    }

    // ALTER TABLE ADD COLUMNS
    // Columns with different supported data types
    AnalyzesOk("alter table functional_kudu.testtbl add columns (a1 tinyint null, a2 " +
        "smallint null, a3 int null, a4 bigint null, a5 string null, a6 float null, " +
        "a7 double null, a8 boolean null comment 'boolean', a9 date null)");
    // Decimal types
    AnalyzesOk("alter table functional_kudu.testtbl add columns (d1 decimal null, d2 " +
        "decimal(9, 2) null, d3 decimal(15, 15) null, d4 decimal(38, 0) null)");
    // Complex types
    AnalysisError("alter table functional_kudu.testtbl add columns ( "+
        "a struct<f1:int>)", "Kudu tables do not support complex types: " +
        "a STRUCT<f1:INT>");
    // Add primary key
    AnalysisError("alter table functional_kudu.testtbl add columns (a int primary key)",
        "Cannot add a PRIMARY KEY using an ALTER TABLE ADD COLUMNS statement: " +
        "a INT PRIMARY KEY");
    AnalysisError("alter table functional_kudu.testtbl add columns (a int non unique " +
        "primary key)", "Cannot add a NON UNIQUE PRIMARY KEY using an ALTER TABLE ADD " +
        "COLUMNS statement: a INT NON UNIQUE PRIMARY KEY");
    // Columns requiring a default value
    AnalyzesOk("alter table functional_kudu.testtbl add columns (a1 int not null " +
        "default 10)");
    AnalyzesOk("alter table functional_kudu.testtbl add columns (a1 int null " +
        "default 10)");
    AnalyzesOk("alter table functional_kudu.testtbl add columns (d1 decimal(9, 2) null " +
        "default 99.99)");
    // Other Kudu column options
    AnalyzesOk("alter table functional_kudu.testtbl add columns (a int encoding rle)");
    AnalyzesOk("alter table functional_kudu.testtbl add columns (a int compression lz4)");
    AnalyzesOk("alter table functional_kudu.testtbl add columns (a int block_size 10)");
    // Test 'if not exists'
    AnalyzesOk("alter table functional_kudu.testtbl add if not exists columns " +
        "(name string null)");
    AnalyzesOk("alter table functional_kudu.testtbl add if not exists columns " +
        "(a string null, b int, c string null)");

    // REPLACE columns is not supported for Kudu tables
    AnalysisError("alter table functional_kudu.testtbl replace columns (a int null)",
        "ALTER TABLE REPLACE COLUMNS is not supported on Kudu tables");
    // Conflict with existing column
    AnalysisError("alter table functional_kudu.testtbl add columns (zip int)",
        "Column already exists: zip");
    // Kudu column options on an HDFS table
    AnalysisError("alter table functional.alltypes add columns (a int not null)",
        "The specified column options are only supported in Kudu tables: " +
        "a INT NOT NULL");

    // ALTER TABLE DROP COLUMN
    AnalyzesOk("alter table functional_kudu.testtbl drop column name");
    AnalysisError("alter table functional_kudu.testtbl drop column no_col",
        "Column 'no_col' does not exist in table: functional_kudu.testtbl");

    // ALTER TABLE CHANGE COLUMN on Kudu tables
    AnalyzesOk("alter table functional_kudu.testtbl change column name new_name string");
    AnalyzesOk("alter table functional_kudu.testtbl change column zip " +
        "zip int comment 'comment'");
    // Unsupported column options
    AnalysisError("alter table functional_kudu.testtbl change column zip zip_code int " +
        "encoding rle compression lz4 default 90000", "Unsupported column options in " +
        "ALTER TABLE CHANGE COLUMN statement: 'zip_code INT ENCODING RLE COMPRESSION " +
        "LZ4 DEFAULT 90000'. Use ALTER TABLE ALTER COLUMN instead.");
    // Changing the column type is not supported for Kudu tables
    AnalysisError("alter table functional_kudu.testtbl change column zip zip bigint",
        "Cannot change the type of a Kudu column using an ALTER TABLE CHANGE COLUMN " +
        "statement: (INT vs BIGINT)");

    // Setting kudu.table_id is not allowed for Kudu tables.
    AnalysisError("ALTER TABLE functional_kudu.testtbl SET " +
        "TBLPROPERTIES ('kudu.table_id' = '1234')",
        "Property 'kudu.table_id' cannot be altered for Kudu tables");

    // Unsetting kudu.table_id is not allowed for Kudu tables.
    AnalysisError("ALTER TABLE functional_kudu.testtbl UNSET " +
        "TBLPROPERTIES ('kudu.table_id')",
        "Unsetting the 'kudu.table_id' table property is not supported for Kudu table");

    // Unsetting kudu.master_addresses is not allowed for Kudu tables.
    AnalysisError("ALTER TABLE functional_kudu.testtbl UNSET " +
        "TBLPROPERTIES ('kudu.master_addresses')",
        "Unsetting the 'kudu.master_addresses' table property is not supported for " +
        "Kudu table");

    // Setting 'external.table.purge' is allowed for Kudu tables.
    AnalyzesOk("ALTER TABLE functional_kudu.testtbl SET " +
        "TBLPROPERTIES ('external.table.purge' = 'true')");

    AnalyzesOk("ALTER TABLE functional_kudu.testtbl SET " +
        "TBLPROPERTIES ('external.table.purge' = 'false')");

    // Rename the underlying Kudu table is not supported for managed Kudu tables.
    AnalysisError("ALTER TABLE functional_kudu.testtbl SET " +
        "TBLPROPERTIES ('kudu.table_name' = 'Hans')",
        "Not allowed to set 'kudu.table_name' manually for synchronized Kudu tables");

    // Unsetting the underlying Kudu table is not supported for managed Kudu tables
    // as setting them is not allowed
    AnalysisError("ALTER TABLE functional_kudu.testtbl UNSET " +
        "TBLPROPERTIES ('kudu.table_name')",
        "Unsetting the 'kudu.table_name' table property is not supported for " +
        "synchronized Kudu table.");

    // TODO IMPALA-6375: Allow setting kudu.table_name for managed Kudu tables
    // if the 'EXTERNAL' property is set to TRUE in the same step.
    AnalysisError("ALTER TABLE functional_kudu.testtbl SET " +
        "TBLPROPERTIES ('EXTERNAL' = 'TRUE','kudu.table_name' = 'Hans')",
        "Not allowed to set 'kudu.table_name' manually for synchronized Kudu tables");

    // ALTER TABLE RENAME TO
    AnalyzesOk("ALTER TABLE functional_kudu.testtbl RENAME TO new_testtbl");

    // ALTER TABLE SORT BY
    AnalysisError("alter table functional_kudu.alltypes sort by (int_col)",
        "ALTER TABLE SORT BY not supported on Kudu tables.");

    // ALTER TABLE SORT BY ZORDER
    AnalysisError("alter table functional_kudu.alltypes sort by zorder (int_col)",
        "ALTER TABLE SORT BY not supported on Kudu tables.");

    // ALTER TABLE SET TBLPROPERTIES for sort.columns
    AnalysisError("alter table functional_kudu.alltypes set tblproperties(" +
        "'sort.columns'='int_col')",
        "'sort.*' table properties are not supported for Kudu tables.");

    // ALTER TABLE SET TBLPROPERTIES for sort.order
    AnalysisError("alter table functional_kudu.alltypes set tblproperties("
        + "'sort.order'='true')",
        "'sort.*' table properties are not supported for Kudu tables.");

    // ALTER TABLE SET OWNER USER
    AnalyzesOk("ALTER TABLE functional_kudu.testtbl SET OWNER USER new_owner");
  }
}
