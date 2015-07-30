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

package com.cloudera.impala.service;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.metadata.formatting.MetaDataFormatUtils;

import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.thrift.TColumnValue;
import com.cloudera.impala.thrift.TDescribeTableOutputStyle;
import com.cloudera.impala.thrift.TDescribeTableResult;
import com.cloudera.impala.thrift.TResultRow;
import com.google.common.collect.Lists;

/*
 * Builds results for DESCRIBE statements by constructing and populating a
 * TDescribeTableResult object.
 */
public class DescribeResultFactory {
  // Number of columns in each row of the DESCRIBE FORMATTED result set.
  private final static int NUM_DESC_FORMATTED_RESULT_COLS = 3;

  public static TDescribeTableResult buildDescribeTableResult(Table table,
      TDescribeTableOutputStyle outputFormat) {
    switch (outputFormat) {
      case MINIMAL: return describeTableMinimal(table);
      case FORMATTED: return describeTableFormatted(table);
      default: throw new UnsupportedOperationException(
          "Unknown TDescribeTableOutputStyle value: " + outputFormat);
    }
  }

  /*
   * Builds results for a DESCRIBE <table> command. This consists of the column
   * definition for each column in the table.
   */
  private static TDescribeTableResult describeTableMinimal(Table table) {
    TDescribeTableResult descResult = new TDescribeTableResult();
    descResult.results = Lists.newArrayList();

    // Get description of all the table's columns (includes partition columns).
    for (Column column: table.getColumnsInHiveOrder()) {
      TColumnValue colNameCol = new TColumnValue();
      colNameCol.setString_val(column.getName());
      TColumnValue dataTypeCol = new TColumnValue();
      dataTypeCol.setString_val(column.getType().prettyPrint().toLowerCase());
      TColumnValue commentCol = new TColumnValue();
      commentCol.setString_val(column.getComment() != null ? column.getComment() : "");
      descResult.results.add(
          new TResultRow(Lists.newArrayList(colNameCol, dataTypeCol, commentCol)));
    }
    return descResult;
  }

  /*
   * Builds a TDescribeTableResult that contains the result of a DESCRIBE FORMATTED
   * <table> command. For the formatted describe output the goal is to be exactly the
   * same as what Hive (via HiveServer2) outputs, for compatibility reasons. To do this,
   * Hive's MetadataFormatUtils class is used to build the results.
   */
  private static TDescribeTableResult describeTableFormatted(Table table) {
    TDescribeTableResult descResult = new TDescribeTableResult();
    descResult.results = Lists.newArrayList();

    org.apache.hadoop.hive.metastore.api.Table msTable =
        table.getMetaStoreTable().deepCopy();
    // Fixup the metastore table so the output of DESCRIBE FORMATTED matches Hive's.
    // This is to distinguish between empty comments and no comments (value is null).
    for (FieldSchema fs: msTable.getSd().getCols())
      fs.setComment(table.getColumn(fs.getName()).getComment());
    for (FieldSchema fs: msTable.getPartitionKeys()) {
      fs.setComment(table.getColumn(fs.getName()).getComment());
    }

    // To avoid initializing any of the SerDe classes in the metastore table Thrift
    // struct, create the ql.metadata.Table object by calling the empty c'tor and
    // then calling setTTable().
    org.apache.hadoop.hive.ql.metadata.Table hiveTable =
        new org.apache.hadoop.hive.ql.metadata.Table();
    hiveTable.setTTable(msTable);
    StringBuilder sb = new StringBuilder();
    // First add all the columns (includes partition columns).
    sb.append(MetaDataFormatUtils.getAllColumnsInformation(msTable.getSd().getCols(),
        msTable.getPartitionKeys(), true, false, true));
    // Add the extended table metadata information.
    sb.append(MetaDataFormatUtils.getTableInformation(hiveTable));

    for (String line: sb.toString().split("\n")) {
      // To match Hive's HiveServer2 output, split each line into multiple column
      // values based on the field delimiter.
      String[] columns = line.split(MetaDataFormatUtils.FIELD_DELIM);
      TResultRow resultRow = new TResultRow();
      for (int i = 0; i < NUM_DESC_FORMATTED_RESULT_COLS; ++i) {
        TColumnValue colVal = new TColumnValue();
        colVal.setString_val(null);
        if (columns.length > i) {
          // Add the column value.
          colVal.setString_val(columns[i]);
        }
        resultRow.addToColVals(colVal);
      }
      descResult.results.add(resultRow);
    }
    return descResult;
  }
}
