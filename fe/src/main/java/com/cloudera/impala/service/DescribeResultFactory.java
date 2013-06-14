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
      colNameCol.setStringVal(column.getName());
      TColumnValue dataTypeCol = new TColumnValue();
      dataTypeCol.setStringVal(column.getType().toString().toLowerCase());
      TColumnValue commentCol = new TColumnValue();
      commentCol.setStringVal(column.getComment() != null ? column.getComment() : "");
      descResult.results.add(
          new TResultRow(Lists.newArrayList(colNameCol, dataTypeCol, commentCol)));
    }
    return descResult;
  }

  /*
   * Builds a TDescribeTableResult that contains the result of a DESCRIBE FORMATTED
   * <table> command. For the formatted describe output the goal is to be exactly the
   * same as what Hive outputs, for compatibility reasons. To do this, Hive's
   * MetadataFormatUtils class is used to build the results.
   */
  private static TDescribeTableResult describeTableFormatted(Table table) {
    TDescribeTableResult descResult = new TDescribeTableResult();
    descResult.results = Lists.newArrayList();

    org.apache.hadoop.hive.metastore.api.Table msTable = table.getMetaStoreTable();
    org.apache.hadoop.hive.ql.metadata.Table hiveTable =
        new org.apache.hadoop.hive.ql.metadata.Table(msTable);
    StringBuilder sb = new StringBuilder();
    // First add all the columns (includes partition columns).
    sb.append(MetaDataFormatUtils.getAllColumnsInformation(msTable.getSd().getCols(),
        msTable.getPartitionKeys()));
    // Add the extended table metadata information.
    sb.append(MetaDataFormatUtils.getTableInformation(hiveTable));

    for (String line: sb.toString().split("\n")) {
      TColumnValue descFormattedEntry = new TColumnValue();
      descFormattedEntry.setStringVal(line);
      descResult.results.add(new TResultRow(Lists.newArrayList(descFormattedEntry)));
    }
    return descResult;
  }
}