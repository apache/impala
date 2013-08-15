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


package com.cloudera.impala.planner;

import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.thrift.TDataSink;
import com.cloudera.impala.thrift.TDataSinkType;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TTableSink;
import com.cloudera.impala.thrift.TTableSinkType;

/**
 * Class used to represent a Sink that will transport
 * data from a plan fragment into an HBase table using HTable.
 */
public class HBaseTableSink extends TableSink {
  public HBaseTableSink(Table targetTable) {
    super(targetTable);
  }

  @Override
  public String getExplainString(String prefix, TExplainLevel explainLevel) {
    StringBuilder output = new StringBuilder();
    output.append(prefix + "WRITE TO HBASE table=" + targetTable.getFullName() + "\n");
    return output.toString();
  }

  @Override
  protected TDataSink toThrift() {
    TDataSink result = new TDataSink(TDataSinkType.TABLE_SINK);
    TTableSink tTableSink =
        new TTableSink(targetTable.getId().asInt(), TTableSinkType.HBASE);
    result.table_sink = tTableSink;
    return result;
  }
}
