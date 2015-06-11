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
import com.cloudera.impala.common.PrintUtils;
import com.cloudera.impala.thrift.TDataSink;
import com.cloudera.impala.thrift.TDataSinkType;
import com.cloudera.impala.thrift.TExplainLevel;
import com.cloudera.impala.thrift.TTableSink;
import com.cloudera.impala.thrift.TTableSinkType;

/**
 * Class used to represent a Sink that will transport
 * data from a plan fragment into an Kudu table using the Kudu client.
 */
public class KuduTableSink extends TableSink {

   // Sink type e.g. INSERT, UPDATE
  private Type sinkType_;

  /**
   * Creates a Kudu table sink with a given operation type (e.g. INSERT, UPDATE)
   */
  public KuduTableSink(Table targetTable, Type sinkType) {
    super(targetTable);
    sinkType_ = sinkType;
  }

  @Override
  public String getExplainString(String prefix, String detailPrefix,
      TExplainLevel explainLevel) {
    StringBuilder output = new StringBuilder();
    output.append(
        prefix + sinkType_.toExplainString() + " KUDU ["
            + targetTable_.getFullName() + "]\n");
    if (explainLevel.ordinal() >= TExplainLevel.EXTENDED.ordinal()) {
      output.append(PrintUtils.printHosts(detailPrefix, fragment_.getNumNodes()));
      output.append(PrintUtils.printMemCost(" ", perHostMemCost_));
      output.append("\n");
    }
    return output.toString();
  }

  @Override
  protected TDataSink toThrift() {
    TDataSink result = new TDataSink(TDataSinkType.TABLE_SINK);
    TTableSink tTableSink =
        new TTableSink(targetTable_.getId().asInt(), sinkType_.toThrift());
    result.table_sink = tTableSink;
    return result;
  }

  /**
   * Enum to specify the sink type
   */
  public enum Type {
    INSERT {
      @Override
      public String toExplainString() { return "INSERT INTO"; }

      @Override
      public TTableSinkType toThrift() { return TTableSinkType.KUDU_INSERT; }
    },
    UPDATE {
      @Override
      public String toExplainString() { return "UPDATE"; }

      @Override
      public TTableSinkType toThrift() { return TTableSinkType.KUDU_UPDATE; }
    };

    public abstract String toExplainString();

    public abstract TTableSinkType toThrift();
  }
}
