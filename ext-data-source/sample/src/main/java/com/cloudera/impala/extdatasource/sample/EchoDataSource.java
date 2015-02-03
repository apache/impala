// Copyright 2014 Cloudera Inc.
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

package com.cloudera.impala.extdatasource.sample;

import com.cloudera.impala.extdatasource.thrift.TCloseParams;
import com.cloudera.impala.extdatasource.thrift.TCloseResult;
import com.cloudera.impala.extdatasource.thrift.TGetNextParams;
import com.cloudera.impala.extdatasource.thrift.TGetNextResult;
import com.cloudera.impala.extdatasource.thrift.TOpenParams;
import com.cloudera.impala.extdatasource.thrift.TOpenResult;
import com.cloudera.impala.extdatasource.thrift.TPrepareParams;
import com.cloudera.impala.extdatasource.thrift.TPrepareResult;
import com.cloudera.impala.extdatasource.thrift.TRowBatch;
import com.cloudera.impala.extdatasource.v1.ExternalDataSource;
import com.cloudera.impala.thrift.TColumnData;
import com.cloudera.impala.thrift.TStatus;
import com.cloudera.impala.thrift.TErrorCode;
import com.google.common.collect.Lists;

/**
 * Sample data source that always returns a single column containing the initString.
 */
public class EchoDataSource implements ExternalDataSource {
  private static final TStatus STATUS_OK =
      new TStatus(TErrorCode.OK, Lists.<String>newArrayList());

  private String initString_;

  @Override
  public TPrepareResult prepare(TPrepareParams params) {
    return new TPrepareResult(STATUS_OK)
      .setAccepted_conjuncts(Lists.<Integer>newArrayList())
      .setNum_rows_estimate(1);
  }

  @Override
  public TOpenResult open(TOpenParams params) {
    initString_ = params.getInit_string();
    return new TOpenResult(STATUS_OK).setScan_handle("dummy-handle");
  }

  @Override
  public TGetNextResult getNext(TGetNextParams params) {
    boolean eos = true;
    TGetNextResult result = new TGetNextResult(STATUS_OK).setEos(eos);
    TRowBatch rowBatch = new TRowBatch();
    TColumnData colData = new TColumnData();
    colData.addToIs_null(false);
    colData.addToString_vals(initString_);
    rowBatch.addToCols(colData);
    rowBatch.setNum_rows(1);
    result.setRows(rowBatch);
    return result;
  }

  @Override
  public TCloseResult close(TCloseParams params) {
    return new TCloseResult(STATUS_OK);
  }
}
