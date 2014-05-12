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

package com.cloudera.impala.extdatasource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;

import com.cloudera.impala.extdatasource.thrift.TCloseParams;
import com.cloudera.impala.extdatasource.thrift.TCloseResult;
import com.cloudera.impala.extdatasource.thrift.TColumnDesc;
import com.cloudera.impala.extdatasource.thrift.TGetNextParams;
import com.cloudera.impala.extdatasource.thrift.TGetNextResult;
import com.cloudera.impala.extdatasource.thrift.TOpenParams;
import com.cloudera.impala.extdatasource.thrift.TOpenResult;
import com.cloudera.impala.extdatasource.thrift.TPrepareParams;
import com.cloudera.impala.extdatasource.thrift.TPrepareResult;
import com.cloudera.impala.extdatasource.thrift.TRowBatch;
import com.cloudera.impala.extdatasource.thrift.TTableSchema;
import com.cloudera.impala.extdatasource.util.SerializationUtils;
import com.cloudera.impala.extdatasource.v1.ExternalDataSource;
import com.cloudera.impala.thrift.TColumnData;
import com.cloudera.impala.thrift.TColumnType;
import com.cloudera.impala.thrift.TStatus;
import com.cloudera.impala.thrift.TStatusCode;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

/**
 * Data source implementation for tests that:
 * (a) Accepts every other offered conjunct for testing planning, though
 *     predicates are not actually evaluated.
 * (b) Returns trivial data of all supported types for query tests.
 */
public class AllTypesDataSource implements ExternalDataSource {
  // Total number of rows to return
  private static final int NUM_ROWS_RETURNED = 5000;

  // Change the size of the batches that are returned
  private static final int INITIAL_BATCH_SIZE = 500;
  private static final int BATCH_SIZE_INCREMENT = 100;

  private static final TStatus STATUS_OK =
      new TStatus(TStatusCode.OK, Lists.<String>newArrayList());

  private int currRow_;
  private boolean eos_;
  private int batchSize_;
  private TTableSchema schema_;
  private DataSourceState state_;
  private String scanHandle_;

  // Enumerates the states of the data source.
  private enum DataSourceState {
    CREATED,
    OPENED,
    CLOSED
  }

  public AllTypesDataSource() {
    eos_ = false;
    currRow_ = 0;
    state_ = DataSourceState.CREATED;
  }

  /**
   * Accepts every other conjunct and returns the constant number of rows that
   * is always returned.
   */
  @Override
  public TPrepareResult prepare(TPrepareParams params) {
    Preconditions.checkState(state_ == DataSourceState.CREATED);
    List<Integer> accepted = Lists.newArrayList();
    for (int i = 0; i < params.getPredicatesSize(); ++i) {
      if (i % 2 == 0) accepted.add(i);
    }
    return new TPrepareResult(STATUS_OK)
      .setAccepted_conjuncts(accepted)
      .setNum_rows_estimate(NUM_ROWS_RETURNED);
  }

  /**
   * Initializes the batch size and stores the table schema.
   */
  @Override
  public TOpenResult open(TOpenParams params) {
    Preconditions.checkState(state_ == DataSourceState.CREATED);
    state_ = DataSourceState.OPENED;
    batchSize_ = INITIAL_BATCH_SIZE;
    schema_ = params.getRow_schema();
    scanHandle_ = UUID.randomUUID().toString();
    return new TOpenResult(STATUS_OK).setScan_handle(scanHandle_);
  }

  /**
   * Returns row batches with generated rows based on the row index. Called multiple
   * times, so the current row is stored between calls. Each row batch is a different
   * size (not necessarily the size specified by TOpenParams.batch_size to ensure
   * that Impala can handle unexpected batch sizes.
   */
  @Override
  public TGetNextResult getNext(TGetNextParams params) {
    Preconditions.checkState(state_ == DataSourceState.OPENED);
    Preconditions.checkArgument(params.getScan_handle().equals(scanHandle_));
    if (eos_) return new TGetNextResult(STATUS_OK).setEos(eos_);

    List<TColumnData> cols = Lists.newArrayList();
    for (int i = 0; i < schema_.getColsSize(); ++i) {
      cols.add(new TColumnData().setIs_null(Lists.<Boolean>newArrayList()));
    }

    int numAdded = 0;
    while (currRow_ < NUM_ROWS_RETURNED && numAdded < batchSize_) {
      addRow(cols);
      ++numAdded;
      ++currRow_;
    }

    batchSize_ += BATCH_SIZE_INCREMENT;
    if (currRow_ == NUM_ROWS_RETURNED) eos_ = true;
    return new TGetNextResult(STATUS_OK).setEos(eos_)
        .setRows(new TRowBatch().setCols(cols).setNum_rows(numAdded));
  }

  /**
   * Adds a row to the set of columns. For all numeric types the value is set to the
   * row index (mod the size for integer types). For strings it is just a string
   * containing the row index and every 5th result is null.
   */
  private void addRow(List<TColumnData> cols) {
    for (int i = 0; i < cols.size(); ++i) {
      TColumnDesc colDesc = schema_.getCols().get(i);
      TColumnData colData = cols.get(i);
      TColumnType type = colDesc.getType();
      switch (type.getType()) {
        case TINYINT:
          colData.addToIs_null(false);
          colData.addToByte_vals((byte) (currRow_ % 10));
          break;
        case SMALLINT:
          colData.addToIs_null(false);
          colData.addToShort_vals((short) (currRow_ % 100));
          break;
        case INT:
          colData.addToIs_null(false);
          colData.addToInt_vals(currRow_);
          break;
        case BIGINT:
          colData.addToIs_null(false);
          colData.addToLong_vals((long) currRow_ * 10);
          break;
        case DOUBLE:
          colData.addToIs_null(false);
          colData.addToDouble_vals(currRow_);
          break;
        case FLOAT:
          colData.addToIs_null(false);
          colData.addToDouble_vals((float) (1.1 * currRow_));
          break;
        case STRING:
          if (currRow_ % 5 == 0) {
            colData.addToIs_null(true);
          } else {
            colData.addToIs_null(false);
            colData.addToString_vals(String.valueOf(currRow_));
          }
          break;
        case BOOLEAN:
          colData.addToIs_null(false);
          colData.addToBool_vals(currRow_ % 2 == 0);
          break;
        case TIMESTAMP:
          colData.addToIs_null(false);
          colData.addToBinary_vals(
            SerializationUtils.encodeTimestamp(new Timestamp(currRow_)));
          break;
        case DECIMAL:
          colData.addToIs_null(false);
          BigInteger maxUnscaled = BigInteger.TEN.pow(type.getPrecision());
          BigInteger val = maxUnscaled.subtract(BigInteger.valueOf(currRow_ + 1));
          val = val.mod(maxUnscaled);
          if (currRow_ % 2 == 0) val = val.negate();
          colData.addToBinary_vals(SerializationUtils.encodeDecimal(new BigDecimal(val)));
          break;
        case BINARY:
        case CHAR:
        case DATE:
        case DATETIME:
        case INVALID_TYPE:
        case NULL_TYPE:
        default:
          // Unsupported.
          throw new UnsupportedOperationException("Unsupported column type: " +
              colDesc.getType().getType());
      }
    }
  }

  @Override
  public TCloseResult close(TCloseParams params) {
    Preconditions.checkState(state_ == DataSourceState.OPENED);
    Preconditions.checkArgument(params.getScan_handle().equals(scanHandle_));
    state_ = DataSourceState.CLOSED;
    return new TCloseResult(STATUS_OK);
  }
}
