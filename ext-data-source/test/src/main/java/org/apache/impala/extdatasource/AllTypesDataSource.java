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

package org.apache.impala.extdatasource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;

import org.apache.impala.extdatasource.thrift.TBinaryPredicate;
import org.apache.impala.extdatasource.thrift.TCloseParams;
import org.apache.impala.extdatasource.thrift.TCloseResult;
import org.apache.impala.extdatasource.thrift.TColumnDesc;
import org.apache.impala.extdatasource.thrift.TGetNextParams;
import org.apache.impala.extdatasource.thrift.TGetNextResult;
import org.apache.impala.extdatasource.thrift.TOpenParams;
import org.apache.impala.extdatasource.thrift.TOpenResult;
import org.apache.impala.extdatasource.thrift.TPrepareParams;
import org.apache.impala.extdatasource.thrift.TPrepareResult;
import org.apache.impala.extdatasource.thrift.TRowBatch;
import org.apache.impala.extdatasource.thrift.TTableSchema;
import org.apache.impala.extdatasource.util.SerializationUtils;
import org.apache.impala.extdatasource.v1.ExternalDataSource;
import org.apache.impala.thrift.TColumnData;
import org.apache.impala.thrift.TColumnType;
import org.apache.impala.thrift.TPrimitiveType;
import org.apache.impala.thrift.TScalarType;
import org.apache.impala.thrift.TStatus;
import org.apache.impala.thrift.TErrorCode;
import org.apache.impala.thrift.TTypeNodeType;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Data source implementation for tests that can:
 * (a) Accepts every other offered conjunct for testing planning (though predicates are
 *     not actually evaluated) and returns trivial data of all supported types for
 *     query tests.
 * (b) Validate the predicates offered by Impala.
 */
public class AllTypesDataSource implements ExternalDataSource {
  // Total number of rows to return
  private static final int NUM_ROWS_RETURNED = 5000;

  // Change the size of the batches that are returned
  private static final int INITIAL_BATCH_SIZE = 500;
  private static final int BATCH_SIZE_INCREMENT = 100;

  private static final TStatus STATUS_OK =
      new TStatus(TErrorCode.OK, Lists.<String>newArrayList());

  private int currRow_;
  private boolean eos_;
  private int batchSize_;
  private TTableSchema schema_;
  private DataSourceState state_;
  private String scanHandle_;
  private String validatePredicatesResult_;

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
    int numRowsReturned = 0;
    if (validatePredicates(params.getPredicates())) {
      // Indicate all predicates are applied because we return a dummy row with the
      // result later to validate the result in tests. Impala shouldn't try to apply
      // predicates to that dummy row.
      for (int i = 0; i < params.getPredicatesSize(); ++i) accepted.add(i);
      numRowsReturned = 1;
    } else {
      // Default behavior is to accept every other predicate. They are not actually
      // applied, but we want to validate that Impala applies the correct predicates.
      for (int i = 0; i < params.getPredicatesSize(); ++i) {
        if (i % 2 == 0) accepted.add(i);
      }
      numRowsReturned = NUM_ROWS_RETURNED;
    }
    return new TPrepareResult(STATUS_OK)
      .setAccepted_conjuncts(accepted)
      .setNum_rows_estimate(numRowsReturned);
  }

  /**
   * If the predicate value (assuming STRING) starts with 'VALIDATE_PREDICATES##',
   * we validate the TPrepareParams.predicates against predicates specified after the
   * 'VALIDATE_PREDICATES##' and return true. The result of the validation is stored
   * in validatePredicatesResult_.
   *
   * The expected predicates are specified in the form "{slot} {TComparisonOp} {val}",
   * and conjunctive predicates are separated by '&&'.
   *
   * For example, the predicates_spec validates the predicates in the following query:
   *    select * from table_name
   *    where predicates_spec = 'x LT 1 && y GT 2' and
   *          x < 1 and
   *          2 > y;
   *
   * Current limitations:
   *  - Disjunctive predicates are not supported (e.g. "expr1 or expr2")
   *  - Only INT is supported
   */
  private boolean validatePredicates(List<List<TBinaryPredicate>> predicates) {
    if (predicates == null || predicates.isEmpty()) return false;
    TBinaryPredicate firstPredicate = predicates.get(0).get(0);
    if (!firstPredicate.getValue().isSetString_val()) return false;
    String colVal = firstPredicate.getValue().getString_val();
    if (!colVal.toUpperCase().startsWith("VALIDATE_PREDICATES##")) return false;

    String[] colValParts = colVal.split("##");
    Preconditions.checkArgument(colValParts.length == 2);
    String[] expectedPredicates = colValParts[1].split("&&");
    Preconditions.checkArgument(expectedPredicates.length == predicates.size() - 1);

    String result = "SUCCESS";
    for (int i = 1; i < predicates.size(); ++i) {
      String[] predicateParts = expectedPredicates[i - 1].trim().split(" ");
      Preconditions.checkArgument(predicateParts.length == 3);
      TBinaryPredicate predicate =
          Iterables.getOnlyElement(predicates.get(i));
      Preconditions.checkArgument(predicate.getValue().isSetInt_val());

      String slotName = predicate.getCol().getName().toUpperCase();
      int intVal = predicate.getValue().getInt_val();
      if (!predicateParts[0].toUpperCase().equals(slotName) ||
          !predicateParts[1].toUpperCase().equals(predicate.getOp().name()) ||
          !predicateParts[2].equals(Integer.toString(intVal))) {
        result = "Failed predicate, expected=" + expectedPredicates[i - 1].trim() +
            " actual=" + predicate.toString();
      }
    }
    validatePredicatesResult_ = result;
    return true;
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
    // Need to check validatePredicates again because the call in Prepare() was from
    // the frontend and used a different instance of this data source class.
    if (validatePredicates(params.getPredicates())) {
      // If validating predicates, only one STRING column should be selected.
      Preconditions.checkArgument(schema_.getColsSize() == 1);
      TColumnDesc firstCol = schema_.getCols().get(0);
      TColumnType firstType = firstCol.getType();
      Preconditions.checkState(firstType.getTypesSize() == 1);
      Preconditions.checkState(firstType.types.get(0).getType() == TTypeNodeType.SCALAR);
      Preconditions.checkArgument(
          firstType.types.get(0).scalar_type.getType() == TPrimitiveType.STRING);
    }
    scanHandle_ = UUID.randomUUID().toString();
    return new TOpenResult(STATUS_OK).setScan_handle(scanHandle_);
  }

  /**
   * If validating predicates, returns a single row with the result of the validation.
   * Otherwise returns row batches with generated rows based on the row index. Called
   * multiple times, so the current row is stored between calls. Each row batch is a
   * different size (not necessarily the size specified by TOpenParams.batch_size to
   * ensure that Impala can handle unexpected batch sizes.
   */
  @Override
  public TGetNextResult getNext(TGetNextParams params) {
    Preconditions.checkState(state_ == DataSourceState.OPENED);
    Preconditions.checkArgument(params.getScan_handle().equals(scanHandle_));
    if (eos_) return new TGetNextResult(STATUS_OK).setEos(eos_);

    if (validatePredicatesResult_ != null) {
      TColumnData colData = new TColumnData();
      colData.setIs_null(Lists.newArrayList(false));
      colData.setString_vals(Lists.newArrayList(validatePredicatesResult_));
      eos_ = true;
      return new TGetNextResult(STATUS_OK).setEos(eos_)
          .setRows(new TRowBatch().setCols(Lists.newArrayList(colData)).setNum_rows(1));
    }

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
      if (type.types.get(0).getType() != TTypeNodeType.SCALAR) {
        // Unsupported non-scalar type.
        throw new UnsupportedOperationException("Unsupported column type: " +
            type.types.get(0).getType());
      }
      Preconditions.checkState(type.getTypesSize() == 1);
      TScalarType scalarType = type.types.get(0).scalar_type;
      switch (scalarType.type) {
        case TINYINT:
          colData.addToIs_null(false);
          colData.addToByte_vals((byte) (currRow_ % 10));
          break;
        case SMALLINT:
          colData.addToIs_null(false);
          colData.addToShort_vals((short) (currRow_ % 100));
          break;
        case INT:
        case DATE:
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
          BigInteger maxUnscaled = BigInteger.TEN.pow(scalarType.getPrecision());
          BigInteger val = maxUnscaled.subtract(BigInteger.valueOf(currRow_ + 1));
          val = val.mod(maxUnscaled);
          if (currRow_ % 2 == 0) val = val.negate();
          colData.addToBinary_vals(SerializationUtils.encodeDecimal(new BigDecimal(val)));
          break;
        case BINARY:
        case CHAR:
        case DATETIME:
        case INVALID_TYPE:
        case NULL_TYPE:
        default:
          // Unsupported.
          throw new UnsupportedOperationException("Unsupported column type: " +
              scalarType.getType());
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
