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

package org.apache.impala.extdatasource.jdbc;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.impala.extdatasource.jdbc.conf.JdbcStorageConfig;
import org.apache.impala.extdatasource.jdbc.conf.JdbcStorageConfigManager;
import org.apache.impala.extdatasource.jdbc.dao.DatabaseAccessor;
import org.apache.impala.extdatasource.jdbc.dao.DatabaseAccessorFactory;
import org.apache.impala.extdatasource.jdbc.dao.JdbcRecordIterator;
import org.apache.impala.extdatasource.jdbc.exception.JdbcDatabaseAccessException;
import org.apache.impala.extdatasource.jdbc.util.QueryConditionUtil;
import org.apache.impala.extdatasource.util.SerializationUtils;
import org.apache.impala.extdatasource.thrift.TBinaryPredicate;
import org.apache.impala.extdatasource.thrift.TCloseParams;
import org.apache.impala.extdatasource.thrift.TCloseResult;
import org.apache.impala.extdatasource.thrift.TColumnDesc;
import org.apache.impala.extdatasource.thrift.TComparisonOp;
import org.apache.impala.extdatasource.thrift.TGetNextParams;
import org.apache.impala.extdatasource.thrift.TGetNextResult;
import org.apache.impala.extdatasource.thrift.TOpenParams;
import org.apache.impala.extdatasource.thrift.TOpenResult;
import org.apache.impala.extdatasource.thrift.TPrepareParams;
import org.apache.impala.extdatasource.thrift.TPrepareResult;
import org.apache.impala.extdatasource.thrift.TRowBatch;
import org.apache.impala.extdatasource.thrift.TTableSchema;
import org.apache.impala.extdatasource.v1.ExternalDataSource;
import org.apache.impala.thrift.TColumnData;
import org.apache.impala.thrift.TErrorCode;
import org.apache.impala.thrift.TScalarType;
import org.apache.impala.thrift.TStatus;
import org.apache.impala.thrift.TTypeNodeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * The Jdbc data source returns the data of the underlying database configured by
 * initString.
 */
public class JdbcDataSource implements ExternalDataSource {

  private final static Logger LOG = LoggerFactory.getLogger(JdbcDataSource.class);

  private static final TStatus STATUS_OK =
          new TStatus(TErrorCode.OK, Lists.newArrayList());

  private volatile boolean eos_;
  private int batchSize_;
  private TTableSchema schema_;
  private DataSourceState state_;
  // Handle to identify JdbcDataSource object.
  // It's assigned with random UUID value in open() API, and returned to the caller.
  // In getNext() and close() APIs, compare this value with the handle value passed in
  // input parameters to make sure the object is valid.
  private String scanHandle_;
  // Set as value of query option 'clean_dbcp_ds_cache'.
  // It is passed to DatabaseAccessor::close() to indicate if dataSourceCache should be
  // cleaned when its reference count equals 0.
  private boolean cleanDbcpDSCache_ = true;

  // Properties of external jdbc table, converted from initString which is specified in
  // create table statement.
  // Supported properties are defined in JdbcStorageConfig.
  private Configuration tableConfig_;
  private DatabaseAccessor dbAccessor_ = null;
  // iterator_ is used when schema_.getColsSize() does not equal 0.
  private JdbcRecordIterator iterator_ = null;
  // scalarTypes_ mirrors the Impala scalar types for each projected column.
  // Populated in buildQueryAndExecute(); used by fetchBatch() to guide raw-value reads.
  private TScalarType[] scalarTypes_;
  // currRow_ and totalNumberOfRecords_ are used when schema_.getColsSize() equals 0.
  private long currRow_;
  private long totalNumberOfRecords_ = 0;

  // Lifecycle state: prepare() runs in the frontend JVM, open()/getNext()/close() in
  // the backend JVM — each uses a separate JdbcDataSource instance, so state set by
  // prepare() is never visible to open(). Transitions must follow CREATED→OPENED→CLOSED.
  private enum DataSourceState { CREATED, OPENED, CLOSED }

  public JdbcDataSource() {
    state_ = DataSourceState.CREATED;
  }

  @Override
  public TPrepareResult prepare(TPrepareParams params) {
    Preconditions.checkState(state_ == DataSourceState.CREATED);
    if (!convertInitStringToConfiguration(params.getInit_string())) {
      return new TPrepareResult(
          new TStatus(TErrorCode.JDBC_CONFIGURATION_ERROR,
              Lists.newArrayList("Invalid init_string value")));
    }
    List<Integer> acceptedPredicates = acceptedPredicates(params.getPredicates());
    long numRecords = 0;
    try {
      dbAccessor_ = DatabaseAccessorFactory.getAccessor(tableConfig_);
      numRecords = dbAccessor_.getTotalNumberOfRecords(tableConfig_);
      LOG.info("Estimated number of records: {}", numRecords);
    } catch (JdbcDatabaseAccessException e) {
      return new TPrepareResult(
          new TStatus(TErrorCode.RUNTIME_ERROR,
              Lists.newArrayList("Failed to retrieve total number of records: "
                  + e.getMessage())));
    }
    if (dbAccessor_ != null) {
      if (params.isSetClean_dbcp_ds_cache()) {
        cleanDbcpDSCache_ = params.isClean_dbcp_ds_cache();
      }
      dbAccessor_.close(null, cleanDbcpDSCache_);
      dbAccessor_ = null;
    }
    return new TPrepareResult(STATUS_OK)
            .setAccepted_conjuncts(acceptedPredicates)
            .setNum_rows_estimate(numRecords);
  }

  @Override
  public TOpenResult open(TOpenParams params) {
    Preconditions.checkState(state_ == DataSourceState.CREATED);
    state_ = DataSourceState.OPENED;
    batchSize_ = params.getBatch_size();
    schema_ = params.getRow_schema();
    if (params.isSetClean_dbcp_ds_cache()) {
      cleanDbcpDSCache_ = params.isClean_dbcp_ds_cache();
    }

    // Opaque handle returned to the caller and used in subsequent
    // getNext()/close() calls.
    scanHandle_ = UUID.randomUUID().toString();

    // 1. Check init string again because the call in prepare() was from
    // the frontend and used a different instance of this JdbcDataSource class.
    if (!convertInitStringToConfiguration(params.getInit_string())) {
      return new TOpenResult(
          new TStatus(TErrorCode.JDBC_CONFIGURATION_ERROR,
              Lists.newArrayList("Invalid init_string value")));
    }
    // 2. Build the query and execute it
    try {
      Preconditions.checkState(tableConfig_ != null);
      dbAccessor_ = DatabaseAccessorFactory.getAccessor(tableConfig_);
      buildQueryAndExecute(params);
    } catch (JdbcDatabaseAccessException e) {
      if (dbAccessor_ != null) {
        dbAccessor_.close(null, cleanDbcpDSCache_);
        dbAccessor_ = null;
      }
      return new TOpenResult(
          new TStatus(TErrorCode.RUNTIME_ERROR, Lists.newArrayList(e.getMessage())));
    }

    return new TOpenResult(STATUS_OK).setScan_handle(scanHandle_);
  }

  /**
   * Returns the next batch of rows. May be called concurrently by multiple C++ scanner
   * threads; fetchBatch() serializes JDBC cursor access internally. For count(*) queries
   * (schema_.getColsSize() == 0), cursor arithmetic is protected by synchronized(this).
   */
  @Override
  public TGetNextResult getNext(TGetNextParams params) {
    Preconditions.checkState(state_ == DataSourceState.OPENED);
    Preconditions.checkArgument(params.getScan_handle().equals(scanHandle_));
    if (eos_) return new TGetNextResult(STATUS_OK).setEos(true);

    List<TColumnData> cols = Lists.newArrayList();
    List<Object[]> rowBatch = Lists.newArrayList();

    long numRows = 0;
    long cursorFetchNs = 0;
    long lockWaitNs = 0;
    if (schema_.getColsSize() != 0) {
      if (iterator_ == null) {
        return new TGetNextResult(
            new TStatus(TErrorCode.RUNTIME_ERROR,
                Lists.newArrayList("Iterator of JDBC resultset is null")));
      }

      try {
        JdbcRecordIterator.FetchResult fetchResult =
            iterator_.fetchBatch(scalarTypes_, batchSize_);
        rowBatch = fetchResult.rows;
        cursorFetchNs = fetchResult.cursorFetchNs;
        lockWaitNs = fetchResult.lockWaitNs;
        numRows = rowBatch.size();
        eos_ = (numRows < batchSize_);
        // Pre-allocate TColumnData with exact capacity now that we know the row count.
        for (int i = 0; i < schema_.getColsSize(); ++i) {
          cols.add(new TColumnData().setIs_null(new ArrayList<>((int) numRows)));
        }
        materializeRowBatch(rowBatch, schema_.getCols(), cols);
      } catch (UnsupportedOperationException e) {
        // Unsupported column type: close resources and propagate as a config error.
        try {
          Connection connToBeClosed = null;
          if (iterator_ != null) {
            Preconditions.checkNotNull(dbAccessor_);
            connToBeClosed = iterator_.getConnection();
            iterator_.close();
          }
          if (dbAccessor_ != null) dbAccessor_.close(connToBeClosed, cleanDbcpDSCache_);
          iterator_ = null;
          dbAccessor_ = null;
        } catch (JdbcDatabaseAccessException e2) {
          LOG.warn("Failed to close connection or DataSource", e2);
        }
        return new TGetNextResult(new TStatus(
            TErrorCode.JDBC_CONFIGURATION_ERROR, Lists.newArrayList(e.getMessage())));
      } catch (Exception e) {
        return new TGetNextResult(
            new TStatus(TErrorCode.RUNTIME_ERROR, Lists.newArrayList(e.getMessage())));
      }
    } else { // for count(*)
      // RowBatch capacity is bounded by Integer.MAX_VALUE in the backend.
      synchronized (this) {
        numRows = totalNumberOfRecords_ - currRow_ <= Integer.MAX_VALUE ?
            totalNumberOfRecords_ - currRow_ : Integer.MAX_VALUE;
        currRow_ += numRows;
        eos_ = (currRow_ == totalNumberOfRecords_);
      }
    }
    TGetNextResult result = new TGetNextResult(STATUS_OK).setEos(eos_)
        .setRows(new TRowBatch().setCols(cols).setNum_rows(numRows));
    if (schema_.getColsSize() != 0) {
      result.setCursor_fetch_time_ns(cursorFetchNs);
      result.setLock_wait_time_ns(lockWaitNs);
    }
    return result;
  }

  @Override
  public TCloseResult close(TCloseParams params) {
    Preconditions.checkState(state_ == DataSourceState.OPENED);
    Preconditions.checkArgument(params.getScan_handle().equals(scanHandle_));
    try {
      // Use dbAccessor_.close() rather than iterator_.getConnection().close() so that
      // the connection is returned via BasicDataSource.invalidateConnection(), which is
      // more correct than java.sql.Connection.close() for pooled connections.
      Connection connToBeClosed = null;
      if (iterator_ != null) {
        Preconditions.checkNotNull(dbAccessor_);
        connToBeClosed = iterator_.getConnection();
        iterator_.close();
      }
      if (dbAccessor_ != null) dbAccessor_.close(connToBeClosed, cleanDbcpDSCache_);
      return new TCloseResult(STATUS_OK);
    } catch (Exception e) {
      return new TCloseResult(
          new TStatus(TErrorCode.RUNTIME_ERROR, Lists.newArrayList(e.getMessage())));
    } finally {
      state_ = DataSourceState.CLOSED;
    }
  }

  protected boolean convertInitStringToConfiguration(String initString) {
    Preconditions.checkState(initString != null);
    if (tableConfig_ == null) {
      try {
        TypeReference<HashMap<String, String>> typeRef
            = new TypeReference<HashMap<String, String>>() {
        };
        // Replace '\n' with single space character so that one property setting in
        // initString can be broken into multiple lines for better readability.
        initString = initString.replace('\n', ' ');
        Map<String, String> config = new ObjectMapper().readValue(initString, typeRef);
        tableConfig_ = JdbcStorageConfigManager.convertMapToConfiguration(config);
      } catch (JsonProcessingException e) {
        String errorMessage = String
            .format("Invalid JSON from initString_ '%s'", initString);
        LOG.error(errorMessage, e);
        return false;
      }
    }
    return true;
  }

  private List<Integer> acceptedPredicates(List<List<TBinaryPredicate>> predicates) {
    List<Integer> acceptedPredicates = Lists.newArrayList();
    if (predicates == null || predicates.isEmpty()) {
      return acceptedPredicates;
    }
    for (int i = 0; i < predicates.size(); ++i) {
      boolean accepted = true;
      for (TBinaryPredicate predicate : predicates.get(i)) {
        // Don't support 'IS DISTINCT FROM' and 'IS NOT DISTINCT FROM' operators now.
        if (predicate.getOp() == TComparisonOp.DISTINCT_FROM
            || predicate.getOp() == TComparisonOp.NOT_DISTINCT) {
          accepted = false;
          break;
        }
      }
      if (accepted) acceptedPredicates.add(i);
    }
    return acceptedPredicates;
  }

  private void buildQueryAndExecute(TOpenParams params)
      throws JdbcDatabaseAccessException {
    Map<String, String> columnMapping = getColumnMapping(tableConfig_
        .get(JdbcStorageConfig.COLUMN_MAPPING.getPropertyName()));
    // Build query statement
    String query;

    String tableName = tableConfig_.get(JdbcStorageConfig.TABLE.getPropertyName());
    // Check if 'table' property is not null or empty
    if (!Strings.isNullOrEmpty(tableName)) {
      StringBuilder sb = new StringBuilder("SELECT ");
      String project;
      // If cols size equals to 0, it is 'select count(*) from tbl' statement.
      if (schema_.getColsSize() == 0) {
        project = "*";
      } else {
        String driverClass = JdbcStorageConfigManager.getConfigValue(
            JdbcStorageConfig.JDBC_DRIVER_CLASS, tableConfig_);
        final String quoteChar;
        if (driverClass != null && (driverClass.toLowerCase().contains("impala") ||
            driverClass.toLowerCase().contains("hive") ||
            driverClass.toLowerCase().contains("mysql"))) {
          quoteChar = "`";
        } else {
          quoteChar = "\"";
        }

        project = schema_.getCols().stream()
                .map(TColumnDesc::getName)
                .map(name -> columnMapping.containsKey(name)
                        ? columnMapping.get(name)
                        : quoteChar + name + quoteChar)
                .collect(Collectors.joining(", "));
      }
      sb.append(project);
      sb.append(" FROM ");
      // Make jdbc table name to be quoted with double quotes if
      // columnMapping is not empty
      if (!columnMapping.isEmpty()) {
        tableName = dbAccessor_.getCaseSensitiveName(tableName);
      }
      sb.append(tableName);
      String condition = QueryConditionUtil
          .buildCondition(params.getPredicates(), columnMapping, dbAccessor_);
      if (StringUtils.isNotBlank(condition)) {
        sb.append(" WHERE ").append(condition);
      }

      query = sb.toString();
    } else {
      // Use 'query' property if 'table' is null
      query = tableConfig_.get(JdbcStorageConfig.QUERY.getPropertyName());
      Preconditions.checkState(!Strings.isNullOrEmpty(query), "Query is null or empty");
      // Wrap in a subquery projecting only the needed columns so that the result-set
      // column positions align with the scalarTypes_ array used in fetchBatch(). Without
      // this, a raw query that returns more columns than Impala projects would cause
      // positional mis-alignment (e.g. reading bool_col's "t" as a DATE value).
      if (schema_.getColsSize() != 0) {
        String cols = schema_.getCols().stream()
            .map(TColumnDesc::getName)
            .collect(Collectors.joining(", "));
        query = "SELECT " + cols + " FROM (" + query + ") _q";
      }
    }

    // Store the generated query
    tableConfig_.set(JdbcStorageConfig.QUERY.getPropertyName(), query);
    LOG.trace("JDBC Query: {}", query);

    if (schema_.getColsSize() != 0) {
      scalarTypes_ = new TScalarType[schema_.getColsSize()];
      for (int i = 0; i < schema_.getColsSize(); i++) {
        Preconditions.checkState(
            schema_.getCols().get(i).getType().types.get(0).getType()
                == TTypeNodeType.SCALAR,
            "Non-scalar column type not supported");
        scalarTypes_[i] =
            schema_.getCols().get(i).getType().types.get(0).scalar_type;
      }
      int limit = -1;
      if (params.isSetLimit()) limit = (int) params.getLimit();
      iterator_ = dbAccessor_.getRecordIterator(tableConfig_, limit, 0);
    } else {
      totalNumberOfRecords_ = dbAccessor_.getTotalNumberOfRecords(tableConfig_);
    }
  }

  /**
   * Converts a batch of raw JDBC rows (Object[]) into Impala TColumnData lists.
   */
  private void materializeRowBatch(List<Object[]> rows, List<TColumnDesc> colDescs,
      List<TColumnData> colDatas) {
    Preconditions.checkState(colDescs.size() == colDatas.size());
    int n = rows.size();
    // Pre-allocate the typed value list for each column based on its scalar type.
    // is_null is already pre-allocated in getNext(); value lists use n as an upper bound
    // (actual size may be smaller when there are nulls).
    for (int i = 0; i < colDescs.size(); ++i) {
      TColumnData col = colDatas.get(i);
      switch (scalarTypes_[i].type) {
        case TINYINT:   col.setByte_vals(new ArrayList<>(n)); break;
        case SMALLINT:  col.setShort_vals(new ArrayList<>(n)); break;
        case INT:
        case DATE:      col.setInt_vals(new ArrayList<>(n)); break;
        case BIGINT:    col.setLong_vals(new ArrayList<>(n)); break;
        case DOUBLE:
        case FLOAT:     col.setDouble_vals(new ArrayList<>(n)); break;
        case BOOLEAN:   col.setBool_vals(new ArrayList<>(n)); break;
        case STRING:    col.setString_vals(new ArrayList<>(n)); break;
        case TIMESTAMP:
        case DECIMAL:   col.setBinary_vals(new ArrayList<>(n)); break;
        default: break;
      }
    }
    for (Object[] row : rows) {
      Preconditions.checkState(row.length == colDescs.size());
      for (int i = 0; i < colDescs.size(); ++i) {
        appendValue(scalarTypes_[i], colDescs.get(i), colDatas.get(i), row[i]);
      }
    }
  }

  /**
   * Appends a single raw JDBC value to a TColumnData column buffer, performing
   * all type conversion and Impala encoding. Any exception thrown here propagates
   * up to getNext(), which aborts the batch and returns an error to the backend.
   */
  private void appendValue(TScalarType scalarType, TColumnDesc colDesc,
      TColumnData colData, Object value) {
    boolean isNull = (value == null);
    colData.addToIs_null(isNull);
    if (isNull) return;
    switch (scalarType.type) {
      case TINYINT:
        colData.addToByte_vals(((Number) value).byteValue());
        break;
      case SMALLINT:
        colData.addToShort_vals(((Number) value).shortValue());
        break;
      case INT:
        colData.addToInt_vals(((Number) value).intValue());
        break;
      case DATE:
        colData.addToInt_vals(
            (int) ((java.sql.Date) value).toLocalDate().toEpochDay());
        break;
      case BIGINT:
        colData.addToLong_vals(((Number) value).longValue());
        break;
      case DOUBLE:
        colData.addToDouble_vals(((Number) value).doubleValue());
        break;
      case FLOAT:
        colData.addToDouble_vals((double) ((Number) value).floatValue());
        break;
      case STRING:
        colData.addToString_vals(value.toString());
        break;
      case BOOLEAN:
        colData.addToBool_vals(Boolean.parseBoolean(value.toString()));
        break;
      case TIMESTAMP:
        colData.addToBinary_vals(
            SerializationUtils.encodeTimestamp((java.sql.Timestamp) value));
        break;
      case DECIMAL:
        BigDecimal val = (BigDecimal) value;
        int valPrecision = val.precision();
        int valScale = val.scale();
        if (scalarType.scale < valScale ||
            scalarType.precision < valPrecision + scalarType.scale - valScale) {
          throw new UnsupportedOperationException(String.format(
              "Invalid DECIMAL(%d, %d) for column %s since there is possible loss of "
                  + "precision when casting from DECIMAL(%d, %d)",
              scalarType.precision, scalarType.scale, colDesc.getName(),
              valPrecision, valScale));
        } else if (scalarType.scale > valScale) {
          val = val.setScale(scalarType.scale);
        }
        colData.addToBinary_vals(SerializationUtils.encodeDecimal(val));
        break;
      case BINARY:
      case CHAR:
      case VARCHAR:
      case DATETIME:
      case INVALID_TYPE:
      case NULL_TYPE:
      default:
        throw new UnsupportedOperationException(
            "Unsupported column type: " + scalarType.getType());
    }
  }

  /*
   * Return Impala-to-X column mapping, or empty if it is not set.
   *
   */
  private Map<String, String> getColumnMapping(String columnMapping) {
    if ((columnMapping == null) || (columnMapping.trim().isEmpty())) {
      return Maps.newHashMap();
    }

    Map<String, String> columnMap = Maps.newHashMap();
    String[] mappingPairs = columnMapping.split(",");
    for (String mapPair : mappingPairs) {
      String[] columns = mapPair.split("=");
      // Make jdbc column name to be quoted with double quotes
      String jdbcColumnName = dbAccessor_.getCaseSensitiveName(columns[1].trim());
      columnMap.put(columns[0].trim(), jdbcColumnName);
    }

    return columnMap;
  }
}
