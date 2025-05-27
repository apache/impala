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

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.impala.extdatasource.jdbc.conf.JdbcStorageConfig;
import org.apache.impala.extdatasource.jdbc.conf.JdbcStorageConfigManager;
import org.apache.impala.extdatasource.jdbc.dao.DatabaseAccessor;
import org.apache.impala.extdatasource.jdbc.dao.DatabaseAccessorFactory;
import org.apache.impala.extdatasource.jdbc.dao.JdbcRecordIterator;
import org.apache.impala.extdatasource.jdbc.exception.JdbcDatabaseAccessException;
import org.apache.impala.extdatasource.jdbc.util.QueryConditionUtil;
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
import org.apache.impala.thrift.TStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
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

  private boolean eos_;
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
  // currRow_ and totalNumberOfRecords_ are used when schema_.getColsSize() equals 0.
  private long currRow_;
  private long totalNumberOfRecords_ = 0;

  // Enumerates the states of the data source, which indicates which ExternalDataSource
  // API has been called. The states are checked in each API to make sure that the APIs
  // are called in right order, e.g. state transitions must be in the below order:
  // CREATED -> OPENED -> CLOSED.
  // Note that the ExternalDataSourceExecutors of frontend and backend will create
  // separate JdbcDataSource objects so that the state of JdbcDataSource which is set
  // by frontend will not be transferred to backend. The prepare() is called by frontend,
  // open(), getNext() and close() are called by backend. We don't need to change state
  // in prepare() since the state will not be transferred to other APIs, and the input
  // state for open() must be 'CREATED'.
  private enum DataSourceState {
    // The object is created.
    CREATED,
    // The open() API is called.
    OPENED,
    // The close() API is called.
    CLOSED
  }

  public JdbcDataSource() {
    eos_ = false;
    currRow_ = 0;
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
    return new TPrepareResult(STATUS_OK)
            .setAccepted_conjuncts(acceptedPredicates);
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
    scanHandle_ = UUID.randomUUID().toString();
    return new TOpenResult(STATUS_OK).setScan_handle(scanHandle_);
  }

  @Override
  public TGetNextResult getNext(TGetNextParams params) {
    Preconditions.checkState(state_ == DataSourceState.OPENED);
    Preconditions.checkArgument(params.getScan_handle().equals(scanHandle_));
    if (eos_) return new TGetNextResult(STATUS_OK).setEos(eos_);

    List<TColumnData> cols = Lists.newArrayList();
    long numRows = 0;
    if (schema_.getColsSize() != 0) {
      if (iterator_ == null) {
        return new TGetNextResult(
            new TStatus(TErrorCode.RUNTIME_ERROR,
                Lists.newArrayList("Iterator of JDBC resultset is null")));
      }
      for (int i = 0; i < schema_.getColsSize(); ++i) {
        cols.add(new TColumnData().setIs_null(Lists.newArrayList()));
      }

      boolean hasNext = true;
      try {
        while (numRows < batchSize_ && (hasNext = iterator_.hasNext())) {
          iterator_.next(schema_.getCols(), cols);
          ++numRows;
        }
      } catch (UnsupportedOperationException e) {
        try {
          Connection connToBeClosed = null;
          if (iterator_ != null) {
            Preconditions.checkNotNull(dbAccessor_);
            connToBeClosed = iterator_.getConnection();
            iterator_.close();
          }
          if (dbAccessor_ != null) {
            dbAccessor_.close(connToBeClosed, cleanDbcpDSCache_);
          }
          iterator_ = null;
          dbAccessor_ = null;
        } catch (JdbcDatabaseAccessException e2) {
          LOG.warn("Failed to close connection or DataSource", e2);
        }
        return new TGetNextResult(new TStatus(
            TErrorCode.JDBC_CONFIGURATION_ERROR, Lists.newArrayList(e.getMessage())));
      } catch (Exception e) {
        hasNext = false;
      }
      if (!hasNext) eos_ = true;
    } else { // for count(*)
      // Don't need to check batchSize_. But number of rows returned in a RowBatch can
      // not exceed Integer.MAX_VALUE due to the restriction of RowBatch capacity in
      // backend.
      numRows = totalNumberOfRecords_ - currRow_ <= Integer.MAX_VALUE ?
          totalNumberOfRecords_ - currRow_ : Integer.MAX_VALUE;
      currRow_ += numRows;
      eos_ = (currRow_ == totalNumberOfRecords_);
    }
    return new TGetNextResult(STATUS_OK).setEos(eos_)
        .setRows(new TRowBatch().setCols(cols).setNum_rows(numRows));
  }

  @Override
  public TCloseResult close(TCloseParams params) {
    Preconditions.checkState(state_ == DataSourceState.OPENED);
    Preconditions.checkArgument(params.getScan_handle().equals(scanHandle_));
    try {
      // JdbcRecordIterator.close() call java.sql.Connection.close() to close connection.
      // But that API does not effectively add closed connection back to connection pool.
      // We should call GenericJdbcDatabaseAccessor.close() to close a connection since
      // the function call BasicDataSource.invalidateConnection() to close connection
      // which is more efficient than java.sql.Connection.close().
      Connection connToBeClosed = null;
      if (iterator_ != null) {
        Preconditions.checkNotNull(dbAccessor_);
        connToBeClosed = iterator_.getConnection();
        iterator_.close();
      }
      if (dbAccessor_ != null) {
        dbAccessor_.close(connToBeClosed, cleanDbcpDSCache_);
      }
      state_ = DataSourceState.CLOSED;
      return new TCloseResult(STATUS_OK);
    } catch (Exception e) {
      return new TCloseResult(
          new TStatus(TErrorCode.RUNTIME_ERROR, Lists.newArrayList(e.getMessage())));
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
    // Return the indexes of accepted predicates.
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
    StringBuilder sb = new StringBuilder("SELECT ");
    String project;
    // If cols size equals to 0, it is 'select count(*) from tbl' statement.
    if (schema_.getColsSize() == 0) {
      project = "*";
    } else {
      project =
          schema_.getCols().stream().map(
              TColumnDesc::getName).map(
              name -> columnMapping.getOrDefault(name, name))
              .collect(Collectors.joining(", "));
    }
    sb.append(project);
    sb.append(" FROM ");
    // Make jdbc table name to be quoted with double quotes if columnMapping is not empty
    String jdbcTableName = tableConfig_.get(JdbcStorageConfig.TABLE.getPropertyName());
    if (!columnMapping.isEmpty()) {
      jdbcTableName = dbAccessor_.getCaseSensitiveName(jdbcTableName);
    }
    sb.append(jdbcTableName);
    String condition = QueryConditionUtil
        .buildCondition(params.getPredicates(), columnMapping, dbAccessor_);
    if (StringUtils.isNotBlank(condition)) {
      sb.append(" WHERE ").append(condition);
    }
    // Execute query and get iterator
    tableConfig_.set(JdbcStorageConfig.QUERY.getPropertyName(), sb.toString());
    LOG.trace("JDBC Query: " + sb.toString());

    if (schema_.getColsSize() != 0) {
      int limit = -1;
      if (params.isSetLimit()) limit = (int) params.getLimit();
      iterator_ = dbAccessor_.getRecordIterator(tableConfig_, limit, 0);
    } else {
      totalNumberOfRecords_ = dbAccessor_.getTotalNumberOfRecords(tableConfig_);
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
