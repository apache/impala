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

package org.apache.impala.extdatasource.jdbc.dao;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.impala.extdatasource.jdbc.exception.JdbcDatabaseAccessException;
import org.apache.impala.extdatasource.thrift.TColumnDesc;
import org.apache.impala.extdatasource.util.SerializationUtils;
import org.apache.impala.thrift.TColumnData;
import org.apache.impala.thrift.TColumnType;
import org.apache.impala.thrift.TScalarType;
import org.apache.impala.thrift.TTypeNodeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * An iterator that allows iterating through a SQL resultset. Includes methods to clear up
 * resources.
 */
public class JdbcRecordIterator {

  private static final Logger LOGGER = LoggerFactory.getLogger(JdbcRecordIterator.class);

  private final Connection conn;
  private final PreparedStatement ps;
  private final ResultSet rs;
  private final List<String> jdbcColumnNames;

  public JdbcRecordIterator(Connection conn, PreparedStatement ps, ResultSet rs,
      Configuration conf) throws JdbcDatabaseAccessException {
    this.conn = conn;
    this.ps = ps;
    this.rs = rs;

    try {
      ResultSetMetaData metadata = rs.getMetaData();
      int numColumns = metadata.getColumnCount();
      List<String> columnNames = new ArrayList<>(numColumns);
      List<Integer> jdbcColumnTypes = new ArrayList<>(numColumns);
      for (int i = 0; i < numColumns; i++) {
        columnNames.add(metadata.getColumnName(i + 1));
        jdbcColumnTypes.add(metadata.getColumnType(i + 1));
      }
      jdbcColumnNames = columnNames;
    } catch (Exception e) {
      LOGGER.error("Error while trying to get column names.", e);
      throw new JdbcDatabaseAccessException(
          "Error while trying to get column names: " + e.getMessage(), e);
    }
    LOGGER.debug("Iterator ColumnNames = {}", jdbcColumnNames);
  }

  public Connection getConnection() { return conn; }

  public boolean hasNext() throws JdbcDatabaseAccessException {
    try {
      return rs.next();
    } catch (Exception e) {
      LOGGER.warn("hasNext() threw exception", e);
      throw new JdbcDatabaseAccessException(
          "Error while retrieving next batch of rows: " + e.getMessage(), e);
    }
  }

  public void next(List<TColumnDesc> colDescs, List<TColumnData> colDatas)
      throws UnsupportedOperationException {
    Preconditions.checkState(colDescs.size() == colDatas.size());
    for (int i = 0; i < colDescs.size(); ++i) {
      TColumnType type = colDescs.get(i).getType();
      TColumnData colData = colDatas.get(i);
      if (type.types.get(0).getType() != TTypeNodeType.SCALAR) {
        // Unsupported non-scalar type.
        throw new UnsupportedOperationException("Unsupported column type: " +
            type.types.get(0).getType());
      }
      Preconditions.checkState(type.getTypesSize() == 1);
      TScalarType scalarType = type.types.get(0).scalar_type;
      try {
        Object value = rs.getObject(i + 1);
        if (value == null) {
          colData.addToIs_null(true);
          continue;
        }
        switch (scalarType.type) {
          case TINYINT:
            colData.addToByte_vals(rs.getByte(i + 1));
            break;
          case SMALLINT:
            colData.addToShort_vals(rs.getShort(i + 1));
            break;
          case INT:
            colData.addToInt_vals(rs.getInt(i + 1));
            break;
          case DATE:
            LocalDate localDate = Instant.ofEpochMilli(rs.getDate(i + 1).getTime())
                .atZone(ZoneId.systemDefault())
                .toLocalDate();
            colData.addToInt_vals((int) localDate.toEpochDay());
            break;
          case BIGINT:
            colData.addToLong_vals(rs.getLong(i + 1));
            break;
          case DOUBLE:
            colData.addToDouble_vals(rs.getDouble(i + 1));
            break;
          case FLOAT:
            colData.addToDouble_vals(rs.getFloat(i + 1));
            break;
          case STRING:
            colData.addToString_vals(rs.getString(i + 1));
            break;
          case BOOLEAN:
            colData.addToBool_vals(rs.getBoolean(i + 1));
            break;
          case TIMESTAMP:
            // Use UTC time zone instead of system default time zone
            colData.addToBinary_vals(
                SerializationUtils.encodeTimestamp(rs.getTimestamp(i + 1,
                    Calendar.getInstance(TimeZone.getTimeZone(ZoneOffset.UTC)))));
            break;
          case DECIMAL:
            BigDecimal val = rs.getBigDecimal(i + 1);
            int valPrecision = val.precision();
            int valScale = val.scale();
            // Check if there is enough precision and scale in the destination decimal.
            if (scalarType.scale < valScale ||
                scalarType.precision < valPrecision + scalarType.scale - valScale) {
              throw new UnsupportedOperationException(String.format("Invalid DECIMAL" +
                  "(%d, %d) for column %s since there is possible loss of precision " +
                  "when casting from DECIMAL(%d, %d)",
                  scalarType.precision, scalarType.scale, colDescs.get(i).getName(),
                  valPrecision, valScale));
            } else if (scalarType.scale > valScale) {
              val = val.setScale(scalarType.scale);
            }
            colData.addToBinary_vals(SerializationUtils.encodeDecimal(val));
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
        colData.addToIs_null(false);
      } catch (SQLException throwables) {
        colData.addToIs_null(true);
      }
    }
  }

  /**
   * Release all DB resources
   */
  public void close() throws JdbcDatabaseAccessException {
    try {
      rs.close();
      ps.close();
      // Connection is closed in GenericJdbcDatabaseAccessor.close().
    } catch (Exception e) {
      LOGGER.warn("Caught exception while trying to close database objects", e);
      throw new JdbcDatabaseAccessException(
          "Error while releasing database resources: " + e.getMessage(), e);
    }
  }


}
