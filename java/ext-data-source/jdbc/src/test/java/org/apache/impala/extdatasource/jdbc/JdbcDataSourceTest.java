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

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.impala.thrift.TColumnData;
import org.apache.impala.thrift.TColumnType;
import org.apache.impala.thrift.TColumnValue;
import org.apache.impala.thrift.TErrorCode;
import org.apache.impala.thrift.TPrimitiveType;
import org.apache.impala.thrift.TScalarType;
import org.apache.impala.thrift.TTypeNode;
import org.apache.impala.thrift.TTypeNodeType;
import org.apache.impala.thrift.TUniqueId;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class JdbcDataSourceTest {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcDataSourceTest.class);

  private static String initString_ = "CACHE_CLASS::{\"database.type\":\"H2\", "
      + "\"jdbc.url\":\"jdbc:h2:mem:test;MODE=MySQL;INIT=runscript from "
      + "'classpath:test_script.sql'\", "
      + "\"jdbc.driver\":\"org.h2.Driver\", "
      + "\"table\":\"test_strategy\","
      + "\"column.mapping\":\"id=strategy_id\"}";

  // Share data between tests
  private static JdbcDataSource jdbcDataSource_ = new JdbcDataSource();
  private static String scanHandle_;
  private static TTableSchema schema_;
  private static List<List<TBinaryPredicate>> predicates_ = Lists.newArrayList();
  private static List<List<TBinaryPredicate>> acceptedPredicates_ = Lists.newArrayList();
  private static long expectReturnRows_ = 5L;

  @Test
  public void test01Init() {
    String colName = "id";

    TComparisonOp op = TComparisonOp.LE;

    TTypeNode typeNode = new TTypeNode();
    typeNode.setType(TTypeNodeType.SCALAR);
    TScalarType scalarType = new TScalarType();
    scalarType.setType(TPrimitiveType.INT);
    typeNode.setScalar_type(scalarType);
    TColumnType colType = new TColumnType();
    colType.setTypes(Lists.newArrayList(typeNode));
    TColumnDesc col = new TColumnDesc().setName(colName).setType(colType);
    TColumnValue value = new TColumnValue();
    value.setInt_val(3);
    TBinaryPredicate idPredicate = new TBinaryPredicate().setCol(col).setOp(op)
        .setValue(value);

    // predicates filter
    predicates_.add(Lists.newArrayList(idPredicate));
    expectReturnRows_ = 3L;
    LOG.info("setup predicates:{}, expectReturnRows: {}", predicates_, expectReturnRows_);

    boolean ret = jdbcDataSource_.convertInitStringToConfiguration(initString_);
    Assert.assertTrue(ret);
  }

  @Test
  public void test02Prepare() {
    TPrepareParams params = new TPrepareParams();
    params.setTable_name("test_strategy");
    params.setInit_string(initString_);
    params.setPredicates(Lists.newArrayList());
    params.setPredicates(predicates_);
    TPrepareResult resp = jdbcDataSource_.prepare(params);
    Assert.assertEquals(TErrorCode.OK, resp.getStatus().status_code);
    if (resp.isSetAccepted_conjuncts()) {
      acceptedPredicates_ = Lists.newArrayList();
      // @see org.apache.impala.planner.DataSourceScanNode#removeAcceptedConjuncts
      List<Integer> acceptedPredicatesIdx = resp.getAccepted_conjuncts();
      // Because conjuncts_ is modified in place using positional indexes from
      // conjunctsIdx, we remove the accepted predicates in reverse order.
      for (int i = acceptedPredicatesIdx.size() - 1; i >= 0; --i) {
        int acceptedPredIdx = acceptedPredicatesIdx.get(i);
        acceptedPredicates_.add(predicates_.remove(acceptedPredIdx));
      }
      // Returns a view of the list in the original order as we will print these
      // in the explain string and it's convenient to have predicates printed
      // in the same order that they're specified.
      acceptedPredicates_ = Lists.reverse(acceptedPredicates_);
    }
    if (resp.isSetNum_rows_estimate()) {
      long estimate = resp.getNum_rows_estimate();
      Assert.assertEquals(5, estimate);
    }
  }

  @Test
  public void test03Open() {
    TOpenParams params = new TOpenParams();
    TUniqueId unique_id = new TUniqueId();
    unique_id.hi = 0xfeedbeeff00d7777L;
    unique_id.lo = 0x2020202020202020L;
    String str = "feedbeeff00d7777:2020202020202020";
    params.setQuery_id(unique_id);
    params.setTable_name("test_strategy");
    params.setInit_string(initString_);
    schema_ = initSchema();
    params.setRow_schema(schema_);
    params.setBatch_size(5);
    params.setPredicates(acceptedPredicates_);
    TOpenResult resp = jdbcDataSource_.open(params);
    Assert.assertEquals(TErrorCode.OK, resp.getStatus().status_code);
    scanHandle_ = resp.getScan_handle();
    Assert.assertTrue(StringUtils.isNoneBlank(scanHandle_));
  }

  @Test
  public void test04GetNext() {
    TGetNextParams params = new TGetNextParams();
    params.setScan_handle(scanHandle_);
    boolean eos;
    long totalNumRows = 0;
    do {
      TGetNextResult resp = jdbcDataSource_.getNext(params);
      Assert.assertEquals(TErrorCode.OK, resp.getStatus().status_code);
      eos = resp.isEos();
      TRowBatch rowBatch = resp.getRows();
      long numRows = rowBatch.getNum_rows();
      totalNumRows += numRows;
      List<TColumnData> cols = rowBatch.getCols();
      Assert.assertEquals(schema_.getColsSize(), cols.size());
    } while (!eos);
    Assert.assertEquals(expectReturnRows_, totalNumRows);
  }

  @Test
  public void test05Close() {
    TCloseParams params = new TCloseParams();
    params.setScan_handle(scanHandle_);
    TCloseResult resp = jdbcDataSource_.close(params);
    Assert.assertEquals(TErrorCode.OK, resp.getStatus().status_code);
  }

  private static TTableSchema initSchema() {
    // strategy_id int, name string, referrer string, landing string, priority  int,
    // implementation string, last_modified timestamp
    TTableSchema schema_ = new TTableSchema();
    TColumnDesc col = new TColumnDesc();
    col.setName("id");
    TTypeNode typeNode = new TTypeNode();
    typeNode.setType(TTypeNodeType.SCALAR);
    TScalarType scalarType = new TScalarType();
    scalarType.setType(TPrimitiveType.INT);
    typeNode.setScalar_type(scalarType);
    TColumnType colType = new TColumnType();
    colType.setTypes(Lists.newArrayList(typeNode));
    col.setType(colType);
    schema_.addToCols(col);

    col = new TColumnDesc();
    col.setName("name");
    typeNode = new TTypeNode();
    typeNode.setType(TTypeNodeType.SCALAR);
    scalarType = new TScalarType();
    scalarType.setType(TPrimitiveType.STRING);
    typeNode.setScalar_type(scalarType);
    colType = new TColumnType();
    colType.setTypes(Lists.newArrayList(typeNode));
    col.setType(colType);
    schema_.addToCols(col);

    col = new TColumnDesc();
    col.setName("priority");
    typeNode = new TTypeNode();
    typeNode.setType(TTypeNodeType.SCALAR);
    scalarType = new TScalarType();
    scalarType.setType(TPrimitiveType.INT);
    typeNode.setScalar_type(scalarType);
    colType = new TColumnType();
    colType.setTypes(Lists.newArrayList(typeNode));
    col.setType(colType);
    schema_.addToCols(col);

    col = new TColumnDesc();
    col.setName("implementation");
    typeNode = new TTypeNode();
    typeNode.setType(TTypeNodeType.SCALAR);
    scalarType = new TScalarType();
    scalarType.setType(TPrimitiveType.STRING);
    typeNode.setScalar_type(scalarType);
    colType = new TColumnType();
    colType.setTypes(Lists.newArrayList(typeNode));
    col.setType(colType);
    schema_.addToCols(col);

    col = new TColumnDesc();
    col.setName("last_modified");
    typeNode = new TTypeNode();
    typeNode.setType(TTypeNodeType.SCALAR);
    scalarType = new TScalarType();
    scalarType.setType(TPrimitiveType.TIMESTAMP);
    typeNode.setScalar_type(scalarType);
    colType = new TColumnType();
    colType.setTypes(Lists.newArrayList(typeNode));
    col.setType(colType);
    schema_.addToCols(col);
    return schema_;
  }

  public static void printData(List<TColumnDesc> colDescs, List<TColumnData> colDatas) {
    for (int i = 0; i < colDatas.size(); ++i) {
      TColumnDesc colDesc = colDescs.get(i);
      TColumnData colData = colDatas.get(i);
      System.out.println("idx: " + i);
      System.out.println(" Name: " + colDesc);
      System.out.println(" Data: " + colData);
    }
  }

}
