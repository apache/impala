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

package org.apache.impala.planner;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.stream.Stream;

import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.Frontend.PlanCtx;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.TExecRequest;
import org.apache.impala.thrift.TQueryCtx;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class ColumnsTest extends FrontendTestBase {
  private static final String ALLTYPES = "functional.alltypes";

  private void testColumns(String query, String db, List<String> select,
      List<String> where, List<String> join, List<String> aggregate,
      List<String> orderBy) {
    TQueryCtx queryCtx =
        TestUtils.createQueryContext(db, System.getProperty("user.name"));
    queryCtx.client_request.setStmt(query);
    PlanCtx planCtx = new PlanCtx(queryCtx);
    planCtx.disableDescTblSerialization();
    TExecRequest execRequest = null;
    try {
      execRequest = frontend_.createExecRequest(planCtx);
    } catch (ImpalaException e) { fail(e.toString()); }
    assertNotNull(execRequest);

    assertEquals(select, execRequest.getSelect_columns());
    assertEquals(where, execRequest.getWhere_columns());
    assertEquals(join, execRequest.getJoin_columns());
    assertEquals(aggregate, execRequest.getAggregate_columns());
    assertEquals(orderBy, execRequest.getOrderby_columns());
  }

  // Runs a junit test against the default db.
  private void testColumns(String query, List<String> select, List<String> where,
      List<String> join, List<String> aggregate, List<String> orderBy) {
    testColumns(query, "default", select, where, join, aggregate, orderBy);
  }

  private List<String> fullColumns(String table, Stream<String> names) {
    return names.map(name -> table + "." + name).sorted().collect(toList());
  }

  private List<String> allTypesColumns() {
    Stream<String> names = Stream.of("id", "bool_col", "tinyint_col", "smallint_col",
        "int_col", "bigint_col", "float_col", "double_col", "date_string_col",
        "string_col", "timestamp_col", "year", "month");
    return fullColumns(ALLTYPES, names);
  }

  @Test
  public void testOrderByAlias() {
    // Test Order By claus that contains a column alias reports the actual column name
    // and not the alias.
    testColumns("select  dt.d_year \n"
            + "       ,item.i_brand_id brand_id \n"
            + "       ,item.i_brand brand\n"
            + "       ,sum(ss_ext_sales_price) sum_agg\n"
            + " from  date_dim dt \n"
            + "      ,store_sales\n"
            + "      ,item\n"
            + " where dt.d_date_sk = store_sales.ss_sold_date_sk\n"
            + "   and store_sales.ss_item_sk = item.i_item_sk\n"
            + "   and item.i_manufact_id = 436\n"
            + "   and dt.d_moy=12\n"
            + " group by dt.d_year\n"
            + "      ,item.i_brand\n"
            + "      ,item.i_brand_id\n"
            + " order by dt.d_year\n"
            + "         ,sum_agg desc\n"
            + "         ,brand_id\n"
            + " limit 100",
        "tpcds",
        Stream
            .of("tpcds.date_dim.d_year", "tpcds.item.i_brand", "tpcds.item.i_brand_id",
                "tpcds.store_sales.ss_ext_sales_price")
            .collect(toList()),
        Stream
            .of("tpcds.date_dim.d_date_sk", "tpcds.date_dim.d_moy",
                "tpcds.item.i_item_sk", "tpcds.item.i_manufact_id",
                "tpcds.store_sales.ss_item_sk", "tpcds.store_sales.ss_sold_date_sk")
            .collect(toList()),
        Stream
            .of("tpcds.date_dim.d_date_sk", "tpcds.item.i_item_sk",
                "tpcds.store_sales.ss_item_sk", "tpcds.store_sales.ss_sold_date_sk")
            .collect(toList()),
        Stream.of("tpcds.date_dim.d_year", "tpcds.item.i_brand", "tpcds.item.i_brand_id")
            .collect(toList()),
        Stream
            .of("tpcds.date_dim.d_year", "tpcds.item.i_brand_id",
                "tpcds.store_sales.ss_ext_sales_price")
            .collect(toList()));
  }

  @Test
  public void testSelect() {
    testColumns("select count(*) from functional.alltypes", null, null, null, null, null);

    testColumns("select count(int_col), count(*) from functional.alltypes",
        fullColumns(ALLTYPES, Stream.of("int_col")), null, null, null, null);

    testColumns(
        "select * from functional.alltypes", allTypesColumns(), null, null, null, null);
  }

  @Test
  public void testSelectWhereGroupOrder() {
    testColumns("select bool_col, int_col from functional.alltypes"
            + " where smallint_col > 1 order by year, month",
        fullColumns(ALLTYPES, Stream.of("bool_col", "int_col")),
        fullColumns(ALLTYPES, Stream.of("smallint_col")), null, null,
        fullColumns(ALLTYPES, Stream.of("year", "month")));

    testColumns("select bool_col, sum(int_col) from functional.alltypes"
        + " where smallint_col > 1 group by bool_col, year, month order by year, month",
        fullColumns(ALLTYPES, Stream.of("bool_col", "int_col")),
        fullColumns(ALLTYPES, Stream.of("smallint_col")), null,
        fullColumns(ALLTYPES, Stream.of("bool_col", "month", "year")),
        fullColumns(ALLTYPES, Stream.of("year", "month")));
  }

  @Test
  public void testSelectJoin() {
    testColumns("select a.month, sum(a.int_col) from functional.alltypes a join "
            + "functional.alltypestiny b using (id) where a.year > 2000 and "
            + "b.year > 2000 group by a.month order by a.month",
        fullColumns(ALLTYPES, Stream.of("month", "int_col")),
        ImmutableList.of("functional.alltypes.year", "functional.alltypestiny.year"),
        ImmutableList.of("functional.alltypes.id", "functional.alltypestiny.id"),
        ImmutableList.of("functional.alltypes.month"),
        ImmutableList.of("functional.alltypes.month"));

    testColumns("select a.month, sum(a.int_col) from functional.alltypes a, "
            + "functional.alltypestiny b where a.id = b.id and "
            + "a.year > 2000 and b.year > 2000 group by a.month order by a.month",
        fullColumns(ALLTYPES, Stream.of("month", "int_col")),
        ImmutableList.of("functional.alltypes.id", "functional.alltypes.year",
            "functional.alltypestiny.id", "functional.alltypestiny.year"),
        ImmutableList.of("functional.alltypes.id", "functional.alltypestiny.id"),
        ImmutableList.of("functional.alltypes.month"),
        ImmutableList.of("functional.alltypes.month"));
  }

  @Test
  public void testSelectJoinAcid() {
    // Test ACID table for DELETE EVENTS HASH JOIN
    testColumns("select a.month, sum(a.int_col) from functional.alltypes a, "
            + "functional_orc_def.alltypes_deleted_rows b where a.id = b.id and "
            + "a.year > 2000 and b.year > 2000 group by a.month order by a.month",
        fullColumns(ALLTYPES, Stream.of("month", "int_col")),
        ImmutableList.of("functional.alltypes.id", "functional.alltypes.year",
            "functional_orc_def.alltypes_deleted_rows.id",
            "functional_orc_def.alltypes_deleted_rows.year"),
        ImmutableList.of("functional.alltypes.id",
            "functional_orc_def.alltypes_deleted_rows.id",
            "functional_orc_def.alltypes_deleted_rows.month",
            "functional_orc_def.alltypes_deleted_rows.row__id",
            "functional_orc_def.alltypes_deleted_rows.year"),
        ImmutableList.of("functional.alltypes.month"),
        ImmutableList.of("functional.alltypes.month"));
  }

  @Test
  public void testSelectNestedLoopJoin() {
    // Test Nested Loop Join
    testColumns(
        "select DENSE_RANK() OVER (ORDER BY t1.day ASC) FROM functional.alltypesagg t1"
            + " WHERE EXISTS (SELECT t1.year AS int_col_1 FROM "
            + "functional.alltypesagg t1)",
        ImmutableList.of("functional.alltypesagg.day", "functional.alltypesagg.year"),
        null, null, null, null);

    testColumns("select count(*) from tpch_parquet.lineitem, tpch_parquet.orders where "
            + "l_orderkey = o_orderkey and (2 * log10(l_quantity) < 3 and "
            + "cast(l_returnflag as varchar(2)) = 'Y') or l_quantity >= 50",
        null,
        ImmutableList.of("tpch_parquet.lineitem.l_orderkey",
            "tpch_parquet.lineitem.l_quantity", "tpch_parquet.lineitem.l_returnflag",
            "tpch_parquet.orders.o_orderkey"),
        ImmutableList.of("tpch_parquet.lineitem.l_orderkey",
            "tpch_parquet.lineitem.l_quantity", "tpch_parquet.lineitem.l_returnflag",
            "tpch_parquet.orders.o_orderkey"),
        null, null);

    testColumns("select count(*) from functional.alltypestiny a inner join " +
        "functional.alltypessmall b on a.id < b.id " +
        "right outer join functional.alltypesagg c on a.int_col != c.int_col " +
        "right semi join functional.alltypes d on c.tinyint_col < d.tinyint_col " +
        "right anti join functional.alltypesnopart e on d.tinyint_col > e.tinyint_col " +
        "where e.id < 10", null,
        ImmutableList.of("functional.alltypesnopart.id"),
        ImmutableList.of("functional.alltypes.tinyint_col",
            "functional.alltypesagg.int_col", "functional.alltypesagg.tinyint_col",
            "functional.alltypesnopart.tinyint_col", "functional.alltypessmall.id",
            "functional.alltypestiny.id", "functional.alltypestiny.int_col"),
        null, null);
  }

  @Test
  public void testIcebergEqualityDeleteJoin() {
    // Test Iceberg EQUALITY-DELETE
    testColumns("select * from functional_parquet.iceberg_v2_delete_equality",
        ImmutableList.of("functional_parquet.iceberg_v2_delete_equality.data",
            "functional_parquet.iceberg_v2_delete_equality.id"),
        null,
        ImmutableList.of(
            "functional_parquet.iceberg_v2_delete_equality-EQUALITY-DELETE-01."
                + "iceberg__data__sequence__number",
            "functional_parquet.iceberg_v2_delete_equality-EQUALITY-DELETE-01.id",
            "functional_parquet.iceberg_v2_delete_equality."
                + "iceberg__data__sequence__number",
            "functional_parquet.iceberg_v2_delete_equality.id"),
        null, null);
  }

  @Test
  public void testGroupHaving() {
    // Test aggregation with HAVING clause
    testColumns("select c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, " +
        "sum(l_quantity) from tpch.customer, tpch.orders, tpch.lineitem where " +
        "o_orderkey in (select l_orderkey from tpch.lineitem group by l_orderkey " +
        "having sum(l_quantity) > 300) and c_custkey = o_custkey and " +
        "o_orderkey = l_orderkey group by c_name, c_custkey, o_orderkey, o_orderdate, " +
        "o_totalprice order by o_totalprice desc, o_orderdate, o_orderkey limit 100",
        ImmutableList.of("tpch.customer.c_custkey", "tpch.customer.c_name",
            "tpch.lineitem.l_orderkey", "tpch.lineitem.l_quantity",
            "tpch.orders.o_orderdate", "tpch.orders.o_orderkey",
            "tpch.orders.o_totalprice"),
        ImmutableList.of("tpch.customer.c_custkey", "tpch.lineitem.l_orderkey",
            "tpch.orders.o_custkey", "tpch.orders.o_orderkey"),
        ImmutableList.of("tpch.customer.c_custkey", "tpch.lineitem.l_orderkey",
            "tpch.orders.o_custkey", "tpch.orders.o_orderkey"),
        ImmutableList.of("tpch.customer.c_custkey", "tpch.customer.c_name",
            "tpch.lineitem.l_orderkey", "tpch.lineitem.l_quantity",
            "tpch.orders.o_orderdate", "tpch.orders.o_orderkey",
            "tpch.orders.o_totalprice"),
        ImmutableList.of("tpch.orders.o_orderdate", "tpch.orders.o_orderkey",
            "tpch.orders.o_totalprice"));
  }

  @Test
  public void testUpdate() {
    testColumns("UPDATE functional_parquet.iceberg_v2_delete_positional "
            + "SET `data` = concat(`data`,'a') where id = 15",
        ImmutableList.of("functional_parquet.iceberg_v2_delete_positional.data",
            "functional_parquet.iceberg_v2_delete_positional.file__position",
            "functional_parquet.iceberg_v2_delete_positional.id",
            "functional_parquet.iceberg_v2_delete_positional.input__file__name"),
        ImmutableList.of("functional_parquet.iceberg_v2_delete_positional.id"),
        ImmutableList.of(
            "functional_parquet.iceberg_v2_delete_positional-POSITION-DELETE-01."
                + "file_path",
            "functional_parquet.iceberg_v2_delete_positional-POSITION-DELETE-01.pos",
            "functional_parquet.iceberg_v2_delete_positional.file__position",
            "functional_parquet.iceberg_v2_delete_positional.input__file__name"),
        null, null);
  }

  @Test
  public void testDelete() {
    testColumns(
        "DELETE FROM functional_parquet.iceberg_v2_delete_positional where id = 15",
        ImmutableList.of("functional_parquet.iceberg_v2_delete_positional.file__position",
            "functional_parquet.iceberg_v2_delete_positional.input__file__name"),
        ImmutableList.of("functional_parquet.iceberg_v2_delete_positional.id"),
        ImmutableList.of(
            "functional_parquet.iceberg_v2_delete_positional-POSITION-DELETE-01."
                + "file_path",
            "functional_parquet.iceberg_v2_delete_positional-POSITION-DELETE-01.pos",
            "functional_parquet.iceberg_v2_delete_positional.file__position",
            "functional_parquet.iceberg_v2_delete_positional.input__file__name"),
        null, null);
  }
}
