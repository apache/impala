// Copyright 2015 Cloudera Inc.
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

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.kududb.ColumnSchema;
import org.kududb.Schema;
import org.kududb.Type;
import org.kududb.client.CreateTableResponse;
import org.kududb.client.Insert;
import org.kududb.client.KuduClient;
import org.kududb.client.KuduSession;
import org.kududb.client.KuduTable;

import com.cloudera.impala.common.RuntimeEnv;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;

/**
 * This class contains all plan generation related tests with Kudu as a storage backend.
 */
public class KuduPlannerTest extends PlannerTestBase {


  @BeforeClass
  public static void setUp() throws Exception {
    // Use 8 cores for resource estimation.
    RuntimeEnv.INSTANCE.setNumCores(8);
    // Set test env to control the explain level.
    RuntimeEnv.INSTANCE.setTestEnv(true);

    KuduClient c = new KuduClient(Lists.newArrayList(
        HostAndPort.fromParts("127.0.0.1", 7051)));

    // Create the Schema
    List<org.kududb.ColumnSchema> cols = Lists.newArrayList(
        new ColumnSchema.ColumnSchemaBuilder("str_col", Type.STRING).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("match_like_col", Type.STRING).build(),
        new ColumnSchema.ColumnSchemaBuilder("no_match_like_col", Type.STRING).build(),
        new ColumnSchema.ColumnSchemaBuilder("match_regex_col", Type.STRING).build(),
        new ColumnSchema.ColumnSchemaBuilder("no_match_regex_col", Type.STRING).build());


    if (c.tableExists("liketbl")) {
      c.deleteTable("liketbl");
    }

    // Create the table
    CreateTableResponse res = c.createTable("liketbl", new Schema(cols));

    KuduTable table = c.openTable("liketbl");
    KuduSession session = c.newSession();
    // Load data
    for (int i = 0; i < 1000; ++i) {
      Insert insert = table.newInsert();
      insert.addString("str_col", String.valueOf(i));
      insert.addString("match_like_col", String.valueOf(i));
      insert.addString("no_match_like_col", String.valueOf(i));
      insert.addString("match_regex_col", String.valueOf(i));
      insert.addString("no_match_regex_col", String.valueOf(i));
      session.apply(insert);
    }
    session.flush();
  }

  @Test
  public void testKudu() {
    runPlannerTestFile("kudu");
  }

}
