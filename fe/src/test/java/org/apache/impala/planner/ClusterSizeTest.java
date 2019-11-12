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

import org.apache.impala.common.FrontendTestBase;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.testutil.TestUtils;
import org.apache.impala.thrift.TQueryCtx;
import org.apache.impala.thrift.TUpdateExecutorMembershipRequest;
import org.apache.impala.util.ExecutorMembershipSnapshot;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;
import com.google.common.collect.Sets;

/**
 * Unit tests to make sure that the planner handles changes to the cluster size correctly.
 */
public class ClusterSizeTest extends FrontendTestBase {

  /**
   * Retrieves the explain string for query statement 'stmt' from the frontend.
   */
  private String getExplainString(String stmt) {
    String ret = "";
    try {
      TQueryCtx queryCtx = TestUtils.createQueryContext(
          "functional", System.getProperty("user.name"));
      queryCtx.client_request.setStmt(stmt);
      ret = frontend_.getExplainString(queryCtx);
    } catch (ImpalaException e) {
      fail(e.getMessage());
    }
    return ret;
  }

  /**
   * Sends an update to the ExecutorMembershipSnapshot containing the specified number of
   * executors. The host list will only contain localhost.
   */
  private void setNumExecutors(int num) {
    TUpdateExecutorMembershipRequest updateReq = new TUpdateExecutorMembershipRequest();
    updateReq.setIp_addresses(Sets.newHashSet("127.0.0.1"));
    updateReq.setHostnames(Sets.newHashSet("localhost"));
    updateReq.setNum_executors(num);
    ExecutorMembershipSnapshot.update(updateReq);
  }

  /**
   * IMPALA-9151: Tests that the planner selects the correct join strategy based on the
   * number of executors in the cluster.
   */
  @Test
  public void testChangeClusterSize() {
    final String query = "select * from alltypes a inner join alltypes b on a.id = b.id";
    final String broadcast_exchange = ":EXCHANGE [BROADCAST]";
    final String hash_exchange = ":EXCHANGE [HASH(b.id)]";

    // By default no executors are registered and the planner falls back to the value of
    // -num_expected_executors, which is 20 by default.
    assertTrue(getExplainString(query).contains(hash_exchange));

    // Adding a single executor will make the planner switch to a broadcast join.
    setNumExecutors(1);
    assertTrue(getExplainString(query).contains(broadcast_exchange));

    // Adding two or more executors will make the planner switch to a partitioned hash
    // join.
    for (int n = 2; n < 5; ++n) {
      setNumExecutors(n);
      assertTrue(getExplainString(query).contains(hash_exchange));
    }

    // If the backend reports a single executor, the planner should fall back to a
    // broadcast join.
    setNumExecutors(1);
    assertTrue(getExplainString(query).contains(broadcast_exchange));
  }
}
