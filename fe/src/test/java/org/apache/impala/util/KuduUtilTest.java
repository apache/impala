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

package org.apache.impala.util;
import static org.junit.Assert.*;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.List;

import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TBackendGflags;
import org.apache.kudu.client.KuduClient;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for KuduUtil functions.
 */
public class KuduUtilTest {
  private static TBackendGflags origFlags;

  @BeforeClass
  public static void setup() {
    // The original BackendConfig need to be mocked, we are saving the values here, so
    // they can be restored and not break other tests
    if (BackendConfig.INSTANCE == null) {
      BackendConfig.create(new TBackendGflags());
    }
    origFlags = BackendConfig.INSTANCE.getBackendCfg();
  }

  @AfterClass
  public static void teardown() {
    BackendConfig.create(origFlags);
  }

  @Test
  public void testGetKuduClient() {
    int size = KuduUtil.getkuduClientsSize();
    int concurrent = 5;
    CountDownLatch latch = new CountDownLatch(concurrent);
    List<KuduClient> clients = new CopyOnWriteArrayList<>();
    for (int i = 0; i < concurrent; i++) {
      new Thread() {
        public void run() {
          KuduClient client = KuduUtil.getKuduClient("master0");
          clients.add(client);
          latch.countDown();
        }
      }.start();
    }
    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    for (int i = 1; i < concurrent; i++) {
      assertSame(clients.get(0), clients.get(i));
    }
    KuduUtil.getKuduClient("master1");
    assertEquals(size + 2, KuduUtil.getkuduClientsSize());
  }
}