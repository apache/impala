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

package org.apache.impala.testutil;

import java.net.ServerSocket;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.metastore.CatalogMetastoreServer;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.service.CatalogOpExecutor;

/**
 * Test Catalog metastore server which starts on a random port which is available.
 */
public class CatalogTestMetastoreServer extends CatalogMetastoreServer {

  private final int port_;

  public CatalogTestMetastoreServer(
      CatalogOpExecutor catalogOpExecutor) throws ImpalaException {
    super(catalogOpExecutor);
    try {
      port_ = getRandomPort();
    } catch (Exception e) {
      throw new CatalogException(e.getMessage());
    }
  }

  @Override
  public int getPort() {
    return port_;
  }

  private static int getRandomPort() throws Exception {
    for (int i = 0; i < 5; i++) {
      ServerSocket serverSocket = null;
      try {
        serverSocket = new ServerSocket(0);
        return serverSocket.getLocalPort();
      } finally {
        if (serverSocket != null) {
          serverSocket.close();
        }
      }
    }
    throw new Exception("Could not find a free port");
  }
}