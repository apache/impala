/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// We use the org.apache.iceberg.rest package because some classes
// are package-private. This means this code is more likely to
// break on Iceberg version updates. On the long-term we might
// switch to an open-source Iceberg REST Catalog.
package org.apache.iceberg.rest;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IcebergRestCatalogTest {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergRestCatalogTest.class);
  private static final ObjectMapper MAPPER = RESTObjectMapper.mapper();

  static final int REST_PORT = 9084;

  private Server httpServer;

  public IcebergRestCatalogTest() {}

  private static String getWarehouseLocation() {
    String FILESYSTEM_PREFIX = System.getenv("FILESYSTEM_PREFIX");
    String HADOOP_CATALOG_LOCATION = "/test-warehouse/iceberg_test/hadoop_catalog";
    if (FILESYSTEM_PREFIX != null && !FILESYSTEM_PREFIX.isEmpty()) {
      return FILESYSTEM_PREFIX + HADOOP_CATALOG_LOCATION;
    }
    String DEFAULT_FS = System.getenv("DEFAULT_FS");
    return DEFAULT_FS + HADOOP_CATALOG_LOCATION;
  }

  private Catalog initializeBackendCatalog() throws IOException {
    HdfsConfiguration conf = new HdfsConfiguration();
    return new HadoopCatalog(conf, getWarehouseLocation());
  }

  public void start(boolean join) throws Exception {
    Catalog catalog = initializeBackendCatalog();
    RESTCatalogAdapter adapter = new RESTCatalogAdapter(catalog) {
      @Override
      public <T extends RESTResponse> T execute(
          RESTCatalogAdapter.HTTPMethod method,
          String path,
          Map<String, String> queryParams,
          Object body,
          Class<T> responseType,
          Map<String, String> headers,
          Consumer<ErrorResponse> errorHandler) {
        Object request = roundTripSerialize(body, "request");
        T response =
            super.execute(
                method, path, queryParams, request, responseType, headers, errorHandler);
        T responseAfterSerialization = roundTripSerialize(response, "response");
        return responseAfterSerialization;
      }
    };

    RESTCatalogServlet servlet = new RESTCatalogServlet(adapter);
    ServletContextHandler context = new ServletContextHandler(
        ServletContextHandler.NO_SESSIONS);
    ServletHolder servletHolder = new ServletHolder(servlet);
    context.addServlet(servletHolder, "/*");
    context.insertHandler(new GzipHandler());

    this.httpServer = new Server(REST_PORT);
    httpServer.setHandler(context);
    httpServer.start();

    if (join) {
      httpServer.join();
    }
  }

  public void stop() throws Exception {
    if (httpServer != null) {
      httpServer.stop();
    }
  }

  public static void main(String[] args) throws Exception {
    new IcebergRestCatalogTest().start(true);
  }

  public static <T> T roundTripSerialize(T payload, String description) {
   if (payload != null) {
     LOG.trace(payload.toString());
     try {
       if (payload instanceof RESTMessage) {
         return (T) MAPPER.readValue(
             MAPPER.writeValueAsString(payload), payload.getClass());
       } else {
         // use Map so that Jackson doesn't try to instantiate ImmutableMap
         // from payload.getClass()
         return (T) MAPPER.readValue(
             MAPPER.writeValueAsString(payload), Map.class);
       }
     } catch (Exception e) {
       LOG.warn(e.toString());
       throw new RuntimeException(
          String.format("Failed to serialize and deserialize %s: %s",
              description, payload), e);
     }
   }
   return null;
 }
}
