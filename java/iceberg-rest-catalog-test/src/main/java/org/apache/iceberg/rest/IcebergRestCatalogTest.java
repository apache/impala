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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.function.Consumer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
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

  private static final int DEFAULT_REST_PORT = 9084;
  private static final String DEFAULT_HADOOP_CATALOG_LOCATION =
      "/test-warehouse/iceberg_test/hadoop_catalog";
  private static final String USAGE_PREFIX = "java -jar your-iceberg-rest-catalog.jar";

  private static final String CATALOG_LOCATION_LONGOPT = "catalog-location";
  private static final String PORT_LONGOPT = "port";
  private static final String HELP_LONGOPT = "help";

  private Server httpServer;
  private final int port;
  private final String warehouseLocation;

  public IcebergRestCatalogTest(int port, String warehouseLocation) {
    this.port = port;
    this.warehouseLocation = warehouseLocation;
  }

  private String getWarehouseLocation() {
    String filesystemPrefix = System.getenv("FILESYSTEM_PREFIX");
    if (filesystemPrefix != null && !filesystemPrefix.isEmpty()) {
      return filesystemPrefix + warehouseLocation;
    }
    String defaultFs = System.getenv("DEFAULT_FS");
    return defaultFs + warehouseLocation;
  }

  private Catalog initializeBackendCatalog() {
    Configuration conf = new Configuration();
    conf.set("io-impl", "org.apache.iceberg.hadoop.HadoopFileIO");
    String actualWarehouseLocation = getWarehouseLocation();
    LOG.info("Initializing Hadoop Catalog at: {}", actualWarehouseLocation);
    String defaultFs = conf.get("fs.defaultFS");
    LOG.info("Default filesystem configured for this Iceberg REST Catalog is {}",
        defaultFs);
    return new HadoopCatalog(conf, actualWarehouseLocation);
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
        return roundTripSerialize(response, "response");
      }
    };

    RESTCatalogServlet servlet = new RESTCatalogServlet(adapter);
    ServletContextHandler context = new ServletContextHandler(
        ServletContextHandler.NO_SESSIONS);
    ServletHolder servletHolder = new ServletHolder(servlet);
    context.addServlet(servletHolder, "/*");
    context.insertHandler(new GzipHandler());

    this.httpServer = new Server(port);
    httpServer.setHandler(context);
    httpServer.start();
    LOG.info("Iceberg REST Catalog started on port: {}", port);

    if (join) {
      httpServer.join();
    }
  }

  public void stop() throws Exception {
    if (httpServer != null) {
      httpServer.stop();
      LOG.info("Iceberg REST Catalog stopped.");
    }
  }

  public static void main(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(new Option(null, PORT_LONGOPT, true,
        "Port for the REST catalog server (default: " + DEFAULT_REST_PORT + ")"));

    options.addOption(new Option(null, CATALOG_LOCATION_LONGOPT, true,
        "Base location for the Hadoop catalog (default: "
            + DEFAULT_HADOOP_CATALOG_LOCATION + ")"));

    options.addOption(new Option(null, HELP_LONGOPT, false, "Display this help message"));

    CommandLineParser parser = new BasicParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd;

    int port = DEFAULT_REST_PORT;
    String catalogLocation = DEFAULT_HADOOP_CATALOG_LOCATION;

    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      LOG.error("Error: {}", e.getMessage());
      formatter.printHelp(USAGE_PREFIX, options);
      System.exit(1);
      return;
    }

    if (cmd.hasOption(HELP_LONGOPT)) {
      formatter.printHelp(USAGE_PREFIX, options);
      System.exit(0);
    }

    if (cmd.hasOption(PORT_LONGOPT)) {
      try {
        port = Integer.parseInt(cmd.getOptionValue(PORT_LONGOPT));
      } catch (NumberFormatException e) {
        LOG.error("Error: --port requires a valid integer value. Got: {}",
            cmd.getOptionValue(PORT_LONGOPT));
        formatter.printHelp(USAGE_PREFIX, options);
        System.exit(1);
      }
    }

    if (cmd.hasOption(CATALOG_LOCATION_LONGOPT)) {
      catalogLocation = cmd.getOptionValue(CATALOG_LOCATION_LONGOPT);
    }

    new IcebergRestCatalogTest(port, catalogLocation).start(true);
  }


  public static <T> T roundTripSerialize(T payload, String description) {
    if (payload != null) {
      if (LOG.isTraceEnabled()) {
        LOG.trace(payload.toString());
      }
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
