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

package org.apache.impala.catalog.metastore;

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.hadoop.hive.metastore.TServerSocketKeepAlive;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.CatalogServiceCatalog;
import org.apache.impala.common.Metrics;
import org.apache.impala.service.BackendConfig;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.ServerContext;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServerEventHandler;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CatalogMetastoreServer implements ThriftHiveMetastore interface. This is useful to
 * expose HMS APIs via catalog server. Currently, most of the APIs implementations are
 * "pass-through" to the Metastore server except for the following 3 which are mostly
 * useful for getting table and partition level metadata during query planning.
 * 1. get_table_req
 * 2. get_partitions_by_expr
 * 3. get_partitions_by_names_req
 *
 * This class mostly deals with the thrift server instantiation and its lifecycle
 * management. The actual implementation of the HMS APIs is done in
 * {@link CatalogMetastoreServiceHandler} class.
 */
public class CatalogMetastoreServer extends ThriftHiveMetastore implements
    ICatalogMetastoreServer {

  private static final Logger LOG = LoggerFactory.getLogger(CatalogMetastoreServer.class);

  // Maximum number of bytes to read from transport for variable length fields
  // (strings, bytes). Also, used as a maximum number of elements to read for
  // containers (maps, lists etc) fields.
  private static final int MAX_MESSAGE_SIZE = 100 * 1024 * 1024;

  // Minimum number of thrift server threads (concurrent connections) which serve the
  // clients. // TODO make this configurable
  private static final int MIN_SERVER_THREADS = 1;
  // Maximum number of thrift server threads (concurrent connections) which serve the
  // clients. // TODO make this configurable. A connection which is beyond this limit
  // will be blocked until a server thread is closed.
  private static final int MAX_SERVER_THREADS = 500;

  private static final String ACTIVE_CONNECTIONS_METRIC = "metastore.active.connections";
  private static final String RPC_DURATION_FORMAT_METRIC = "metastore.rpc.duration.%s";

  // flag to indicate if the server is started or not
  private final AtomicBoolean started_ = new AtomicBoolean(false);

  // the server is started in a daemon thread so that instantiating this is not
  // a blocking call.
  private CompletableFuture<Void> serverHandle_;

  // reference to the catalog Service catalog object
  private final CatalogServiceCatalog catalog_;

  // Metrics for this Metastore server. Also this instance is passed to the
  // TServerEventHandler when the server is started so that RPC metrics can be registered.
  private final Metrics metrics_ = new Metrics();

  public CatalogMetastoreServer(CatalogServiceCatalog catalogServiceCatalog) {
    Preconditions.checkNotNull(catalogServiceCatalog);
    catalog_ = catalogServiceCatalog;
  }

  /**
   * Simple RpcEventHandler which adds metrics for this Metastore server
   */
  private class RpcMetricsEventHandler implements TServerEventHandler {

    @Override
    public void preServe() {}

    @Override
    public ServerContext createContext(TProtocol tProtocol, TProtocol tProtocol1) {
      metrics_.getCounter(ACTIVE_CONNECTIONS_METRIC).inc();
      return null;
    }

    @Override
    public void deleteContext(ServerContext serverContext, TProtocol tProtocol,
        TProtocol tProtocol1) {
      metrics_.getCounter(ACTIVE_CONNECTIONS_METRIC).dec();
    }

    @Override
    public void processContext(ServerContext serverContext, TTransport tTransport,
        TTransport tTransport1) {
    }
  }

  /**
   * Simple wrapper InvocationHandler which registers the duration metrics for each method
   * called on the Proxy instance. The method execution is delegated to the the handler
   * instance in the invoke method. Using such a invocation handler is much simpler than
   * wrapping all the methods in the {@link CatalogMetastoreServiceHandler}. Additionally,
   * this class also logs an error with the full trace in case the method invocation
   * fails.
   */
  private class TimingInvocationHandler implements InvocationHandler {

    private final CatalogMetastoreServiceHandler handler_;

    TimingInvocationHandler(CatalogMetastoreServiceHandler handler) {
      Preconditions.checkNotNull(handler);
      handler_ = handler;
    }

    /**
     * This method is called on every HMS API invocation. We invoke the method on the
     * handler class with the given set of arguments. Additionally, this class is used to
     * register the duration of such API calls.
     */
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      Timer.Context context =
          metrics_.getTimer(String.format(RPC_DURATION_FORMAT_METRIC, method.getName()))
              .time();
      try {
        LOG.debug("Invoking HMS API: {}", method.getName());
        return method.invoke(handler_, args);
      } catch (Exception ex) {
        Throwable unwrapped = unwrap(ex);
        LOG.error("Received exception while executing " + method.getName() + " : ",
            unwrapped);
        throw unwrapped;
      } finally {
        context.stop();
      }
    }

    /**
     * The InvocationHandler throws an InvocationTargetException if the underlying method
     * throws and exception. This method unwraps the underlying cause of such an exception
     * and returns it if available.
     */
    private Throwable unwrap(Exception ex) {
      if (ex instanceof InvocationTargetException) {
        return ((InvocationTargetException) ex).getTargetException();
      }
      return ex;
    }
  }

  @VisibleForTesting
  protected int getPort() throws CatalogException {
    return BackendConfig.INSTANCE.getHMSPort();
  }

  /**
   * Starts the thrift server in a background thread and the configured port. Currently,
   * only support NOSASL mode. TODO Add SASL and ssl support (IMPALA-10638)
   *
   * @throws CatalogException
   */
  public synchronized void start() throws CatalogException {
    final int portNumber = getPort();
    Preconditions.checkState(portNumber > 0);
    Preconditions.checkState(!started_.get(), "Metastore server is already started");
    LOG.info("Starting the Metastore server at port number {}", portNumber);
    CatalogMetastoreServiceHandler handler =
        new CatalogMetastoreServiceHandler(catalog_, metrics_,
            BackendConfig.INSTANCE.fallbackToHMSOnErrors());
    // create a proxy class for the ThriftMetastore.Iface and ICatalogMetastoreServer
    // so that all the APIs can be invoked via a TimingInvocationHandler
    ThriftHiveMetastore.Iface proxyCatalogHMSIFace =
        (ThriftHiveMetastore.Iface) Proxy
            .newProxyInstance(ThriftHiveMetastore.Iface.class.getClassLoader(),
                new Class[]{ThriftHiveMetastore.Iface.class,
                    ICatalogMetastoreServer.class},
                new TimingInvocationHandler(handler));
    //TODO Add Sasl support (IMPALA-10638)
    final TProtocolFactory protocolFactory;
    final TProtocolFactory inputProtoFactory;
    //TODO add config for this (IMPALA-10639)
    boolean useCompactProtocol = false;
    if (useCompactProtocol) {
      protocolFactory = new TCompactProtocol.Factory();
      inputProtoFactory = new TCompactProtocol.Factory(MAX_MESSAGE_SIZE,
          MAX_MESSAGE_SIZE);
    } else {
      protocolFactory = new TBinaryProtocol.Factory();
      inputProtoFactory = new TBinaryProtocol.Factory(true, true, MAX_MESSAGE_SIZE,
          MAX_MESSAGE_SIZE);
    }

    TProcessor processor;
    try {
      processor =
          new ThriftHiveMetastore.Processor<>(proxyCatalogHMSIFace);
    } catch (Exception e) {
      throw new CatalogException("Unable to create processor for catalog metastore "
          + "server", e);
    }

    //TODO add SSL support
    boolean useSSL = false;
    TServerSocket serverSocket;
    try {
      serverSocket =
          new TServerSocketKeepAlive(
              new TServerSocket(new InetSocketAddress(portNumber)));
    } catch (TTransportException e) {
      throw new CatalogException(
          "Unable to create server socket at port number " + portNumber, e);
    }

    TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverSocket)
        .processor(processor)
        .transportFactory(new TTransportFactory())
        .protocolFactory(protocolFactory)
        .inputProtocolFactory(inputProtoFactory)
        .minWorkerThreads(MIN_SERVER_THREADS)
        .maxWorkerThreads(MAX_SERVER_THREADS);

    TServer tServer = new TThreadPoolServer(args);
    TServerEventHandler rpcMetricsEventHandler = new RpcMetricsEventHandler();

    tServer.setServerEventHandler(rpcMetricsEventHandler);
    LOG.info("Started the new metaserver on port [" + portNumber
        + "]...");
    LOG.info("minWorkerThreads = "
        + MIN_SERVER_THREADS);
    LOG.info("maxWorkerThreads = "
        + MAX_SERVER_THREADS);
    LOG.info("Enable SSL = " + useSSL);
    serverHandle_ = CompletableFuture.runAsync(() -> tServer.serve());
    started_.set(true);
  }

  /**
   * Returns the RPC and connection metrics for this metastore server. //TODO hook this
   * method to the Catalog's debug UI
   */
  @Override
  public String getMetrics() {
    return metrics_.toString();
  }

  /**
   * Stops this CatalogMetastoreServer on a best-effort basis. May interrupt running
   * threads in the server.
   * <p>
   * // TODO currently this method is not used anywhere. We should hook this method to the
   * shutdown process of catalogd
   */
  public void stop() throws CatalogException {
    serverHandle_.cancel(true);
  }
}
