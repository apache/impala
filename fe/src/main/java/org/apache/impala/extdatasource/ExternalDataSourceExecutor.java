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

package org.apache.impala.extdatasource;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.InternalException;
import org.apache.impala.common.JniUtil;
import org.apache.impala.extdatasource.thrift.TCloseParams;
import org.apache.impala.extdatasource.thrift.TCloseResult;
import org.apache.impala.extdatasource.thrift.TGetNextParams;
import org.apache.impala.extdatasource.thrift.TGetNextResult;
import org.apache.impala.extdatasource.thrift.TOpenParams;
import org.apache.impala.extdatasource.thrift.TOpenResult;
import org.apache.impala.extdatasource.thrift.TPrepareParams;
import org.apache.impala.extdatasource.thrift.TPrepareResult;
import org.apache.impala.extdatasource.v1.ExternalDataSource;
import org.apache.impala.thrift.TErrorCode;
import org.apache.impala.thrift.TStatus;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Wraps and executes an ExternalDataSource specified in an external jar. Used
 * in planning to call prepare() and in the backend to fetch results. The
 * executor takes the API version and abstracts the versioning from the caller,
 * e.g. calling the correct API interface and massaging any parameters that can
 * be handled here. There are thrift structures for all param and return types
 * representing the necessary structures. If future versions of the API are
 * added, the executor should be updated to call the appropriate API and handle
 * any differences. It is assumed that the API is updated in a way that
 * backwards compatibility is possible.
 */
public class ExternalDataSourceExecutor {
  private final static Logger LOG = LoggerFactory.getLogger(
      ExternalDataSourceExecutor.class);
  private final static TBinaryProtocol.Factory protocolFactory_ =
      new TBinaryProtocol.Factory();

  // Init string prefix used to indicate if the class should be cached. When this
  // is specified, the Class is loaded and initialized at most once. Instances of
  // the cached Class are still created for every query.
  private final static String CACHE_CLASS_PREFIX = "CACHE_CLASS::";

  // Map of class name to cached ExternalDataSource classes.
  // Protected by cachedClassesLock_.
  private final static Map<String, Class<?>> cachedClasses_ =
      Maps.newHashMap();

  // Number of cache hits/misses in cachedClasses_. Protected by cachedClassesLock_.
  private static long numClassCacheHits_ = 0;
  private static long numClassCacheMisses_ = 0;

  // Protects cachedClasses_, numClassCacheHits_, and numClassCacheMisses_.
  private final static Object cachedClassesLock_ = new Object();

  // setup by ctor() and cleared by release()
  private URLClassLoader classLoader_;

  private final ApiVersion apiVersion_;
  private final ExternalDataSource dataSource_;
  private final String jarPath_;
  private final String className_;
  private final String initString_;

  public static long getNumClassCacheHits() {
    synchronized (cachedClassesLock_) {
      return numClassCacheHits_;
    }
  }

  public static long getNumClassCacheMisses() {
    synchronized (cachedClassesLock_) {
      return numClassCacheMisses_;
    }
  }

  /**
   * @param jarPath The local path to the jar containing the ExternalDataSource.
   *     It is null or empty if the jar file of data source is already in classpath.
   * @param className The name of the class implementing the ExternalDataSource.
   * @param apiVersionStr The API version the ExternalDataSource implements.
   *     Must be a valid value of {@link ApiVersion}.
   * @param initString The init string registered with this data source.
   */
  public ExternalDataSourceExecutor(String jarPath, String className,
      String apiVersionStr, String initString) throws ImpalaException {
    apiVersion_ = ApiVersion.valueOf(apiVersionStr);
    if (apiVersion_ == null) {
      throw new ImpalaRuntimeException("Invalid API version: " + apiVersionStr);
    }
    jarPath_ = jarPath;
    className_ = className;
    initString_ = initString;

    try {
      Class<?> c = getDataSourceClass();
      Constructor<?> ctor = c.getConstructor();
      dataSource_ = (ExternalDataSource) ctor.newInstance();
    } catch (Exception ex) {
      throw new ImpalaRuntimeException(String.format("Unable to load external data " +
          "source library from path=%s className=%s apiVersion=%s",
          jarPath != null ? jarPath : "Impala classpath", className, apiVersionStr), ex);
    }
  }

  /**
   * Returns the ExternalDataSource class, loading the jar if necessary. The
   * class is cached if initString_ starts with CACHE_CLASS_PREFIX.
   */
  private Class<?> getDataSourceClass() throws Exception {
    Class<?> c = null;
    if (Strings.isNullOrEmpty(jarPath_)) {
      c = Class.forName(className_);
      LOG.trace("Get instance of DataSourceClass in current ClassLoader");
      return c;
    }
    // Cache map key needs to contain both the class name and init string in case
    // the same class is used for multiple tables where some are cached and others
    // are not.
    String cacheMapKey = String.format("%s.%s", className_, initString_);
    synchronized (cachedClassesLock_) {
      c = cachedClasses_.get(cacheMapKey);
      if (c == null) {
        URL url = new File(jarPath_).toURI().toURL();
        URLClassLoader loader = URLClassLoader.newInstance( new URL[] { url },
            getClass().getClassLoader());
        c = Class.forName(className_, true, loader);
        if (!ArrayUtils.contains(c.getInterfaces(), apiVersion_.getApiInterface())) {
          throw new ImpalaRuntimeException(String.format(
              "Class '%s' does not implement interface '%s' required for API version %s",
              className_, apiVersion_.getApiInterface().getName(), apiVersion_.name()));
        }
        // Only cache the class if the init string starts with CACHE_CLASS_PREFIX
        if (initString_ != null && initString_.startsWith(CACHE_CLASS_PREFIX)) {
          cachedClasses_.put(cacheMapKey, c);
        } else {
          classLoader_ = loader;
        }
        if (LOG.isTraceEnabled()) {
          LOG.trace("Loaded jar for class {} at path {}", className_, jarPath_);
        }
        numClassCacheMisses_++;
      } else {
        numClassCacheHits_++;
      }
    }
    return c;
  }

  @Override
  protected void finalize() throws Throwable {
    release();
    super.finalize();
  }

  /**
   * Release the class loader we have created if the class is not cached.
   */
  public void release() {
    if (classLoader_ != null) {
      try {
        classLoader_.close();
      } catch (IOException e) {
        // Log and ignore.
        LOG.warn("Error closing the URLClassloader.", e);
      }
      classLoader_ = null;
    }
  }

  public byte[] prepare(byte[] thriftParams) throws ImpalaException {
    TPrepareParams params = new TPrepareParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftParams);
    TPrepareResult result = prepare(params);
    try {
      return new TSerializer(protocolFactory_).serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage(), e);
    }
  }

  public byte[] open(byte[] thriftParams) throws ImpalaException {
    TOpenParams params = new TOpenParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftParams);
    TOpenResult result = open(params);
    try {
      return new TSerializer(protocolFactory_).serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage(), e);
    }
  }

  public byte[] getNext(byte[] thriftParams) throws ImpalaException {
    TGetNextParams params = new TGetNextParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftParams);
    TGetNextResult result = getNext(params);
    try {
      return new TSerializer(protocolFactory_).serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage(), e);
    }
  }

  public byte[] close(byte[] thriftParams) throws ImpalaException {
    TCloseParams params = new TCloseParams();
    JniUtil.deserializeThrift(protocolFactory_, params, thriftParams);
    TCloseResult result = close(params);
    try {
      return new TSerializer(protocolFactory_).serialize(result);
    } catch (TException e) {
      throw new InternalException(e.getMessage(), e);
    }
  }

  // Helper method to log the exception to capture the stack and return an error TStatus
  private TStatus logAndMakeErrorStatus(String opName, Exception e) {
    String exceptionMessage = e.getMessage();
    if (exceptionMessage == null) {
      exceptionMessage = "No error message returned by data source. Check the " +
          "impalad log for more information.";
    }
    String errorMessage = String.format(
        "Error in data source (path=%s, class=%s, version=%s) %s: %s",
        jarPath_ != null ? jarPath_ : "Impala classpath", className_, apiVersion_.name(),
        opName, exceptionMessage);
    LOG.error(errorMessage, e); // Logs the stack
    return new TStatus(TErrorCode.RUNTIME_ERROR, Lists.newArrayList(errorMessage));
  }

  public TPrepareResult prepare(TPrepareParams params) {
    try {
      TPrepareResult result = dataSource_.prepare(params);
      result.validate();
      return result;
    } catch (Exception e) {
      return new TPrepareResult(logAndMakeErrorStatus("prepare()", e));
    }
  }

  public TOpenResult open(TOpenParams params) {
    try {
      TOpenResult result = dataSource_.open(params);
      result.validate();
      return result;
    } catch (Exception e) {
      return new TOpenResult(logAndMakeErrorStatus("open()", e));
    }
  }

  public TGetNextResult getNext(TGetNextParams params) {
    try {
      TGetNextResult result = dataSource_.getNext(params);
      result.validate();
      return result;
    } catch (Exception e) {
      return new TGetNextResult(logAndMakeErrorStatus("getNext()", e));
    }
  }

  public TCloseResult close(TCloseParams params) {
    try {
      TCloseResult result = dataSource_.close(params);
      result.validate();
      return result;
    } catch (Exception e) {
      return new TCloseResult(logAndMakeErrorStatus("close()", e));
    }
  }
}
