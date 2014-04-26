// Copyright 2014 Cloudera Inc.
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

package com.cloudera.impala.extdatasource;

import java.io.File;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;

import org.apache.commons.lang.ArrayUtils;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.common.ImpalaRuntimeException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.common.JniUtil;
import com.cloudera.impala.extdatasource.thrift.TCloseParams;
import com.cloudera.impala.extdatasource.thrift.TCloseResult;
import com.cloudera.impala.extdatasource.thrift.TGetNextParams;
import com.cloudera.impala.extdatasource.thrift.TGetNextResult;
import com.cloudera.impala.extdatasource.thrift.TOpenParams;
import com.cloudera.impala.extdatasource.thrift.TOpenResult;
import com.cloudera.impala.extdatasource.thrift.TPrepareParams;
import com.cloudera.impala.extdatasource.thrift.TPrepareResult;
import com.cloudera.impala.extdatasource.v1.ExternalDataSource;
import com.cloudera.impala.thrift.TStatus;
import com.cloudera.impala.thrift.TStatusCode;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

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

  private final ApiVersion apiVersion_;
  private final ExternalDataSource dataSource_;
  private final String jarPath_;
  private final String className_;

  /**
   * @param jarPath The local path to the jar containing the ExternalDataSource.
   * @param className The name of the class implementing the ExternalDataSource.
   * @param apiVersionStr The API version the ExternalDataSource implements.
   *                         Must be a valid value of {@link ApiVersion}.
   */
  public ExternalDataSourceExecutor(String jarPath, String className,
      String apiVersionStr) throws ImpalaException {
    Preconditions.checkNotNull(jarPath);

    apiVersion_ = ApiVersion.valueOf(apiVersionStr);
    if (apiVersion_ == null) {
      throw new ImpalaRuntimeException("Invalid API version: " + apiVersionStr);
    }
    jarPath_ = jarPath;
    className_ = className;

    try {
      URL url = new File(jarPath).toURI().toURL();
      URLClassLoader loader = URLClassLoader.newInstance(
          new URL[] { url }, getClass().getClassLoader());
      Class<?> c = Class.forName(className, true, loader);
      if (!ArrayUtils.contains(c.getInterfaces(), apiVersion_.getApiInterface())) {
        throw new ImpalaRuntimeException(String.format(
            "Class '%s' does not implement interface '%s' required for API version %s",
            className, apiVersion_.getApiInterface().getName(), apiVersionStr));
      }
      Constructor<?> ctor = c.getConstructor();
      dataSource_ = (ExternalDataSource) ctor.newInstance();
    } catch (Exception ex) {
      throw new ImpalaRuntimeException(String.format("Unable to load external data " +
          "source library from path=%s className=%s apiVersion=%s", jarPath,
          className, apiVersionStr), ex);
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
        jarPath_, className_, apiVersion_.name(), opName,
        exceptionMessage);
    LOG.error(errorMessage, e); // Logs the stack
    return new TStatus(TStatusCode.RUNTIME_ERROR, Lists.newArrayList(errorMessage));
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
