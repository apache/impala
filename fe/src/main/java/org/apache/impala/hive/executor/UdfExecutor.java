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

package org.apache.impala.hive.executor;

import java.io.File;
import java.lang.reflect.Method;

import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.Type;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.common.JniUtil;
import org.apache.impala.thrift.THiveUdfExecutorCtorParams;
import org.apache.thrift.protocol.TBinaryProtocol;

import org.apache.log4j.Logger;

// Wrapper object to run hive UDFs. This class works with UdfCallExpr in the
// backend to marshall data back and forth between the execution engine and
// the java udf classes.
// See the comments in be/src/exprs/hive-udf-call.h for more details.
// TODO: should we cache loaded jars and classes?
@SuppressWarnings("restriction")
public class UdfExecutor implements AutoCloseable {
  private static final Logger LOG = Logger.getLogger(UdfExecutor.class);

  private final static TBinaryProtocol.Factory PROTOCOL_FACTORY =
    new TBinaryProtocol.Factory();

  private final HiveUdfExecutor hiveUdfExecutor_;

  private HiveUdfLoader udfLoader_;

  /**
   * Create a UdfExecutor, using parameters from a serialized thrift object. Used by
   * the backend.
   */
  public UdfExecutor(byte[] thriftParams) throws ImpalaException {
    THiveUdfExecutorCtorParams request = new THiveUdfExecutorCtorParams();
    JniUtil.deserializeThrift(PROTOCOL_FACTORY, request, thriftParams);
    String location = null;
    try {
      if (request.local_location != null) {
        location = new File(request.local_location).toURI().toString();
      }
    } catch (Exception e) {
      String errorMsg = "Could not load jar file " + request.local_location;
      throw new ImpalaRuntimeException(errorMsg, e);
    }
    try {
      checkValidRequest(request);
      udfLoader_ = new HiveUdfLoader(location, request.fn.scalar_fn.symbol);
      hiveUdfExecutor_ = createHiveUdfExecutor(request, udfLoader_);
      LOG.debug("Loaded UDF '" + request.fn.scalar_fn.symbol + "' from "
          + request.local_location);
    } catch (Exception e) {
      String errorMsg = "Could not load class " + request.fn.scalar_fn.symbol + " from "
          + "jar " + location + ": " + e.getMessage();
      throw new ImpalaRuntimeException(errorMsg, e);
    }
  }

  @Override
  public void close() {
    hiveUdfExecutor_.close();
    udfLoader_.close();
  }

  /**
   * evaluate function called by the backend. The inputs to the UDF have
   * been serialized to 'input'
   */
  public void evaluate() throws ImpalaRuntimeException {
    hiveUdfExecutor_.evaluate();
  }

  /**
   * Evalutes the UDF with 'args' as the input to the UDF. This is exposed
   * for testing and not the version of evaluate() the backend uses.
   */
  public long evaluateForTesting(Object... args) throws ImpalaRuntimeException {
    return hiveUdfExecutor_.evaluateForTesting(args);
  }

  public Method getMethod() {
    return hiveUdfExecutor_.getMethod();
  }

  /**
   * Finds the correct HiveUdfExecutor to use.
   */
  private HiveUdfExecutor createHiveUdfExecutor(THiveUdfExecutorCtorParams request,
      HiveUdfLoader udfLoader) throws ImpalaRuntimeException {
    try {
      switch (udfLoader.getUDFClassType()) {
        case UDF: {
            HiveLegacyJavaFunction function =
                new HiveLegacyJavaFunction(udfLoader.getUDFClass(),
                    HiveUdfExecutor.getRetType(request),
                    HiveUdfExecutor.getParameterTypes(request));
            return new HiveUdfExecutorLegacy(request, function);
          }
        case GENERIC_UDF: {
            HiveGenericJavaFunction function =
                new HiveGenericJavaFunction(udfLoader.getUDFClass(),
                    HiveUdfExecutor.getRetType(request),
                    HiveUdfExecutor.getParameterTypes(request));
            return new HiveUdfExecutorGeneric(request, function);
          }
        default:
          throw new ImpalaRuntimeException("The class " + request.fn.scalar_fn.symbol +
              " does not derive from a known supported Hive UDF class " +
              "(UDF or GenericUDF).");
      }
    } catch (CatalogException e) {
      throw new ImpalaRuntimeException(e.getMessage(), e);
    }
  }

  private void checkValidRequest(THiveUdfExecutorCtorParams request
      ) throws ImpalaRuntimeException {
    Type retType = Type.fromThrift(request.fn.ret_type);
    if (!JavaUdfDataType.isSupported(retType)) {
      throw new ImpalaRuntimeException("Unsupported return type: " + retType.toSql());
    }
  }
}
