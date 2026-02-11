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

import java.lang.reflect.Method;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.THiveUdfExecutorCtorParams;
import org.apache.impala.util.UnsafeUtil;

import com.google.common.base.Preconditions;

// Wrapper object to run hive UDFs. This class works with UdfCallExpr in the
// backend to marshall data back and forth between the execution engine and
// the java UDF class.
// See the comments in be/src/exprs/hive-udf-call.h for more details.
// TODO: should we cache loaded jars and classes?
@SuppressWarnings("restriction")
public class HiveUdfExecutorLegacy extends HiveUdfExecutor {
  // TODO UDF is deprecated in Hive and newer implementation of built-in functions using
  // GenericUDF interface, we should consider supporting GenericUDFs in the future
  private UDF udf_;

  // setup by init() and cleared by close()
  private Method method_;

  private final Object[] inputArgs_; // inputArgs_[i] is either inputObjects_[i] or null

  /**
   * Create a HiveUdfExecutorLegacy, using parameters from a serialized thrift object.
   * Used by the backend.
   */
  public HiveUdfExecutorLegacy(THiveUdfExecutorCtorParams request,
      HiveLegacyJavaFunction hiveJavaFn, HiveUdfInputHandler inputHandler)
      throws ImpalaRuntimeException {
    super(request, JavaUdfDataType.getType(hiveJavaFn.getRetType()), inputHandler);
    udf_ = hiveJavaFn.getUDFInstance();
    method_ = hiveJavaFn.getMethod();
    inputArgs_ = new Object[getNumParams()];
  }

  /**
   * Releases any resources allocated off the native heap and close the class
   * loader we may have created.
   */
  @Override
  public void closeDerived() {
    method_ = null;
  }

  /**
   * Evalutes the UDF with 'args' as the input to the UDF.
   * Returns Object returned by UDF.
   */
  @Override
  protected Object evaluateDerived(Object[] inputObjects)
      throws ImpalaRuntimeException {
    try {
      inputHandler_.fillArgArray(inputArgs_, inputObjects, false);
      return method_.invoke(udf_, inputArgs_);
    } catch (Exception e) {
      e.printStackTrace(System.err);
      throw new ImpalaRuntimeException("UDF::evaluate() ran into a problem.", e);
    }
  }

  @Override
  public Method getMethod() { return method_; }
}
