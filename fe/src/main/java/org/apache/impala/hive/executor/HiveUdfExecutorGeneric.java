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

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.impala.common.ImpalaRuntimeException;
import org.apache.impala.thrift.THiveUdfExecutorCtorParams;
import org.apache.impala.util.UnsafeUtil;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

/**
 * Wrapper object to run hive GenericUDFs. This class works with UdfCallExpr in the
 * backend to marshall data back and forth between the execution engine and
 * the java UDF class.
 * See the comments in be/src/exprs/hive-udf-call.h for more details.
 */
public class HiveUdfExecutorGeneric extends HiveUdfExecutor {
  private static final Logger LOG = Logger.getLogger(HiveUdfExecutorGeneric.class);

  private GenericUDF genericUDF_;

  // Hive Generic UDFs expect a DeferredObject for each parameter passed in. However,
  // if the ith parameter is NULL, then Hive expects the deferredObject[i].get() to
  // return null. The deferredParameters array is populated at initialization time. The
  // runtimeDeferredParameters_ is passed into the Hive Generic UDF code at runtime and
  // the runtimeDeferredParameters_[i] value will either contain deferredParameters_[i]
  // or a shared empty DeferredObject deferredNullParameter_.
  private DeferredObject[] deferredParameters_;
  private DeferredObject deferredNullParameter_;
  private DeferredObject[] runtimeDeferredParameters_;

  /**
   * Create a UdfExecutor, using parameters from a serialized thrift object. Used by
   * the backend.
   */
  public HiveUdfExecutorGeneric(THiveUdfExecutorCtorParams request,
      HiveGenericJavaFunction hiveJavaFn) throws ImpalaRuntimeException {
    super(request, JavaUdfDataType.getType(hiveJavaFn.getReturnObjectInspector()),
        JavaUdfDataType.getTypes(hiveJavaFn.getParameterTypes()));
    genericUDF_ = hiveJavaFn.getGenericUDFInstance();
    deferredParameters_ = createDeferredObjects();
    deferredNullParameter_ = new DeferredJavaObject(null);
    runtimeDeferredParameters_ = new DeferredObject[getNumParams()];
  }

  @Override
  public void closeDerived() {
  }

  /**
   * Evaluates the UDF with 'args' as the input to the UDF.
   */
  @Override
  protected Object evaluateDerived(JavaUdfDataType[] argTypes,
      long inputNullsPtr, Object[] inputObjectArgs) throws ImpalaRuntimeException {
    try {
      for (int i = 0; i < runtimeDeferredParameters_.length; ++i) {
        if (UnsafeUtil.UNSAFE.getByte(inputNullsPtr + i) == 0) {
          runtimeDeferredParameters_[i] = deferredParameters_[i];
          // argument 'i' is unused in DeferredJavaObject and in DeferredWritable as well
          runtimeDeferredParameters_[i].prepare(0);
        } else {
          runtimeDeferredParameters_[i] = deferredNullParameter_;
        }
      }
      return genericUDF_.evaluate(runtimeDeferredParameters_);
    } catch (HiveException e) {
      throw new ImpalaRuntimeException("UDF failed to evaluate", e);
    } catch (IllegalArgumentException e) {
      throw new ImpalaRuntimeException("UDF failed to evaluate", e);
    }
  }

  /**
   * Special method for unit testing. In the Java unit tests, the arguments
   * will change on every iteration, forcing us to create new a DeferredJavaObject
   * each time. In the python E2E tests, the normal pathway will be tested.
   */
  @Override
  public long evaluateForTesting(Object... args) throws ImpalaRuntimeException {
    Preconditions.checkState(args.length == deferredParameters_.length);
    for (int i = 0; i < deferredParameters_.length; ++i) {
      deferredParameters_[i] = new DeferredJavaObject(args[i]);
    }
    return evaluate();
  }

  @Override
  public Method getMethod() {
    return null;
  }

  private DeferredObject[] createDeferredObjects() {
    DeferredObject[] deferredObjects = new DeferredObject[getNumParams()];
    for (int i = 0; i < deferredObjects.length; ++i) {
      Object inputObject = getInputObject(i);
      if (inputObject instanceof Reloadable) {
        deferredObjects[i] = new DeferredWritable<>((Reloadable) inputObject);
      } else {
        deferredObjects[i] = new DeferredJavaObject(inputObject);
      }
    }
    return deferredObjects;
  }

  private static class DeferredWritable<T extends Reloadable> implements DeferredObject {
    private final T writable;

    public DeferredWritable(T writable) { this.writable = writable; }

    @Override
    public void prepare(int ignored) throws HiveException {
      writable.reload();
    }

    @Override
    public Object get() throws HiveException {
      return writable;
    }
  }
}
