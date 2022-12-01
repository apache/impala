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

import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.WritableIntObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.List;

/**
 * Simple Generic UDFs for testing.
 *
 * This class overrides a few methods in  TestGenericUdf to return primitive
 * Java types instead of Hive's writable classes. Otherwise this class behaves
 * exactly the same way as TestGenericUdf. See TestGenericUdf for more information.
 *
 * Similarly to TestGenericUdf this class also has copy in java/test-hive-udfs.
 *
 */
public class TestGenericUdfWithJavaReturnTypes extends TestGenericUdf {

  public TestGenericUdfWithJavaReturnTypes() {
  }

  @Override
  protected PrimitiveObjectInspector getReturnObjectInspector(
        PrimitiveObjectInspector oi) {
    PrimitiveTypeInfo typeInfo = oi.getTypeInfo();
    return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(typeInfo);
  }


  @Override
  public String getDisplayString(String[] children) {
    return "TestGenericUdfWithJavaReturnTypes";
  }

  // The evaluate*Wrapped functions below simply return the results of
  // evaluate*.

  @Override
  protected Object evaluateBooleanWrapped(DeferredObject[] inputs)
      throws HiveException {
    return evaluateBoolean(inputs);
  }

  @Override
  protected Object evaluateByteWrapped(DeferredObject[] inputs)
      throws HiveException {
    return evaluateByte(inputs);
  }

  @Override
  protected Object evaluateShortWrapped(DeferredObject[] inputs)
     throws HiveException {
    return evaluateShort(inputs);
  }

  @Override
  protected Object evaluateIntWrapped(DeferredObject[] inputs)
      throws HiveException {
    return evaluateInt(inputs);
  }

  @Override
  protected Object evaluateLongWrapped(DeferredObject[] inputs)
      throws HiveException {
    return evaluateLong(inputs);
  }

  @Override
  protected Object evaluateFloatWrapped(DeferredObject[] inputs)
      throws HiveException {
    return evaluateFloat(inputs);
  }

  @Override
  protected Object evaluateDoubleWrapped(DeferredObject[] inputs)
      throws HiveException {
    return evaluateDouble(inputs);
  }

  @Override
  protected Object evaluateStringWrapped(DeferredObject[] inputs) throws HiveException {
    return evaluateString(inputs);
  }

  @Override
  protected Object evaluateBinaryWrapped(DeferredObject[] inputs)
      throws HiveException {
    return evaluateBinary(inputs);
  }

  @Override
  protected String getSignatureString(PrimitiveCategory argAndRetType_,
      List<PrimitiveCategory> inputTypes_) {
    return argAndRetType_ + "TestGenericUdfWithJavaReturnTypes(" +
        Joiner.on(",").join(inputTypes_) + ")";
  }
}
