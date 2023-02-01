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

package org.apache.impala;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

/**
 * This class is the generic version of BufferAlteringUdf. The main purpose is to check
 * data retention capability after IMPALA-11854.
 */
public class GenericBufferAlteringUdf extends GenericUDF {
  public static final String ARGUMENT_LIST_LENGTH_FORMAT =
      "This function takes 1 argument, %d argument(s) provided";
  private PrimitiveCategory argAndRetType_;

  @Override
  public ObjectInspector initialize(ObjectInspector[] objectInspectors)
      throws UDFArgumentException {
    if (objectInspectors.length != 1) {
      throw new UDFArgumentException(
          String.format(ARGUMENT_LIST_LENGTH_FORMAT, objectInspectors.length));
    }
    if (!(objectInspectors[0] instanceof PrimitiveObjectInspector)) {
      throw new UDFArgumentException("Found an input that is not a primitive.");
    }
    PrimitiveObjectInspector objectInspector =
        (PrimitiveObjectInspector) objectInspectors[0];
    argAndRetType_ = objectInspector.getPrimitiveCategory();

    // Return type is same as the input parameter
    return objectInspector;
  }

  /**
   * This function expects a Text or a BytesWritable and increments their underlying byte
   * array's first element by one.
   */
  @Override
  public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
    if (deferredObjects.length != 1) {
      throw new UDFArgumentException(
          String.format(ARGUMENT_LIST_LENGTH_FORMAT, deferredObjects.length));
    }
    DeferredObject argument = deferredObjects[0];
    if (argument.get() == null) {
      return null;
    }
    Object object = argument.get();

    switch (argAndRetType_) {
      case STRING: {
        if (!(object instanceof Text)) {
          throw new HiveException("Expected Text but got " + object.getClass());
        }
        Text text = (Text) object;
        byte[] bytes = text.getBytes();
        incrementByteArray(bytes);
        return text;
      }
      case BINARY: {
        if (!(object instanceof BytesWritable)) {
          throw new HiveException("Expected BytesWritable but got " + object.getClass());
        }
        BytesWritable bytesWritable = (BytesWritable) object;
        byte[] bytes = bytesWritable.getBytes();
        incrementByteArray(bytes);
        return bytesWritable;
      }
      default: throw new IllegalStateException("Unexpected type: " + argAndRetType_);
    }
  }

  @Override
  public String getDisplayString(String[] strings) {
    return "GenericBufferAltering";
  }

  private void incrementByteArray(byte[] array) {
    if (array.length > 0) {
      array[0] += 1;
    }
  }
}
