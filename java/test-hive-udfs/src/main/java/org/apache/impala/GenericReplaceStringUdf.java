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
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

public class GenericReplaceStringUdf extends GenericUDF {
  public GenericReplaceStringUdf() {
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentException("GenericReplaceStringUdf takes one argument.");
    }
    if (!(arguments[0] instanceof PrimitiveObjectInspector)) {
      throw new UDFArgumentException("Found an input that is not a primitive.");
    }
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector) arguments[0];
    if (poi.getPrimitiveCategory() != PrimitiveCategory.STRING) {
      throw new UDFArgumentException("GenericReplaceStringUdf needs one STRING arg.");
    }
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments)
      throws HiveException {
    if (arguments.length != 1) {
      throw new RuntimeException("Number of expected args did not match.");
    }
    DeferredObject input = arguments[0];
    if (input == null) {
      return new Text("");
    }

    if (!(input.get() instanceof Text)) {
      throw new RuntimeException("Expected String but got " + input.get().getClass());
    }
    String currentString = ((Text) input.get()).toString();
    Text resultString = new Text();
    resultString.set(currentString.replace("s", "ss"));
    return resultString;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "GenericReplaceStringUdf";
  }
}
