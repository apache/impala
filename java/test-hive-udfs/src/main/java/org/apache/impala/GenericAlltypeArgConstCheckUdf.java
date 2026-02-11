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

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.
    PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.
    PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

// string GenericAlltypeArgConstCheckUdf(bool, tinyint, smallint, int, bigint, float,
//     double, string, binary)
// The returned string prints arguments and contains info on whether they are const.
public class GenericAlltypeArgConstCheckUdf extends GenericUDF {
  public GenericAlltypeArgConstCheckUdf() {
  }

  static final List<String> ARGS = Arrays.asList(
      "BOOLEAN", "BYTE", "SHORT", "INT", "LONG",
      "FLOAT", "DOUBLE", "STRING", "BINARY");

  boolean[] isConstArg_;
  Object[] constArgs_;
  PrimitiveObjectInspector[] inspectors_;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
    if (arguments.length != ARGS.size()) {
      throw new UDFArgumentException(
          "GenericAlltypeArgConstCheckUdf takes 9 arguments.");
    }
    isConstArg_ = new boolean[arguments.length];
    constArgs_ = new Object[arguments.length];
    inspectors_ = new PrimitiveObjectInspector[arguments.length];
    for (int i = 0; i < arguments.length; i++) {
      ObjectInspector oi = arguments[i];
      if (!(oi instanceof PrimitiveObjectInspector)) {
        throw new UDFArgumentException("Found an input that is not a primitive.");
      }
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
      if (poi.getPrimitiveCategory() != PrimitiveCategory.valueOf(ARGS.get(i))) {
        throw new UDFArgumentException("Incorrect input type");
      }
      inspectors_[i] = poi;
      isConstArg_[i] = oi instanceof ConstantObjectInspector;
      if (isConstArg_[i]) {
        ConstantObjectInspector coi = (ConstantObjectInspector) oi;
        constArgs_[i] = coi.getWritableConstantValue();
      }
    }
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments)
      throws HiveException {
    if (arguments.length != ARGS.size()) {
      throw new RuntimeException("Number of expected args did not match.");
    }
    StringBuilder sb = new StringBuilder();
    sb.append("Args: ");
    for (int i = 0; i < arguments.length; i++) {
      Object o = arguments[i].get();
      if (isConstArg_[i] && o != constArgs_[i]) {
        throw new RuntimeException("Got different object for const argument.");
      }
      sb.append(isConstArg_[i] ? "const " : "non-const ");
      if (o == null) {
         sb.append("null");
      } else {
        Object jo = inspectors_[i].getPrimitiveJavaObject(o);
        if (jo instanceof byte[]) {
          jo = new String((byte[])jo, StandardCharsets.UTF_8);
        }
        sb.append(jo.toString());
      }
      sb.append("; ");
    }
    Text resultString = new Text();
    resultString.set(sb.toString());
    return resultString;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "GenericAlltypeArgConstCheckUdf";
  }
}
