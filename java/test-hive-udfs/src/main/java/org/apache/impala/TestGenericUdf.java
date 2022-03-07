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
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Simple Generic UDFs for testing.
 *
 * Udf that takes a variable number of arguments of the same type and applies
 * the "+" operator to them. The "+" is a concatenation for string types. For
 * boolean types, it applies the OR operation. If only one argument is provided,
 * it returns that argument.
 *
 * This class is a copy of the TestGenericUdf class in the FE. We need this class in a
 * separate project so we can test loading UDF jars that are not already on the
 * classpath, and we can't delete the FE's class because UdfExecutorTest depends
 * on it.
 *
 * The jar for this file can be built by running "mvn clean package" in
 * tests/test-hive-udfs. This is run in testdata/bin/create-load-data.sh, and
 * copied to HDFS in testdata/bin/copy-udfs-uda.sh.
 *
 */
public class TestGenericUdf extends GenericUDF {

  private List<PrimitiveCategory> inputTypes_ = new ArrayList<>();
  private PrimitiveObjectInspector retTypeOI_;
  private PrimitiveCategory argAndRetType_;

  private static final Set SUPPORTED_ARG_TYPES =
      new ImmutableSet.Builder<PrimitiveCategory>()
          .add(PrimitiveCategory.BOOLEAN)
          .add(PrimitiveCategory.BYTE)
          .add(PrimitiveCategory.SHORT)
          .add(PrimitiveCategory.INT)
          .add(PrimitiveCategory.LONG)
          .add(PrimitiveCategory.FLOAT)
          .add(PrimitiveCategory.DOUBLE)
          .add(PrimitiveCategory.STRING)
          .build();

  public TestGenericUdf() {
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {

    if (arguments.length == 0) {
      throw new UDFArgumentException("No arguments provided.");
    }

    for (ObjectInspector oi : arguments) {
      if (!(oi instanceof PrimitiveObjectInspector)) {
        throw new UDFArgumentException("Found an input that is not a primitive.");
      }
      PrimitiveObjectInspector poi = (PrimitiveObjectInspector) oi;
      inputTypes_.add(poi.getPrimitiveCategory());
    }

    // return type is always same as last argument
    retTypeOI_ = (PrimitiveObjectInspector) arguments[0];

    argAndRetType_ = retTypeOI_.getPrimitiveCategory();

    verifyArgs(argAndRetType_, inputTypes_);
    return retTypeOI_;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments)
      throws HiveException {
    if (arguments.length != inputTypes_.size()) {
      throw new HiveException("Number of arguments passed in did not match number of " +
          "arguments expected.");
    }
    switch (argAndRetType_) {
      case BOOLEAN:
        return evaluateBoolean(arguments);
      case BYTE:
        return evaluateByte(arguments);
      case SHORT:
        return evaluateShort(arguments);
      case INT:
        return evaluateInt(arguments);
      case LONG:
        return evaluateLong(arguments);
      case FLOAT:
        return evaluateFloat(arguments);
      case DOUBLE:
        return evaluateDouble(arguments);
      case STRING:
        return evaluateString(arguments);
      case DATE:
      case TIMESTAMP:
      default:
        throw new HiveException("Unsupported argument type " + argAndRetType_);
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return "TestGenericUdf";
  }

  private void verifyArgs(PrimitiveCategory argAndRetType,
      List<PrimitiveCategory> inputTypes) throws UDFArgumentException {

    if (!SUPPORTED_ARG_TYPES.contains(argAndRetType)) {
      throw new UDFArgumentException("Unsupported argument type " + argAndRetType_);
    }

    for (PrimitiveCategory inputType : inputTypes) {
      if (inputType != argAndRetType) {
        throw new UDFArgumentException("Invalid function for " +
            getSignatureString(argAndRetType, inputTypes));
      }
    }
  }

  public BooleanWritable evaluateBoolean(DeferredObject[] inputs) throws HiveException {
    List<BooleanWritable> booleanInputs = new ArrayList<>();
    boolean finalBoolean = false;
    for (DeferredObject input : inputs) {
      if (input == null) {
        return null;
      }
      if (!(input.get() instanceof BooleanWritable)) {
        throw new HiveException("Expected BooleanWritable but got " + input.getClass());
      }
      boolean currentBool = ((BooleanWritable) input.get()).get();
      finalBoolean |= currentBool;
    }
    BooleanWritable resultBool = new BooleanWritable();
    resultBool.set(finalBoolean);
    return resultBool;
  }

  public ByteWritable evaluateByte(DeferredObject[] inputs) throws HiveException {
    List<ByteWritable> byteInputs = new ArrayList<>();
    byte finalByte = 0;
    for (DeferredObject input : inputs) {
      if (input == null) {
        return null;
      }
      if (!(input.get() instanceof ByteWritable)) {
        throw new HiveException("Expected ByteWritable but got " + input.getClass());
      }
      byte currentByte = ((ByteWritable) input.get()).get();
      finalByte += currentByte;
    }
    ByteWritable resultByte = new ByteWritable();
    resultByte.set(finalByte);
    return resultByte;
  }

  public ShortWritable evaluateShort(DeferredObject[] inputs) throws HiveException {
    List<ShortWritable> shortInputs = new ArrayList<>();
    short finalShort = 0;
    for (DeferredObject input : inputs) {
      if (input == null) {
        return null;
      }
      if (!(input.get() instanceof ShortWritable)) {
        throw new HiveException("Expected ShortWritable but got " + input.getClass());
      }
      short currentShort = ((ShortWritable) input.get()).get();
      finalShort += currentShort;
    }
    ShortWritable resultShort = new ShortWritable();
    resultShort.set(finalShort);
    return resultShort;
  }

  public IntWritable evaluateInt(DeferredObject[] inputs) throws HiveException {
    List<IntWritable> intInputs = new ArrayList<>();
    int finalInt = 0;
    for (DeferredObject input : inputs) {
      if (input == null) {
        return null;
      }
      if (!(input.get() instanceof IntWritable)) {
        throw new HiveException("Expected IntWritable but got " + input.getClass());
      }
      int currentInt = ((IntWritable) input.get()).get();
      finalInt += currentInt;
    }
    IntWritable resultInt = new IntWritable();
    resultInt.set(finalInt);
    return resultInt;
  }

  public LongWritable evaluateLong(DeferredObject[] inputs) throws HiveException {
    List<LongWritable> longInputs = new ArrayList<>();
    long finalLong = 0;
    for (DeferredObject input : inputs) {
      if (input == null) {
        return null;
      }
      if (!(input.get() instanceof LongWritable)) {
        throw new HiveException("Expected LongWritable but got " + input.getClass());
      }
      long currentLong = ((LongWritable) input.get()).get();
      finalLong += currentLong;
    }
    LongWritable resultLong = new LongWritable();
    resultLong.set(finalLong);
    return resultLong;
  }

  public FloatWritable evaluateFloat(DeferredObject[] inputs) throws HiveException {
    List<FloatWritable> floatInputs = new ArrayList<>();
    float finalFloat = 0.0F;
    for (DeferredObject input : inputs) {
      if (input == null) {
        return null;
      }
      if (!(input.get() instanceof FloatWritable)) {
        throw new HiveException("Expected FloatWritable but got " + input.getClass());
      }
      float currentFloat = ((FloatWritable) input.get()).get();
      finalFloat += currentFloat;
    }
    FloatWritable resultFloat = new FloatWritable();
    resultFloat.set(finalFloat);
    return resultFloat;
  }

  public DoubleWritable evaluateDouble(DeferredObject[] inputs) throws HiveException {
    List<DoubleWritable> doubleInputs = new ArrayList<>();
    double finalDouble = 0.0;
    for (DeferredObject input : inputs) {
      if (input == null) {
        return null;
      }
      if (!(input.get() instanceof DoubleWritable)) {
        throw new HiveException("Expected DoubleWritable but got " + input.getClass());
      }
      double currentDouble = ((DoubleWritable) input.get()).get();
      finalDouble  += currentDouble;
    }
    DoubleWritable resultDouble = new DoubleWritable();
    resultDouble.set(finalDouble);
    return resultDouble;
  }

  public Text evaluateString(DeferredObject[] inputs) throws HiveException {
    List<String> stringInputs = new ArrayList<>();
    String finalString = "";
    for (DeferredObject input : inputs) {
      if (input == null) {
        return null;
      }
      if (!(input.get() instanceof Text)) {
        throw new HiveException("Expected String but got " + input.get().getClass());
      }
      String currentString = ((Text) input.get()).toString();
      finalString += currentString;
    }
    Text resultString = new Text();
    resultString.set(finalString);
    return resultString;
  }

  private String getSignatureString(PrimitiveCategory argAndRetType_,
      List<PrimitiveCategory> inputTypes_) {
    return argAndRetType_ + "TestGenericUdf(" + Joiner.on(",").join(inputTypes_) + ")";
  }
}
