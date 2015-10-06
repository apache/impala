// Copyright 2012 Cloudera Inc.
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

package com.cloudera.impala.hive.executor;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.util.ArrayList;

import org.apache.hadoop.hive.ql.udf.UDFAcos;
import org.apache.hadoop.hive.ql.udf.UDFAscii;
import org.apache.hadoop.hive.ql.udf.UDFAsin;
import org.apache.hadoop.hive.ql.udf.UDFAtan;
import org.apache.hadoop.hive.ql.udf.UDFBin;
import org.apache.hadoop.hive.ql.udf.UDFConv;
import org.apache.hadoop.hive.ql.udf.UDFCos;
import org.apache.hadoop.hive.ql.udf.UDFDegrees;
import org.apache.hadoop.hive.ql.udf.UDFE;
import org.apache.hadoop.hive.ql.udf.UDFExp;
import org.apache.hadoop.hive.ql.udf.UDFFindInSet;
import org.apache.hadoop.hive.ql.udf.UDFHex;
import org.apache.hadoop.hive.ql.udf.UDFLength;
import org.apache.hadoop.hive.ql.udf.UDFLn;
import org.apache.hadoop.hive.ql.udf.UDFLog;
import org.apache.hadoop.hive.ql.udf.UDFLog10;
import org.apache.hadoop.hive.ql.udf.UDFLog2;
import org.apache.hadoop.hive.ql.udf.UDFPI;
import org.apache.hadoop.hive.ql.udf.UDFRadians;
import org.apache.hadoop.hive.ql.udf.UDFRand;
import org.apache.hadoop.hive.ql.udf.UDFRepeat;
import org.apache.hadoop.hive.ql.udf.UDFReverse;
import org.apache.hadoop.hive.ql.udf.UDFSign;
import org.apache.hadoop.hive.ql.udf.UDFSin;
import org.apache.hadoop.hive.ql.udf.UDFSpace;
import org.apache.hadoop.hive.ql.udf.UDFSqrt;
import org.apache.hadoop.hive.ql.udf.UDFSubstr;
import org.apache.hadoop.hive.ql.udf.UDFTan;
import org.apache.hadoop.hive.ql.udf.UDFUnhex;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.junit.Test;

import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.Type;
import com.cloudera.impala.common.ImpalaRuntimeException;
import com.cloudera.impala.util.UnsafeUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

@SuppressWarnings("restriction")
public class UdfExecutorTest {
  private final String HIVE_BUILTIN_JAR = System.getenv("HIVE_HOME") + "/" +
      "lib/hive-exec-" + System.getenv("IMPALA_HIVE_VERSION") + ".jar";

  // Allocations from the native heap. These are freed in bulk.
  ArrayList<Long> allocations_ = Lists.newArrayList();

  // Allocates 'byteSize' from the native heap and returns the ptr. The allocation
  // is added to allocations_.
  long allocate(int byteSize) {
    long ptr = UnsafeUtil.UNSAFE.allocateMemory(byteSize);
    allocations_.add(ptr);
    return ptr;
  }

  // Creates a string value object (in the native heap) for v.
  long createStringValue(String v) {
    byte[] array = v.getBytes();
    long ptr = allocate(16);
    UnsafeUtil.UNSAFE.putInt(ptr + 8, 0);
    ImpalaStringWritable sw = new ImpalaStringWritable(ptr);
    sw.set(array, 0, array.length);
    return ptr;
  }

  // Frees all allocations in allocations
  void freeAllocations() {
    for (Long l: allocations_) {
      UnsafeUtil.UNSAFE.freeMemory(l.longValue());
    }
    allocations_.clear();
  }

  // Creates the Impala wrapper Writable object.
  Writable createObject(PrimitiveType t, Object o) {
    long ptr = allocate(t.getSlotSize());
    switch (t) {
      case BOOLEAN: {
        ImpalaBooleanWritable w = new ImpalaBooleanWritable(ptr);
        w.set((Boolean)o);
        return w;
      }
      case TINYINT: {
        ImpalaTinyIntWritable w = new ImpalaTinyIntWritable(ptr);
        w.set((byte)((Integer)o).intValue());
        return w;
      }
      case SMALLINT: {
        ImpalaSmallIntWritable w = new ImpalaSmallIntWritable(ptr);
        w.set(((Integer)o).shortValue());
        return w;
      }
      case INT: {
        ImpalaIntWritable w = new ImpalaIntWritable(ptr);
        w.set((Integer)o);
        return w;
      }
      case BIGINT: {
        ImpalaBigIntWritable w = new ImpalaBigIntWritable(ptr);
        w.set((Long)o);
        return w;
      }
      case FLOAT: {
        ImpalaFloatWritable w = new ImpalaFloatWritable(ptr);
        w.set((Float)o);
        return w;
      }
      case DOUBLE: {
        ImpalaDoubleWritable w = new ImpalaDoubleWritable(ptr);
        w.set((Double)o);
        return w;
      }
    }
    return null;
  }

  Writable createBoolean(boolean v) { return createObject(PrimitiveType.BOOLEAN, v); }
  Writable createTinyInt(int v) { return createObject(PrimitiveType.TINYINT, v); }
  Writable createSmallInt(int v) { return createObject(PrimitiveType.SMALLINT, v); }
  Writable createInt(int v) { return createObject(PrimitiveType.INT, v); }
  Writable createBigInt(long v) { return createObject(PrimitiveType.BIGINT, v); }
  Writable createFloat(float v) { return createObject(PrimitiveType.FLOAT, v); }
  Writable createDouble(double v) { return createObject(PrimitiveType.DOUBLE, v); }
  Writable createBytes(String v) {
    ImpalaBytesWritable w = new ImpalaBytesWritable(createStringValue(v));
    return w;
  }

  Writable createText(String v) {
    ImpalaTextWritable w = new ImpalaTextWritable(createStringValue(v));
    return w;
  }

  // Returns the primitive type for w
  Type getType(Object w) {
    if (w instanceof ImpalaBooleanWritable) {
      return Type.BOOLEAN;
    } else if (w instanceof ImpalaTinyIntWritable) {
      return Type.TINYINT;
    } else if (w instanceof ImpalaSmallIntWritable) {
      return Type.SMALLINT;
    } else if (w instanceof ImpalaIntWritable) {
      return Type.INT;
    } else if (w instanceof ImpalaBigIntWritable) {
      return Type.BIGINT;
    } else if (w instanceof ImpalaFloatWritable) {
      return Type.FLOAT;
    } else if (w instanceof ImpalaDoubleWritable) {
      return Type.DOUBLE;
    } else if (w instanceof ImpalaBytesWritable || w instanceof ImpalaTextWritable
        || w instanceof String) {
      return Type.STRING;
    }
    Preconditions.checkArgument(false);
    return Type.INVALID;
  }

  // Runs the hive udf contained in c. Validates that c.evaluate(args) == retValue.
  // Arguments and return value cannot be NULL.
  void TestUdfImpl(String jar, Class<?> c, Object expectedValue,
      Type expectedType, boolean validate, Object... args)
      throws MalformedURLException, ImpalaRuntimeException {
    Type[] argTypes = new Type[args.length];
    for (int i = 0; i < args.length; ++i) {
      Preconditions.checkNotNull(args[i]);
      argTypes[i] = getType(args[i]);
    }

    UdfExecutor e = new UdfExecutor(jar, c.getName(), expectedType, argTypes);
    Method method = e.getMethod();
    Object[] inputArgs = new Object[args.length];
    for (int i = 0; i < args.length; ++i) {
      if (args[i] instanceof String) {
        // For authoring the test, we'll just pass string and make the proper
        // object here.
        if (method.getParameterTypes()[i] == Text.class) {
          inputArgs[i] = createText((String)args[i]);
        } else if (method.getParameterTypes()[i] == BytesWritable.class) {
          inputArgs[i] = createBytes((String)args[i]);
        } else {
          Preconditions.checkState(method.getParameterTypes()[i] == String.class);
          inputArgs[i] = args[i];
        }
      } else {
        inputArgs[i] = args[i];
      }
    }

    // Run the executor a few times to make sure nothing gets messed up
    // between runs.
    for (int i = 0; i < 10; ++i) {
      long r = e.evaluateForTesting(inputArgs);
      if (validate) {
        switch (expectedType.getPrimitiveType()) {
          case BOOLEAN: {
            boolean v = UnsafeUtil.UNSAFE.getByte(r) != 0;
            assertTrue(v == ((ImpalaBooleanWritable)expectedValue).get());
            break;
          }
          case TINYINT:
            assertEquals(UnsafeUtil.UNSAFE.getByte(r),
                ((ImpalaTinyIntWritable)expectedValue).get());
            break;
          case SMALLINT:
            assertEquals(UnsafeUtil.UNSAFE.getShort(r),
                ((ImpalaSmallIntWritable)expectedValue).get());
            break;
          case INT:
            assertEquals(UnsafeUtil.UNSAFE.getInt(r),
                ((ImpalaIntWritable)expectedValue).get());
            break;
          case BIGINT:
            assertEquals(UnsafeUtil.UNSAFE.getLong(r),
                ((ImpalaBigIntWritable)expectedValue).get());
            break;
          case FLOAT:
            assertEquals(UnsafeUtil.UNSAFE.getFloat(r),
                ((ImpalaFloatWritable)expectedValue).get(), 0);
            break;
          case DOUBLE:
            assertEquals(UnsafeUtil.UNSAFE.getDouble(r),
                ((ImpalaDoubleWritable)expectedValue).get(), 0);
            break;
          case STRING: {
            byte[] expectedBytes = null;
            if (expectedValue instanceof ImpalaBytesWritable) {
              expectedBytes = ((ImpalaBytesWritable)expectedValue).getBytes();
            } else if (expectedValue instanceof ImpalaTextWritable) {
              expectedBytes = ((ImpalaTextWritable)expectedValue).getBytes();
            } else if (expectedValue instanceof String) {
              expectedBytes = ((String)expectedValue).getBytes();
            } else {
              Preconditions.checkState(false);
            }
            ImpalaStringWritable sw = new ImpalaStringWritable(r);
            assertArrayEquals(sw.getBytes(), expectedBytes);
            break;
          }
          default:
            Preconditions.checkArgument(false);
        }
      }
    }
  }

  void TestUdf(String jar, Class<?> c, Writable expectedValue, Object... args)
      throws MalformedURLException, ImpalaRuntimeException {
    TestUdfImpl(jar, c, expectedValue, getType(expectedValue), true, args);
  }

  void TestUdf(String jar, Class<?> c, String expectedValue, Object... args)
      throws MalformedURLException, ImpalaRuntimeException {
    TestUdfImpl(jar, c, expectedValue, getType(expectedValue), true, args);
  }

  void TestHiveUdf(Class<?> c, Writable expectedValue, Object... args)
      throws MalformedURLException, ImpalaRuntimeException {
    TestUdfImpl(HIVE_BUILTIN_JAR, c, expectedValue, getType(expectedValue), true, args);
  }

  void TestHiveUdfNoValidate(Class<?> c, Writable expectedValue, Object... args)
      throws MalformedURLException, ImpalaRuntimeException {
    TestUdfImpl(HIVE_BUILTIN_JAR, c, expectedValue, getType(expectedValue), false, args);
  }

  @Test
  // Tests all the hive math UDFs. We are not trying to thoroughly test that the Hive
  // UDFs are correct (i.e. replicate expr-test). The most interesting thing to test for
  // here is that we can drive all the UDFs.
  public void HiveMathTest()
      throws ImpalaRuntimeException, MalformedURLException {
    TestHiveUdfNoValidate(UDFRand.class, createDouble(0));
    TestHiveUdfNoValidate(UDFRand.class, createDouble(0), createBigInt(10));
    TestHiveUdf(UDFExp.class, createDouble(Math.exp(10)), createDouble(10));
    TestHiveUdf(UDFLn.class, createDouble(Math.log(10)), createDouble(10));
    TestHiveUdf(UDFLog10.class, createDouble(Math.log10(10)), createDouble(10));
    TestHiveUdf(UDFLog2.class, createDouble(Math.log(10) / Math.log(2)),
        createDouble(10));
    TestHiveUdf(UDFLog.class, createDouble(Math.log(3) / Math.log(10)),
        createDouble(10), createDouble(3));
    TestHiveUdf(UDFSqrt.class, createDouble(Math.sqrt(3)), createDouble(3));
    TestHiveUdf(UDFSin.class, createDouble(Math.sin(1)), createDouble(1));
    TestHiveUdf(UDFAsin.class, createDouble(Math.asin(1)), createDouble(1));
    TestHiveUdf(UDFCos.class, createDouble(Math.cos(1)), createDouble(1));
    TestHiveUdf(UDFAcos.class, createDouble(Math.acos(1)), createDouble(1));
    TestHiveUdf(UDFTan.class, createDouble(Math.tan(1)), createDouble(1));
    TestHiveUdf(UDFAtan.class, createDouble(Math.atan(1)), createDouble(1));
    TestHiveUdf(UDFDegrees.class, createDouble(0), createDouble(0));
    TestHiveUdf(UDFRadians.class, createDouble(0), createDouble(0));

    TestHiveUdf(UDFPI.class, createDouble(Math.PI));
    TestHiveUdf(UDFE.class, createDouble(Math.E));
    TestHiveUdf(UDFSign.class, createDouble(1), createDouble(3));

    TestHiveUdf(UDFBin.class, createBytes("1100100"), createBigInt(100));

    TestHiveUdf(UDFHex.class, createBytes("1F4"), createBigInt(500));
    TestHiveUdf(UDFHex.class, createBytes("3E8"), createBigInt(1000));

    TestHiveUdf(UDFHex.class, createText("31303030"), "1000");
    TestHiveUdf(UDFUnhex.class, createText("aAzZ"), "61417A5A");
    TestHiveUdf(UDFConv.class, createText("1111011"),
        "123", createInt(10), createInt(2));
    freeAllocations();
  }

  @Test
  // Tests all the hive string UDFs. We are not testing for correctness of the UDFs
  // so it is sufficient to just cover the types.
  public void HiveStringsTest() throws ImpalaRuntimeException, MalformedURLException {
    TestHiveUdf(UDFAscii.class, createInt('1'), "123");
    TestHiveUdf(UDFFindInSet.class, createInt(2), "31", "12,31,23");
    TestHiveUdf(UDFLength.class, createInt(5), createText("Hello"));
    TestHiveUdf(UDFRepeat.class, createText("abcabc"), "abc", createInt(2));
    TestHiveUdf(UDFReverse.class, createText("cba"), "abc");
    TestHiveUdf(UDFSpace.class, createText("    "), createInt(4));
    TestHiveUdf(UDFSubstr.class, createText("World"),
        "HelloWorld", createInt(6), createInt(5));
    freeAllocations();
  }

  @Test
  // Test identity for all types
  public void BasicTest() throws ImpalaRuntimeException, MalformedURLException {
    TestUdf(null, TestUdf.class, createBoolean(true), createBoolean(true));
    TestUdf(null, TestUdf.class, createTinyInt(1), createTinyInt(1));
    TestUdf(null, TestUdf.class, createSmallInt(1), createSmallInt(1));
    TestUdf(null, TestUdf.class, createInt(1), createInt(1));
    TestUdf(null, TestUdf.class, createBigInt(1), createBigInt(1));
    TestUdf(null, TestUdf.class, createFloat(1.1f), createFloat(1.1f));
    TestUdf(null, TestUdf.class, createDouble(1.1), createDouble(1.1));
    TestUdf(null, TestUdf.class, createBytes("ABCD"), "ABCD");
    TestUdf(null, TestUdf.class, "ABCD", "ABCD");
    TestUdf(null, TestUdf.class, createDouble(3),
        createDouble(1), createDouble(2));
    TestUdf(null, TestUdf.class, "ABCXYZ", "ABC", "XYZ");
    TestUdf(null, TestUdf.class, createInt(3), createInt(1), createInt(2));
    TestUdf(null, TestUdf.class, createFloat(1.1f + 1.2f),
        createFloat(1.1f), createFloat(1.2f));
    TestUdf(null, TestUdf.class, createDouble(1.1 + 1.2 + 1.3),
        createDouble(1.1), createDouble(1.2), createDouble(1.3));
    TestUdf(null, TestUdf.class, createSmallInt(1 + 2), createSmallInt(1),
        createSmallInt(2));
    TestUdf(null, TestUdf.class, createBoolean(true && true),
        createBoolean(true), createBoolean(true));
    TestUdf(null, TestUdf.class, createInt(5 + 6 + 7), createInt(5),
        createInt(6), createInt(7));
    TestUdf(null, TestUdf.class, createBoolean(true && true && true),
        createBoolean(true), createBoolean(true), createBoolean(true));
    TestUdf(null, TestUdf.class, createFloat(1.1f + 1.2f + 1.3f),
        createFloat(1.1f), createFloat(1.2f), createFloat(1.3f));
    TestUdf(null, TestUdf.class, createDouble(1 + 1.2), createInt(1),
        createDouble(1.2));
    TestUdf(null, TestUdf.class, createInt(5 + 6 + 7 + 8), createInt(5),
        createInt(6), createInt(7), createInt(8));
    TestUdf(null, TestUdf.class, createBoolean(true && true && true && true),
        createBoolean(true), createBoolean(true), createBoolean(true),
        createBoolean(true));
    freeAllocations();
  }
}
