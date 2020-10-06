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

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * Simple UDFs for testing.
 *
 * This class is a copy of the TestUdf class in the FE. We need this class in a
 * separate project so we can test loading UDF jars that are not already on the
 * classpath, and we can't delete the FE's class because UdfExecutorTest depends
 * on it.
 *
 * The jar for this file can be built by running "mvn clean package" in
 * tests/test-hive-udfs. This is run in testdata/bin/create-load-data.sh, and
 * copied to HDFS in testdata/bin/copy-udfs-uda.sh.
 */
public class TestUdf extends UDF {
  public TestUdf() {
  }

  // Identity UDFs for all the supported types
  public BooleanWritable evaluate(BooleanWritable a) {
    if (a == null) return null;
    return new BooleanWritable(a.get());
  }
  public ByteWritable evaluate(ByteWritable a) {
    if (a == null) return null;
    return new ByteWritable(a.get());
  }
  public ShortWritable evaluate(ShortWritable a) {
    if (a == null) return null;
    return new ShortWritable(a.get());
  }
  public IntWritable evaluate(IntWritable a) {
    if (a == null) return null;
    return new IntWritable(a.get());
  }
  public LongWritable evaluate(LongWritable a) {
    if (a == null) return null;
    return new LongWritable(a.get());
  }
  public FloatWritable evaluate(FloatWritable a) {
    if (a == null) return null;
    return new FloatWritable(a.get());
  }
  public DoubleWritable evaluate(DoubleWritable a) {
    if (a == null) return null;
    return new DoubleWritable(a.get());
  }
  public TimestampWritable evaluate(TimestampWritable a) {
    if (a == null) return a;
    return new TimestampWritable(a);
  }
  public DateWritable evaluate(DateWritable a) {
    if (a == null) return a;
    return new DateWritable(a);
  }
  public DoubleWritable evaluate(DoubleWritable arg1, DoubleWritable arg2) {
    if (arg1 == null || arg2 == null) return null;
    return new DoubleWritable(arg1.get() + arg2.get());
  }
  // At runtime BytesWritable and Text are equivalent with regards to signature lookup.
  // If the functions below took the same number of args, one would override the other.
  // An additional argument is added to be sure each type is tested.
  public BytesWritable evaluate(BytesWritable a) {
    if (a == null) return null;
    return new BytesWritable(a.copyBytes());
  }
  public Text evaluate(Text a, Text b) {
    if (a == null || b == null) return null;
    return new Text(a.toString() + b.toString());
  }
  public String evaluate(String a, String b, String c) {
    if (a == null || b == null || c == null) return null;
    return a + b + c;
  }

  // Udfs returning primitive types
  public int evaluate(IntWritable a, IntWritable b) {
    if (a == null || b == null) return -1;
    return a.get() + b.get();
  }

  public float evaluate(FloatWritable a, FloatWritable b) {
    if (a == null || b == null) return -1;
    return a.get() + b.get();
  }

  public double evaluate(DoubleWritable d1,
      DoubleWritable d2, DoubleWritable d3) {
    if (d1 == null || d2 == null) return -1;
    return d1.get() + d2.get() + d3.get();
  }

  public short evaluate(ShortWritable s1, ShortWritable s2) {
    if (s1 == null || s2 == null) return -1;
    return (short)(s1.get() + s2.get());
  }

  public boolean evaluate(BooleanWritable b1, BooleanWritable b2) {
    if (b1 == null || b2 == null) return false;
    return b1.get() && b2.get();
  }

  // Udfs with primitive argument types
  public int evaluate(int a, int b, int c) {
    return a + b + c;
  }

  public boolean evaluate(boolean a, boolean b, boolean c) {
    return a && b && c;
  }

  public short evaluate(short a, short b, short c) {
    return (short)(a + b + c);
  }

  public float evaluate(float a, float b, float c) {
    return a + b + c;
  }

  // Udfs with mixed types
  public double evaluate(IntWritable a, double b) {
    if (a == null) return -1;
    return ((double)a.get()) + b;
  }
}
