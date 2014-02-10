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

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
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
  public BytesWritable evaluate(BytesWritable a) {
    if (a == null) return null;
    return new BytesWritable(a.getBytes());
  }
  public Text evaluate(Text a) {
    if (a == null) return null;
    return new Text(a.getBytes());
  }
  public TimestampWritable evaluate(TimestampWritable a) {
    if (a == null) return a;
    return new TimestampWritable(a);
  }
  public String evaluate(String a) {
    if (a == null) return a;
    return a;
  }

  public DoubleWritable evaluate(DoubleWritable arg1, DoubleWritable arg2) {
    if (arg1 == null || arg2 == null) return null;
    return new DoubleWritable(arg1.get() + arg2.get());
  }

  public String evaluate(String a, String b) {
    if (a == null || b == null) return null;
    return a + b;
  }

}
