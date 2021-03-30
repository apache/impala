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

import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.List;

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
 * A bunch of incompatible Java UDFs for testing. Impala shouldn't load
 * the function since none of the signatures are compatible.
 *
 * The jar for this file can be built by running "mvn clean package" in
 * tests/test-hive-udfs. This is run in testdata/bin/create-load-data.sh, and
 * copied to HDFS in testdata/bin/copy-udfs-uda.sh.
 */
public class IncompatibleUdfTest extends UDF {
  public IncompatibleUdfTest() {
  }

  public IntWritable evaluate(List<String> list) {
    if (list == null) return null;
    return new IntWritable(list.size());
  }
  public IntWritable evaluate(HashMap<String,String> m) {
    if (m == null) return null;
    return new IntWritable(m.size());
  }
  public LongWritable evaluate(LongWritable a, List<IntWritable> b) {
    if (a == null) return null;
    return new LongWritable(a.get());
  }
  public List<Text> evaluate(Text a) {
    if (a == null) return null;
    return Lists.newArrayList(a);
  }
  public Object evaluate(Object a, Object b, Object c) {
    return null;
  }
  public IntWritable evaluate(Object a) {
    return null;
  }
}
