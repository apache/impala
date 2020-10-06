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

// Import a class that is not shaded in the UDF jar.
import com.google.i18n.phonenumbers.NumberParseException;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;

/**
 * This class intends to test a UDF that has some classes that the Catalog's
 * class loader can't resolve. We import the class NumberParseException from
 * Google's phone number library that won't be shaded with the UDF jar. The
 * Catalog should gracefully handle this situation by ignoring the load of
 * Java UDF that references this jar.
 *
 * The jar for this file can be built by running "mvn clean package" in
 * tests/test-hive-udfs. This is run in testdata/bin/create-load-data.sh, and
 * copied to HDFS in testdata/bin/copy-udfs-uda.sh.
 */
public class UnresolvedUdf extends UDF {

  public IntWritable evaluate(IntWritable a) throws NumberParseException {
    return a;
  }
}
