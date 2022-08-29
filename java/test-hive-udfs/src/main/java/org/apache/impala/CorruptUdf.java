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
import org.apache.hadoop.io.IntWritable;

/*
 * This class is one of two classes used to test for a corrupt UDF loaded
 * into the Hive MetaStore. This class contains a valid UDF. This file will
 * be used to create the function through SQL within the Hive MetaStore.
 * Later, this object will be replaced by a corrupt Java object (under
 * the hive-corrupt-test-udfs directory). The test will ensure that catalogd
 * is still able to start up fine (though the function will be disabled).
 */
public class CorruptUdf extends UDF {
  public IntWritable evaluate(IntWritable a) {
    return a;
  }
}
