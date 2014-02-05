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

package com.cloudera.impala;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * Simple UDF for testing updating a UDF in HDFS. This file is built and
 * the old version of the jar is saved to $IMPALA_HOME/testdata/udfs/
 *
 * The build produces a new version of the UDF. The tests make sure the
 * jar can be updated without having to restart Impalad.
 */
public class TestUpdateUdf extends UDF {
  public TestUpdateUdf() {
  }

  public Text evaluate() {
    return new Text("New UDF");
  }

  // Use this function instead of the other evaluate to regenerate the checked in jar.
  /*
  public Text evaluate() {
    return new Text("Old UDF");
  }
  */
}
