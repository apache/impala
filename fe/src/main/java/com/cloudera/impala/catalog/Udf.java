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

package com.cloudera.impala.catalog;

import java.util.ArrayList;

import com.cloudera.impala.analysis.HdfsURI;

/**
 * Internal representation of a UDF description.
 * TODO: unify this with builtins.
 */
public class Udf {
  private final Function desc_;
  // Absolute path in HDFS for the binary that contains this UDF.
  // e.g. /udfs/udfs.jar
  private final HdfsURI location_;

  // The name inside the binary at location_ that contains this particular
  // UDF. e.g. org.example.MyUdf.class.
  private final String binaryName_;

  public Udf(String fnName, ArrayList<PrimitiveType> argTypes,
      PrimitiveType retType, HdfsURI location, String binaryName) {
    PrimitiveType[] args = null;
    if (argTypes.size() > 0) {
      args = argTypes.toArray(new PrimitiveType[argTypes.size()]);
    }
    this.desc_ = new Function(fnName, args, retType, false);
    this.location_ = location;
    this.binaryName_ = binaryName;
  }

  public Function getDesc() { return desc_; }
  public HdfsURI getLocation() { return location_; }
  public String getBinaryName() { return binaryName_; }
}
