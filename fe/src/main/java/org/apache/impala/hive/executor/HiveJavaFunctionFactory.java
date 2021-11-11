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

package org.apache.impala.hive.executor;

import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.impala.catalog.CatalogException;
import org.apache.impala.catalog.ScalarFunction;
import org.apache.impala.catalog.Type;

/**
 * Interface of Factory class to create the HiveJavaFunction instance.
 */
public interface HiveJavaFunctionFactory {
  /**
   * The local path contains a directory on the local file system to which the
   * jar file on hdfs can be copied.
   */
  public HiveJavaFunction create(String localLibPath, Function hiveFn,
      Type retType, Type[] paramTypes) throws CatalogException;

  public HiveJavaFunction create(String localLibPath,
      ScalarFunction fn) throws CatalogException;

  public HiveJavaFunction create(String localLibPath, Function hiveFn)
      throws CatalogException;
}
