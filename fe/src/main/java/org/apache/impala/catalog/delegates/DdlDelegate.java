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

package com.cloudera.impala.catalog.delegates;

import java.util.List;

import com.cloudera.impala.thrift.TDistributeParam;
import org.apache.hadoop.hive.metastore.api.Table;

import com.cloudera.impala.common.ImpalaRuntimeException;
import com.cloudera.impala.thrift.TAlterTableParams;

/**
 * Abstract class to implement the storage specific portion of DDL requests.
 *
 * During catalog DDL operations the CatalogOpExecutor will instantiate the correct
 * subclass of this class to handle the DDL operation to the storage backend. See,
 * CatalogOpExecutor::createDDLDelegate() for details.
 *
 */
public abstract class DdlDelegate {

  protected Table msTbl_;
  protected TAlterTableParams tAlterTableParams_;
  protected List<TDistributeParam> distributeParams_;

  /**
   * Creates a new delegate to modify Table 'msTbl'.
   */
  public DdlDelegate setMsTbl(Table msTbl) {
    msTbl_ = msTbl;
    return this;
  }

  public DdlDelegate setAlterTableParams(TAlterTableParams p) {
    tAlterTableParams_ = p;
    return this;
  }

  public DdlDelegate setDistributeParams(List<TDistributeParam> p) {
    distributeParams_ = p;
    return this;
  }

  /**
   * Creates the table.
   */
  public abstract void createTable() throws ImpalaRuntimeException;

  /**
   * Drops the table.
   */
  public abstract void dropTable() throws ImpalaRuntimeException;

  /**
   * Performs an alter table with the parameters set with setAlterTableParams().
   */
  public abstract boolean alterTable() throws ImpalaRuntimeException;

}
