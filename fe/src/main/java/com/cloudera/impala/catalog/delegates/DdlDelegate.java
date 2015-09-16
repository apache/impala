// Copyright 2015 Cloudera Inc.
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

package com.cloudera.impala.catalog.delegates;

import java.util.List;

import com.cloudera.impala.thrift.TDistributeParam;
import org.apache.hadoop.hive.metastore.api.Table;

import com.cloudera.impala.common.ImpalaRuntimeException;
import com.cloudera.impala.thrift.TAlterTableParams;

/**
 * Abstract class for handlers to implement the storage specific portion of DDL requests.
 *
 * During catalog DDL operations the CatalogOpExecutor will instantiate the correct
 * subclass of this class to handle the DDL operation to the storage backend. See,
 * CatalogOpExecutor::createDDLDelegate() for details.
 *
 * If canHandle() returns true, the necessary DDL methods are called
 * on the delegate implementation.
  */
public abstract class DdlDelegate {

  protected Table msTbl_;
  protected TAlterTableParams tAlterTableParams_;
  protected List<TDistributeParam> distributeParams_;

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
   * The parameter is the HMS table instances that is created in the frontend.
   * The delegate can extract all necessary information to propagate the creation of
   * the table if necessary.
   */
  public abstract void createTable() throws ImpalaRuntimeException;

  /**
   * Drops the table identified by the HMS table.
   */
  public abstract void dropTable() throws ImpalaRuntimeException;

  /**
   * Given a HMS table, perform an alter table operation specified by params.
   */
  public abstract boolean alterTable() throws ImpalaRuntimeException;

}
