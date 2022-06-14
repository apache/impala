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
package org.apache.impala.catalog.local;

import java.util.Set;

import org.apache.impala.catalog.FeIncompleteTable;
import org.apache.impala.catalog.TableLoadingException;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.thrift.TImpalaTableType;
import org.apache.impala.thrift.TTableDescriptor;

import com.google.common.base.Preconditions;

/**
 * FeTable implementation which represents a table that failed to load in the
 * LocalCatalog catalog implementation. This is used so that statements like
 * DROP TABLE can get a handle to a table object even when that table is in some
 * unloadable state.
 */
public class FailedLoadLocalTable extends LocalTable implements FeIncompleteTable {
  private final TableLoadingException cause_;
  private TImpalaTableType tableType_;
  private String comment_;

  public FailedLoadLocalTable(LocalDb db, String tblName, TImpalaTableType type,
      String comment, TableLoadingException cause) {
    super(db, tblName);
    this.cause_ = Preconditions.checkNotNull(cause);
    this.tableType_ = type;
    this.comment_ = comment;
  }

  @Override
  public TImpalaTableType getTableType() {
    return tableType_;
  }

  @Override
  public String getTableComment() {
    return comment_;
  }

  @Override
  public ImpalaException getCause() { return cause_; }

  @Override
  public TTableDescriptor toThriftDescriptor(int tableId,
      Set<Long> referencedPartitions) {
    throw new RuntimeException("Not serializable as descriptor");
  }
}
