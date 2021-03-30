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

import org.apache.impala.catalog.FeIncompleteTable;
import org.apache.impala.common.ImpalaException;
import org.apache.impala.thrift.TBriefTableMeta;
import org.apache.impala.thrift.TTableDescriptor;

import javax.annotation.Nullable;
import java.util.Set;

/**
 * FeTable implementation which represents a table with its brief metadata required by
 * operations like GetTables HiveServer2 op that don't require the whole table meta.
 */
public class LocalIncompleteTable extends LocalTable implements FeIncompleteTable {
  // These are null if the table is unloaded in catalogd.
  @Nullable
  private final String msTableType;
  @Nullable
  private final String tableComment;

  public LocalIncompleteTable(LocalDb db, TBriefTableMeta tableMeta) {
    super(db, tableMeta.getName());
    msTableType = tableMeta.getMsType();
    tableComment = tableMeta.getComment();
  }

  public String getMsTableType() { return msTableType; }
  public String getTableComment() { return tableComment; }

  @Override
  public ImpalaException getCause() { return null; }

  @Override
  public TTableDescriptor toThriftDescriptor(int tableId,
      Set<Long> referencedPartitions) {
    throw new RuntimeException("Not serializable as descriptor");
  }

  @Override
  public boolean isLoaded() { return false; }
}
