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
import org.apache.impala.thrift.TTableDescriptor;

import java.util.Set;

/**
 * FeTable implementation which represents an unloaded table in the LocalCatalog
 * implementation. This is used so operations that don't require the whole table meta,
 * like GetTables HiveServer2 operation, can get a quick response.
 */
public class LocalIncompleteTable extends LocalTable implements FeIncompleteTable {

  public LocalIncompleteTable(LocalDb db, String tblName) {
    super(db, tblName);
  }

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
