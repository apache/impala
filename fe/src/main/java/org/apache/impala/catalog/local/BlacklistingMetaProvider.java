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

import static org.apache.impala.util.CatalogBlacklistUtils.getBlacklistedDbsCount;
import static org.apache.impala.util.CatalogBlacklistUtils.getBlacklistedTablesCount;
import static org.apache.impala.util.CatalogBlacklistUtils.getBlacklistedTablesDbs;
import static org.apache.impala.util.CatalogBlacklistUtils.isDbBlacklisted;
import static org.apache.impala.util.CatalogBlacklistUtils.isTableBlacklisted;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.impala.thrift.TBriefTableMeta;
import org.apache.thrift.TException;

import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;

/**
 * A {@link MetaProvider} that decorates another {@link MetaProvider} adding functionality
 * to filter out blacklisted databases and tables based on the blacklists defined in
 * {@link CatalogBlacklistUtils}.
 */
public class BlacklistingMetaProvider extends MetaProviderDecorator {

  public BlacklistingMetaProvider(final MetaProvider delegate) {
    super(delegate);
  }

  @Override
  public ImmutableList<String> loadDbList() throws TException {
    if (getBlacklistedDbsCount() == 0) {
      return super.loadDbList();
    }

    return super.loadDbList().stream().filter(
        dbName -> !isDbBlacklisted(dbName)).collect(ImmutableList.toImmutableList());
  }

  @Override
  public ImmutableCollection<TBriefTableMeta> loadTableList(String dbName)
      throws MetaException, UnknownDBException, TException {
    if (getBlacklistedTablesCount() == 0
        || !getBlacklistedTablesDbs().contains(dbName.toLowerCase())) {
      return super.loadTableList(dbName);
    }

    return super.loadTableList(dbName).stream().filter(
        tableMeta -> !isTableBlacklisted(dbName, tableMeta.getName()))
            .collect(ImmutableList.toImmutableList());
  }

}
