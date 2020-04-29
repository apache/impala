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

package org.apache.impala.catalog.metastore;

import org.apache.impala.catalog.CatalogException;

/**
 * This is the main Interface which a CatalogMetastore service should implement. It
 * provides lifecycle and monitoring methods which are called from
 * CatalogServiceCatalog to instantiate a Metastore service.
 */
public interface ICatalogMetastoreServer {

  /**
   * Starts the metastore service
   * @throws CatalogException
   */
  void start() throws CatalogException;

  /**
   * Stop the metastore service.
   * @throws CatalogException
   */
  void stop() throws CatalogException;

  /**
   * Returns the metrics for this Catalog Metastore service.
   * @return Encoded String (eg. Json format) representation of all the metrics for this
   * metastore service.
   */
  String getMetrics();
}