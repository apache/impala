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

package org.apache.impala.catalog.events;

import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.impala.catalog.events.MetastoreEvents.MetastoreEvent;
import org.apache.impala.common.Metrics;

/**
 * Factory interface to generate a {@link MetastoreEvent} from a {@link NotificationEvent}
 * object.
 */
public interface EventFactory {

  /**
   * Generates a {@link MetastoreEvent} representing {@link NotificationEvent}
   * @param hmsEvent the event as received from Hive Metastore.
   * @param metrics metrics which gets updated when MetastoreEvent from this api
   *                is processed.
   * @return {@link MetastoreEvent} representing hmsEvent.
   * @throws MetastoreNotificationException If the hmsEvent information cannot be parsed.
   */
  MetastoreEvent get(NotificationEvent hmsEvent, Metrics metrics)
      throws MetastoreNotificationException;
}
