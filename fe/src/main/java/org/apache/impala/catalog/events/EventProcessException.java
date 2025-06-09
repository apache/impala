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
import org.apache.impala.common.ImpalaException;


/**
 * Wrapper exception class thrown for errors during event processing in
 * {@link DbEventExecutor.DbProcessor#process()} and
 * {@link TableEventExecutor.TableProcessor#process()}
 */
public class EventProcessException extends ImpalaException {
  // Notification event under processing when exception occurred
  private final NotificationEvent event_;

  // Inherent exception during event process
  private final Exception exception_;

  public EventProcessException(NotificationEvent event, Exception exception) {
    super(exception);
    event_ = event;
    exception_ = exception;
  }

  public Exception getException() {
    return exception_;
  }

  public NotificationEvent getEvent() {
    return event_;
  }
}
