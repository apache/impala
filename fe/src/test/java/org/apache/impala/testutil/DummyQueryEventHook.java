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
package org.apache.impala.testutil;

import org.apache.impala.hooks.QueryCompleteContext;
import org.apache.impala.hooks.QueryEventHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a dummy {@link QueryEventHook} implementation that always succeeds.
 * It is also used by test_query_event_hooks.py.
 */
public class DummyQueryEventHook implements QueryEventHook {
  private static final Logger LOG = LoggerFactory.getLogger(DummyQueryEventHook.class);

  @Override
  public void onImpalaStartup() {
    LOG.info("{}.onImpalaStartup", this.getClass().getName());
  }

  @Override
  public void onQueryComplete(QueryCompleteContext context) {
    LOG.info("{}.onQueryComplete", this.getClass().getName());
  }
}
