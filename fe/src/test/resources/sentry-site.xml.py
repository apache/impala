#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
import os

_SETTINGS_BY_VARIANT = dict({
  'oo': 'all_with_grant',
  'oo_nogrant': 'all',
  'no_oo': 'none'})

variant = os.environ.get('SENTRY_VARIANT')

CONFIG = {
  'hive.sentry.server': 'server1',

  # Use a space to mean allow all privileges. See SENTRY-2424
  'sentry.db.explicit.grants.permitted': ' ',

  'sentry.service.admin.group': '${USER}',
  # TODO(IMPALA-5686): remove one of the following two properties when Sentry
  # standardizes on one.
  'sentry.service.client.server.rpc-address': '${INTERNAL_LISTEN_HOST}',
  'sentry.service.client.server.rpc-addresses': '${INTERNAL_LISTEN_HOST}',
  'sentry.service.client.server.rpc-port': '30911',

  # Enable HMS follower.
  'sentry.service.security.mode': 'none',
  'sentry.service.server.rpc-address': '${INTERNAL_LISTEN_HOST}',
  'sentry.service.server.rpc-port': '30911',

  'sentry.store.jdbc.driver': 'org.postgresql.Driver',
  'sentry.store.jdbc.password': 'password',
  'sentry.store.jdbc.url': 'jdbc:postgresql://localhost:5432/${SENTRY_POLICY_DB}',
  'sentry.store.jdbc.user': 'hiveuser',
  'sentry.verify.schema.version': 'false',
}

if variant is not None:
  # TODO(todd): these settings seem generic, rather than related to the object ownership
  # feature, but they're only set in the "variants". Is that right?
  CONFIG.update({
   'sentry.hive.testing.mode': 'true',
   'sentry.policy.store.plugins': 'org.apache.sentry.hdfs.SentryPlugin',
   # Custom group mapping for custom cluster tests .
   'sentry.store.group.mapping': 'org.apache.impala.testutil.TestSentryGroupMapper',
   'sentry.service.processor.factories': '${SENTRY_PROCESSOR_FACTORIES}',
  })

  CONFIG['sentry.db.policy.store.owner.as.privilege'] = _SETTINGS_BY_VARIANT[variant]
