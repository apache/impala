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

import os

hive_major_version = int(os.environ['IMPALA_HIVE_VERSION'][0])
kerberize = os.environ.get('IMPALA_KERBERIZE') == '1'

CONFIG = {
  'dfs.replication': '3'
}

# General Hive configuration.
CONFIG.update({
  # IMPALA-781: Impala doesn't support the default LazyBinaryColumnarSerde for RCFile.
  'hive.default.rcfile.serde': 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe',

  'hive.metastore.client.connect.retry.delay': '0',
  'hive.metastore.client.socket.timeout': '120',
  'hive.metastore.uris': 'thrift://${INTERNAL_LISTEN_HOST}:9083',
  'hive.metastore.warehouse.dir': '${WAREHOUSE_LOCATION_PREFIX}/test-warehouse',

  # Location of Hive per-query log files of the form: hive_job_log_<hive_query_id>.txt
  'hive.querylog.location': '${IMPALA_CLUSTER_LOGS_DIR}/hive',
  'hive.sentry.conf.url': 'file:///${IMPALA_HOME}/fe/src/test/resources/sentry-site.xml',

  # Change back to NOSASL when HIVE-4232 is fixed.
  # With NONE, Hive uses the plain SASL transport.
  'hive.server2.authentication': '${HIVE_S2_AUTH}',
  # Disable user impersonation for HiveServer2 to avoid launch failure
  # if username contains dots (IMPALA-6789).
  'hive.server2.enable.doAs': 'false',

  # TODO(todd): should we be enabling stats autogather?
  'hive.stats.autogather': 'false',
  'hive.support.concurrency': 'true',
})

# HBase-related configs.
# Impala processes need to connect to zookeeper on INTERNAL_LISTEN_HOST for HBase.
CONFIG.update({
  'hive.cluster.delegation.token.store.zookeeper.connectString': '${INTERNAL_LISTEN_HOST}:2181',
})

if kerberize:
  CONFIG.update({
   'hive.server2.authentication.kerberos.keytab': '${KRB5_KTNAME}',
   'hive.server2.authentication.kerberos.principal': '${MINIKDC_PRINC_HIVE}',

   # These are necessary to connect to Kerberized HBase from within Hive jobs.
   'hbase.coprocessor.region.classes': 'org.apache.hadoop.hbase.security.token.TokenProvider',
   'hbase.master.kerberos.principal': '${MINIKDC_PRINC_HBSE}',
   'hbase.regionserver.kerberos.principal': '${MINIKDC_PRINC_HBSE}',
   'hbase.security.authentication': 'kerberos',
   'hbase.zookeeper.quorum': '${INTERNAL_LISTEN_HOST}',
  })
  # TODO: we currently don't kerberize the metastore. If we want to, we need to
  # set:
  #   hive.metastore.sasl.enabled
  #   hive.metastore.kerberos.keytab.file
  #   hive.metastore.kerberos.principal

# Enable Tez and ACID for Hive 3
if hive_major_version >= 3:
  CONFIG.update({
   'hive.tez.container.size': '512',
   'hive.txn.manager': 'org.apache.hadoop.hive.ql.lockmgr.DbTxnManager',
   # We run YARN with Tez on the classpath directly
   'tez.ignore.lib.uris': 'true',
   'tez.use.cluster.hadoop-libs': 'true',
   # Some of the tests change the columns in a incompatible manner
   # (eg. string to timestamp) this is disallowed by default in Hive-3 which causes
   # these tests to fail. We disable this behavior in minicluster to keep running the
   # same tests on both hms-2 and hms-3
   'hive.metastore.disallow.incompatible.col.type.changes': 'false'
  })
else:
  CONFIG.update({
   'hive.metastore.event.listeners': 'org.apache.sentry.binding.metastore.SentrySyncHMSNotificationsPostEventListener',
  })

# Notifications-related configuration.
# These are for enabling notification between Hive and Sentry as well as
# metastore event processing in Impala (see IMPALA-7954)
CONFIG.update({
 'hive.metastore.alter.notifications.basic': 'false',
 'hive.metastore.notification.parameters.exclude.patterns': '',
 'hive.metastore.notifications.add.thrift.objects': 'true',
 'hive.metastore.transactional.event.listeners': 'org.apache.hive.hcatalog.listener.DbNotificationListener',
 'hcatalog.message.factory.impl.json': 'org.apache.sentry.binding.metastore.messaging.json.SentryJSONMessageFactory',
 'hive.metastore.dml.events': 'true',
})

# Database and JDO-related configs:
db_type = os.environ.get('HMS_DB_TYPE', 'postgres')
CONFIG.update({
  'datanucleus.autoCreateSchema': 'false',
  'datanucleus.fixedDatastore': 'false',
  'datanucleus.metadata.validate': 'false',
  'javax.jdo.option.ConnectionUserName': 'hiveuser',
  'javax.jdo.option.ConnectionPassword': 'password',
})
if db_type == 'postgres':
  CONFIG.update({
   'javax.jdo.option.ConnectionDriverName': 'org.postgresql.Driver',
   'javax.jdo.option.ConnectionURL': 'jdbc:postgresql://localhost:5432/${METASTORE_DB}',
  })
elif db_type == 'mysql':
  CONFIG.update({
   'javax.jdo.option.ConnectionDriverName': 'com.mysql.jdbc.Driver',
   'javax.jdo.option.ConnectionURL': 'jdbc:mysql://localhost:3306/${METASTORE_DB}?createDatabaseIfNotExist=true'
  })
else:
  raise Exception("Unknown db type: %s", db_type)
