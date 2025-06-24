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

from __future__ import absolute_import, division, print_function
import os

HIVE_MAJOR_VERSION = int(os.environ['IMPALA_HIVE_VERSION'][0])
USE_APACHE_HIVE = os.environ['USE_APACHE_HIVE']
KERBERIZE = os.environ.get('IMPALA_KERBERIZE') == 'true'
VARIANT = os.environ.get('HIVE_VARIANT')
IMPALA_JAVA_TOOL_OPTIONS=os.environ.get("IMPALA_JAVA_TOOL_OPTIONS")

CONFIG = {
  'dfs.replication': '3'
}

# General Hive configuration.
CONFIG.update({
  # IMPALA-781: Impala doesn't support the default LazyBinaryColumnarSerde for RCFile.
  'hive.default.rcfile.serde': 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe',

  # IMPALA-7154: Some of the HMS operations on S3 can take long time and any timeouts
  # on the HMS Client side can result in flaky test failures
  # Following configuration specifies the time in seconds between successive retry
  # attempts by metastore client in case of failures. By default, metastore client
  # retries once based on the config value of hive.metastore.failure.retries
  'hive.metastore.client.connect.retry.delay': '1',
  # set the metastore client timeout to 10 min
  'hive.metastore.client.socket.timeout': '600',

  'hive.metastore.uris': 'thrift://${INTERNAL_LISTEN_HOST}:9083',

  # Location of Hive per-query log files of the form: hive_job_log_<hive_query_id>.txt
  'hive.querylog.location': '${IMPALA_CLUSTER_LOGS_DIR}/hive',

  # Change back to NOSASL when HIVE-4232 is fixed.
  # With NONE, Hive uses the plain SASL transport.
  'hive.server2.authentication': '${HIVE_S2_AUTH}',
  # Disable user impersonation for HiveServer2 to avoid launch failure
  # if username contains dots (IMPALA-6789).
  'hive.server2.enable.doAs': 'false',

  # TODO(todd): should we be enabling stats autogather?
  'hive.stats.autogather': 'false',
  'hive.support.concurrency': 'true',
  # There are tests which issue Hive's replication command. The repl
  # dump command will wait in case there are any open transactions.
  # The default value of this configuration is 1h which will block
  # the test for long time. Overriding this configuration helps with
  # the runtime of the test.
  'hive.repl.bootstrap.dump.open.txn.timeout': '120s',

  # allow both hs2 and hs2-http protocols
  'hive.server2.transport.mode': 'all',

  # Don't filter out incremental stats of Impala. The default is "impala_intermediate_stats_chunk%".
  'hive.metastore.partitions.parameters.exclude.pattern': '""',
})

if VARIANT == 'changed_external_dir':
  CONFIG.update({
    'hive.metastore.warehouse.external.dir': '${WAREHOUSE_LOCATION_PREFIX}/test-warehouse-external',
  })
elif VARIANT == 'ranger_auth':
  CONFIG.update({
    'hive.security.authorization.manager':
        'org.apache.ranger.authorization.hive.authorizer.RangerHiveAuthorizerFactory',
    'hive.metastore.pre.event.listeners':
        'org.apache.hadoop.hive.ql.security.authorization.plugin.metastore.HiveMetaStoreAuthorizer',
  })
elif VARIANT == 'events_cleanup':
  # HMS configs needed for regression test for IMPALA-11028
  CONFIG.update({
    'hive.metastore.event.db.listener.timetolive': '60s',
    'hive.metastore.event.db.listener.clean.interval': '10s'
  })
elif VARIANT == 'housekeeping_on':
  # HMS configs needed for regression test for IMPALA-12827
  CONFIG.update({
    'hive.metastore.housekeeping.threads.on': 'true',
  })

# HBase-related configs.
# Impala processes need to connect to zookeeper on INTERNAL_LISTEN_HOST for HBase.
CONFIG.update({
  'hive.cluster.delegation.token.store.zookeeper.connectString': '${INTERNAL_LISTEN_HOST}:2181',
})

if KERBERIZE:
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

# Enable Tez, ACID and proleptic Gregorian calendar DATE types for Hive 3
if HIVE_MAJOR_VERSION >= 3:
  CONFIG.update({
   'hive.execution.engine': 'tez',
   'hive.tez.container.size': '512',
   'hive.txn.manager': 'org.apache.hadoop.hive.ql.lockmgr.DbTxnManager',
   # We run YARN with Tez on the classpath directly
   'tez.ignore.lib.uris': 'true',
   'tez.use.cluster.hadoop-libs': 'true',
   'tez.am.tez-ui.webservice.port-range': '32000-32100',

   'tez.task.launch.cluster-default.cmd-opts': IMPALA_JAVA_TOOL_OPTIONS,
   'tez.am.launch.cluster-default.cmd-opts': IMPALA_JAVA_TOOL_OPTIONS,

   # Some of the tests change the columns in a incompatible manner
   # (eg. string to timestamp) this is disallowed by default in Hive-3 which causes
   # these tests to fail. We disable this behavior in minicluster to keep running the
   # same tests on both hms-2 and hms-3
   'hive.metastore.disallow.incompatible.col.type.changes': 'false',

   # Group input splits to run in a small number of mappers, and merge small
   # files at the end of jobs if necessary, to be more similar to the legacy
   # MR execution defaults. This helps ensure that we produce the same
   # dataload results with Hive2-MR vs Hive3-Tez.
   #
   # NOTE: This currently doesn't seem to take effect on our pseudo-distributed
   # test cluster, because the hostname is 'localhost' and some Tez code path
   # gets triggered which ignores the min-size parameter. See TEZ-3310.
   'tez.grouping.min-size': 256 * 1024 * 1024,

   # Instead, we use post-process merging to make sure that we merge files
   # where possible at the end of jobs.
   # TODO(todd) re-evaluate whether this is necessary once TEZ-3310 is fixed
   # (see above).
   'hive.merge.tezfiles': 'true',

   # Enable compaction workers. The compaction initiator is off by default
   # but configuring a worker thread allows manual compaction.
   'hive.compactor.worker.threads': 4,

   # Hive 3 now requires separate directories for managed vs external tables.
   # Since only transactional tables are considered managed, most tests run against
   # tables that are now considered external. So, use /test-warehouse for the external
   # directory so that most tests don't need to change their paths. Data snapshots
   # are built around populating /test-warehouse, so use /test-warehouse/managed
   # to allow that logic to remain the same.
   'hive.metastore.warehouse.dir': '${WAREHOUSE_LOCATION_PREFIX}/test-warehouse/managed',
   'hive.metastore.warehouse.external.dir': '${WAREHOUSE_LOCATION_PREFIX}/test-warehouse',

   # Due to HIVE-23102 Hive will wait for at least this amount of time for compactions
   # (the default value is 5 mins which is way too long). Setting it to 2 seconds.
   'hive.compactor.wait.timeout': '2000',

   # No need to automatically compute stats after compactions. It might cause failures
   # if we trigger compaction on temp tables in tests. The stats computation is async and
   # will fail if the temp tables are removed. See an example in IMPALA-11756.
   'hive.compactor.gather.stats': 'false',

   # Since HIVE-22589, Hive uses Julian Calendar for writing dates before 1582-10-15,
   # whereas Impala uses proleptic Gregorian Calendar. This affects the results Impala
   # gets when querying tables written by Hive.
   'hive.avro.proleptic.gregorian': 'true',
   'hive.avro.proleptic.gregorian.default': 'true',
   'hive.parquet.date.proleptic.gregorian': 'true',
   'hive.parquet.date.proleptic.gregorian.default': 'true',
   'orc.proleptic.gregorian': 'true',
   'orc.proleptic.gregorian.default': 'true',

   # Disable auto compaction of Hive Metastore after HIVE-28662 to prevent Hive from
   # automatically compacting files associated with ACID tables, which could make some
   # test cases non-deterministic, e.g., those in acid.test.
   'hive.compactor.initiator.on': 'false',
   'hive.compactor.cleaner.on': 'false'
  })
  # 'hive.metastore.housekeeping.threads.on' was also set to true by default in
  # HIVE-28662. But since IMPALA-12827 requires that this configuration be true when
  # 'VARIANT' is 'housekeeping_on', we set this configuration to true only if
  # 'VARIANT' is not 'housekeeping_on'.
  if VARIANT != 'housekeeping_on':
    CONFIG.update({
     'hive.metastore.housekeeping.threads.on': 'false'
    })
else:
  CONFIG.update({
   # HMS-2 based environments have a different set of expected configurations for event processor
   'hive.metastore.alter.notifications.basic': 'false',
   'hive.metastore.notification.parameters.exclude.patterns': '',
   'hive.metastore.notifications.add.thrift.objects': 'true',
    # HMS-2 doesn't have a distinction between external and managed warehouse directories,
    # so only hive.metastore.warehouse.dir is necessary.
    'hive.metastore.warehouse.dir': '${WAREHOUSE_LOCATION_PREFIX}/test-warehouse'
  })

# Notifications-related configuration.
# These are for enabling notification for Hive as well as metastore event processing
# in Impala (see IMPALA-7954)
CONFIG.update({
 'hive.metastore.transactional.event.listeners': 'org.apache.hive.hcatalog.listener.DbNotificationListener,org.apache.kudu.hive.metastore.KuduMetastorePlugin',
 'hive.metastore.dml.events': 'true',
})

# Add Iceberg catalog configurations.
CONFIG.update({
  # Hive catalog:
  'iceberg.catalog.ice_hive_cat.type': 'hive',
  # Hadoop catalog:
  'iceberg.catalog.ice_hadoop_cat.type': 'hadoop',
  'iceberg.catalog.ice_hadoop_cat.warehouse': '${WAREHOUSE_LOCATION_PREFIX}/test-warehouse/ice_hadoop_cat',
})

if VARIANT == 'without_hms_config':
  CONFIG.clear()

# Database and JDO-related configs:
db_type = os.environ.get('HMS_DB_TYPE', 'postgres')
CONFIG.update({
  'datanucleus.autoCreateSchema': 'false',
  'datanucleus.fixedDatastore': 'false',
  'datanucleus.metadata.validate': 'false',
  'datanucleus.connectionPool.maxPoolSize': 20,
  'javax.jdo.option.ConnectionUserName': 'hiveuser',
  'javax.jdo.option.ConnectionPassword': 'password',
})
# Before HIVE-19486 (in Apache Hive 4 and CDP Hive versions), Hikari CP configs are prefixed with "hikari.*".
# After HIVE-19486, the prefix is "hikaricp.*".
if USE_APACHE_HIVE and HIVE_MAJOR_VERSION == 3:
  CONFIG.update({
    'hikari.connectionTimeout': 60000,
  })
else:
  CONFIG.update({
    'hikaricp.connectionTimeout': 60000,
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
