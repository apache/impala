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
import sys

kerberize = os.environ.get('IMPALA_KERBERIZE') == '1'
target_filesystem = os.environ.get('TARGET_FILESYSTEM')

compression_codecs = [
  'org.apache.hadoop.io.compress.GzipCodec',
  'org.apache.hadoop.io.compress.DefaultCodec',
  'com.hadoop.compression.lzo.LzoCodec',
  'com.hadoop.compression.lzo.LzopCodec',
  'org.apache.hadoop.io.compress.BZip2Codec'
]

auth_to_local_rules = [
  'RULE:[2:$1@$0](authtest@REALM.COM)s/(.*)@REALM.COM/auth_to_local_user/',
  'RULE:[1:$1]',
  'RULE:[2:$1]',
  'DEFAULT'
]

CONFIG = {
  'fs.defaultFS': '${DEFAULT_FS}',
  'dfs.replication': '${HDFS_REPLICATION}',

  # Compression codecs
  'io.compression.codecs': ",".join(compression_codecs),
  'io.compression.deoc.lzo.class': 'com.hadoop.compression.lzo.LzoCodec',

  # Set up proxyuser
  'hadoop.proxyuser.${USER}.hosts': '*',
  'hadoop.proxyuser.${USER}.groups': '*',

  # Trash is enabled since some tests (in metadata/test_ddl.py) depend on it
  # The trash interval is set to 1030 years to avoid checkpointing until 3000 AD
  'fs.trash.interval': 541728000,

  # AuthorizationTest depends on auth_to_local configs. These tests are run
  # irrespective of whether kerberos is enabled.
  'hadoop.security.auth_to_local': '\n'.join(auth_to_local_rules),

  # Location of the KMS key provider
  'hadoop.security.key.provider.path': 'kms://http@${INTERNAL_LISTEN_HOST}:9600/kms',

  # Needed as long as multiple nodes are running on the same address. For Impala
  # testing only.
  'yarn.scheduler.include-port-in-node-name': 'true',

  # ADLS configuration
  # Note: This is needed even when not running on ADLS, because some frontend tests
  # include ADLS paths that require initializing an ADLS filesystem. See
  # ExplainTest.testScanNodeFsScheme().
  'dfs.adls.oauth2.access.token.provider.type': 'ClientCredential',
  'dfs.adls.oauth2.client.id': '${azure_client_id}',
  'dfs.adls.oauth2.credential': '${azure_client_secret}',
  'dfs.adls.oauth2.refresh.url':
    'https://login.windows.net/${azure_tenant_id}/oauth2/token',

  # ABFS configuration
  # Note: This is needed even when not running on ABFS for the same reason as for ADLS.
  # See ExplainTest.testScanNodeFsScheme().
  'fs.azure.account.auth.type': 'OAuth',
  'fs.azure.account.oauth.provider.type':
    'org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider',
  'fs.azure.account.oauth2.client.id': '${azure_client_id}',
  'fs.azure.account.oauth2.client.secret': '${azure_client_secret}',
  'fs.azure.account.oauth2.client.endpoint':
    'https://login.microsoftonline.com/${azure_tenant_id}/oauth2/token',

  # This property can be used in tests to ascertain that this core-site.xml from
  # the classpath has been loaded. (Ex: TestRequestPoolService)
  'impala.core-site.overridden': 'true',
}

if target_filesystem == 's3':
  CONFIG.update({'fs.s3a.connection.maximum': 1500})
  s3guard_enabled = os.environ.get("S3GUARD_ENABLED") == 'true'
  if s3guard_enabled:
    CONFIG.update({
      'fs.s3a.metadatastore.impl':
        'org.apache.hadoop.fs.s3a.s3guard.DynamoDBMetadataStore',
      'fs.s3a.s3guard.ddb.table': '${S3GUARD_DYNAMODB_TABLE}',
      'fs.s3a.s3guard.ddb.region': '${S3GUARD_DYNAMODB_REGION}',
    })

if kerberize:
  CONFIG.update({
    'hadoop.security.authentication': 'kerberos',
    'hadoop.security.authorization': 'true',
    'hadoop.proxyuser.hive.hosts': '*',
    'hadoop.proxyuser.hive.groups': '*',
  })
