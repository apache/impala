#!/usr/bin/env ambari-python-wrap
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

CONFIG = {
  # Host/port configs
  'ozone.om.http-port': '${OZONE_WEBUI_PORT}',
  'dfs.container.ratis.server.port': '${DATANODE_HTTP_PORT}',
  'dfs.container.ratis.admin.port': '${DATANODE_HTTPS_PORT}',
  'dfs.container.ratis.ipc': '${DATANODE_IPC_PORT}',
  'dfs.container.ipc': '${DATANODE_PORT}',
  'ozone.scm.block.client.address': '${INTERNAL_LISTEN_HOST}',
  'ozone.scm.client.address': '${INTERNAL_LISTEN_HOST}',
  'ozone.scm.names': '${INTERNAL_LISTEN_HOST}',
  'ozone.om.address': '${INTERNAL_LISTEN_HOST}',
  # Select a random available port
  'hdds.datanode.http-address': '${EXTERNAL_LISTEN_HOST}:0',
  'hdds.datanode.replication.port': '0',
  'hdds.datanode.client.port': '${DATANODE_CLIENT_PORT}',

  # Directories
  'ozone.metadata.dirs': '${NODE_DIR}/data/ozone',
  'hdds.datanode.dir': '${NODE_DIR}/data/ozone/dn',
  'ozone.om.ratis.storage.dir': '${NODE_DIR}/data/ozone/om-ratis',
  'dfs.container.ratis.datanode.storage.dir': '${NODE_DIR}/data/ozone/ratis',
}

if os.environ.get('OZONE_ERASURECODE_POLICY'):
  CONFIG.update({
    'ozone.server.default.replication.type': 'EC',
    'ozone.server.default.replication': os.environ['OZONE_ERASURECODE_POLICY'],
  })
