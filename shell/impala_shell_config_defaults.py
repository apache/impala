#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

# default options used by the Impala shell stored in a dict
from __future__ import print_function, unicode_literals

import getpass
import os
import socket

_histfile_from_env = os.environ.get(
    'IMPALA_HISTFILE', '~/.impalahistory')

impala_shell_defaults = {
            'ca_cert': None,
            'config_file': os.path.expanduser("~/.impalarc"),
            'default_db': None,
            'history_file': _histfile_from_env,
            'history_max': 1000,
            'ignore_query_failure': False,
            'impalad': socket.getfqdn(),
            'kerberos_host_fqdn': None,
            'kerberos_service_name': 'impala',
            'output_delimiter': '\\t',
            'output_file': None,
            'print_header': False,
            'live_progress': True,  # The option only applies to interactive shell session
            'live_summary': False,
            'query': None,
            'query_file': None,
            'show_profiles': False,
            'ssl': False,
            'use_kerberos': False,
            'use_ldap': False,
            'user': getpass.getuser(),
            'verbose': True,
            'version': False,
            'write_delimited': False,
            'client_connect_timeout_ms': 60000,
            'http_socket_timeout_s': None,
            'global_config_default_path': '/etc/impalarc',
            'strict_hs2_protocol': False,
    }
