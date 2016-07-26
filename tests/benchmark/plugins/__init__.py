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


class Plugin(object):
  '''Base plugin class.

  Defines the interfaces that all plugins will use.
  The interface consists of:
     * The scope, which defines when the plugin will run.
       The different scopes are:
         * per query
         * per workload
         * per test suite run
     * A pre-hook method, which is run at the beginning of the 'scope'
     * A post-hook method, which is run at the end of the scope.
  '''
  __name__ = "BasePlugin"
  def __init__(self, scope=None):
    self.scope = scope

  def run_pre_hook(self, context=None):
    pass

  def run_post_hook(self, context=None):
    pass
