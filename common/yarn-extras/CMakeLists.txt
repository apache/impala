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

# The dependency on ext-data-source here is fictional, but Maven does not like
# concurrent invocations. These lead to opaque, non-deterministic errors due to
# races in how Maven handles its ~/.m2/repository doc.
add_custom_target(yarn-extras ALL DEPENDS impala-parent ext-data-source
  COMMAND $ENV{IMPALA_HOME}/bin/mvn-quiet.sh -B install -DskipTests
)
