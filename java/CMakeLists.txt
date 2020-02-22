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

add_custom_target(validate_java_pom_versions ALL
  COMMAND $ENV{IMPALA_HOME}/bin/validate-java-pom-versions.sh
)

SET(JAMM_MAVEN_FLAGS --show-version --batch-mode)
# Discover and download dependencies in parallel, using 10 threads
SET(JAMM_MAVEN_FLAGS ${JAMM_MAVEN_FLAGS} -Daether.dependencyCollector.impl=bf)
SET(JAMM_MAVEN_FLAGS ${JAMM_MAVEN_FLAGS} -Daether.connector.basic.threads=10)
SET(JAMM_MAVEN_FLAGS ${JAMM_MAVEN_FLAGS} -Daether.dependencyCollector.bf.threads=10)

include(ExternalProject)
ExternalProject_Add(jamm
  GIT_REPOSITORY    https://github.com/jbellis/jamm
  # 0.4.0 RC. A release is expected soon: https://github.com/jbellis/jamm/issues/44
  GIT_TAG           49c3e691c43a39ddaccaf19839ef316807009377
  PATCH_COMMAND     sed -i.bak s/0.4.0-SNAPSHOT/0.4.0-IMPALA/ pom.xml
  CONFIGURE_COMMAND sed s:JAVA_HOME:$ENV{JAVA_HOME}:g ${CMAKE_CURRENT_SOURCE_DIR}/toolchains.xml.tmpl > toolchains.xml
  BUILD_IN_SOURCE   true
  BUILD_COMMAND     mvn install ${JAMM_MAVEN_FLAGS} --toolchains toolchains.xml -Dmaven.test.skip=true -DskipTests
  INSTALL_COMMAND   ""
)

add_custom_target(java ALL DEPENDS gen-deps function-registry geospatial-udf-wrappers validate_java_pom_versions jamm
  COMMAND $ENV{IMPALA_HOME}/bin/mvn-quiet.sh -B install -DskipTests
)
