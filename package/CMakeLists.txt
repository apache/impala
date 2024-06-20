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

project(Impala_packaging)

cmake_host_system_information(RESULT OS_DISTRIB_ID QUERY DISTRIB_ID)
cmake_host_system_information(RESULT OS_DISTRIB_VERSION_ID QUERY DISTRIB_VERSION_ID)
message(STATUS "DISTRIB_ID: ${OS_DISTRIB_ID}")
message(STATUS "DISTRIB_VERSION_ID: ${OS_DISTRIB_VERSION_ID}")

install(FILES ${CMAKE_SOURCE_DIR}/LICENSE.txt DESTINATION ${IMPALA_INSTALLDIR} RENAME LICENSE)
install(FILES ${CMAKE_SOURCE_DIR}/NOTICE.txt DESTINATION ${IMPALA_INSTALLDIR} RENAME NOTICE)
install(DIRECTORY "${CMAKE_SOURCE_DIR}/www/" DESTINATION ${IMPALA_INSTALLDIR}/www)
install(FILES ${CMAKE_SOURCE_DIR}/fe/target/impala-frontend-$ENV{IMPALA_VERSION}.jar
  DESTINATION ${IMPALA_INSTALLDIR}/lib/jars)
install(DIRECTORY ${CMAKE_SOURCE_DIR}/fe/target/dependency/
  DESTINATION ${IMPALA_INSTALLDIR}/lib/jars
  FILES_MATCHING PATTERN "*.jar")

set(IMPALA_GCC_HOME $ENV{IMPALA_TOOLCHAIN_PACKAGES_HOME}/gcc-$ENV{IMPALA_GCC_VERSION})
FILE(GLOB gcc_lib ${IMPALA_GCC_HOME}/lib64/libgcc_s.so.1*)
if("${gcc_lib}" STREQUAL "")
  message(FATAL_ERROR "libgcc_s.so.1 was not found in '${IMPALA_GCC_HOME}/lib64'!")
endif()
install(FILES ${gcc_lib} DESTINATION ${IMPALA_INSTALLDIR}/lib/native)

FILE(GLOB cpp_lib ${IMPALA_GCC_HOME}/lib64/libstdc++.so.6*)
if("${cpp_lib}" STREQUAL "")
  message(FATAL_ERROR "libstdc++.so.6 was not found in '${IMPALA_GCC_HOME}/lib64'!")
endif()
install(FILES ${cpp_lib} DESTINATION ${IMPALA_INSTALLDIR}/lib/native)

set(KUDU_HOME $ENV{IMPALA_TOOLCHAIN_PACKAGES_HOME}/kudu-$ENV{IMPALA_KUDU_VERSION}/release)
# The parent folder is lib64 on centos/redhat, while on ubuntu it's lib.
FILE(GLOB kudu_lib ${KUDU_HOME}/lib*/libkudu_client.so*)
if("${kudu_lib}" STREQUAL "")
  message(FATAL_ERROR "libkudu_client.so was not found in '${KUDU_HOME}/lib'!")
endif()
install(FILES ${kudu_lib} DESTINATION ${IMPALA_INSTALLDIR}/lib/native)

FILE(GLOB hadoop_lib $ENV{HADOOP_LIB_DIR}/native/libhadoop.so*)
if("${hadoop_lib}" STREQUAL "")
  message(FATAL_ERROR "libhadoop.so was not found in '$ENV{HADOOP_LIB_DIR}/native'!")
endif()
install(FILES ${hadoop_lib} DESTINATION ${IMPALA_INSTALLDIR}/lib/native)

string(TOLOWER ${OS_DISTRIB_ID} OS_DISTRIB_ID)
if (${OS_DISTRIB_ID} STREQUAL "rhel" AND ${OS_DISTRIB_VERSION_ID} VERSION_GREATER_EQUAL 8)
  # TODO: remove this after resolving IMPALA-12255
  message(STATUS "Not packaging impala-shell on redhat8+ due to ambiguous python shebang")
else()
  install(DIRECTORY ${CMAKE_SOURCE_DIR}/shell/build/impala-shell-$ENV{IMPALA_VERSION}/
    DESTINATION ${IMPALA_INSTALLDIR}/shell
    USE_SOURCE_PERMISSIONS PATTERN "*.pyc" EXCLUDE)
endif()

install(DIRECTORY bin/ DESTINATION ${IMPALA_INSTALLDIR}/bin
  USE_SOURCE_PERMISSIONS FILES_MATCHING PATTERN "*.sh")
install(DIRECTORY conf/ DESTINATION ${IMPALA_INSTALLDIR}/conf)

# packaging
option(BUILD_PACKAGES "Build deployment packages")
if (NOT BUILD_PACKAGES)
  return()
endif()

if (${OS_DISTRIB_ID} MATCHES "(rhel|centos)")
  set(CPACK_GENERATOR "RPM")
elseif (${OS_DISTRIB_ID} MATCHES "ubuntu")
  set(CPACK_GENERATOR "DEB")
else()
  message(FATAL_ERROR "OS is ${OS_DISTRIB_ID}. No packages will be generated.")
endif()

message(STATUS "Packaging enabled: ${CPACK_GENERATOR}")
set(CPACK_PACKAGE_NAME ${CMAKE_PROJECT_NAME})
set(CPACK_PACKAGE_VENDOR "Apache")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "An open source massively parallel processing SQL query engine.")
set(CPACK_PACKAGE_CONTACT "dev@impala.apache.org")
set(CPACK_PACKAGE_HOMEPAGE_URL "https://impala.apache.org")
set(CPACK_PACKAGE_VERSION "$ENV{IMPALA_VERSION}")
set(CPACK_RPM_PACKAGE_LICENSE "ASL-2.0")
set(CPACK_PACKAGE_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/build)
# Set a meaningful package name, e.g. apache-impala-4.4.0_hive-3.1.3-x86_64.el7
set(CPACK_PACKAGE_FILE_NAME "apache-impala-${CPACK_PACKAGE_VERSION}")
set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_FILE_NAME}_hive-$ENV{IMPALA_HIVE_VERSION}")
set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_FILE_NAME}-${CMAKE_SYSTEM_PROCESSOR}")
if (${CPACK_GENERATOR} MATCHES "RPM")
  set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_FILE_NAME}.el${OS_DISTRIB_VERSION_ID}")
elseif (${CPACK_GENERATOR} MATCHES "DEB")
  set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_FILE_NAME}.ubuntu-${OS_DISTRIB_VERSION_ID}")
endif()
message(STATUS "Package name: ${CPACK_PACKAGE_FILE_NAME}")

if ("${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
  set(CPACK_STRIP_FILES TRUE)
  set(CPACK_PRE_BUILD_SCRIPTS "${CMAKE_CURRENT_SOURCE_DIR}/strip.cmake")
  message("Binaries in the package will be stripped")
endif()

set(CPACK_PACKAGING_INSTALL_PREFIX "/opt")
set(CPACK_PACKAGE_INSTALL_DIRECTORY "impala")

execute_process(
  COMMAND bash -c "${CMAKE_SOURCE_DIR}/docker/install_os_packages.sh --dry-run | tail -n1"
  OUTPUT_VARIABLE PKG_LIST
  OUTPUT_STRIP_TRAILING_WHITESPACE
)
if ("${PKG_LIST}" STREQUAL "")
  message(FATAL_ERROR "Package list is empty: '${PKG_LIST}'")
else()
  message(STATUS "Get required package list: '${PKG_LIST}'")
endif()

if (${CPACK_GENERATOR} MATCHES "RPM")
  set(CPACK_RPM_PACKAGE_AUTOREQPROV " no")
  set(CPACK_RPM_PACKAGE_RELOCATABLE TRUE)
  set(CPACK_RPM_PACKAGE_REQUIRES ${PKG_LIST})
else()
  set(CPACK_DEBIAN_PACKAGE_DEPENDS ${PKG_LIST})
endif()

include(CPack)
