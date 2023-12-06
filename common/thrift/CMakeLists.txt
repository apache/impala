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

# Helper function to generate build rules.  For each input thrift file, this function will
# generate a rule that maps the input file to the output c++ file.
# Thrift will generate multiple output files for each input (including java files) and
# ideally, we'd specify all of the outputs for dependency tracking.
# Unfortunately, it's not easy to figure out all the output files without parsing the
# thrift input. (TODO: can thrift tells us what the java output files will be?)
# The list of output files is used for build dependency tracking so it's not necessary to
# capture all the output files.
#
# To call this function, pass it the output file list followed by the input thrift files:
#    i.e. THRIFT_GEN(OUTPUT_FILES, ${THRIFT_FILES})
#
# cmake seems to be case sensitive for some keywords. Changing the first IF check to lower
# case makes it not work.  TODO: investigate this
function(THRIFT_GEN VAR)
  IF (NOT ARGN)
    MESSAGE(SEND_ERROR "Error: THRIFT_GEN called without any src files")
    RETURN()
  ENDIF(NOT ARGN)

  set(${VAR})
  foreach(THRIFT_FILE ${ARGN})
    # Get full path
    get_filename_component(ABS_THRIFT_FILE ${THRIFT_FILE} ABSOLUTE)
    # Get basename without the file extension
    get_filename_component(THRIFT_FILE_WE ${THRIFT_FILE} NAME_WE)

    # All the output files we can determine based on filename.
    #   - Does not include .skeleton.cpp files
    #   - Does not include java output files
    set(OUTPUT_BE_FILE "${BE_OUTPUT_DIR}/gen-cpp/${THRIFT_FILE_WE}_types.cpp")
    set(OUTPUT_BE_FILE ${OUTPUT_BE_FILE} "${BE_OUTPUT_DIR}/gen-cpp/${THRIFT_FILE_WE}_types.h")
    list(APPEND ${VAR} ${OUTPUT_BE_FILE})

    # BeeswaxService thrift generation
    # It depends on hive_meta_store, which in turn depends on fb303.
    # The java dependency is handled by maven.
    # We need to generate C++ src file for the parent dependencies using the "-r" option.
    set(CPP_ARGS ${THRIFT_CPP_INCLUDE_DIR_OPTION}
        --gen cpp:moveable_types,no_default_operators,templates -o ${BE_OUTPUT_DIR})
    IF (THRIFT_FILE STREQUAL "beeswax.thrift")
      set(CPP_ARGS -r ${CPP_ARGS})
    ENDIF(THRIFT_FILE STREQUAL "beeswax.thrift")

    IF (THRIFT_FILE STREQUAL ${TCLI_SERVICE_THRIFT} OR THRIFT_FILE STREQUAL "parquet.thrift")
      # Do not generate Java HiveServer2 and Parquet files because we should use the jar
      # from Hive or Parquet.
      add_custom_command(
        OUTPUT ${OUTPUT_BE_FILE}
        COMMAND ${THRIFT_QUIET_WRAPPER} ${THRIFT_CPP_COMPILER} ${CPP_ARGS} ${THRIFT_FILE}
        COMMAND ${THRIFT_QUIET_WRAPPER} ${THRIFT_PY_COMPILER} ${PYTHON_ARGS} ${THRIFT_FILE}
        DEPENDS ${ABS_THRIFT_FILE}
        COMMENT "Running thrift compiler on ${THRIFT_FILE}"
        VERBATIM
      )
    ELSEIF (THRIFT_FILE STREQUAL "ImpalaService.thrift")
      add_custom_command(
        OUTPUT ${OUTPUT_BE_FILE}
        COMMAND ${THRIFT_QUIET_WRAPPER} ${THRIFT_CPP_COMPILER} ${CPP_ARGS} ${THRIFT_FILE}
        # Because of some CMake bug we can't just use
        #     sed -i.bak "'s|\\(dispatchCallTemplated.*\\));|\\1) override;|'"
        # here because for some reason CMake doesn't remove the quotes and generates
        # wrong Makefiles. Strange, because at global scope add_custom_command works fine.
        COMMAND ${CMAKE_SOURCE_DIR}/bin/cmake_aux/add_override.sh
            ${BE_OUTPUT_DIR}/gen-cpp/ImpalaHiveServer2Service.h
            ${BE_OUTPUT_DIR}/gen-cpp/ImpalaService.h
        COMMAND ${THRIFT_QUIET_WRAPPER} ${THRIFT_JAVA_COMPILER} ${JAVA_FE_ARGS} ${THRIFT_FILE}
        COMMAND ${THRIFT_QUIET_WRAPPER} ${THRIFT_PY_COMPILER} ${PYTHON_ARGS} ${THRIFT_FILE}
        DEPENDS ${ABS_THRIFT_FILE}
        COMMENT "Running thrift compiler on ${THRIFT_FILE}"
        VERBATIM
      )
    ELSE (THRIFT_FILE STREQUAL ${TCLI_SERVICE_THRIFT} OR THRIFT_FILE STREQUAL "parquet.thrift")
      add_custom_command(
        OUTPUT ${OUTPUT_BE_FILE}
        COMMAND ${THRIFT_QUIET_WRAPPER} ${THRIFT_CPP_COMPILER} ${CPP_ARGS} ${THRIFT_FILE}
        COMMAND ${THRIFT_QUIET_WRAPPER} ${THRIFT_JAVA_COMPILER} ${JAVA_FE_ARGS} ${THRIFT_FILE}
        COMMAND ${THRIFT_QUIET_WRAPPER} ${THRIFT_PY_COMPILER} ${PYTHON_ARGS} ${THRIFT_FILE}
        DEPENDS ${ABS_THRIFT_FILE}
        COMMENT "Running thrift compiler on ${THRIFT_FILE}"
        VERBATIM
    )
    ENDIF (THRIFT_FILE STREQUAL ${TCLI_SERVICE_THRIFT} OR THRIFT_FILE STREQUAL "parquet.thrift")
  endforeach(THRIFT_FILE)

  set(${VAR} ${${VAR}} PARENT_SCOPE)
endfunction(THRIFT_GEN)

function(THRIFT_GEN_DS VAR)
  IF (NOT ARGN)
    MESSAGE(SEND_ERROR "Error: THRIFT_GEN_DS called without any src files")
    RETURN()
  ENDIF(NOT ARGN)

  set(${VAR})
  foreach(THRIFT_FILE ${ARGN})
    get_filename_component(ABS_THRIFT_FILE ${THRIFT_FILE} ABSOLUTE)
    get_filename_component(THRIFT_FILE_WE ${THRIFT_FILE} NAME_WE)

    # Create a dummy marker file to track if Thrift compilation of the current file has
    # been run or not. This file is created only if Thrift compilation succeeds. Deletion
    # of the file will re-trigger Thrift compilation for the current file.
    set(OUTPUT_FILE_MESSAGE "CMake state file for Thrift compilation of"
                            "${ABS_THRIFT_FILE}, deletion of this file will cause"
                            "${THRIFT_FILE} to be recompiled during the next build")
    set(OUTPUT_DIR "${EXT_DS_OUTPUT_DIR}/../target/tmp/generated-sources/")
    set(OUTPUT_FILE "${OUTPUT_DIR}/${THRIFT_FILE_WE}.txt")
    list(APPEND ${VAR} ${OUTPUT_FILE})
    add_custom_command(
      OUTPUT ${OUTPUT_FILE}
      COMMAND ${THRIFT_QUIET_WRAPPER} ${THRIFT_JAVA_COMPILER} ${JAVA_EXT_DS_ARGS} ${THRIFT_FILE} &&
              mkdir -p ${OUTPUT_DIR} && echo ${OUTPUT_FILE_MESSAGE} > ${OUTPUT_FILE}
      DEPENDS ${ABS_THRIFT_FILE}
      COMMENT "Running thrift compiler for ext-data-source on ${THRIFT_FILE}"
      VERBATIM
    )
  endforeach(THRIFT_FILE)
  set(${VAR} ${${VAR}} PARENT_SCOPE)
endfunction(THRIFT_GEN_DS)

set(HIVE_THRIFT_SOURCE_DIR "hive-$ENV{IMPALA_HIVE_MAJOR_VERSION}-api")
set(TCLI_SERVICE_THRIFT "${HIVE_THRIFT_SOURCE_DIR}/TCLIService.thrift")
message("Using Thrift CPP compiler: ${THRIFT_CPP_COMPILER}")
message("Using Thrift JAVA compiler: ${THRIFT_JAVA_COMPILER}")
message("Using Thrift PY compiler: ${THRIFT_PY_COMPILER}")
set(THRIFT_QUIET_WRAPPER "${CMAKE_SOURCE_DIR}/bin/thrift-quiet-wrapper.sh")
set(THRIFT_CPP_INCLUDE_DIR_OPTION -I ${THRIFT_CPP_CONTRIB_DIR}
  -I $ENV{HIVE_METASTORE_THRIFT_DIR} -I ${HIVE_THRIFT_SOURCE_DIR})
set(THRIFT_JAVA_INCLUDE_DIR_OPTION -I ${THRIFT_JAVA_CONTRIB_DIR}
  -I $ENV{HIVE_METASTORE_THRIFT_DIR} -I ${HIVE_THRIFT_SOURCE_DIR})
set(THRIFT_PY_INCLUDE_DIR_OPTION -I ${THRIFT_PY_CONTRIB_DIR}
  -I $ENV{HIVE_METASTORE_THRIFT_DIR} -I ${HIVE_THRIFT_SOURCE_DIR})
set(BE_OUTPUT_DIR ${CMAKE_SOURCE_DIR}/be/generated-sources)
set(FE_OUTPUT_DIR ${CMAKE_SOURCE_DIR}/fe/generated-sources)
# TODO: avoid duplicating generated java classes
set(EXT_DS_OUTPUT_DIR ${CMAKE_SOURCE_DIR}/java/ext-data-source/api/generated-sources)
set(PYTHON_OUTPUT_DIR ${CMAKE_SOURCE_DIR}/shell/)
MESSAGE("Found output dir: " ${PYTHON_OUTPUT_DIR})
file(MAKE_DIRECTORY ${BE_OUTPUT_DIR})
file(MAKE_DIRECTORY ${FE_OUTPUT_DIR})
file(MAKE_DIRECTORY ${EXT_DS_OUTPUT_DIR})
file(MAKE_DIRECTORY ${PYTHON_OUTPUT_DIR})
file(MAKE_DIRECTORY ${HIVE_THRIFT_SOURCE_DIR})

# Args passed to thrift for Java gen
set(JAVA_FE_ARGS ${THRIFT_JAVA_INCLUDE_DIR_OPTION} --gen java -o ${FE_OUTPUT_DIR})
set(JAVA_EXT_DS_ARGS ${THRIFT_JAVA_INCLUDE_DIR_OPTION} --gen java -o ${EXT_DS_OUTPUT_DIR})
set(PYTHON_ARGS ${THRIFT_PY_INCLUDE_DIR_OPTION} -r --gen py:no_utf8strings -o
                ${PYTHON_OUTPUT_DIR})

set (EXT_DATA_SRC_FILES
  ErrorCodes.thrift
  ExternalDataSource.thrift
  Data.thrift
  Status.thrift
  Types.thrift
)

set (SRC_FILES
  ErrorCodes.thrift
  beeswax.thrift
  BackendGflags.thrift
  CatalogInternalService.thrift
  CatalogObjects.thrift
  CatalogService.thrift
  DataSinks.thrift
  Descriptors.thrift
  ExecStats.thrift
  Frontend.thrift
  Exprs.thrift
  ExternalDataSource.thrift
  ImpalaInternalService.thrift
  ImpalaService.thrift
  JniCatalog.thrift
  LineageGraph.thrift
  Logging.thrift
  NetworkTest.thrift
  MetricDefs.thrift
  Metrics.thrift
  PlanNodes.thrift
  Planner.thrift
  Partitions.thrift
  parquet.thrift
  ResourceProfile.thrift
  Query.thrift
  Results.thrift
  RuntimeProfile.thrift
  SqlConstraints.thrift
  StatestoreService.thrift
  SystemTables.thrift
  Zip.thrift
  ${TCLI_SERVICE_THRIFT}
  ${EXT_DATA_SRC_FILES}
)

SET_SOURCE_FILES_PROPERTIES(Status.thrift PROPERTIES OBJECT_DEPENDS ErrorCodes.thrift)

add_custom_command(OUTPUT ErrorCodes.thrift
  COMMAND python generate_error_codes.py
  DEPENDS generate_error_codes.py)

add_custom_command(OUTPUT MetricDefs.thrift
  COMMAND python generate_metrics.py
  DEPENDS generate_metrics.py metrics.json)

# The thrift-generated java classes defined in TCLIService are also pulled into our build
# in the Hive jars that are downloaded via Maven. Hive2 moved the classes from
# org.apache.hive.service.cli.thrift to org.apache.hive.service.rpc.thrift. Impala calls
# various Hive methods that have these classes in the interface and if the packages don't
# match it won't compile.
add_custom_command(OUTPUT hive-$ENV{IMPALA_HIVE_MAJOR_VERSION}-api/TCLIService.thrift
  COMMAND sed
      's/namespace java org.apache.hive.service.cli.thrift/namespace java org.apache.hive.service.rpc.thrift/'
      hive-1-api/TCLIService.thrift > hive-$ENV{IMPALA_HIVE_MAJOR_VERSION}-api/TCLIService.thrift
  DEPENDS hive-1-api/TCLIService.thrift
)

# Create a build command for each of the thrift src files and generate
# a list of files they produce
THRIFT_GEN(THRIFT_ALL_FILES ${SRC_FILES})
THRIFT_GEN_DS(THRIFT_DATA_SRC_FILES ${EXT_DATA_SRC_FILES})

add_custom_target(thrift-generated-files-error DEPENDS ErrorCodes.thrift)
add_custom_target(thrift-generated-files-metrics DEPENDS MetricDefs.thrift)
add_custom_target(thrift-generated-files-tcli-service DEPENDS ${TCLI_SERVICE_THRIFT})

# Add a custom target that generates all the thrift files
add_custom_target(thrift-cpp ALL DEPENDS ${THRIFT_ALL_FILES})
add_dependencies(thrift-cpp thrift-generated-files-metrics thrift-generated-files-error
  thrift-generated-files-tcli-service)

add_custom_target(thrift-ext-data-src ALL DEPENDS ${THRIFT_DATA_SRC_FILES})
add_dependencies(thrift-ext-data-src thrift-cpp)

# Combined target for all thrift dependencies
add_custom_target(thrift-deps ALL)
add_dependencies(thrift-deps thrift-ext-data-src)
