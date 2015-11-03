# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


cmake_minimum_required(VERSION 2.6)

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
  foreach(FIL ${ARGN})
    # Get full path
    get_filename_component(ABS_FIL ${FIL} ABSOLUTE)
    # Get basename
    get_filename_component(FIL_WE ${FIL} NAME_WE)

    # All the output files we can determine based on filename.
    #   - Does not include .skeleton.cpp files
    #   - Does not include java output files
    set(OUTPUT_BE_FILE "${BE_OUTPUT_DIR}/gen-cpp/${FIL_WE}_types.cpp")
    set(OUTPUT_BE_FILE ${OUTPUT_BE_FILE} "${BE_OUTPUT_DIR}/gen-cpp/${FIL_WE}_types.h")
    set(OUTPUT_BE_FILE ${OUTPUT_BE_FILE} "${BE_OUTPUT_DIR}/gen-cpp/${FIL_WE}_constants.cpp")
    set(OUTPUT_BE_FILE ${OUTPUT_BE_FILE} "${BE_OUTPUT_DIR}/gen-cpp/${FIL_WE}_constants.h")
    list(APPEND ${VAR} ${OUTPUT_BE_FILE})

    # BeeswaxService thrift generation
    # It depends on hive_meta_store, which in turn depends on fb303.
    # The java dependency is handled by maven.
    # We need to generate C++ src file for the parent dependencies using the "-r" option.
    set(CPP_ARGS ${THRIFT_INCLUDE_DIR_OPTION} --gen cpp -o ${BE_OUTPUT_DIR})
    IF (FIL STREQUAL "beeswax.thrift")
      set(CPP_ARGS -r ${THRIFT_INCLUDE_DIR_OPTION} --gen cpp -o ${BE_OUTPUT_DIR})
    ENDIF(FIL STREQUAL "beeswax.thrift")

    IF (FIL STREQUAL "TCLIService.thrift" OR FIL STREQUAL "parquet.thrift" OR
        FIL STREQUAL "cli_service.thrift")
      # HiveServer2 and Parquet thrift generation
      # Do not generate Java source because we should use the jar from Hive or Parquet
      add_custom_command(
        OUTPUT ${OUTPUT_BE_FILE}
        COMMAND ${THRIFT_COMPILER} ${CPP_ARGS} ${FIL}
        COMMAND ${THRIFT_COMPILER} ${PYTHON_ARGS} ${FIL}
        DEPENDS ${ABS_FIL}
        COMMENT "Running thrift compiler on ${FIL}"
        VERBATIM
      )
    ELSE (FIL STREQUAL "TCLIService.thrift" OR FIL STREQUAL "parquet.thrift" OR
          FIL STREQUAL "cli_service.thrift")
      add_custom_command(
        OUTPUT ${OUTPUT_BE_FILE}
        COMMAND ${THRIFT_COMPILER} ${CPP_ARGS} ${FIL}
        COMMAND ${THRIFT_COMPILER} ${JAVA_FE_ARGS} ${FIL}
        COMMAND ${THRIFT_COMPILER} ${PYTHON_ARGS} ${FIL}
        DEPENDS ${ABS_FIL}
        COMMENT "Running thrift compiler on ${FIL}"
        VERBATIM
    )
    ENDIF(FIL STREQUAL "TCLIService.thrift" OR FIL STREQUAL "parquet.thrift" OR
          FIL STREQUAL "cli_service.thrift")
  endforeach(FIL)

  set(${VAR} ${${VAR}} PARENT_SCOPE)
endfunction(THRIFT_GEN)

function(THRIFT_GEN_DS VAR)
  IF (NOT ARGN)
    MESSAGE(SEND_ERROR "Error: THRIFT_GEN_DS called without any src files")
    RETURN()
  ENDIF(NOT ARGN)

  set(${VAR})
  foreach(FIL ${ARGN})
    get_filename_component(ABS_FIL ${FIL} ABSOLUTE)
    get_filename_component(FIL_WE ${FIL} NAME_WE)

    # This isn't the exact output file that's created, just a unique file
    set(OUTPUT_FILE "${EXT_DS_OUTPUT_DIR}/${FIL_WE}.java")
    list(APPEND ${VAR} ${OUTPUT_FILE})
    add_custom_command(
      OUTPUT ${OUTPUT_FILE}
      COMMAND ${THRIFT_COMPILER} ${JAVA_EXT_DS_ARGS} ${FIL}
      DEPENDS ${ABS_FIL}
      COMMENT "Running thrift compiler for ext-data-source on ${FIL}"
      VERBATIM
    )
  endforeach(FIL)
  set(${VAR} ${${VAR}} PARENT_SCOPE)
endfunction(THRIFT_GEN_DS)

message("Using Thrift compiler: ${THRIFT_COMPILER}")
set(THRIFT_INCLUDE_DIR_OPTION -I ${THRIFT_CONTRIB_DIR} -I $ENV{HIVE_HOME}/src/metastore/if)
set(BE_OUTPUT_DIR ${CMAKE_SOURCE_DIR}/be/generated-sources)
set(FE_OUTPUT_DIR ${CMAKE_SOURCE_DIR}/fe/generated-sources)
# TODO: avoid duplicating generated java classes
set(EXT_DS_OUTPUT_DIR ${CMAKE_SOURCE_DIR}/ext-data-source/api/generated-sources)
set(PYTHON_OUTPUT_DIR ${CMAKE_SOURCE_DIR}/shell/)
MESSAGE("Found output dir: " ${PYTHON_OUTPUT_DIR})
file(MAKE_DIRECTORY ${BE_OUTPUT_DIR})
file(MAKE_DIRECTORY ${FE_OUTPUT_DIR})
file(MAKE_DIRECTORY ${EXT_DS_OUTPUT_DIR})
file(MAKE_DIRECTORY ${PYTHON_OUTPUT_DIR})

# Args passed to thrift for Java gen
set(JAVA_FE_ARGS ${THRIFT_INCLUDE_DIR_OPTION} --gen java:hashcode -o ${FE_OUTPUT_DIR})
set(JAVA_EXT_DS_ARGS ${THRIFT_INCLUDE_DIR_OPTION} --gen java:hashcode -o ${EXT_DS_OUTPUT_DIR})
set(PYTHON_ARGS ${THRIFT_INCLUDE_DIR_OPTION} -r --gen py -o ${PYTHON_OUTPUT_DIR})

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
  CatalogInternalService.thrift
  CatalogObjects.thrift
  CatalogService.thrift
  TCLIService.thrift
  cli_service.thrift # TODO: Remove when thirdparty Hive supports V6
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
  Llama.thrift
  Logging.thrift
  NetworkTest.thrift
  MetricDefs.thrift
  Metrics.thrift
  PlanNodes.thrift
  Planner.thrift
  Partitions.thrift
  parquet.thrift
  ResourceBrokerService.thrift
  Results.thrift
  RuntimeProfile.thrift
  StatestoreService.thrift
  ${EXT_DATA_SRC_FILES}
)

SET_SOURCE_FILES_PROPERTIES(Status.thrift PROPERTIES OBJECT_DEPENDS ErrorCodes.thrift)

add_custom_command(OUTPUT ErrorCodes.thrift
  COMMAND python generate_error_codes.py
  DEPENDS generate_error_codes.py)

add_custom_command(OUTPUT MetricDefs.thrift
  COMMAND python generate_metrics.py
  DEPENDS generate_metrics.py metrics.json)

# Create a build command for each of the thrift src files and generate
# a list of files they produce
THRIFT_GEN(THRIFT_ALL_FILES ${SRC_FILES})
THRIFT_GEN_DS(THRIFT_DATA_SRC_FILES ${EXT_DATA_SRC_FILES})


add_custom_target(thrift-generated-files-error DEPENDS ErrorCodes.thrift)
add_custom_target(thrift-generated-files-metrics DEPENDS MetricDefs.thrift)

# Add a custom target that generates all the thrift files
add_custom_target(thrift-cpp ALL DEPENDS ${THRIFT_ALL_FILES})
add_dependencies(thrift-cpp thrift-generated-files-metrics thrift-generated-files-error)

add_custom_target(thrift-ext-data-src ALL DEPENDS ${THRIFT_DATA_SRC_FILES})
add_dependencies(thrift-ext-data-src thrift-cpp)

# Combined target for all thrift dependencies
add_custom_target(thrift-deps ALL)
add_dependencies(thrift-deps thrift-ext-data-src)
