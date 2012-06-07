# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

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
    
    add_custom_command(
      OUTPUT ${OUTPUT_BE_FILE}
      COMMAND thrift ${CPP_ARGS} ${FIL}
      COMMAND thrift ${JAVA_ARGS} ${FIL}
      DEPENDS ${ABS_FIL}
      COMMENT "Running thrift compiler on ${FIL}"
      VERBATIM
    )
  endforeach(FIL)

  set(${VAR} ${${VAR}} PARENT_SCOPE)
endfunction(THRIFT_GEN)

set(THRIFT_INCLUDE_DIR_OPTION -I ${Thrift_CONTRIB_DIR} -I $ENV{HIVE_HOME}/src/metastore/if)
set(BE_OUTPUT_DIR ${CMAKE_SOURCE_DIR}/be/generated-sources)
set(FE_OUTPUT_DIR ${CMAKE_SOURCE_DIR}/fe/generated-sources)
file(MAKE_DIRECTORY ${BE_OUTPUT_DIR})
file(MAKE_DIRECTORY ${FE_OUTPUT_DIR})

# Args passed to thrift for Java gen
set(JAVA_ARGS ${THRIFT_INCLUDE_DIR_OPTION} --gen java:hashcode -o ${FE_OUTPUT_DIR})

set (GENERATES_SRC_FILES 
  Opcodes.thrift
)

set (SRC_FILES
  beeswax.thrift
  DataSinks.thrift
  Data.thrift
  Descriptors.thrift
  Frontend.thrift
  JavaConstants.thrift
  Exprs.thrift
  ImpalaBackendService.thrift
  ImpalaPlanService.thrift
  ImpalaService.thrift
  PlanNodes.thrift
  RuntimeProfile.thrift
  Types.thrift
  ${GENERATES_SRC_FILES}
)

set_source_files_properties(${GENERATES_SRC_FILES} PROPERTIES GENERATED TRUE)

# Create a build command for each of the thrift src files and generate
# a list of files they produce
THRIFT_GEN(THRIFT_FILES ${SRC_FILES})

# Add a custom target that generates all the thrift files
add_custom_target(thrift-cpp ALL DEPENDS ${THRIFT_FILES})

