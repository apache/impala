# Copyright (c) 2011 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/service")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/service")

add_library(Service
  impala-server.cc
  jni-coordinator.cc
)

# TODO: why do we need this
target_link_libraries(Service
  TestUtil
)

add_library(backend SHARED
  backend.cc
  impala-server.cc
  jni-coordinator.cc
)

target_link_libraries(backend ${IMPALA_LINK_LIBS})

add_executable(runquery
  run-query.cc
)

message(STATUS, "IMPALA_LINK_LIBS: ${IMPALA_LINK_LIBS}")

# Must not include any source libraries other than shim-backend.cc;
# used only to avoid issues when loading backend code twice. See
# IMP-75
# TODO: Remove this, and libbackend, entirely when PlanService is
# launched from C++ like Impalad. 
add_library(shimbackend SHARED shim-backend.cc)

target_link_libraries(shimbackend boost_thread-mt)

target_link_libraries(runquery
  # Library intercepts signal handlers and sets them properly for running the JVM.
  # Must be before other libraries.
  ${JAVA_JSIG_LIBRARY}
  ${IMPALA_LINK_LIBS}
  # this is commented out from IMPALA_LINK_LIBS because it causes crashes in libbackend.so
  # see Imp-39
  tcmallocstatic
)

add_executable(impalad
  impalad-main.cc
  impala-server.cc
)

target_link_libraries(impalad
  ${JAVA_JSIG_LIBRARY}
  ${IMPALA_LINK_LIBS}
# does this crash the FE tests?
#tcmallocstatic
)
