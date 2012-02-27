# - Find Thrift (a cross platform RPC lib/tool)
# This module defines
#  Thrift_VERSION, version string of ant if found
#  Thrift_INCLUDE_DIR, where to find Thrift headers
#  Thrift_LIBS, Thrift libraries
#  Thrift_FOUND, If false, do not try to use ant

exec_program(thrift ARGS -version OUTPUT_VARIABLE Thrift_VERSION
             RETURN_VALUE Thrift_RETURN)

# prefer the thrift version supplied in THRIFT_HOME
find_path(Thrift_INCLUDE_DIR Thrift.h HINTS
  $ENV{THRIFT_HOME}/include/thrift
  /usr/local/include/thrift
  /opt/local/include/thrift
)

set(Thrift_LIB_PATHS
  $ENV{THRIFT_HOME}/lib
  /usr/local/lib
  /opt/local/lib)

# prefer the thrift version supplied in THRIFT_HOME
find_library(Thrift_LIB NAMES thrift HINTS ${Thrift_LIB_PATHS})

if (Thrift_LIB)
  set(Thrift_FOUND TRUE)
  set(Thrift_LIBS ${Thrift_LIB})
else ()
  set(Thrift_FOUND FALSE)
endif ()

if (Thrift_FOUND)
  if (NOT Thrift_FIND_QUIETLY)
    message(STATUS "${Thrift_VERSION}")
  endif ()
else ()
  message(STATUS "Thrift compiler/libraries NOT found. "
          "Thrift support will be disabled (${Thrift_RETURN}, "
          "${Thrift_INCLUDE_DIR}, ${Thrift_LIB})")
endif ()

mark_as_advanced(
  Thrift_LIB
  Thrift_INCLUDE_DIR
  )
