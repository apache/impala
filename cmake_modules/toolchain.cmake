set(IMPALA_TOOLCHAIN ON)

# Set the root directory for the toolchain
set(TOOLCHAIN_ROOT $ENV{IMPALA_TOOLCHAIN})

# If Impala is built with the toolchain, change compiler and link paths
set(GCC_ROOT $ENV{IMPALA_TOOLCHAIN}/gcc-$ENV{IMPALA_GCC_VERSION})
set(CMAKE_C_COMPILER ${GCC_ROOT}/bin/gcc)
set(CMAKE_CXX_COMPILER ${GCC_ROOT}/bin/g++)

# The rpath is needed to be able to run the binaries produced by the toolchain without
# specifying an LD_LIBRARY_PATH
set(TOOLCHAIN_LINK_FLAGS "-Wl,-rpath,${GCC_ROOT}/lib64")
set(TOOLCHAIN_LINK_FLAGS "${TOOLCHAIN_LINK_FLAGS} -L${GCC_ROOT}/lib64")

message(STATUS "Setup toolchain link flags ${TOOLCHAIN_LINK_FLAGS}")
