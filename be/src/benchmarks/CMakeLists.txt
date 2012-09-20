# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/benchmarks")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/benchmarks")

add_executable(parse-timestamp-benchmark parse-timestamp-benchmark.cc)
add_executable(string-search-benchmark string-search-benchmark.cc)
add_executable(atof-benchmark atof-benchmark.cc)
add_executable(atoi-benchmark atoi-benchmark.cc)
add_executable(hash-benchmark hash-benchmark.cc)
add_executable(thread-create-benchmark thread-create-benchmark.cc)

target_link_libraries(parse-timestamp-benchmark ${IMPALA_LINK_LIBS})
target_link_libraries(string-search-benchmark ${IMPALA_LINK_LIBS})
target_link_libraries(atof-benchmark ${IMPALA_LINK_LIBS})
target_link_libraries(atoi-benchmark ${IMPALA_LINK_LIBS})
target_link_libraries(hash-benchmark Experiments ${IMPALA_LINK_LIBS})
target_link_libraries(thread-create-benchmark ${IMPALA_LINK_LIBS})

