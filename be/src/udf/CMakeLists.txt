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


# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/udf")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_OUTPUT_ROOT_DIRECTORY}/udf")

# Build this library twice. Once to be linked into the main impalad. This version
# can have dependencies on our other libs. The second version is shipped as part
# of the UDF sdk, which can't use other libs.
add_library(Udf udf.cc udf-ir.cc udf-test-harness.cc)
add_library(ImpalaUdf udf.cc udf-ir.cc udf-test-harness.cc)
set_target_properties(ImpalaUdf PROPERTIES COMPILE_FLAGS "-DIMPALA_UDF_SDK_BUILD")

ADD_EXECUTABLE(udf-test udf-test.cc)
TARGET_LINK_LIBRARIES(udf-test ${UDF_TEST_LINK_LIBS})
ADD_TEST(udf-test "${EXECUTABLE_OUTPUT_PATH}/udf-test")

ADD_EXECUTABLE(uda-test uda-test.cc)
TARGET_LINK_LIBRARIES(uda-test ${UDF_TEST_LINK_LIBS})
ADD_TEST(uda-test "${EXECUTABLE_OUTPUT_PATH}/uda-test")

