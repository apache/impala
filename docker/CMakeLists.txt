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

set(IMPALA_BASE_BUILD_CONTEXT_DIR
  ${CMAKE_SOURCE_DIR}/docker/build_context
)
set(IMPALA_BASE_BUILD_CONTEXT_TIMESTAMP
  ${IMPALA_BASE_BUILD_CONTEXT_DIR}/.timestamp
)

# Build context depends on daemons, frontend jars and test configurations.
# Sending the whole impala workspace including test binaries, testdata, etc
# to the docker daemon can be very expensive, so we create a build context
# with symlinks
add_custom_command(
  OUTPUT ${IMPALA_BASE_BUILD_CONTEXT_TIMESTAMP}
  COMMAND ${CMAKE_SOURCE_DIR}/docker/setup_build_context.py
  COMMAND touch ${IMPALA_BASE_BUILD_CONTEXT_TIMESTAMP}
  DEPENDS daemons fe ${CMAKE_SOURCE_DIR}/docker/setup_build_context.py
  COMMENT "Creating impala base build context."
  VERBATIM
)
add_custom_target(impala_base_build_context
  DEPENDS ${IMPALA_BASE_BUILD_CONTEXT_TIMESTAMP}
)

# Command and target for the base Impala image.
set(IMPALA_BASE_IMAGE_TIMESTAMP
  ${CMAKE_SOURCE_DIR}/docker/.impala_base_image_timestamp
)
add_custom_command(
  OUTPUT ${IMPALA_BASE_IMAGE_TIMESTAMP}
  # Use tar with -h flag to assemble a tarball including all the symlinked files and
  # directories in the build context.
  COMMAND cd ${IMPALA_BASE_BUILD_CONTEXT_DIR} && tar cvh . | docker build -t impala_base -
  COMMAND touch ${IMPALA_BASE_IMAGE_TIMESTAMP}
  DEPENDS impala_base_build_context ${CMAKE_SOURCE_DIR}/docker/impala_base/Dockerfile
  DEPENDS ${CMAKE_SOURCE_DIR}/docker/daemon_entrypoint.sh
  COMMENT "Building Impala base docker image."
  VERBATIM
)
add_custom_target(impala_base_image
  DEPENDS ${IMPALA_BASE_IMAGE_TIMESTAMP}
)

# Command and target for the various Impalad images.
set(COORD_EXEC_IMAGE_TIMESTAMP
  ${CMAKE_SOURCE_DIR}/docker/.coord_exec_image_timestamp
)
add_custom_command(
  OUTPUT ${COORD_EXEC_IMAGE_TIMESTAMP}
  COMMAND cd ${CMAKE_SOURCE_DIR}/docker/coord_exec && docker build -t impalad_coord_exec .
  COMMAND touch ${COORD_EXEC_IMAGE_TIMESTAMP}
  DEPENDS impala_base_image ${CMAKE_SOURCE_DIR}/docker/coord_exec/Dockerfile
  COMMENT "Building Impalad Coord Exec docker image."
  VERBATIM
)
add_custom_target(coord_exec_image
  DEPENDS ${COORD_EXEC_IMAGE_TIMESTAMP}
)
set(COORDINATOR_IMAGE_TIMESTAMP
  ${CMAKE_SOURCE_DIR}/docker/.coordinator_image_timestamp
)
add_custom_command(
  OUTPUT ${COORDINATOR_IMAGE_TIMESTAMP}
  COMMAND cd ${CMAKE_SOURCE_DIR}/docker/coordinator && docker build -t impalad_coordinator .
  COMMAND touch ${COORDINATOR_IMAGE_TIMESTAMP}
  DEPENDS impala_base_image ${CMAKE_SOURCE_DIR}/docker/coordinator/Dockerfile
  COMMENT "Building Impalad Coordinator docker image."
  VERBATIM
)
add_custom_target(coordinator_image
  DEPENDS ${COORDINATOR_IMAGE_TIMESTAMP}
)
set(EXECUTOR_IMAGE_TIMESTAMP
  ${CMAKE_SOURCE_DIR}/docker/.executor_image_timestamp
)
add_custom_command(
  OUTPUT ${EXECUTOR_IMAGE_TIMESTAMP}
  COMMAND cd ${CMAKE_SOURCE_DIR}/docker/executor && docker build -t impalad_executor .
  COMMAND touch ${EXECUTOR_IMAGE_TIMESTAMP}
  DEPENDS impala_base_image ${CMAKE_SOURCE_DIR}/docker/executor/Dockerfile
  COMMENT "Building Impalad Executor docker image."
  VERBATIM
)
add_custom_target(executor_image
  DEPENDS ${EXECUTOR_IMAGE_TIMESTAMP}
)

# Command and target for the Statestored image.
set(STATESTORED_IMAGE_TIMESTAMP
  ${CMAKE_SOURCE_DIR}/docker/.statestored_image_timestamp
)
add_custom_command(
  OUTPUT ${STATESTORED_IMAGE_TIMESTAMP}
  COMMAND cd ${CMAKE_SOURCE_DIR}/docker/statestored && docker build -t statestored .
  COMMAND touch ${STATESTORED_IMAGE_TIMESTAMP}
  DEPENDS impala_base_image ${CMAKE_SOURCE_DIR}/docker/statestored/Dockerfile
  COMMENT "Building Statestored docker image."
  VERBATIM
)
add_custom_target(statestored_image
  DEPENDS ${STATESTORED_IMAGE_TIMESTAMP}
)

# Command and target for the Catalogd image.
set(CATALOGD_IMAGE_TIMESTAMP
  ${CMAKE_SOURCE_DIR}/docker/.catalogd_image_timestamp
)
add_custom_command(
  OUTPUT ${CATALOGD_IMAGE_TIMESTAMP}
  COMMAND cd ${CMAKE_SOURCE_DIR}/docker/catalogd && docker build -t catalogd .
  COMMAND touch ${CATALOGD_IMAGE_TIMESTAMP}
  DEPENDS impala_base_image ${CMAKE_SOURCE_DIR}/docker/catalogd/Dockerfile
  COMMENT "Building Catalogd docker image."
  VERBATIM
)
add_custom_target(catalogd_image
  DEPENDS ${CATALOGD_IMAGE_TIMESTAMP}
)
add_custom_target(docker_images
  DEPENDS coord_exec_image coordinator_image executor_image
  DEPENDS statestored_image catalogd_image
)
