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

# Build context depends on daemons and frontend jars
# Sending the whole impala workspace including test binaries, testdata, etc
# to the docker daemon can be very expensive, so we create a build context
# with symlinks
add_custom_target(impala_base_build_context
  COMMAND ${CMAKE_SOURCE_DIR}/docker/setup_build_context.py
  DEPENDS daemons fe ${CMAKE_SOURCE_DIR}/docker/setup_build_context.py
  COMMENT "Creating impala base build context."
  VERBATIM
)

# Target for the base Impala image.
add_custom_target(impala_base_image
  # Use tar with -h flag to assemble a tarball including all the symlinked files and
  # directories in the build context.
  COMMAND cd ${IMPALA_BASE_BUILD_CONTEXT_DIR} && tar cvh . | docker build -t impala_base -
  DEPENDS impala_base_build_context ${CMAKE_SOURCE_DIR}/docker/impala_base/Dockerfile
  DEPENDS ${CMAKE_SOURCE_DIR}/docker/daemon_entrypoint.sh
  DEPENDS ${CMAKE_SOURCE_DIR}/bin/graceful_shutdown_backends.sh
  COMMENT "Building Impala base docker image."
  VERBATIM
)

# Target to build all docker images. Dependencies are added for each docker image
# instantiated below.
add_custom_target(docker_images)

set(daemon_image_names "")

function(add_daemon_docker_image image_name)
  add_custom_target(${image_name}_image
    COMMAND cd ${CMAKE_SOURCE_DIR}/docker/${image_name} && docker build -t ${image_name} .
    DEPENDS impala_base_image ${CMAKE_SOURCE_DIR}/docker/${image_name}/Dockerfile
    COMMENT "Building ${image_name} docker image."
    VERBATIM
  )
  ADD_DEPENDENCIES(docker_images ${image_name}_image)
  set(daemon_image_names "${daemon_image_names} ${image_name}" PARENT_SCOPE)
endfunction()

# Command and target for the various Impalad images.
add_daemon_docker_image(impalad_coord_exec)
add_daemon_docker_image(impalad_coordinator)
add_daemon_docker_image(impalad_executor)
add_daemon_docker_image(catalogd)
add_daemon_docker_image(statestored)

# Generate a test file with all the daemon images.
file(WRITE ${CMAKE_SOURCE_DIR}/docker/docker-images.txt "${daemon_image_names}")
