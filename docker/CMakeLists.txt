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

# Add a target to build a base docker image for 'build_type'. 'build_context_args' are
# passed to the setup_build_context.py script.
function(add_base_image build_type build_context_args)
  # Build context depends on daemons and frontend jars
  # Sending the whole impala workspace including test binaries, testdata, etc
  # to the docker daemon can be very expensive, so we create a build context
  # with symlinks
  add_custom_target(impala_base_build_context_${build_type}
    COMMAND ${CMAKE_SOURCE_DIR}/docker/setup_build_context.py ${build_context_args}
    DEPENDS daemons fe ${CMAKE_SOURCE_DIR}/docker/setup_build_context.py
    COMMENT "Creating impala base build context build_type=${build_type}."
    VERBATIM
  )
  # Target for the base Impala image.
  add_custom_target(impala_base_image_${build_type}
    # Use tar with -h flag to assemble a tarball including all the symlinked files and
    # directories in the build context.
    COMMAND cd ${IMPALA_BASE_BUILD_CONTEXT_DIR}/${build_type} && tar cvh . | docker build -t impala_base_${build_type} -
    DEPENDS impala_base_build_context_${build_type} ${CMAKE_SOURCE_DIR}/docker/impala_base/Dockerfile
    DEPENDS ${CMAKE_SOURCE_DIR}/docker/daemon_entrypoint.sh
    DEPENDS ${CMAKE_SOURCE_DIR}/bin/graceful_shutdown_backends.sh
    COMMENT "Building Impala base docker image build_type=${build_type}."
    VERBATIM
  )
endfunction()
add_base_image(release "")
add_base_image(debug "--debug-build")

# Target to build all docker images. Dependencies are added for each docker image
# instantiated below.
add_custom_target(docker_images)
add_custom_target(docker_debug_images)

set(daemon_image_names "")

# Add a target with name 'target' to build a daemon image for the daemon with
# name 'daemon_name', e.g. "impalad_executor". The image is tagged as 'image_name'.
# 'build_type' should be debug or release and determines which base image is used.
function(add_daemon_docker_image target daemon_name image_name build_type)
  set(build_dir ${CMAKE_SOURCE_DIR}/docker/${daemon_name})
  add_custom_target(${target}
    # Supply the appropriate base image as an argument for the Dockerfile.
    COMMAND cd ${build_dir} && docker build --build-arg BASE_IMAGE=impala_base_${build_type} -t ${image_name} .
    DEPENDS impala_base_image_${build_type} ${build_dir}/Dockerfile
    COMMENT "Building ${image_name} docker image."
    VERBATIM
  )
  set(daemon_image_names "${daemon_image_names} ${image_name}" PARENT_SCOPE)
endfunction()

# Add debug and release docker image targets for the given daemon e.g. if called
# with "statestored", targets "statestored_image" and "statestored_debug_image"
# are added.
function(add_daemon_docker_images daemon_name)
  set(release_image ${daemon_name})
  set(release_target ${daemon_name}_image)
  set(debug_image ${daemon_name}_debug)
  set(debug_target ${daemon_name}_debug_image)
  add_daemon_docker_image(${release_target} ${daemon_name} ${release_image} release)
  add_daemon_docker_image(${debug_target} ${daemon_name} ${debug_image} debug)
  ADD_DEPENDENCIES(docker_images ${release_target})
  ADD_DEPENDENCIES(docker_debug_images ${debug_target})
  set(daemon_image_names "${daemon_image_names} ${release_image}" PARENT_SCOPE)
endfunction()

# Stamp out image targets for all of the Impala daemons.
add_daemon_docker_images(impalad_coord_exec)
add_daemon_docker_images(impalad_coordinator)
add_daemon_docker_images(impalad_executor)
add_daemon_docker_images(catalogd)
add_daemon_docker_images(statestored)

# Generate a text file with all of the release daemon images.
file(WRITE ${CMAKE_SOURCE_DIR}/docker/docker-images.txt "${daemon_image_names}")
