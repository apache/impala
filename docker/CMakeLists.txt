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

set(IMPALA_UTILITY_BUILD_CONTEXT_DIR
  ${CMAKE_SOURCE_DIR}/docker/build_context_utility
)

set(DOCKER_BUILD ${CMAKE_SOURCE_DIR}/docker/docker-build.sh)

find_program(LSB_RELEASE_EXEC lsb_release)
execute_process(COMMAND ${LSB_RELEASE_EXEC} -is
  OUTPUT_VARIABLE LSB_RELEASE_ID
  OUTPUT_STRIP_TRAILING_WHITESPACE
)
execute_process(COMMAND ${LSB_RELEASE_EXEC} -rs
  OUTPUT_VARIABLE LSB_RELEASE_VERSION
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

set(QUICKSTART_BASE_IMAGE "UNSUPPORTED")
set(DISTRO_BASE_IMAGE "UNSUPPORTED")

if(${LSB_RELEASE_ID} STREQUAL "Ubuntu")
  if(${LSB_RELEASE_VERSION} STREQUAL "16.04" OR
     ${LSB_RELEASE_VERSION} STREQUAL "18.04" OR
     ${LSB_RELEASE_VERSION} STREQUAL "20.04")
    set(DISTRO_BASE_IMAGE "ubuntu:${LSB_RELEASE_VERSION}")
    set(QUICKSTART_BASE_IMAGE "ubuntu:${LSB_RELEASE_VERSION}")
  endif()
  if (${LSB_RELEASE_VERSION} STREQUAL "16.04" OR
      ${LSB_RELEASE_VERSION} STREQUAL "18.04")
    set(PIP "python-pip")
  elseif (${LSB_RELEASE_VERSION} STREQUAL "20.04")
    set(PIP "python3-pip")
  endif()
elseif(${LSB_RELEASE_ID} STREQUAL "RedHatEnterpriseServer" OR
       ${LSB_RELEASE_ID} STREQUAL "RedHatEnterprise" OR
       ${LSB_RELEASE_ID} STREQUAL "Rocky" OR
       ${LSB_RELEASE_ID} STREQUAL "AlmaLinux" OR
       ${LSB_RELEASE_ID} STREQUAL "CentOS")
  # The Quickstart images currently don't support using a Redhat
  # base image, so this doesn't set QUICKSTART_BASE_IMAGE.
  if(${LSB_RELEASE_VERSION} MATCHES "7.*")
    set(DISTRO_BASE_IMAGE "$ENV{IMPALA_REDHAT7_DOCKER_BASE}")
  elseif(${LSB_RELEASE_VERSION} MATCHES "8.*")
    set(DISTRO_BASE_IMAGE "$ENV{IMPALA_REDHAT8_DOCKER_BASE}")
  endif()
endif()
MESSAGE(STATUS "Picked docker base image based on host OS: ${DISTRO_BASE_IMAGE}")


if (NOT ${DISTRO_BASE_IMAGE} STREQUAL "UNSUPPORTED")
  # Add a target to build a base docker image for 'build_type'. 'build_context_args' are
  # passed to the setup_build_context.py script.
  function(add_base_image build_type build_context_args)
    # Build context depends on daemons and frontend jars.
    # Sending the whole impala workspace including test binaries, testdata, etc
    # to the docker daemon can be very expensive, so we create a build context
    # with symlinks
    add_custom_target(impala_base_build_context_${build_type}
      COMMAND ${CMAKE_SOURCE_DIR}/docker/setup_build_context.py ${build_context_args}
      DEPENDS daemons java ${CMAKE_SOURCE_DIR}/docker/setup_build_context.py
      COMMENT "Creating impala base build context build_type=${build_type}."
      VERBATIM
    )
    # The base image Dockerfile takes an argument INSTALL_OS_PACKAGES_ARGS to specify
    # arguments for install_os_packages.sh. This can can be set to "--install-debug-tools"
    # to add extra utilities. Enable this for debug builds.
    set(INSTALL_OS_PACKAGES_ARGS "")
    if (${build_type} STREQUAL "debug")
      set(INSTALL_OS_PACKAGES_ARGS "--install-debug-tools")
    endif()
    if ($ENV{IMPALA_DOCKER_USE_JAVA11} STREQUAL "true")
      set(INSTALL_OS_PACKAGES_ARGS "${INSTALL_OS_PACKAGES_ARGS} --use-java11")
    endif()
    # Target for the base Impala image.
    add_custom_target(impala_base_image_${build_type}
      # Run docker build inside the build context directory so that all dependencies are
      # sent to the docker daemon. This allows the Dockerfile built to copy all necessary
      # dependencies.
      COMMAND tar cvh . -C ${CMAKE_SOURCE_DIR}/docker/impala_base/ . |
              ${DOCKER_BUILD} -t impala_base_${build_type}
                --build-arg BASE_IMAGE=${DISTRO_BASE_IMAGE}
                --build-arg INSTALL_OS_PACKAGES_ARGS=${INSTALL_OS_PACKAGES_ARGS} -
      WORKING_DIRECTORY ${IMPALA_BASE_BUILD_CONTEXT_DIR}/${build_type}
      DEPENDS impala_base_build_context_${build_type} ${CMAKE_SOURCE_DIR}/docker/impala_base/Dockerfile
      DEPENDS ${CMAKE_SOURCE_DIR}/docker/daemon_entrypoint.sh
      DEPENDS ${CMAKE_SOURCE_DIR}/bin/graceful_shutdown_backends.sh
      DEPENDS ${CMAKE_SOURCE_DIR}/docker/install_os_packages.sh
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
  add_custom_target(quickstart_docker_images)

  set(exported_image_names "")

  # Add a target with name 'target' to build a daemon image for the daemon with
  # name 'daemon_name', e.g. "impalad_executor". The image is tagged as 'image_name'.
  # 'build_type' should be debug or release and determines which base image is used.
  function(add_daemon_docker_image target daemon_name image_name build_type)
    set(build_dir ${CMAKE_SOURCE_DIR}/docker/${daemon_name})
    add_custom_target(${target}
      # Supply the appropriate base image as an argument for the Dockerfile. The same
      # build context used for the base image is used for each daemon image. This allows
      # each daemon image to only copy in the dependencies it requires.
      COMMAND tar cvh . -C ${CMAKE_SOURCE_DIR}/docker/${daemon_name}/ . |
              ${DOCKER_BUILD} --build-arg BASE_IMAGE=impala_base_${build_type}
                  -t ${image_name} -
      WORKING_DIRECTORY ${IMPALA_BASE_BUILD_CONTEXT_DIR}/${build_type}
      DEPENDS impala_base_image_${build_type} ${build_dir}/Dockerfile
      COMMENT "Building ${image_name} docker image."
      VERBATIM
    )
    set(exported_image_names "${exported_image_names} ${image_name}" PARENT_SCOPE)
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
    set(exported_image_names "${exported_image_names} ${release_image}" PARENT_SCOPE)
  endfunction()

  # Stamp out image targets for all of the Impala daemons.
  add_daemon_docker_images(impalad_coord_exec)
  add_daemon_docker_images(impalad_coordinator)
  add_daemon_docker_images(impalad_executor)
  add_daemon_docker_images(catalogd)
  add_daemon_docker_images(statestored)
  add_daemon_docker_images(admissiond)

  # Quickstart is only supported on Ubuntu, so skip generating the Quickstart targets
  # on other distributions.
  # TODO: Support Quickstart images on Redhat
  if (NOT ${QUICKSTART_BASE_IMAGE} STREQUAL "UNSUPPORTED")
    # HMS quickstart image, which requires Hive and Hadoop builds.
    set(QUICKSTART_HMS_IMAGE impala_quickstart_hms)
    set(quickstart_hms_build_dir ${CMAKE_SOURCE_DIR}/docker/quickstart_hms)
    add_custom_target(quickstart_hms_build_setup
      COMMAND rm -f ${quickstart_hms_build_dir}/hive ${quickstart_hms_build_dir}/hadoop
      COMMAND ${CMAKE_COMMAND} -E create_symlink $ENV{HIVE_HOME} ${quickstart_hms_build_dir}/hive
      COMMAND ${CMAKE_COMMAND} -E create_symlink $ENV{HADOOP_HOME} ${quickstart_hms_build_dir}/hadoop
    )
    add_custom_target(quickstart_hms_image
      # Supply the appropriate base image as an argument for the Dockerfile.
      # Use tar with -h flag to assemble a tarball including all the symlinked files and
      # directories in the build context.
      COMMAND tar cvh . -C ${quickstart_hms_build_dir} . | ${DOCKER_BUILD} --build-arg BASE_IMAGE=${QUICKSTART_BASE_IMAGE} -t ${QUICKSTART_HMS_IMAGE} -
      DEPENDS ${quickstart_hms_build_dir}/Dockerfile quickstart_hms_build_setup
      COMMENT "Building quickstart HMS docker image."
      VERBATIM
    )
    ADD_DEPENDENCIES(quickstart_docker_images quickstart_hms_image)
    set(exported_image_names "${exported_image_names} ${QUICKSTART_HMS_IMAGE}")

    # Client quickstart image, which only requires some scripts.
    set(QUICKSTART_CLIENT_IMAGE impala_quickstart_client)
    set(quickstart_client_build_dir ${CMAKE_SOURCE_DIR}/docker/quickstart_client)
    add_custom_target(quickstart_client_image
      # Supply the appropriate base image as an argument for the Dockerfile.
      # Use tar with -h flag to assemble a tarball including all the symlinked files and
      # directories in the build context.
      COMMAND tar cvh . -C ${quickstart_client_build_dir} . | ${DOCKER_BUILD} ${COMMON_DOCKER_BUILD_ARGS} --build-arg BASE_IMAGE=${QUICKSTART_BASE_IMAGE} --build-arg PIP=${PIP} -t ${QUICKSTART_CLIENT_IMAGE} -
      DEPENDS ${quickstart_client_build_dir}/Dockerfile ${quickstart_client_build_dir}/data-load-entrypoint.sh
      COMMENT "Building quickstart client docker image."
      VERBATIM
    )
    ADD_DEPENDENCIES(quickstart_docker_images quickstart_client_image)
    set(exported_image_names "${exported_image_names} ${QUICKSTART_CLIENT_IMAGE}")
  endif()

  # Add a target to build utility docker images for 'build_type'. 'build_context_args' are
  # passed to the setup_build_context.py script.
  function(add_utility_images profile_tool_target profile_tool_image build_type build_context_args)
    # Build context depends on daemons and frontend jars.
    # Sending the whole impala workspace including test binaries, testdata, etc
    # to the docker daemon can be very expensive, so we create a build context
    # with symlinks
    add_custom_target(impala_utility_build_context_${build_type}
      COMMAND ${CMAKE_SOURCE_DIR}/docker/setup_build_context.py ${build_context_args} --utility-context
      DEPENDS impala-profile-tool ${CMAKE_SOURCE_DIR}/docker/setup_build_context.py
      COMMENT "Creating impala utility build context build_type=${build_type}."
      VERBATIM
    )
    # Target for the Impala profile tool image.
    add_custom_target(${profile_tool_target}
      # Run docker build inside the build context directory so that all dependencies are
      # sent to the docker daemon. This allows the Dockerfile build to copy all necessary
      # dependencies.
      # Note: This currently does not specify INSTALL_OS_PACKAGES_ARGS, so it uses the
      # default value, which installs extra debugging tools.
      COMMAND tar cvh . -C ${CMAKE_SOURCE_DIR}/docker/impala_profile_tool/ . |
              ${DOCKER_BUILD} -t ${profile_tool_image}
                  --build-arg BASE_IMAGE=${DISTRO_BASE_IMAGE} -
      WORKING_DIRECTORY ${IMPALA_UTILITY_BUILD_CONTEXT_DIR}/${build_type}
      DEPENDS impala_utility_build_context_${build_type} ${CMAKE_SOURCE_DIR}/docker/impala_profile_tool/Dockerfile
      DEPENDS ${CMAKE_SOURCE_DIR}/docker/utility_entrypoint.sh
      DEPENDS ${CMAKE_SOURCE_DIR}/docker/install_os_packages.sh
      COMMENT "Building Impala profile tool docker image build_type=${build_type}."
      VERBATIM
    )
    set(exported_image_names "${exported_image_names} ${profile_tool_image}" PARENT_SCOPE)
  endfunction()
  add_utility_images(impala_profile_tool_image impala_profile_tool release "")
  add_utility_images(impala_profile_tool_debug_image impala_profile_tool_debug debug "--debug-build")

  ADD_DEPENDENCIES(docker_images impala_profile_tool_image)
  ADD_DEPENDENCIES(docker_debug_images impala_profile_tool_debug_image)

  # Generate a text file with all of the release daemon images.
  file(WRITE ${CMAKE_SOURCE_DIR}/docker/docker-images.txt "${exported_image_names}")
endif()
