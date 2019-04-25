# Docker-related scripts for Impala

`test-with-docker.py` runs the Impala build and tests inside of Docker
containers, parallelizing the test execution across test suites. See that file
for more details.

This also contains infrastructure to build `impala_base`, `catalogd`,
``statestored`, `impalad_coordinator`, `impalad_executor` and
`impalad_coord_exec` container images from the output of an Impala build.
The containers can be built via the CMake target docker_images. See
CMakeLists.txt for the build targets.
