# Copyright (c) 2009-2010 Volvox Development Team
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#
# Author: Konstantin Lepa <konstantin.lepa@gmail.com>
#
# Find the Google Test Framework
#
# GTEST_ROOT hints a location
#
# This module defines
# GTEST_INCLUDE_DIR, where to find gtest include files, etc.
# GTEST_LIBRARIES, the libraries to link against to use gtest.
# GTEST_FOUND, If false, do not try to use gtest.
# GTEST_STATIC_LIB, path to libgtest.a.
# GTEST_SHARED_LIB, path to libgtest.so.

set(GTEST_H gtest/gtest.h)

find_path(GTEST_INCLUDE_DIR ${GTEST_H}
  PATHS ${GTEST_ROOT}/include
        NO_DEFAULT_PATH
  DOC   "Path to the ${GTEST_H} file"
)

find_library(GTEST_STATIC_LIB NAMES libgtest.a
  PATHS ${GTEST_ROOT}/lib
        NO_DEFAULT_PATH
  DOC   "Google's framework for writing C++ tests (gtest)"
)

find_library(GTEST_SHARED_LIB NAMES libgtest.so
  PATHS ${GTEST_ROOT}/lib
        NO_DEFAULT_PATH
  DOC   "Google's framework for writing C++ tests (gtest)"
)

find_library(GTEST_MAIN_LIBRARY NAMES libgtest_main.a
  PATHS ${GTEST_ROOT}/lib
        NO_DEFAULT_PATH
  DOC   "Google's framework for writing C++ tests (gtest_main)"
)

if(GTEST_INCLUDE_DIR AND GTEST_STATIC_LIB AND GTEST_SHARED_LIB AND GTEST_MAIN_LIBRARY)
  set(GTEST_LIBRARIES ${GTEST_STATIC_LIB} ${GTEST_SHARED_LIB} ${GTEST_MAIN_LIBRARY})
  set(GTEST_FOUND TRUE)
else(GTEST_INCLUDE_DIR AND GTEST_STATIC_LIB AND GTEST_SHARED_LIB AND GTEST_MAIN_LIBRARY)
  set(GTEST_FOUND FALSE)
endif(GTEST_INCLUDE_DIR AND GTEST_STATIC_LIB AND GTEST_SHARED_LIB AND GTEST_MAIN_LIBRARY)

if(GTEST_FOUND)
  if(NOT GTEST_FIND_QUIETLY)
    message(STATUS "Found GTest: ${GTEST_LIBRARIES}")
  endif(NOT GTEST_FIND_QUIETLY)
else(GTEST_FOUND)
  message(FATAL_ERROR "Could not find the GTest Library")
endif(GTEST_FOUND)

mark_as_advanced(
  GTEST_INCLUDE_DIR
  GTEST_LIBRARIES
  GTEST_STATIC_LIB
  GTEST_SHARED_LIB
)
