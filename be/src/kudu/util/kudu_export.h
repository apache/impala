// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// This file is generated during Kudu's build. Instead of recreating the necessary steps
// in Impala's build process, we copy it into our repository. See
// kudu/client/CMakeLists.txt in Kudu's repository for details.

#ifndef KUDU_EXPORT_H
#define KUDU_EXPORT_H

#ifdef KUDU_STATIC_DEFINE
#  define KUDU_EXPORT
#  define KUDU_NO_EXPORT
#else
#  ifndef KUDU_EXPORT
#    ifdef kudu_client_exported_EXPORTS
        /* We are building this library */
#      define KUDU_EXPORT __attribute__((visibility("default")))
#    else
        /* We are using this library */
#      define KUDU_EXPORT __attribute__((visibility("default")))
#    endif
#  endif

#  ifndef KUDU_NO_EXPORT
#    define KUDU_NO_EXPORT __attribute__((visibility("hidden")))
#  endif
#endif

#ifndef KUDU_DEPRECATED
#  define KUDU_DEPRECATED __attribute__ ((__deprecated__))
#endif

#ifndef KUDU_DEPRECATED_EXPORT
#  define KUDU_DEPRECATED_EXPORT KUDU_EXPORT KUDU_DEPRECATED
#endif

#ifndef KUDU_DEPRECATED_NO_EXPORT
#  define KUDU_DEPRECATED_NO_EXPORT KUDU_NO_EXPORT KUDU_DEPRECATED
#endif

#if 0 /* DEFINE_NO_DEPRECATED */
#  ifndef KUDU_NO_DEPRECATED
#    define KUDU_NO_DEPRECATED
#  endif
#endif

#endif
