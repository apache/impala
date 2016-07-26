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

#include <errno.h>

// TODO: IMPALA-2764 Remove this file once Impala is dynamically linked against SASL

#ifdef __APPLE__
// ELIBACC is not defined on OS X and this definition should disappear with dynamic
// linking of SASL.
#define ELIBACC -1
#endif

// This file defines the routines that come up undefined when statically
// linking the SASL library.  The library itself is configured to
// dynamically link in the GSSAPI library.  Why it needs these
// defined is not clear.
int gss_accept_sec_context() { errno = ELIBACC; return -1; }
int gss_acquire_cred() { errno = ELIBACC; return -1; }
int gss_compare_name() { errno = ELIBACC; return -1; }
int gss_delete_sec_context() { errno = ELIBACC; return -1; }
int gss_display_name() { errno = ELIBACC; return -1; }
int gss_display_status() { errno = ELIBACC; return -1; }
int gss_import_name() { errno = ELIBACC; return -1; }
int gss_init_sec_context() { errno = ELIBACC; return -1; }
int gss_inquire_context() { errno = ELIBACC; return -1; }
int gss_release_buffer() { errno = ELIBACC; return -1; }
int gss_release_cred() { errno = ELIBACC; return -1; }
int gss_release_name() { errno = ELIBACC; return -1; }
int gss_unwrap() { errno = ELIBACC; return -1; }
int gss_wrap() { errno = ELIBACC; return -1; }
int gss_wrap_size_limit() { errno = ELIBACC; return -1; }
char* GSS_C_NT_HOSTBASED_SERVICE;
char* GSS_C_NT_USER_NAME;
