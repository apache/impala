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

#ifndef IMPALA_UTIL_DEFAULT_PATH_HANDLERS_H
#define IMPALA_UTIL_DEFAULT_PATH_HANDLERS_H

#include <stdio.h>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>

#include "util/webserver.h"

namespace impala {

class MemTracker;
class MetricGroup;

/// Adds a set of default path handlers to the webserver to display
/// logs and configuration flags
void AddDefaultUrlCallbacks(Webserver* webserver, MetricGroup* metric_group = NULL,
    MemTracker* process_mem_tracker = NULL);

/// Registered to handle "/"
/// Populates document with various system-wide information.
void RootHandler(const Webserver::WebRequest& req, rapidjson::Document* document,
    MetricGroup* metric_group);
}

#endif // IMPALA_UTIL_DEFAULT_PATH_HANDLERS_H
