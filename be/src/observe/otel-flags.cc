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

// Contains coordinator startup flags applicable to all of OpenTelemetry.

#include <chrono>

#include <gflags/gflags.h>
#include <opentelemetry/exporters/otlp/otlp_file_client_options.h>

#include "util/gflag-validator-util.h"

using namespace std;
using namespace std::chrono;

DEFINE_bool(otel_debug, false, "If set to true, send debug logs from the "
    "OpenTelemetry SDK to the Impala logging system.");

//
// Start of OTLP File Exporter flags
//
static opentelemetry::exporter::otlp::OtlpFileClientFileSystemOptions file_opts;

DEFINE_string_hidden(otel_file_pattern, file_opts.file_pattern, "Pattern to create "
    "output file for the OTLP file exporter.");

DEFINE_string_hidden(otel_file_alias_pattern, file_opts.alias_pattern, "Pattern to "
    "create alias file path for the latest file rotation for the OTLP file exporter.");

DEFINE_int32_hidden(otel_file_flush_interval_ms, duration_cast<milliseconds>(
    file_opts.flush_interval).count(), "Flush interval in milliseconds for the OTLP "
    "file exporter.");
DEFINE_validator(otel_file_flush_interval_ms, ge_one);

DEFINE_int32_hidden(otel_file_flush_count, file_opts.flush_count, "Flush record count "
    "for the OTLP file exporter.");
DEFINE_validator(otel_file_flush_count, ge_one);

DEFINE_int32_hidden(otel_file_max_file_size, file_opts.file_size, "Maximum file size in "
    "bytes for the OTLP file exporter.");
DEFINE_validator(otel_file_max_file_size, ge_one);

DEFINE_int32_hidden(otel_file_max_file_count, file_opts.rotate_size, "Maximum file count "
    "(rotate size) for the OTLP file exporter.");
DEFINE_validator(otel_file_max_file_count, ge_one);
//
// End of OTLP File Exporter flags
//
