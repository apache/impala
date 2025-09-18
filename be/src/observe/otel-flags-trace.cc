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

// Contains coordinator startup flags applicable only to OpenTelemetry traces.

#include <chrono>
#include <regex>
#include <string>

#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gutil/strings/split.h>
#include <gutil/strings/substitute.h>
#include <opentelemetry/sdk/trace/batch_span_processor_options.h>

#include "common/status.h"
#include "kudu/util/env.h"
#include "kudu/util/faststring.h"
#include "kudu/util/status.h"
#include "observe/otel.h"
#include "util/gflag-validator-util.h"
#include "util/openssl-util.h"

using namespace std;
using namespace std::chrono;

static opentelemetry::sdk::trace::BatchSpanProcessorOptions batch_opts;

DEFINE_bool(otel_trace_enabled, false, "If set to true, OpenTelemetry traces will be "
    "generated and exported to the configured OpenTelemetry collector.");

// Specifies the OTel exporter. This flag is hidden because the file exporter is used for
// testing and not supported in production.
DEFINE_string_hidden(otel_trace_exporter, impala::OTEL_EXPORTER_OTLP_HTTP.c_str(),
    "The trace exporter to use for OpenTelemetry spans. Supported values: 'otlp_http' "
    "and 'file'.");
DEFINE_validator(otel_trace_exporter, [](const char* flagname, const string& value) {
  if (value == impala::OTEL_EXPORTER_OTLP_HTTP
      || value == impala::OTEL_EXPORTER_FILE) {
    return true;
  }

  LOG(ERROR) << "Flag '" << flagname << "' must be one of: '"
      << impala::OTEL_EXPORTER_OTLP_HTTP << "', '" << impala::OTEL_EXPORTER_FILE << "'.";
  return false;
}); // flag otel_trace_exporter

//
// Start of HTTP related flags.
//
DEFINE_string(otel_trace_collector_url, "", "The URL of the OpenTelemetry collector to "
    "which trace data will be exported.");
DEFINE_validator(otel_trace_collector_url, [](const char* flagname, const string& value) {
  if (value.empty()) {
    return true;
  }

  // Check if URL starts with http:// or https://
  if (!(value.rfind("http://", 0) == 0 || value.rfind("https://", 0) == 0)) {
    LOG(ERROR) << "Flag '" << flagname << "' must start with 'http://' or 'https://'";
    return false;
  }

  return true;
}); // flag otel_trace_collector_url

DEFINE_string(otel_trace_additional_headers, "", "List of additional HTTP headers to be "
    "sent with each call to the OTel Collector. Individual headers are separated by a "
    "delimiter of three colons. Format is 'key1=value1:::key2=value2:::key3=value3'.");
DEFINE_validator(otel_trace_additional_headers, [](const char* flagname,
    const string& value) {
  bool valid = true;

  if (!value.empty()) {
    for (auto header : strings::Split(value, ":::")) {
      if (header.find('=') == string::npos) {
        LOG(ERROR) << "Flag '" << flagname << "' contains an invalid header "
            "(missing '='): " << header;
        valid = false;
      }
    }
  }

  return valid;
}); // flag otel_trace_additional_headers

DEFINE_bool(otel_trace_compression, true, "If set to true, uses ZLib compression for "
    "sending data to the OTel Collector. If set to false, sends data uncompressed.");

DEFINE_int32(otel_trace_timeout_s, 10, "Export timeout in seconds.");

DEFINE_validator(otel_trace_timeout_s, ge_one);
//
// End of HTTP related flags.
//


//
// Start of TLS related flags.
//
static bool validate_pem_bundle_string(const char* flagname, const string& value) {
  impala::Status s = impala::ValidatePemBundle(value);

  if (!s.ok()) {
    LOG(ERROR) << "Flag '" << flagname << " failed validation: " << s.GetDetail();
    return false;
  }

  return true;
} // function validate_pem_bundle_string

DEFINE_string(otel_trace_ca_cert_path, "", "Path to a file containing CA certificates "
  "bundle. Combined with 'otel_trace_ca_cert_string' if both are specified. ");
DEFINE_validator(otel_trace_ca_cert_path, [](const char* flagname, const string& value) {
 if (!value.empty()) {
    kudu::faststring contents;
    kudu::Status s = kudu::ReadFileToString(kudu::Env::Default(), value, &contents);

    if (!s.ok()) {
        LOG(ERROR) << "Flag '" << flagname << "' must point to a valid file: " << value;
        return false;
    }

    return validate_pem_bundle_string(flagname, contents.ToString());
  }

  return true;
}); // flag otel_trace_ca_cert_path

DEFINE_string(otel_trace_ca_cert_string, "", "String containing CA certificates bundle. "
  "Combined with 'otel_trace_ca_cert_path' if both are specified.");
DEFINE_validator(otel_trace_ca_cert_string, [](const char* flagname,
    const string& value) {
  if (!value.empty()) {
    return validate_pem_bundle_string(flagname, value);
  }

  return true;
}); // flag otel_trace_ca_cert_string


DEFINE_string(otel_trace_tls_minimum_version, "", "String containing the minimum allowed "
    "TLS version the OpenTelemetry SDK will use when communicating with the collector. "
    "If empty, will use the value of the 'ssl_minimum_version' flag.");
DEFINE_validator(otel_trace_tls_minimum_version, [](const char* flagname,
    const string& value) {
  if (value.empty() || value == impala::TLSVersions::TLSV1_2 || value == "tlsv1.3") {
    return true;
  }
  LOG(ERROR) << "Flag '" << flagname << "' must be empty or one of: '"
      << impala::TLSVersions::TLSV1_2 << "', 'tlsv1.3'.";
  return false;
}); // flag otel_trace_tls_minimum_version

DEFINE_string(otel_trace_ssl_ciphers, "", "List of allowed TLS cipher suites when using "
    "TLS 1.2. If empty, defaults to the value of the 'ssl_cipher_list' startup flag.");

DEFINE_string(otel_trace_tls_cipher_suites, "", "List of allowed TLS cipher suites when "
    "using TLS 1.3. If empty, defaults to the value of the 'tls_ciphersuites' startup "
    "flag.");

DEFINE_bool(otel_trace_tls_insecure_skip_verify, false, "If set to true, skips "
    "verification of collector’s TLS certificate. This should only be set to true for "
    "development / testing");
//
// End of TLS related flags.
//


//
// Start of retry policy flags
//
DEFINE_int32(otel_trace_retry_policy_max_attempts, 5, "Maximum number of call attempts, "
    "including the original attempt.");
DEFINE_validator(otel_trace_retry_policy_max_attempts, gt_zero);

DEFINE_double(otel_trace_retry_policy_initial_backoff_s, 1.0, "Initial backoff delay "
    "between retry attempts in seconds.");
DEFINE_validator(otel_trace_retry_policy_initial_backoff_s, ge_one);

DEFINE_int32(otel_trace_retry_policy_max_backoff_s, 0, "Maximum backoff delay between "
    "retry attempts in seconds. Value of 0 or less indicates not set.");

DEFINE_double(otel_trace_retry_policy_backoff_multiplier, 2.0, "Backoff will be "
    "multiplied by this value after each retry attempt.");
DEFINE_validator(otel_trace_retry_policy_backoff_multiplier, ge_one);
//
// End of retry policy flags
//


//
// Start of Span Processor flags
//
// This flag is hidden because simple span processor blocks the query processing while
// communicating with the OTel collector.
DEFINE_string_hidden(otel_trace_span_processor, impala::SPAN_PROCESSOR_BATCH.c_str(),
    strings::Substitute("The span processor implementation to use for exporting spans to "
    "the OTel Collector. Supported values: '$0' and '$1'.", impala::SPAN_PROCESSOR_BATCH,
    impala::SPAN_PROCESSOR_SIMPLE).c_str());
DEFINE_validator(otel_trace_span_processor, [](const char* flagname,
    const string& value) {
  const std::string trimmed = boost::algorithm::trim_copy(value);
  return boost::iequals(trimmed, impala::SPAN_PROCESSOR_BATCH)
      || boost::iequals(trimmed, impala::SPAN_PROCESSOR_SIMPLE);
});

DEFINE_int32(otel_trace_batch_queue_size, batch_opts.max_queue_size, "The maximum "
    "buffer/queue size. After the size is reached, spans are dropped. Applicable when "
    "'otel_trace_span_processor' is 'batch'.");
DEFINE_validator(otel_trace_batch_queue_size, ge_one);

DEFINE_int32(otel_trace_batch_schedule_delay_ms, batch_opts.schedule_delay_millis.count(),
    "The delay interval in milliseconds between two consecutive batch exports. "
    "Applicable when 'otel_trace_span_processor' is 'batch'.");
DEFINE_validator(otel_trace_batch_schedule_delay_ms, ge_one);

DEFINE_int32(otel_trace_batch_max_batch_size, batch_opts.max_export_batch_size,
    "The maximum batch size of every export to OTel Collector. Applicable when "
    "'otel_trace_span_processor' is 'batch'.");
DEFINE_validator(otel_trace_batch_max_batch_size, ge_one);
//
// End of Span Processor flags
//
