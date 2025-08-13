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

#include "otel.h"

#include <chrono>
#include <memory>
#include <string>
#include <utility>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gutil/strings/split.h>
#include <opentelemetry/exporters/otlp/otlp_file_exporter.h>
#include <opentelemetry/exporters/otlp/otlp_file_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_file_exporter_options.h>
#include <opentelemetry/exporters/otlp/otlp_http.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_factory.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_options.h>
#include <opentelemetry/exporters/otlp/otlp_http_exporter_runtime_options.h>
#include <opentelemetry/sdk/resource/resource.h>
#include <opentelemetry/sdk/trace/batch_span_processor.h>
#include <opentelemetry/sdk/trace/batch_span_processor_factory.h>
#include <opentelemetry/sdk/trace/batch_span_processor_options.h>
#include <opentelemetry/sdk/trace/batch_span_processor_runtime_options.h>
#include <opentelemetry/sdk/trace/exporter.h>
#include <opentelemetry/sdk/trace/processor.h>
#include <opentelemetry/sdk/trace/tracer_provider.h>
#include <opentelemetry/sdk/trace/tracer_provider_factory.h>
#include <opentelemetry/sdk/trace/simple_processor.h>
#include <opentelemetry/trace/provider.h>
#include <opentelemetry/trace/tracer.h>
#include <opentelemetry/version.h>

#include "common/status.h"
#include "common/version.h"
#include "observe/otel-instrument.h"
#include "observe/span-manager.h"
#include "service/client-request-state.h"

using namespace boost::algorithm;
using namespace opentelemetry;
using namespace opentelemetry::exporter::otlp;
using namespace opentelemetry::sdk::trace;
using namespace std;

// OTel related flags
DECLARE_string(otel_trace_additional_headers);
DECLARE_int32(otel_trace_batch_queue_size);
DECLARE_int32(otel_trace_batch_max_batch_size);
DECLARE_int32(otel_trace_batch_schedule_delay_ms);
DECLARE_string(otel_trace_ca_cert_path);
DECLARE_string(otel_trace_ca_cert_string);
DECLARE_string(otel_trace_collector_url);
DECLARE_bool(otel_trace_compression);
DECLARE_bool(otel_debug);
DECLARE_string(otel_trace_exporter);
DECLARE_string(otel_file_pattern);
DECLARE_string(otel_file_alias_pattern);
DECLARE_int32(otel_file_flush_interval_ms);
DECLARE_int32(otel_file_flush_count);
DECLARE_int32(otel_file_max_file_size);
DECLARE_int32(otel_file_max_file_count);
DECLARE_double(otel_trace_retry_policy_backoff_multiplier);
DECLARE_double(otel_trace_retry_policy_initial_backoff_s);
DECLARE_int32(otel_trace_retry_policy_max_attempts);
DECLARE_int32(otel_trace_retry_policy_max_backoff_s);
DECLARE_string(otel_trace_span_processor);
DECLARE_string(otel_trace_ssl_ciphers);
DECLARE_int32(otel_trace_timeout_s);
DECLARE_string(otel_trace_tls_cipher_suites);
DECLARE_bool(otel_trace_tls_insecure_skip_verify);
DECLARE_string(otel_trace_tls_minimum_version);
DECLARE_bool(otel_trace_enabled);

// Other flags
DECLARE_string(ssl_cipher_list);
DECLARE_string(tls_ciphersuites);
DECLARE_string(ssl_minimum_version);

// Constants
static const string SCOPE_SPAN_NAME = "org.apache.impala.impalad.query";

namespace impala {

// TraceProvider is a singleton that provides access to the OpenTelemetry TracerProvider.
// Not shared globally via opentelemetry::trace::Provider::SetTracerProvider to enforce
// all tracing to go through the Impala-specific code interfaces to OpenTelemetry tracing.
static unique_ptr<trace::TracerProvider> provider_;

// Returns true if any TLS configuration flags are set for the OTel exporter.
static inline bool otel_tls_enabled() {
  return !FLAGS_otel_trace_ca_cert_path.empty()
      || !FLAGS_otel_trace_ca_cert_string.empty()
      || !FLAGS_otel_trace_tls_minimum_version.empty()
      || !FLAGS_otel_trace_ssl_ciphers.empty()
      || !FLAGS_otel_trace_tls_cipher_suites.empty();
} // function otel_tls_enabled

bool otel_trace_enabled() {
  return FLAGS_otel_trace_enabled;
} // function otel_trace_enabled

bool should_otel_trace_query(const char* sql) {
  DCHECK(sql != nullptr) << "SQL statement cannot be null.";
  return boost::algorithm::istarts_with(sql, "select ");
} // function should_otel_trace_query

// Initializes an OtlpHttpExporter instance with configuration from global flags. The
// OtlpHttpExporter instance implements the SpanExporter interface. The function parameter
// `exporter` is an in-out parameter that will be populated with the created
// OtlpHttpExporter instance. Returns Status::OK() on success, or an error Status if
// configuration fails.
static Status init_exporter_http(unique_ptr<SpanExporter>& exporter) {
  // Configure OTLP HTTP exporter
  OtlpHttpExporterOptions opts;
  opts.url = FLAGS_otel_trace_collector_url;
  opts.content_type = HttpRequestContentType::kJson;
  opts.timeout = chrono::seconds(FLAGS_otel_trace_timeout_s);
  opts.console_debug = FLAGS_otel_debug;

  // Retry settings
  opts.retry_policy_max_attempts = FLAGS_otel_trace_retry_policy_max_attempts;
  opts.retry_policy_initial_backoff =
      chrono::duration<float>(FLAGS_otel_trace_retry_policy_initial_backoff_s);
  if (FLAGS_otel_trace_retry_policy_max_backoff_s > 0) {
    opts.retry_policy_max_backoff = chrono::duration<float>(
        chrono::seconds(FLAGS_otel_trace_retry_policy_max_backoff_s));
  }
  opts.retry_policy_backoff_multiplier = FLAGS_otel_trace_retry_policy_backoff_multiplier;

  // Compression Type
  if (FLAGS_otel_trace_compression) {
    opts.compression = "zlib";
  }

  // TLS Configurations
  if (otel_tls_enabled()) {
    if (FLAGS_otel_trace_tls_minimum_version.empty()) {
      // Set minimum TLS version to the value of the global ssl_minimum_version flag.
      // Since this flag is in the format "tlv1.2" or "tlsv1.3", we need to
      // convert it to the format expected by OtlpHttpExporterOptions.
      const string min_ssl_ver = to_lower_copy(trim_copy(FLAGS_ssl_minimum_version));

      if (!min_ssl_ver.empty() && min_ssl_ver.rfind("tlsv", 0) != 0) {
        return Status("ssl_minimum_version must start with 'tlsv'");
      }

      opts.ssl_min_tls = min_ssl_ver.substr(4); // Remove "tlsv" prefix
    } else {
      opts.ssl_min_tls = FLAGS_otel_trace_tls_minimum_version;
    }

    opts.ssl_insecure_skip_verify = FLAGS_otel_trace_tls_insecure_skip_verify;
    opts.ssl_ca_cert_path = FLAGS_otel_trace_ca_cert_path;
    opts.ssl_ca_cert_string = FLAGS_otel_trace_ca_cert_string;
    opts.ssl_max_tls = "1.3";
    opts.ssl_cipher = FLAGS_otel_trace_ssl_ciphers.empty() ? FLAGS_ssl_cipher_list :
        FLAGS_otel_trace_ssl_ciphers;
    opts.ssl_cipher_suite = FLAGS_otel_trace_tls_cipher_suites.empty() ?
        FLAGS_tls_ciphersuites : FLAGS_otel_trace_tls_cipher_suites;
  }

  // Additional HTTP headers
  if (!FLAGS_otel_trace_additional_headers.empty()) {
    for (auto header : strings::Split(FLAGS_otel_trace_additional_headers, ":::")) {
      auto pos = header.find('=');
      const string key = trim_copy(header.substr(0, pos).as_string());
      const string value = trim_copy(header.substr(pos + 1).as_string());

      VLOG(2) << "Adding additional OTel header: " << key << " = " << value;
      opts.http_headers.emplace(key, value);
    }
  }

  if (FLAGS_otel_debug) {
    opentelemetry::v1::exporter::otlp::OtlpHttpExporterRuntimeOptions runtime_opts;
    runtime_opts.thread_instrumentation =
        make_shared<LoggingInstrumentation>("http_exporter");
    exporter = OtlpHttpExporterFactory::Create(opts, runtime_opts);
  } else {
    exporter = OtlpHttpExporterFactory::Create(opts);
  }

  return Status::OK();
} // function init_exporter_http

// Initializes an OtlpFileExporter instance with configuration from global flags. The
// OtlpFileExporter instance implements the SpanExporter interface. Returns a unique_ptr
// which will always be initialized with the created OtlpHttpExporter instance.
//
// The file exporter is for test use only.
static unique_ptr<SpanExporter> init_exporter_file() {
  OtlpFileClientFileSystemOptions file_client_opts;

  file_client_opts.file_pattern = FLAGS_otel_file_pattern;
  file_client_opts.alias_pattern = FLAGS_otel_file_alias_pattern;
  file_client_opts.flush_interval = chrono::microseconds(chrono::milliseconds(
      FLAGS_otel_file_flush_interval_ms));
  file_client_opts.flush_count = FLAGS_otel_file_flush_count;
  file_client_opts.file_size = FLAGS_otel_file_max_file_size;
  file_client_opts.rotate_size = FLAGS_otel_file_max_file_count;

  OtlpFileExporterOptions exporter_opts;
  exporter_opts.backend_options = file_client_opts;
  exporter_opts.console_debug = FLAGS_otel_debug;

  return OtlpFileExporterFactory::Create(exporter_opts);
} // function init_exporter_file

// Initializes the OpenTelemetry Tracer singleton with the configuration defined in the
// coordinator startup flags. Returns Status::OK() on success, or an error Status if
// configuration fails.
Status init_otel_tracer() {
  LOG(INFO) << "Initializing OpenTelemetry tracing.";
  VLOG(2) << "OpenTelemetry version: " << OPENTELEMETRY_VERSION;
  VLOG(2) << "OpenTelemetry ABI version: " << OPENTELEMETRY_ABI_VERSION;
  VLOG(2) << "OpenTelemetry namespace: "
      << OPENTELEMETRY_STRINGIFY(OPENTELEMETRY_NAMESPACE);

  unique_ptr<SpanExporter> exporter;

  if(FLAGS_otel_trace_exporter == OTEL_EXPORTER_OTLP_HTTP) {
    RETURN_IF_ERROR(init_exporter_http(exporter));
  } else {
    exporter = init_exporter_file();
  }
  VLOG(2) << "OpenTelemetry exporter: " << FLAGS_otel_trace_exporter;

  // Set up tracer provider
  unique_ptr<SpanProcessor> processor;

  if (boost::iequals(trim_copy(FLAGS_otel_trace_span_processor), SPAN_PROCESSOR_BATCH)) {
    VLOG(2) << "Using BatchSpanProcessor for OpenTelemetry spans";
    BatchSpanProcessorOptions batch_opts;

    batch_opts.max_queue_size = FLAGS_otel_trace_batch_queue_size;
    batch_opts.max_export_batch_size = FLAGS_otel_trace_batch_max_batch_size;
    batch_opts.schedule_delay_millis =
        chrono::milliseconds(FLAGS_otel_trace_batch_schedule_delay_ms);

    if (FLAGS_otel_debug) {
      BatchSpanProcessorRuntimeOptions runtime_opts;
      runtime_opts.thread_instrumentation =
          make_shared<LoggingInstrumentation>("batch_span_processor");
      processor = BatchSpanProcessorFactory::Create(move(exporter), batch_opts,
          runtime_opts);
    } else {
      processor = BatchSpanProcessorFactory::Create(move(exporter), batch_opts);
    }
  } else {
    VLOG(2) << "Using SimpleSpanProcessor for OTel spans";
    LOG(WARNING) << "Setting --otel_trace_span_processor=simple blocks the query "
    "processing thread while exporting spans to the OTel collector. This will cause "
    "significant performance degradation and is not recommended for production use.";
    processor = make_unique<SimpleSpanProcessor>(move(exporter));
  }

  provider_ = TracerProviderFactory::Create(move(processor),
      sdk::resource::Resource::Create({
        {"service.name", "Impala"},
        {"service.version", GetDaemonBuildVersion()}
      }));

  return Status::OK();
} // function init_otel_tracer

void shutdown_otel_tracer() {
  LOG(INFO) << "Shutting down OpenTelemetry tracing.";
  DCHECK(provider_) << "OpenTelemetry tracer was not initialized.";

  // Force a reset of the provider_ shared_ptr to ensure that the
  // TracerProvider destructor is called, which will flush any remaining spans.
  provider_.reset();
}

shared_ptr<SpanManager> build_span_manager(ClientRequestState* crs) {
  DCHECK(provider_) << "OpenTelemetry tracer was not initialized.";

  return make_shared<SpanManager>(
      provider_->GetTracer(SCOPE_SPAN_NAME, SCOPE_SPAN_SPEC_VERSION), crs);
} // function build_span_manager

} // namespace impala