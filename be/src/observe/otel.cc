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
#include <functional>
#include <memory>
#include <regex>
#include <string>
#include <string_view>
#include <utility>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <gflags/gflags_declare.h>
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
#include <opentelemetry/nostd/shared_ptr.h>
#include <opentelemetry/sdk/common/global_log_handler.h>
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

#include "common/compiler-util.h"
#include "common/version.h"
#include "gen-cpp/Query_types.h"
#include "observe/otel-log-handler.h"
#include "observe/span-manager.h"
#include "service/client-request-state.h"

using namespace boost::algorithm;
using namespace opentelemetry;
using namespace opentelemetry::exporter::otlp;
using namespace opentelemetry::sdk::common::internal_log;
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

// Other flags
DECLARE_string(ssl_cipher_list);
DECLARE_string(tls_ciphersuites);
DECLARE_string(ssl_minimum_version);

// Constants
static const string SCOPE_SPAN_NAME = "org.apache.impala.impalad.query";
static const regex query_newline(
    "(select|alter|compute|create|delete|drop|insert|invalidate|update|with)\\s*"
    "(\n|\\s*\\\\*\\/)", regex::icase | regex::optimize | regex::nosubs);

// Holds the custom log handler for OpenTelemetry internal logs.
static nostd::shared_ptr<LogHandler> otel_log_handler_;

// Lambda function to check if SQL starts with relevant keywords for tracing
static const function<bool(std::string_view)> is_traceable_sql =
    [](std::string_view sql_str) -> bool {
      return
          LIKELY(boost::algorithm::istarts_with(sql_str, "select ")
              || boost::algorithm::istarts_with(sql_str, "alter ")
              || boost::algorithm::istarts_with(sql_str, "compute ")
              || boost::algorithm::istarts_with(sql_str, "create ")
              || boost::algorithm::istarts_with(sql_str, "delete ")
              || boost::algorithm::istarts_with(sql_str, "drop ")
              || boost::algorithm::istarts_with(sql_str, "insert ")
              || boost::algorithm::istarts_with(sql_str, "invalidate ")
              || boost::algorithm::istarts_with(sql_str, "update ")
              || boost::algorithm::istarts_with(sql_str, "with "))
          || regex_search(sql_str.cbegin(), sql_str.cend(), query_newline);
    };

namespace impala {

// TraceProvider is a singleton that provides access to the OpenTelemetry TracerProvider.
// Not shared globally via opentelemetry::trace::Provider::SetTracerProvider to enforce
// all tracing to go through the Impala-specific code interfaces to OpenTelemetry tracing.
static unique_ptr<trace::TracerProvider> provider_;

// Returns true if any TLS configuration flags are set for the OTel exporter.
static inline bool otel_tls_enabled() {
  return boost::algorithm::istarts_with(FLAGS_otel_trace_collector_url, "https://");
} // function otel_tls_enabled

bool should_otel_trace_query(std::string_view sql,
    const TSessionType::type& session_type) {
  if (UNLIKELY(session_type == TSessionType::BEESWAX)) {
    return false;
  }

  if (LIKELY(is_traceable_sql(sql))) {
    return true;
  }

  // Loop until all leading comments and whitespace are skipped.
  while (true) {
    if (boost::algorithm::istarts_with(sql, "/*")) {
      // Handle leading inline comments
      size_t end_comment = sql.find("*/");
      if (end_comment != string_view::npos) {
        sql = sql.substr(end_comment + 2);
        continue;
      }
    } else if (boost::algorithm::istarts_with(sql, "--")) {
      // Handle leading comment lines
      size_t end_comment = sql.find("\n");
      if (end_comment != string_view::npos) {
        sql = sql.substr(end_comment + 1);
        continue;
      }
    } else if (UNLIKELY(boost::algorithm::istarts_with(sql, " "))) {
      // Handle leading whitespace. Since Impala removes leading whitespace from the SQL
      // statement, this case only happens if the sql statement starts with inline
      // comments or there is a leading space on the first non-comment line.
      size_t end_comment = sql.find_first_not_of(" ");
      if (end_comment != string_view::npos) {
        sql = sql.substr(end_comment);
        continue;
      }
    } else if (boost::algorithm::istarts_with(sql, "\n")) {
      // Handline newlines after inline comments.
      size_t end_comment = sql.find_first_not_of("\n");
      if (end_comment != string_view::npos) {
        sql = sql.substr(end_comment);
        continue;
      }
    }

    // Check if the SQL statement starts with any of the keywords we want to trace
    if (LIKELY(is_traceable_sql(sql))) {
      return true;
    }

    // No more patterns to check
    break;
  }

  return false;
} // function should_otel_trace_query

// Creates an OtlpHttpExporterOptions struct instance with configuration from global
// startup flags.
static OtlpHttpExporterOptions http_exporter_config() {
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
    // Set minimum TLS version to the value of the global ssl_minimum_version flag.
    // Since this flag is in the format "tlv1.2" or "tlsv1.3", we need to
    // convert it to the format expected by OtlpHttpExporterOptions by removing the
    // "tlsv" prefix.
    if (FLAGS_otel_trace_tls_minimum_version.empty()) {
      if (!FLAGS_ssl_minimum_version.empty()) {
        opts.ssl_min_tls = FLAGS_ssl_minimum_version.substr(4);
      } else {
        LOG(WARNING) << "TLS is enabled for the OTel exporter, but neither the "
            "'ssl_minimum_version' nor the 'otel_trace_tls_minimum_version' flags are "
            "set.";
      }
    } else {
      opts.ssl_min_tls = FLAGS_otel_trace_tls_minimum_version.substr(4);
    }

    opts.ssl_insecure_skip_verify = FLAGS_otel_trace_tls_insecure_skip_verify;
    opts.ssl_ca_cert_path = FLAGS_otel_trace_ca_cert_path;
    opts.ssl_ca_cert_string = FLAGS_otel_trace_ca_cert_string;
    opts.ssl_max_tls = "1.3";
    opts.ssl_cipher = FLAGS_otel_trace_ssl_ciphers.empty() ? FLAGS_ssl_cipher_list :
        FLAGS_otel_trace_ssl_ciphers;
    opts.ssl_cipher_suite = FLAGS_otel_trace_tls_cipher_suites.empty() ?
        FLAGS_tls_ciphersuites : FLAGS_otel_trace_tls_cipher_suites;

    VLOG(2) << "OTel minimum TLS version set to '" << opts.ssl_min_tls << "'";
    VLOG(2) << "OTel TLS 1.2 allowed ciphers set to '" << opts.ssl_cipher << "'";
    VLOG(2) << "OTel TLS 1.3 allowed ciphers set to '" << opts.ssl_cipher_suite << "'";
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

  return opts;
} // function http_exporter_config

// Creates a BatchSpanProcessorOptions struct instance with configuration from global
// startup flags.
static BatchSpanProcessorOptions batch_processor_config() {
  BatchSpanProcessorOptions batch_opts;

  batch_opts.max_queue_size = FLAGS_otel_trace_batch_queue_size;
  batch_opts.max_export_batch_size = FLAGS_otel_trace_batch_max_batch_size;
  batch_opts.schedule_delay_millis =
      chrono::milliseconds(FLAGS_otel_trace_batch_schedule_delay_ms);

  return batch_opts;
} // function batch_processor_config

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

// Initializes a SpanExporter instance based on the FLAGS_otel_trace_exporter flag.
// Returns a unique_ptr which will always be initialized with the created exporter.
static unique_ptr<SpanExporter> init_exporter() {
  unique_ptr<SpanExporter> exporter;

  if(FLAGS_otel_trace_exporter == OTEL_EXPORTER_OTLP_HTTP) {
    exporter = OtlpHttpExporterFactory::Create(http_exporter_config());
  } else {
    exporter = init_exporter_file();
  }
  VLOG(2) << "OpenTelemetry exporter: " << FLAGS_otel_trace_exporter;

  return exporter;
} // function init_exporter

// Initializes a SpanProcessor instance based on the FLAGS_otel_trace_span_processor flag.
// Returns a unique_ptr which will always be initialized with the created processor.
static unique_ptr<SpanProcessor> init_span_processor() {
  unique_ptr<SpanExporter> exporter = init_exporter();
  unique_ptr<SpanProcessor> processor;

  if (boost::iequals(trim_copy(FLAGS_otel_trace_span_processor), SPAN_PROCESSOR_BATCH)) {
    VLOG(2) << "Using BatchSpanProcessor for OpenTelemetry spans";
    processor = BatchSpanProcessorFactory::Create(move(exporter),
        batch_processor_config());
  } else {
    VLOG(2) << "Using SimpleSpanProcessor for OTel spans";
    LOG(WARNING) << "Setting --otel_trace_span_processor=simple blocks the query "
    "processing thread while exporting spans to the OTel collector. This will cause "
    "significant performance degradation and is not recommended for production use.";
    processor = make_unique<SimpleSpanProcessor>(move(exporter));
  }

  return processor;
} // function init_span_processor

// Initializes the OpenTelemetry Tracer singleton with the configuration defined in the
// coordinator startup flags. This tracer is stored in a static variabled defined in this
// file.
void init_otel_tracer() {
  LOG(INFO) << "Initializing OpenTelemetry tracing.";
  VLOG(2) << "OpenTelemetry version: " << OPENTELEMETRY_VERSION;
  VLOG(2) << "OpenTelemetry ABI version: " << OPENTELEMETRY_ABI_VERSION;
  VLOG(2) << "OpenTelemetry namespace: "
      << OPENTELEMETRY_STRINGIFY(OPENTELEMETRY_NAMESPACE);

  otel_log_handler_ = nostd::shared_ptr<LogHandler>(new OtelLogHandler());
  GlobalLogHandler::SetLogHandler(otel_log_handler_);

  // Set the OpenTelemetry SDK internal log level based on the current glog level. The SDK
  // does not support changing the log level once a Provider has been created.
  if (FLAGS_otel_debug) {
    GlobalLogHandler::SetLogLevel(LogLevel::Debug);
  } else if (VLOG_IS_ON(1)) {
    GlobalLogHandler::SetLogLevel(LogLevel::Info);
  } else {
    GlobalLogHandler::SetLogLevel(LogLevel::None);
  }

  provider_ = TracerProviderFactory::Create(init_span_processor(),
      sdk::resource::Resource::Create({
        {"service.name", "Impala"},
        {"service.version", GetDaemonBuildVersion()}
      }));
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

namespace test {
bool otel_tls_enabled_for_testing() {
  return otel_tls_enabled();
}

OtlpHttpExporterOptions get_http_exporter_config() {
  return http_exporter_config();
}

BatchSpanProcessorOptions get_batch_processor_config() {
  return batch_processor_config();
}

unique_ptr<SpanExporter> get_exporter() {
  return init_exporter();
}

unique_ptr<SpanProcessor> get_span_processor() {
  return init_span_processor();
}

} // namespace test

} // namespace impala