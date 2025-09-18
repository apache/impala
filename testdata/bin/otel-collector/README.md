# OpenTelemetry Collector & Jaeger Integration

This directory contains configuration to run an [OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) and a [Jaeger](https://www.jaegertracing.io/) instance for collecting and visualizing telemetry, primarily for Impala development and testing.

---

## Contents

- [`otel-config-http.yml`](./otel-config-http.yml): OpenTelemetry Collector configuration file with the OTLP receiver supporting http.
- [`otel-config-https.yml`](./otel-config-https.yml): OpenTelemetry Collector configuration file with the OTLP receiver supporting https.
- [`docker-compose.yml`](./docker-compose.yml): Alternative Docker Compose setup for both services.

---

## Quick Start

### Option 1: Run Interactively

   ```bash
   # HTTP (default)
   docker-compose -f testdata/bin/otel-collector/docker-compose.yml up

   # HTTPS
   PROTOCOL=https docker-compose -f testdata/bin/otel-collector/docker-compose.yml up
   ```

   - This command:
     - Starts the OpenTelemetry Collector container with the `otel-config.yml` config.
     - Starts the Jaeger container.
     - Waits for both containers to be running.
     - Continuously outputs both container logs to the terminal window.

### Option 2: Run Detached

   ```bash
   # HTTP (default)
   docker-compose -f testdata/bin/otel-collector/docker-compose.yml up -d

   # HTTP (default)
   PROTOCOL=https docker-compose -f testdata/bin/otel-collector/docker-compose.yml up -d
   ```

   - This command:
     - Starts the OpenTelemetry Collector container with the `otel-config.yml` config.
     - Starts the Jaeger container.
     - Waits for both containers to be running.
     - Exits back to the terminal prompt.

### Stop the Collector and Jaeger

   ```bash
   docker-compose -f testdata/bin/otel-collector/docker-compose.yml down
   ```

   - This command gracefully stops and removes the containers and network. Note this command must always be run. Pressing ctrl+c when the containers are run interactively does not stop the containers.

---

## Configuration Details

- **OpenTelemetry Collector** listens for OTLP traces on port `55888` (HTTP).
- **Jaeger** is configured to receive OTLP traces on port `4317` and exposes its UI on port `16686`.

The collector forwards all received traces to Jaeger using OTLP/gRPC.

---

## Sending Traces from Impala

To send traces from an Impala cluster to this collector, start Impala with the following arguments:

```bash
# HTTP (default)
start-impala-cluster.py \
  --cluster_size=2 \
  --num_coordinators=1 \
  --use_exclusive_coordinators \
  --impalad_args="-v=2 --otel_trace_enabled=true \
    --otel_trace_collector_url=http://localhost:55888/v1/traces
    --otel_trace_span_processor=simple \
    --cluster_id=local_cluster"

# HTTPS
start-impala-cluster.py \
  --cluster_size=2 \
  --num_coordinators=1 \
  --use_exclusive_coordinators \
  --impalad_args="-v=2 --otel_trace_enabled=true \
    --otel_trace_collector_url=https://localhost:55888/v1/traces \
    --otel_trace_ca_cert_path=${IMPALA_HOME}/be/src/testutil/wildcardCA.pem \
    --otel_trace_span_processor=simple \
    --cluster_id=local_cluster"
```

- Ensure the collector is running before starting Impala.
- Adjust `--otel_trace_collector_url` if running on a remote host.

---

## Viewing Traces

- Open the Jaeger UI in your browser: [http://localhost:16686/](http://localhost:16686/)
- If running remotely, use SSH port forwarding:

  ```bash
  ssh -L 16686:localhost:16686 <your-dev-machine>
  ```

---

## Notes

- The scripts and Docker Compose setup are idempotent and safe to run multiple times.
- All containers and the custom network are cleaned up on stop.
- The provided configuration is suitable for local development and testing only.

---

## Troubleshooting

- **Ports already in use:** Ensure no other services are using ports `55888`, `4317`, or `16686`.
- **Containers not starting:** Check Docker logs for `otel-collector` and `jaeger` for errors:

  ```bash
  docker logs otel-collector
  docker logs jaeger
  ```

- **Configuration changes:** Edit `otel-config.yml` as needed and restart the services.

---