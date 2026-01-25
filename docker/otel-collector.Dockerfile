FROM otel/opentelemetry-collector-contrib:0.104.0
COPY docker/otel-collector/config.yml /etc/otelcol-contrib/config.yml