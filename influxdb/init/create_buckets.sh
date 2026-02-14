#!/bin/bash
set -e
echo ">>> Creating additional InfluxDB buckets..."
influx bucket create --name decisions --org "${DOCKER_INFLUXDB_INIT_ORG}" --retention 365d --token "${DOCKER_INFLUXDB_INIT_ADMIN_TOKEN}" 2>/dev/null || echo "Bucket 'decisions' already exists"
influx bucket create --name telemetry --org "${DOCKER_INFLUXDB_INIT_ORG}" --retention 90d --token "${DOCKER_INFLUXDB_INIT_ADMIN_TOKEN}" 2>/dev/null || echo "Bucket 'telemetry' already exists"
echo ">>> InfluxDB init complete."
