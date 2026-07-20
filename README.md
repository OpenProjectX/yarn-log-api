# YARN Log API

Reactive YARN application log streaming for Hadoop 3.4.2. The starter reads
running-container logs from NodeManagers and finished logs through Hadoop's
configured log aggregation file controller. It never launches the `yarn logs`
CLI.

The embedding application must configure Hadoop and perform UGI login before
the auto-configuration creates `YarnClient`. In a Kerberos cluster the library
uses that same login UGI to establish SPNEGO and caches the resulting
`hadoop.auth` cookie per NodeManager origin.

## SSE

```http
GET /api/v1/yarn/applications/application_123_0001/logs?follow=true&logFiles=stdout,stderr
Accept: text/event-stream
```

Log event payloads use Base64 so offsets remain byte-accurate.

## WebSocket

Connect to `/api/v1/yarn/logs` and send:

```json
{
  "applicationId": "application_123_0001",
  "follow": true,
  "logFiles": ["stdout", "stderr"],
  "tailBytes": 65536,
  "pollIntervalMs": 1000
}
```

Provide a `YarnLogAuthorizer` bean to enforce the embedding application's
authorization policy.

## Container image

The `app` module is published as `ghcr.io/openprojectx/yarn-log-api`. It runs as
UID/GID `10001`, listens on port `8080`, uses container-aware JVM memory limits,
and exposes Spring Boot liveness and readiness endpoints.

Run against a non-Kerberos cluster by mounting the Hadoop client configuration:

```bash
docker run --rm -p 8080:8080 \
  -e HADOOP_CONF_DIR=/etc/hadoop/conf \
  -v /path/to/hadoop-conf:/etc/hadoop/conf:ro \
  ghcr.io/openprojectx/yarn-log-api:0.1.0
```

For a Kerberos cluster, also mount a service keytab and provide its principal:

```bash
docker run --rm -p 8080:8080 \
  -e HADOOP_CONF_DIR=/etc/hadoop/conf \
  -e YARN_LOG_API_KERBEROS_PRINCIPAL=yarn-log-api/host.example.com@EXAMPLE.COM \
  -e YARN_LOG_API_KERBEROS_KEYTAB=/etc/security/keytabs/yarn-log-api.keytab \
  -v /path/to/hadoop-conf:/etc/hadoop/conf:ro \
  -v /path/to/yarn-log-api.keytab:/etc/security/keytabs/yarn-log-api.keytab:ro \
  ghcr.io/openprojectx/yarn-log-api:0.1.0
```

The mounted Hadoop directory should contain at least `core-site.xml` and
`yarn-site.xml`; include `hdfs-site.xml` when aggregated logs are stored in
HDFS. The service account needs permission to query YARN and read the configured
remote log directory. Put the API behind your authenticated gateway and provide
a `YarnLogAuthorizer` implementation before exposing application logs to users.

Container probes:

```text
GET /actuator/health/liveness
GET /actuator/health/readiness
```

Build the image into the local Docker daemon:

```bash
./gradlew :app:jibDockerBuild
docker run --rm -p 8080:8080 ghcr.io/openprojectx/yarn-log-api:0.1.0-SNAPSHOT
```

Publish to a registry explicitly:

```bash
JIB_TO_IMAGE=registry.example.com/platform/yarn-log-api \
JIB_TO_USERNAME="$REGISTRY_USER" \
JIB_TO_PASSWORD="$REGISTRY_PASSWORD" \
./gradlew :app:jib
```
