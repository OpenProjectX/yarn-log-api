# YARN Log API

Reactive YARN application log streaming for Hadoop 3.4.2. The starter reads
running-container logs from NodeManagers and finished logs through Hadoop's
configured log aggregation file controller. It never launches the `yarn logs`
CLI.

The embedding application must configure Hadoop and perform UGI login before
the auto-configuration creates `YarnClient`. In a Kerberos cluster the library
uses that same login UGI to establish SPNEGO and caches the resulting
`hadoop.auth` cookie per NodeManager origin.

### Embedding the starter in your own image

Hadoop must be able to resolve the container process UID to an operating-system
username. If your image uses a numeric non-root UID, add a matching entry to
both `/etc/passwd` and `/etc/group`. Otherwise UGI initialization can fail with
`YarnRuntimeException: Unable to determine user`, even on a non-Kerberos
cluster. For example:

```text
# /etc/passwd
yarn-log-api:x:10001:10001:YARN Log API:/app:/usr/sbin/nologin

# /etc/group
yarn-log-api:x:10001:
```

Make `core-site.xml`, `yarn-site.xml`, and any storage-specific configuration
available on the application classpath. Setting `HADOOP_CONF_DIR` alone does not
alter the classpath of a directly launched JVM; the Hadoop shell scripts usually
perform that step. Add the mounted directory to the JVM classpath or load its XML
files into your `Configuration` bean explicitly.

In a secured cluster, call `UserGroupInformation.setConfiguration` and complete
the keytab or ticket-cache login while creating that bean—not from an
`ApplicationRunner`, which executes after the auto-configured `YarnClient` has
already been created:

```kotlin
@Bean
fun yarnConfiguration(): Configuration = YarnConfiguration().also { configuration ->
    UserGroupInformation.setConfiguration(configuration)
    UserGroupInformation.loginUserFromKeytab(principal, keytab)
}
```

The starter then uses the same login UGI for YARN client calls, aggregated-log
filesystem access, and NodeManager SPNEGO. Do not configure a second independent
Kerberos identity on `WebClient`.

## SSE

```http
GET /api/v1/yarn/applications/application_123_0001/logs?follow=true&logFiles=stdout,stderr
Accept: text/event-stream
```

Log events include both representations:

- `data`: Base64 containing the exact bytes used by `offset`.
- `text`: a convenient UTF-8 view with newlines, tabs, backslashes, and control
  characters escaped so the value is printable on one line.

For example, a log chunk containing two lines has a payload like:

```json
{
  "type": "LOG",
  "encoding": "BASE64",
  "data": "Zmlyc3QKc2Vjb25kCg==",
  "text": "first\\nsecond\\n"
}
```

Use `data` for exact reconstruction because a byte chunk can split a multi-byte
UTF-8 character; use `text` for terminals, dashboards, and diagnostics.

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
