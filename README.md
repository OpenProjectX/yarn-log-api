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
