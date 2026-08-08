# GoAI Response Contract

This document defines the request-level response contract shared by GoAI HTTP APIs and the `POST /api/chat` debug stream.

## JSON Envelope

Every JSON response uses the following shape:

```json
{
  "code": "OK",
  "message": "success",
  "data": {},
  "trace_id": "trace_xxx"
}
```

Fields:

- `code`: stable string code. Successful responses use `OK`.
- `message`: stable, client-facing description. It is not a database or provider error dump.
- `data`: business payload. It is `null` when the response has no payload.
- `trace_id`: request correlation identifier.

The response body is an API contract, not a log format. Internal errors are logged with their cause while the client receives the mapped public error code and message.

## Trace Propagation

- Clients may send `X-Trace-ID`.
- When the header is absent, GoAI generates a request trace ID.
- The same value is returned in the `X-Trace-ID` response header and the `trace_id` envelope field.
- Run creation includes the trace ID in the Kafka execution message.
- The worker restores the trace ID into its execution context and includes it in worker logs.

Example:

```http
X-Trace-ID: trace-client-001
```

```http
HTTP/1.1 202 Accepted
X-Trace-ID: trace-client-001
Content-Type: application/json
```

```json
{
  "code": "OK",
  "message": "run queued",
  "data": {
    "run_id": "run-001",
    "status": "queued"
  },
  "trace_id": "trace-client-001"
}
```

## Error Mapping

| Code | HTTP status | Meaning |
| --- | ---: | --- |
| `AUTH_MISSING_TOKEN` | 401 | Authorization header is missing |
| `AUTH_INVALID_TOKEN` | 401 | JWT is invalid or expired |
| `AUTH_INVALID_CREDENTIALS` | 401 | Login credentials are invalid |
| `AUTH_FORBIDDEN` | 403 | The authenticated user lacks permission |
| `VALIDATION_FAILED` | 400 | Request fields failed validation |
| `INVALID_ID` | 400 | A path or query ID is invalid |
| `USER_NOT_FOUND` | 404 | User does not exist |
| `USER_ALREADY_EXISTS` | 409 | Username or email is already registered |
| `RUN_NOT_FOUND` | 404 | Run does not exist or is not accessible |
| `LOOP_NOT_FOUND` | 404 | Loop does not exist or its associated Run does not exist |
| `AGENT_NOT_FOUND` | 404 | Agent does not exist |
| `AGENT_ALREADY_EXISTS` | 409 | Agent code is already registered |
| `AGENT_INVALID_STATE` | 409 | Agent or Endpoint changed during the requested state transition |
| `AGENT_PUBLISH_VALIDATION_FAILED` | 422 | Agent publication assets are incomplete or inconsistent |
| `CAPABILITY_NOT_FOUND` | 404 | Agent capability does not exist |
| `CAPABILITY_ALREADY_EXISTS` | 409 | Capability code already exists for the Agent |
| `ENDPOINT_NOT_FOUND` | 404 | Agent endpoint does not exist |
| `ENDPOINT_ALREADY_EXISTS` | 409 | Endpoint code already exists for the Agent |
| `ENDPOINT_HEALTH_CHECK_FAILED` | 502 | A2A Agent Card discovery or identity validation failed |
| `WORKFLOW_NOT_FOUND` | 404 | Workflow version does not exist for the Agent |
| `WORKFLOW_ALREADY_EXISTS` | 409 | Workflow version already exists for the Agent |
| `WORKFLOW_INVALID_STATE` | 409 | Workflow cannot be edited or deactivated in its current state |
| `IDEMPOTENCY_KEY_REUSED` | 409 | The same idempotency key was reused for another request |
| `MCP_SERVER_NOT_FOUND` | 404 | MCP Server does not exist or is outside the owner scope |
| `MCP_SERVER_ALREADY_EXISTS` | 409 | MCP Server code already exists for the owner |
| `MCP_SERVER_INVALID_STATE` | 409 | MCP configuration or active Workflow reference is in conflict |
| `MCP_SERVER_UNHEALTHY` | 502 | MCP initialize or tools/list health check failed |
| `MCP_TOOL_NOT_FOUND` | 404 | Tool is absent from the discovery snapshot |
| `MCP_TOOL_INVOCATION_FAILED` | 502 | MCP tools/call failed |
| `MCP_INVALID_CONFIG` | 400 | MCP configuration or Tool input is invalid |
| `MCP_CREDENTIAL_NOT_FOUND` | 503 | Credential reference cannot be resolved |
| `MCP_TRANSPORT_FAILED` | 502 | MCP Streamable HTTP transport failed |
| `MCP_PROTOCOL_FAILED` | 502 | MCP protocol response is invalid or unsupported |
| `MCP_TOOL_REPORTED_ERROR` | 502 | Tool returned `isError` |
| `PROVIDER_NOT_FOUND` | 404 | Requested provider is not registered |
| `PROVIDER_DRIVER_NOT_FOUND` | 500 | Provider has no usable driver |
| `PROVIDER_INVALID_CONFIG` | 500 | Provider configuration is invalid |
| `MODEL_NOT_CONFIGURED` | 500 | No model is configured for the request |
| `STREAM_INTERRUPTED` | 502 | An active model stream was interrupted |
| `RBAC_PERMISSION_LOAD_FAILED` | 500 | Permissions could not be loaded |
| `KAFKA_PUBLISH_FAILED` | 503 | Run execution could not be published |
| `INTERNAL_ERROR` | 500 | An unexpected internal error occurred |

Error example:

```json
{
  "code": "AUTH_FORBIDDEN",
  "message": "permission denied",
  "data": null,
  "trace_id": "trace-client-001"
}
```

## Chat SSE

`POST /api/chat` keeps `Content-Type: text/event-stream`. Before the stream starts, validation and provider errors are returned as a normal JSON envelope. After the stream starts, the HTTP status remains `200`; stream failures are sent as an `error` event.

Every SSE `data:` payload is the same JSON envelope shape and carries the same `trace_id`.

### Chunk

```text
event: chunk
data: {"code":"OK","message":"chunk","data":{"content":"hello"},"trace_id":"trace-client-001"}

```

### Done

```text
event: done
data: {"code":"OK","message":"done","data":{"done":true},"trace_id":"trace-client-001"}

```

### Error

```text
event: error
data: {"code":"STREAM_INTERRUPTED","message":"model stream interrupted","data":null,"trace_id":"trace-client-001"}

```

Clients should parse both the SSE event name and the JSON envelope. A stream is complete after `done`; `error` is terminal for that stream.

## Compatibility Boundary

This response envelope is for GoAI HTTP and debug SSE surfaces. AG-UI and A2A keep their protocol-specific wire formats at their gateways. Gateway handlers map those protocol messages to internal `Thread`, `Message`, `Run`, and `Delegation` objects instead of leaking the envelope into Agent-to-Agent protocol payloads.
