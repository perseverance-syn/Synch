# Request receipts — v3.0.0

Source: spec "Command semantics", "Persistent state"; decision 18 (same principle for outbound requests). API binding: `api/openapi.yaml` (`Synch-Request-Id`, `Synch-Receipt-Id`, `Synch-Receipt-Replayed` headers, `Receipt` schema). Executable reference for §2–3: `check.mjs` section 6 with `tests/digest-vectors.json`.

## 1. Processing order
Every mutating request goes through these steps in order. Steps 1–3 run **before** any receipt lookup, and their failures never create, read or consume a receipt.

| step | check | failure | stored? |
|---|---|---|---|
| 1 | caller token valid; it resolves to a caller identity | `401 unauthorized` | no (there is no caller to key on) |
| 2 | `Synch-Request-Id` present and matching `^[A-Za-z0-9._:-]{1,128}$` | `400 request_id_required` / `400 request_id_invalid` | no |
| 3 | body admission (§3) | `400 malformed_body` (`detail` names the reason: `invalid_utf8`, `invalid_unicode`, `duplicate_member`, `number_not_integer`, `number_out_of_range`, `malformed_json`, `body_must_be_object`, `body_not_allowed`) | no |
| 4 | compute `payload_digest` (§2); look up key `(caller_identity, request_id)` | — | — |
| 5a | key present, same digest | **replay** the stored response (§4) | already stored |
| 5b | key present, different digest | `409 request_id_conflict` with `stored_payload_digest` and `receipt_id` | no; the stored receipt is unchanged |
| 6 | key absent: validate and apply the command | success → `accepted` receipt; deterministic rejection → `rejected` receipt (§5) | yes, in the same SQLite transaction as the command's events |
| 7 | 5xx / transaction rolled back | `500 internal` or `503` | no; a retry is a first request |

- The key uses `caller_identity`: the identity behind the token, not the token string (Q-04). Retries survive a token rotation.
- Steps 4–6 run in **one** SQLite transaction. Two concurrent requests with the same key serialize, and the second sees the first's receipt.
- The receipt is the **acceptance** outcome only. Delivery and execution follow as later events, linked by `request_id`.

## 2. Payload identity
`payload_digest = "sha256:" + lowercase_hex(SHA-256(UTF-8(JCS(identity))))`

```json
{ "method": "POST", "route": "/v1/threads/{thread_id}/commands", "path_params": { "thread_id": "01JD..." }, "body": { } }
```
- `route` is the OpenAPI path template. `path_params` holds the decoded path parameters (`{}` if none).
- `body` is the admitted body value (§3), or `null` when the body is absent on a route that takes none.
- JCS = RFC 8785: members sorted by UTF-16 code units, no whitespace, ECMAScript number serialization, and **no Unicode normalization**. NFC and NFD forms of the same text are different payloads, so reusing an ID across them returns `409`.
- Query strings and headers are excluded. Mutating commands carry no meaning in query strings.

## 3. Body admission (before hashing)
The daemon parses the **raw bytes** with an admission parser, not a lenient JSON library:
1. The bytes must be valid UTF-8, with no BOM. Otherwise `invalid_utf8`.
2. The text must be RFC 8259 JSON. Otherwise `malformed_json`.
3. Every string, after unescaping, must be valid Unicode with no lone surrogates. Otherwise `invalid_unicode` (I-JSON, RFC 7493).
4. Duplicate member names in any object → `duplicate_member`. Admission never picks one of them.
4a. **Every admitted member counts.** Member names are kept verbatim, including names that are special in some runtimes (`__proto__`, `constructor`, `prototype`, `toString`), and every member contributes to the digest. Implementations must not parse into structures with inherited or magic keys (JavaScript plain objects, for example). The reference parser in `check.mjs` builds null-prototype objects. Vectors `member-proto` and `member-constructor` must differ from `reorder-a`, and `member-proto-duplicate` must be rejected.
5. **Number domain:** every number must be an integer lexeme (`-?(0|[1-9][0-9]*)`) within ±(2^53−1), and not `-0`. Any fraction or exponent form, even an integral one like `1.0` or `1e0`, → `number_not_integer`. Out of range → `number_out_of_range`. v1 commands have no non-integer numbers, so this rule changes no command's meaning, and no two admitted bodies are numerically equal with different lexemes.
6. **Body presence per route:**

| route | body rule |
|---|---|
| `POST /v1/threads`, `POST /v1/threads/{thread_id}/commands`, `POST /v1/threads/{thread_id}/approvals`, `POST /v1/threads/{thread_id}/grants` | must be present and a JSON **object**. Absent, `null` or a non-object → `body_must_be_object` |
| `POST /v1/grants/{grant_id}/revoke` | must be **absent** (zero-length). Any body, including `null` or `{}` → `body_not_allowed` |

7. **Explicit `null` members** are admitted, so they get a digest distinct from the member being absent (vector `member-explicit-null`). In v1 no command field accepts `null`, so schema validation (step 6) then stores a `rejected` receipt. That receipt can never be confused with the success receipt of the body without the member, because the digests differ.

Distinct invalid requests therefore never map onto a prior successful receipt. A request is either refused before the lookup (steps 1–3) or carries its own digest.

## 4. Replay
A replay returns the stored `http_status` and the stored response body **byte-for-byte**, with the stored `Content-Type`. Headers:
- `Synch-Receipt-Id: <receipt_id>` on every response that has a receipt (first response and replays, accepted or rejected).
- `Synch-Receipt-Replayed: false` on the first response, and `true` on replays.
- Pre-receipt errors (steps 1–3), `409 request_id_conflict` and 5xx carry neither header. Their absence tells the client there is no receipt.

`GET /v1/receipts/{request_id}` returns the `Receipt` record, keyed by the caller's identity.

## 5. Stored rejections
A rejection is stored when step 6 refuses the command deterministically from the request and the state at that moment:

| code | status |
|---|---|
| `validation_failed` | 400 |
| `policy_denied`, `grant_required`, `not_owner`, `approval_required`, `approval_stale` | 403 |
| `not_found` | 404 |
| `invalid_transition` | 409 |
| `disk_pressure` | 422 |

**Receipt status vocabulary** (identical in this document, `Receipt` in openapi.yaml, and thread-state-machine.md §6): `status` is `accepted` or `rejected` and nothing else. `accepted` ⇒ `http_status` ∈ {201, 202} and no `problem`. `rejected` ⇒ `http_status` ∈ {400, 403, 404, 409, 422} and `problem` present. 5xx, pre-receipt errors and `409 request_id_conflict` never produce a receipt.

The receipt has `status: rejected` and its `problem` holds the problem body. The response body is that problem (`application/problem+json`) plus `receipt_id`. A retry with the same ID returns the same rejection **even if the state has since changed**. To try again after the state changes, the client uses a new request ID (Q-38).

## 6. Worked examples (valid against api/openapi.yaml 3.0.0)
Caller identity `operator:alice`. Thread `01JD3Q8Z7Y6X5W4V3T2S1R0QPN`.

**A. Retry, same payload.**
```
POST /v1/threads
Synch-Request-Id: c-1
{"project_id":"p1","brief":"Fix CI"}
```
digest = `sha256:89242a142a3c69cebbe2f28bf31cf7adcc97db62f1c1d3bbe0c9a2173e725883` (vector `reorder-a`). No receipt exists, so the daemon creates thread `01JD3Q8Z7Y6X5W4V3T2S1R0QPN`, stores receipt `01JD3QB0000000000000000001`, commits, and responds `201` with `Synch-Receipt-Id: 01JD3QB0000000000000000001` and `Synch-Receipt-Replayed: false`:
```json
{"receipt_id":"01JD3QB0000000000000000001","request_id":"c-1","payload_digest":"sha256:89242a142a3c69cebbe2f28bf31cf7adcc97db62f1c1d3bbe0c9a2173e725883","command":"createThread","status":"accepted","http_status":201,"thread_id":"01JD3Q8Z7Y6X5W4V3T2S1R0QPN","event_seq":1,"result":{"thread_id":"01JD3Q8Z7Y6X5W4V3T2S1R0QPN"},"created_at":"2026-11-18T10:00:00Z"}
```
The response is lost and the client retries with the same bytes, or with the keys reordered (vector `reorder-b`, same digest). The daemon returns the identical `201` body with `Synch-Receipt-Replayed: true`. No second thread is created.

**B. Retry, changed payload (conflict).** The client reuses `c-1` with `{"project_id":"p1","brief":"Fix CI and deploy"}`. The digest differs, so the response is `409`:
```json
{"type":"https://synch.local/problems/request_id_conflict","title":"Request ID reused with a different payload","status":409,"code":"request_id_conflict","stored_payload_digest":"sha256:89242a142a3c69cebbe2f28bf31cf7adcc97db62f1c1d3bbe0c9a2173e725883","receipt_id":"01JD3QB0000000000000000001"}
```
It carries no receipt headers. Nothing changes.

**C. Retry after daemon restart.**
```
POST /v1/threads/01JD3Q8Z7Y6X5W4V3T2S1R0QPN/commands
Synch-Request-Id: c-2
{"command":"cancel"}
```
The daemon commits receipt `01JD3QB0000000000000000002` + `request.accepted` + `thread.state_changed{T30}` in one transaction, then crashes before responding. After restart, the retry with `c-2` finds the durable receipt and returns the stored `202` with `Synch-Receipt-Replayed: true`. The cancel is applied once. If the crash came **before** commit, nothing was stored, and the retry is a first request that is applied once. The client cannot tell these apart and does not need to.

**D. Stored rejection.** `c-3` sends `{"command":"accept","revision":"0000000000000000000000000000000000000000"}` while the thread is `running`. The response is `409 invalid_transition`, stored with `status: rejected` and the problem body including `receipt_id`. A retry of `c-3` returns that same `409` with `Synch-Receipt-Replayed: true`, even after the thread reaches `awaiting_acceptance`. The client uses `c-4` to accept then.

**E. Same ID, different caller.** `operator:bob` sends `c-1`. The key `(operator:bob, c-1)` is a different key, so it is an independent receipt.
