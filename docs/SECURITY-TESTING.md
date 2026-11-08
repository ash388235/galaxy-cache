# Galaxy Cache security testing

Threat model, findings and a repeatable test plan. All tests so far ran on one Windows laptop against nodes on
localhost. Nothing here has been tested over a real network or by an independent reviewer.

## What needs protecting

| Asset | Worst outcome |
|---|---|
| Trade data on node disks and in client memory | Read, changed or deleted by someone unauthorized |
| Grid availability | One bad request stops every node (fail-stop makes any node crash a full outage) |
| Authentication token, TLS keys | Full access to the grid |
| Machines running clients and nodes | Code execution through hostile stored data |

## Who could attack, and from where

| Attacker | Position | What stops them |
|---|---|---|
| Anyone on the network, no credentials | Can open TCP connections to nodes | Bound listeners, handshake limits, token auth, connection cap |
| Network observer or man in the middle | Can read or alter traffic | TLS. **Without TLS only the login is protected; data and later frames are not** |
| Holder of the token (buggy or malicious client) | Full protocol access | Request validation and limits. The token grants everything: there are no roles |
| Someone who can write to the store | Plants hostile records read by clients | Decoder allow-lists and size limits |
| Web page in an operator's browser | Reaches localhost services | Host header check, GET only, no CORS |
| Local user on a node | Reads data folders and configs | Not covered: use operating system permissions and disk encryption |

## Trust boundaries

1. Client or peer to node, grid TCP port: frames are untrusted until the handshake ends, and only partly trusted after it.
2. Browser to dashboard (HTTP, read-only, no login).
3. Monitoring to node admin port (HTTP, read-only, no login, counts only).
4. Client to index pod (HTTP, no login). Used only by namespaces not on the grid.
5. Storage to client decoder: stored bytes are untrusted.

## Findings

Status is as of this document. "Confirmed" means the attack was run and worked before the fix.

| # | Severity | Finding | Status |
|---|---|---|---|
| 1 | High | A small query with many nested NOTs exhausted the heap and stopped the grid (confirmed: 11 KB request) | Fixed: filter limits (depth 8, 256 conditions, 4 NOTs, 10,000 values), concurrent query limit |
| 2 | High | A request claiming 2 billion items allocated for them and stopped the grid (confirmed) | Fixed: every claimed count must fit in the bytes received |
| 3 | High | No limit on connections, and 1.25 MB of buffers per connection before login, so anyone could exhaust a node's memory and stop the grid | Fixed: connection cap (`maxConnections`), 32 KB per connection before login, 5 s handshake timeout |
| 4 | High | The binary decoder loaded any class named in stored data and could instantiate it | Fixed: only classes in the domain's package that fit the field; nesting and size limits |
| 5 | High | Java deserialization allowed every `java.*` class (gadget risk) with no depth or size limits | Fixed: exact package allow-list, depth, reference and array limits |
| 6 | High | Index pods listened on all network interfaces with no login, including the drop API, and read request bodies of any size | Partly fixed: listens only on the host in its URL, 64 MB body limit, filter limits. **Still no login (#13)** |
| 7 | High | One saved index document with a bad value stopped that node from starting ever again | Fixed: values checked before saving; unreadable documents skipped at startup and counted |
| 8 | Medium | Authentication was one-way and a proof was not tied to the protocol or configuration: a fake node could accept a client | Fixed: mutual proofs bound to both nonces, direction and fingerprint; a client with a token refuses nodes offering no authentication |
| 9 | Medium | One connection could queue unlimited requests, and large requests could fill memory | Fixed: per-connection in-flight limit, total request-memory budget, 30 s limit to receive a request |
| 10 | Medium | A node could listen on the network with no authentication | Fixed: refuses unless `allowNoAuthOnNetwork: true` |
| 11 | Medium | Windows device names (`NUL`, `CON`...) and trailing dots were accepted as dataset names | Fixed |
| 12 | Medium | Stack overflow in a request stopped the whole grid (any `Error` caused fail-stop) | Fixed: only out-of-memory causes fail-stop |
| 13 | Medium | Index pods have no authentication | **Open.** Keep them on loopback or a private network, or move those namespaces to the grid |
| 14 | Medium | Without TLS, data and every frame after login can be read or altered on the network. Off in the local config | **Open by configuration.** Set `authToken` and `tls: true` before using a real network |
| 15 | Medium | One shared token: no users, roles or audit of who did what. Any client can drop datasets or start compaction | **Open** (design limit) |
| 16 | Medium | Data at rest is not encrypted | **Open.** Use disk encryption and folder permissions |
| 17 | Low | Dashboard could be read by a web page through DNS rebinding; no security headers; accepted any method | Fixed: Host check, GET only, CSP and related headers |
| 18 | Low | Printing configuration objects would include tokens and passwords | Fixed: redacted |
| 19 | Low | `tls: true` without a keystore failed only when a client connected | Fixed: refuses to start |
| 20 | Low | A self-signed certificate used directly as a trust anchor is not checked for expiry (Java behaviour) | **Open by configuration.** Use a CA; expired CA-issued certificates are refused (tested) |
| 21 | Low | Query logs include filter values, which may be sensitive | **Open.** Protect the log folder |
| 22 | Low | Dashboard and admin port have no login (read-only; admin shows counts and dataset names only) | **Open.** Keep them on loopback or a management network |
| 23 | Low | No throttling of failed logins (a long random token makes guessing impractical; failures are counted) | **Open** |
| 24 | Low | Passing the token with `-D` exposes it in process listings | **Open.** Use `${env:NAME}` |
| 25 | Low | Slow-request limits on the JDK HTTP servers (dashboard, admin, index pod) are not configured | **Open** |

Reviewed with no issue found: dataset names cannot traverse paths (strict character rules, tested); JSON has no
polymorphic type handling and Jackson's nesting limit applies; dashboard output is escaped; the token is compared in
constant time; replayed proofs fail because each connection has fresh nonces.

## Dependencies

Runtime libraries shipped in the node: Jackson 2.17.2 (databind, core, annotations, jsr310, yaml) and SnakeYAML 2.2.
JUnit is test-only. Runtime: Temurin 21.0.11.

**No vulnerability database was queried**, because this work was kept offline and the OWASP plugin is not in the local
Maven cache. Going from memory, the well-known issues (SnakeYAML CVE-2022-1471, fixed in 2.0; Jackson core deep-nesting
CVE-2025-52999, fixed in 2.15) do not affect these versions, but that is not a scan. On a machine allowed to reach the
internet, run:

```bash
mvn org.owasp:dependency-check-maven:aggregate -DfailBuildOnCVSS=7
```

## Repeatable tests

Run from the repository root. Everything uses local nodes on loopback.

```bash
mvn test
```

| Area | Test class | Covers |
|---|---|---|
| Login | `GridAttackTest` | No proof, wrong token, replayed proof, node proof reused as client proof, fake node, auth downgrade, short token |
| Bad bytes before login | `GridAttackTest` | HTTP or TLS sent to the port, garbage, huge proof length, silent and one-byte-at-a-time connections, 300-connection flood |
| Bad frames after login | `GridAttackTest` | Negative, zero and huge lengths, truncated frames, claimed billions of items, expensive filters, very deep JSON, 3,000 random frames, 3,000 requests unread |
| Paths and stored data | `GridAttackTest`, `HostileDataTest` | Dataset path tricks with a check of files on disk, poisoned index document and restart |
| Hostile records | `HostileDataTest` | Gadget class names, subclass smuggling, huge sizes, 2 million nested levels, every truncation, 20,000 corrupted payloads, URLDNS, SerializedLambda, deep and huge Java streams |
| TLS | `GridSecurityTest` | Encrypted round trip, plain client refused, untrusted certificate, wrong host name, expired CA-issued certificate, no keystore |
| Exposure | `GridAttackTest`, `DashboardSecurityTest`, `IndexServerSecurityTest` | Admin port leaks nothing, secrets redacted, no-auth network listen refused, Host header, methods, hostile parameters, loopback only, oversized bodies |

Every grid attack ends by checking that no node stopped and a normal client can still read and write.

## What still needs a real network or a pen test

- Independent penetration test and code review. This was reviewed and tested by its author.
- TLS and token auth across separate machines, including certificate rotation and a real CA.
- Packet capture confirming nothing sensitive crosses the network with TLS on.
- Network flood tests at realistic rates from several hosts, including connection and bandwidth exhaustion.
- A long hostile-load run. Memory limits here were exercised with a 1.5 GB node heap on one machine.
- Online dependency scan, and container or host hardening review when the deployment files are used.
- Firewall rules for the grid, admin, dashboard and index ports.
