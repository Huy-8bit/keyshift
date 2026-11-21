# Security policy

## Supported versions

| Version | Supported |
|---|---|
| 0.1.x | ✅ |

## Reporting a vulnerability

Please report vulnerabilities **privately** through GitHub Security Advisories:
<https://github.com/Huy-8bit/keyshift/security/advisories/new>

Include the affected version, configuration (driver, topology, Redis version), reproduction steps, and impact. We aim to acknowledge reports within 3 business days and to agree on a disclosure timeline with you.

Issues that cause KeyShift to serve stale or incorrect data in a way that violates the documented [consistency contract](docs/consistency.md), to leak key names or values through telemetry, metrics, or logs, or to bypass controller authentication are treated as security issues.

## Hardening guidance

See [docs/security-model.md](docs/security-model.md) for the threat model, data retention, and deployment recommendations (tokens, TLS, network policies, fingerprint secrets).
