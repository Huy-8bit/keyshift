# Contributing to KeyShift

Thank you for helping. KeyShift is infrastructure that sits in front of production data, so changes are judged first on **correctness and invalidation safety**, then on **resource bounds**, and only then on features or performance.

## Development setup

Requirements: Go 1.26+, Node.js 20+ (dashboard), Docker (integration tests and lab).

```bash
make help              # all targets
make lint              # gofmt check, go vet, golangci-lint, dashboard typecheck
make test-race         # unit tests with the race detector
make test-integration  # starts Redis 8.6, 7.4, and Cluster in Docker; runs integration and fault tests
make test-fuzz         # fuzz targets (FUZZTIME=15s each)
make bench             # micro-benchmarks
make build             # dashboard + binaries in bin/
make docker-up         # local lab with dashboard on :8080
```

## Ground rules

- **Do not implement Redis protocol or tracking logic** that the client libraries already provide. Use their documented extension points.
- **Every new map, queue, goroutine, or label set needs a bound** and a test for it.
- **Never put Redis keys or values in metric labels or logs.**
- **Safety-critical changes need a real-Redis test** (`//go:build integration`) that would fail without the change.
- **Do not weaken assertions to make tests pass.** If a test is flaky, find out why. Several real bugs in this project were found that way.
- Benchmarks must include unfavorable workloads, and numbers in docs must come from runs you can reproduce with `benchmarks/run-all.sh`.

## Pull requests

1. Open an issue first for significant changes.
2. Keep PRs focused; include tests and docs updates (`docs/`, `CHANGELOG.md`).
3. Fill in the PR template checklist.
4. CI must pass: lint, unit tests with race detection on Go 1.26 and 1.27, integration tests, build, and security scans.

Commits should explain *why*. By contributing you agree that your contributions are licensed under the Apache License 2.0.

## Reporting security issues

Do not open public issues for vulnerabilities; see [SECURITY.md](SECURITY.md).
