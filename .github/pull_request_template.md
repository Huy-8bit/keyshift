## Summary

<!-- What does this change and why? -->

## Correctness and safety

- [ ] Invalidation safety is preserved (no new path serves cached data without tracking)
- [ ] Resource bounds are preserved (no unbounded maps, queues, goroutines, or label sets)
- [ ] No Redis keys or values are added to metric labels or logs

## Testing

- [ ] `make lint test-race`
- [ ] `make test-integration` (if Redis behavior is affected)
- [ ] Benchmarks re-run and docs/benchmarks.md updated (if performance is affected)

## Documentation

- [ ] Docs and CHANGELOG.md updated for user-visible changes
