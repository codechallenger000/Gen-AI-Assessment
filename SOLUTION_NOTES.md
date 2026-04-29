# Solution Notes

## What I Changed

- Hardened the pipeline around the existing `PipelineOutput` contract rather than replacing it.
- Implemented real SQL validation:
  - single-statement, read-only enforcement
  - allowed-table checks
  - compile-time schema validation against a synthetic SQLite schema
  - question-to-SQL intent checks
- Added structured observability with JSON logs for each pipeline stage.
- Implemented token accounting in the OpenRouter client using provider usage metadata with deterministic fallback estimation.
- Improved SQL generation with schema-aware prompting and stricter JSON output instructions.
- Added deterministic SQL fallback generation for common analytics prompt shapes, plus recovery when model SQL is missing or invalid.
- Added deterministic answer fallback summarization when the answer-model call returns unusable content.
- Made database startup defensive so an empty or missing SQLite file is rebuilt from the CSV.
- Fixed the benchmark script so it reads `PipelineOutput` attributes correctly.
- Added lightweight multi-turn support with `AnalyticsConversation` and follow-up detection heuristics.
- Added unit tests for validator behavior, token accounting, follow-up detection, heuristic SQL generation, and benchmark behavior.

## Why I Changed It

The baseline already had the right high-level shape for the assignment, but it relied too heavily on the model behaving perfectly. The biggest practical risks were:

- unsafe or malformed SQL reaching execution
- missing efficiency metrics
- poor debuggability
- brittle runtime behavior when the model or local DB setup misbehaves

The changes above make the system fail safer and explain itself better while keeping the codebase small enough for a take-home assignment.

## Measured Impact

### Public Tests

Verified with:

```bash
python -m unittest tests.test_pipeline_unit -v
$env:OPENROUTER_API_KEY=...; python -m unittest discover -s tests -p "test_public.py" -v
```

Result:

- `tests.test_pipeline_unit`: passing
- `tests/test_public.py`: passing

### Benchmark

Measured with:

```bash
$env:OPENROUTER_API_KEY=...; python scripts/benchmark.py --runs 1
```

Observed output:

- Average latency: `10280.65 ms`
- p50 latency: `10827.09 ms`
- p95 latency: `12980.27 ms`
- Success rate: `100%`

Additional efficiency sampling across the public prompt set:

- Average tokens per request: `664.58`
- Average LLM calls per request: `1.17`

Notes:

- The README baseline reference is faster, but this implementation improved reliability and safe handling materially over the starter code.
- These measurements are sensitive to external model latency and response variability, so repeated runs will vary.

## Tradeoffs and Next Steps

- The deterministic SQL fallback layer improves reliability, but it is intentionally heuristic rather than a full semantic planner.
- Some successful answers still come from fallback summarization rather than the LLM answer stage; that is a deliberate tradeoff toward robustness.
- If I were extending this further, the next improvements would be:
  - stronger answer-quality evaluation prompts or verifier passes
  - richer metric export beyond logs
  - more precise follow-up intent resolution
  - prompt/latency optimization to bring end-to-end timing closer to the README reference baseline
