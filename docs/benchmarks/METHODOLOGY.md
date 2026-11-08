# Measurement methodology

API acceptance, HTTP attempts and SQL-confirmed outcomes are separate quantities.
Completed deliveries require matching terminal SQL state; attempt counts are not
assumed equal to accepted events under failures. Healthy fixed-size batches had
one observed HTTP attempt per delivered event. Finite retries can exhaust and
external duplicates remain possible in failure scenarios.

Setup and migrations precede measurement. p50/p95/p99 represent the retained
per-run latency summaries in controlled-runs.csv; they are not pooled percentiles
and should not be averaged into a service SLA. Worker comparisons use fixed batch
size only in the controlled Stage5B experiment. Exploratory Stage5A changes batch
sizes too. Three repetitions expose variation but do not characterize sustained
capacity. Private raw logs and SQL fixture identifiers are intentionally not
republished. Public CSV projections and provenance hashes support arithmetic
review without exposing the original environment/history.

Run `python3 scripts/check-benchmarks.py` for a bounded arithmetic check of the
curated observations. It creates no events,load,containers or database connections.
Historical load harnesses depended on the original private audit layout/baseline
and are deliberately not presented as fresh-clone commands in this public selection.
