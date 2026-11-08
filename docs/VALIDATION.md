# Validation and provenance

This repository is a deliberately selected representation of a locally developed
GoRelay project. A fresh history with GitHub noreply authorship avoids exposing
private commit email/local-user/environment records from the original repository.
The original engineering history,raw logs,patches and excluded screenshots were
preserved separately;they were not rewritten or uploaded. Curated benchmark data
retain numeric observations and checksums without SQL fixture identifiers.

## Final local regression on the selected source

- Go1.27.3,PostgreSQL17.11,real SQL integration with required mode enabled.
- gofmt checked without rewriting;go vet passed.
-85top-level PASS records normally and85under the race detector;zero SQL skips.
  Four opt-in experiment gates per run return without exercising their experiments;
  these are not counted as evidence of a new performance/resilience campaign.
- Static Go build and runtime Docker build passed. The image includes project
  license/scope and actual upstream runtime notices;no binary release is published.
- Actual Chromium desktop1440/mobile390 checks and bounded local HTTP demo outcomes
  are summarized in public-validation.json. No Internet-facing service was deployed.
- A new govulncheck source/symbol scan reported0finding events for its recorded
  database;that is not proof of absolute security. See DEPENDENCIES.md.
- python3 scripts/check-benchmarks.py verifies historical arithmetic only,no load.

The isolated regression used a dedicated Docker network/PGvolume,hard2CPU/1536MiB
limits for Go and1CPU/512MiB for PostgreSQL,max60connections. Native browser-smoke
Go runtime controls are advisory rather than hard CPU/RSS quotas. Synthetic
fixtures only were used. No unrelated service or database was modified. Temporary
resources are stopped after reconciliation and preserved rather than removed.

## Independent review

Separate agent contexts independently checked the selected files for secrets,
privacy,asset provenance,scope/rights,CSV projections and source/module integrity.
They used their own byte/Git/static/tool checks and primary dependency sources.
The full local Go suite was executed by the producer and independently reconciled
from its real logs;it was not falsely described as a second full suite execution.
Some remediation context was known to reviewers,so this is independent authorship
and verification,not a claim of a blind audit or external human/legal certification.

N0 denotes human assertions,N1 inspection,N2 own execution andN3 independent
verification of the exact artifact. These levels do not certify legal ownership,
production security,full accessibility or sustained capacity. No mutation
calibration or exhaustive image OCR/steganography analysis was performed.

## Remote CI

The workflow requires real PostgreSQL,gofmt,vet,normal/race tests,a staticGo build,
curated-data arithmetic and a runtime Docker build. Its Actions runs record the
actual outcome for each exact Git commit: [GitHub Actions](https://github.com/rodrigoniskier/gorelay/actions).
Local results are not a substitute for those runs. The workflow has read-only
permissions,pinned official actions and no deploy,image push or release step.

## Rights and limitations

Only20original source files are MIT;49historical visual contents have reuse
reserved and only three selected visual files are present here. Source hashes and
visual provenance are recorded in root LICENSE-SCOPE.json. No other content gains
automatic MIT. Third-party terms remain applicable. Exact crash placement before
the first send,complete late responses,real partitions/physical failures,full
accessibility and sustained production capacity remain incompletely demonstrated.
