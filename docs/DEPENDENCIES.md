# Dependency review

The previous graph produced 25advisory IDs: 20module-only matches in x/crypto,
two package matches and one symbol match in pgx,and two symbol matches in x/text.
These were not 25demonstrated exploits. SQL/protocol/password/profile prerequisites
were inspected separately;no application exploit was reproduced.

## Applied compatible graph update

- pgx/v5 v5.7.6 → v5.12.0 (same module major,requires Go>=1.25).
- Its required transitives selected puddle/v2 v2.2.3,x/sync v0.22.0 and x/text v0.41.0.
- x/crypto v0.37.0 was removed by go mod tidy;pgx now uses standard-library PBKDF2.
- No checksum verification was disabled. Application source and migrations are unchanged.

A new govulncheck 1.9.0 source/symbol scan on Go 1.27.3 reported **zero finding events**.
Database last-modified metadata: 2026-11-05T23:49:57Z. This is the result
for the selected revision and database,not proof of absolute vulnerability absence.
Full normal/race required-PostgreSQL regression passed;see VALIDATION.md.

| Prior advisory | Old component/version | Prior matching level | Resolution in selected graph |
| --- | --- | --- | --- |
| [GO-2025-4116](https://pkg.go.dev/vuln/GO-2025-4116) | golang.org/x/crypto v0.37.0 | module | Module removed;no current matching event |
| [GO-2025-4134](https://pkg.go.dev/vuln/GO-2025-4134) | golang.org/x/crypto v0.37.0 | module | Module removed;no current matching event |
| [GO-2025-4135](https://pkg.go.dev/vuln/GO-2025-4135) | golang.org/x/crypto v0.37.0 | module | Module removed;no current matching event |
| [GO-2026-4771](https://pkg.go.dev/vuln/GO-2026-4771) | github.com/jackc/pgx/v5 v5.7.6 | package | Updated to v5.12.0;no current matching event |
| [GO-2026-4772](https://pkg.go.dev/vuln/GO-2026-4772) | github.com/jackc/pgx/v5 v5.7.6 | package | Updated to v5.12.0;no current matching event |
| [GO-2026-5004](https://pkg.go.dev/vuln/GO-2026-5004) | github.com/jackc/pgx/v5 v5.7.6 | symbol | Updated to v5.12.0;no current matching event |
| [GO-2026-5005](https://pkg.go.dev/vuln/GO-2026-5005) | golang.org/x/crypto v0.37.0 | module | Module removed;no current matching event |
| [GO-2026-5006](https://pkg.go.dev/vuln/GO-2026-5006) | golang.org/x/crypto v0.37.0 | module | Module removed;no current matching event |
| [GO-2026-5013](https://pkg.go.dev/vuln/GO-2026-5013) | golang.org/x/crypto v0.37.0 | module | Module removed;no current matching event |
| [GO-2026-5014](https://pkg.go.dev/vuln/GO-2026-5014) | golang.org/x/crypto v0.37.0 | module | Module removed;no current matching event |
| [GO-2026-5015](https://pkg.go.dev/vuln/GO-2026-5015) | golang.org/x/crypto v0.37.0 | module | Module removed;no current matching event |
| [GO-2026-5016](https://pkg.go.dev/vuln/GO-2026-5016) | golang.org/x/crypto v0.37.0 | module | Module removed;no current matching event |
| [GO-2026-5017](https://pkg.go.dev/vuln/GO-2026-5017) | golang.org/x/crypto v0.37.0 | module | Module removed;no current matching event |
| [GO-2026-5018](https://pkg.go.dev/vuln/GO-2026-5018) | golang.org/x/crypto v0.37.0 | module | Module removed;no current matching event |
| [GO-2026-5019](https://pkg.go.dev/vuln/GO-2026-5019) | golang.org/x/crypto v0.37.0 | module | Module removed;no current matching event |
| [GO-2026-5020](https://pkg.go.dev/vuln/GO-2026-5020) | golang.org/x/crypto v0.37.0 | module | Module removed;no current matching event |
| [GO-2026-5021](https://pkg.go.dev/vuln/GO-2026-5021) | golang.org/x/crypto v0.37.0 | module | Module removed;no current matching event |
| [GO-2026-5023](https://pkg.go.dev/vuln/GO-2026-5023) | golang.org/x/crypto v0.37.0 | module | Module removed;no current matching event |
| [GO-2026-5033](https://pkg.go.dev/vuln/GO-2026-5033) | golang.org/x/crypto v0.37.0 | module | Module removed;no current matching event |
| [GO-2026-5932](https://pkg.go.dev/vuln/GO-2026-5932) | golang.org/x/crypto v0.37.0 | module | Module removed;no current matching event |
| [GO-2026-5970](https://pkg.go.dev/vuln/GO-2026-5970) | golang.org/x/text v0.24.0 | symbol | Updated to v0.41.0;no current matching event |
| [GO-2026-6303](https://pkg.go.dev/vuln/GO-2026-6303) | golang.org/x/crypto v0.37.0 | module | Module removed;no current matching event |
| [GO-2026-6354](https://pkg.go.dev/vuln/GO-2026-6354) | golang.org/x/crypto v0.37.0 | module | Module removed;no current matching event |
| [GO-2026-6355](https://pkg.go.dev/vuln/GO-2026-6355) | golang.org/x/crypto v0.37.0 | module | Module removed;no current matching event |
| [GO-2026-6629](https://pkg.go.dev/vuln/GO-2026-6629) | golang.org/x/text v0.24.0 | symbol | Updated to v0.41.0;no current matching event |

## Scope and residuals

Prior retained OSV records did not supply vendor severity values;none are invented.
Priority based on static reachability is not vendor CVSS or confirmed exploitation.
The old OpenPGP inventory advisory would not be cleared simply by choosing stable
x/crypto v0.58.0;the now-unused module was removed rather than forcing a pseudo-version.

Independent old/new pgx source review checked parameter sanitization,driver APIs,
SCRAM and module requirements. ASCII-password local PostgreSQL and application
invariants passed real regression. Unicode-password edge cases,TLS channel binding
and every custom Go 1.27 scanning implementation are not exhaustively covered.
Those are residual compatibility limits,not a claim that current HTTP exploits exist.

The local-only demo has no authentication,tenant isolation or production transport.
Reassess future configuration/features and repeat the scan after dependency changes.
Upstream terms and full notices are in THIRD-PARTY-NOTICES.md and licenses/.

## Continuous scanning

The toolchain moved from Go 1.27.3 to 1.27.4 because the first CI `govulncheck` run
reported GO-2026-6859 (net/http `MaxBytesReader` header race, fixed in 1.27.4) as
reachable from the event API. The 1.27.3 scan above is therefore historical.

CI runs `govulncheck` on every push and pull request, and `.github/dependabot.yml`
proposes weekly updates for Go modules, Docker base images and GitHub Actions.
The one-time classification above remains a historical snapshot, not a guarantee.
