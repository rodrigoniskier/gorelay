# Local troubleshooting

- **Compose rejects password**: copy `.env.example` to `.env` and replace its
  placeholder. `.env` is not loaded by the Go application. Never print it in a report.
- **Port already used**: choose an unused POSTGRES_PORT, and check that port18084
  is available before demo startup. Do not stop unrelated processes.
- **Readiness503/startup fails**: verify the owned database is healthy and runtime
  DSN correct. Migration checksum/version failures need investigation, not editing
  old migrations or dropping the database.
- **No deliveries**: workers default to zero. Use the explicit demo helper or
  provide workers, local HTTP mode and a configured destination. Finite budgets,
  expired leases and retry schedules explain temporarily pending/terminal events.
- **Cannot reach Docker/WSL demo**: host networking may terminate in the DockerVM.
  Use the native-binary path in DEMO.md; never widen the listener to 0.0.0.0.
- **Demo container name exists**: review its image/environment/DB ownership before
  `docker start -a gorelay-dashboard-demo`; do not delete it or silently reuse a
  stale configuration. Native mode avoids that application-container name.
- **Go missing**: Docker builds need no host Go. Native builds/tests require
  Go1.27.3 and GOTOOLCHAIN=local; Docker's own compiled Go version is unrelated.
- **Tests skip SQL**: run scripts/verify.sh with a dedicated TEST_DATABASE_URL.
  Never substitute the runtime DB to hide a configuration failure.
- **CI unavailable**: local success is not a remote run. Check toolchain registry,
  action versions and service startup output before claiming remote compatibility.
