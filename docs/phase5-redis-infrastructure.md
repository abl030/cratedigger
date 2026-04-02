# Phase 5: Redis Infrastructure

## Goal
Deploy Redis as an nspawn container on doc2, wire into soularr-web systemd service.

## Redis nspawn container

Similar to the PostgreSQL nspawn setup. Needs:
- Container config in nixosconfig
- Data persistence (optional — cache is ephemeral, can lose it on restart)
- Network: accessible from doc2 host at `192.168.100.XX:6379` (or localhost if using host networking)

## NixOS module changes

### soularr-web service (`nixosconfig/modules/nixos/services/soularr.nix`)

Add `--redis-host` argument to the soularr-web service command:
```nix
ExecStart = "${wrappedScript} ... --redis-host 192.168.100.XX";
```

### Python environment

Add `redis` (redis-py) package to the Nix Python environment that wraps `web/server.py`.

### Soularr main loop cache invalidation

After soularr completes an import cycle, POST to the web UI to bust stale cache:
```bash
curl -s -X POST http://localhost:8085/api/cache/invalidate \
  -H 'Content-Type: application/json' \
  -d '{"groups": ["pipeline", "library"]}'
```

Options:
1. Add as a `ExecStopPost` or post-run hook in the soularr systemd service
2. Add to soularr.py `main()` after the download/import loop completes
3. Add as a separate systemd service triggered after soularr completes

Option 2 is simplest — add a single HTTP POST at the end of `main()` in soularr.py.

## Deployment steps

1. Create Redis nspawn container in nixosconfig (on doc1)
2. Add `redis` to Python deps in soularr.nix
3. Add `--redis-host` to soularr-web ExecStart
4. Push nixosconfig, rebuild doc2
5. Verify: `ssh doc2 'curl -s http://localhost:8085/api/search?q=beatles'` — first call slow, second call instant
