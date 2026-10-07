# I2P Network Bootstrap Guide

## Status

Attempted to seed initial I2P eepsites but encountered connectivity issues:
- `http://reg.i2p/` → 500 Internal Server Error
- `http://stats.i2p/` → 403 Forbidden

## Recommended I2P Seed Sites

Once i2pd is fully integrated into the network (10-15 minutes after startup):

```bash
# I2P Registry (eepsite directory)
I2P_PROXY=http://127.0.0.1:4444 cargo run --bin spyder -- add http://reg.i2p

# Search engine
I2P_PROXY=http://127.0.0.1:4444 cargo run --bin spyder -- add http://search.i2p

# Legwork (another directory)
I2P_PROXY=http://127.0.0.1:4444 cargo run --bin spyder -- add http://legwork.i2p

# Stats/monitoring
I2P_PROXY=http://127.0.0.1:4444 cargo run --bin spyder -- add http://stats.i2p

# notbob (popular directory)
I2P_PROXY=http://127.0.0.1:4444 cargo run --bin spyder -- add http://notbob.i2p

# inr (I2P Name Registry)
I2P_PROXY=http://127.0.0.1:4444 cargo run --bin spyder -- add http://inr.i2p
```

## Troubleshooting

### Check i2pd Status
```bash
# Web console should be accessible
curl http://127.0.0.1:7070/

# Check tunnel status
curl http://127.0.0.1:7070/?page=i2p_tunnels
```

### Verify HTTP Proxy
```bash
# Test proxy directly
curl -x http://127.0.0.1:4444 http://stats.i2p/ -m 60 -v
```

### Wait for Network Integration
I2P requires time to build tunnels and integrate into the network:
- **Initial**: 1-2 minutes for basic connectivity
- **Optimal**: 10-15 minutes for stable performance
- **Full integration**: 30+ minutes for all features

### Check Logs
```bash
# i2pd logs (Ubuntu/Debian)
journalctl -u i2pd -f

# Check for tunnel building messages
```

## Once Connected

After successfully seeding sites, process the queue:

```bash
# Process I2P work queue
I2P_PROXY=http://127.0.0.1:4444 cargo run --bin spyder -- work

# Or mixed network (Tor + I2P)
ALL_PROXY=socks5h://127.0.0.1:9050 I2P_PROXY=http://127.0.0.1:4444 \\
  cargo run --bin spyder -- work
```

## Expected Behavior

Successful seed:
```
INFO Fetching seed page http://reg.i2p/
INFO Enqueued seed and 15 discovered links
```

Network still integrating:
```
ERROR non-success status 500 for http://reg.i2p/
→ Wait 5-10 minutes and retry
```

Proxy not running:
```
ERROR error sending request
→ Start i2pd: sudo systemctl start i2pd
```
