#!/bin/bash
source .env 2>/dev/null || true
psql "$DATABASE_URL" -c "SELECT COUNT(*) as i2p_pages FROM page WHERE network = 'i2p';" 2>/dev/null
psql "$DATABASE_URL" -c "SELECT COUNT(*) as i2p_queue FROM work_unit WHERE url LIKE '%.i2p%';" 2>/dev/null
