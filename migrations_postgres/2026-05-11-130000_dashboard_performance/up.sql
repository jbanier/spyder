CREATE INDEX IF NOT EXISTS idx_page_last_scanned_id ON page(last_scanned_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_host_http_observation_success_recency
  ON host_http_observation(last_success_at DESC, last_attempt_at DESC)
  WHERE last_success_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_host_service_observation_success_recency
  ON host_service_observation(last_success_at DESC, last_attempt_at DESC)
  WHERE last_success_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_host_ssh_observation_success_key
  ON host_ssh_observation(host_key_algorithm, host_key_fingerprint, last_success_at DESC)
  WHERE host_key_algorithm IS NOT NULL
    AND host_key_algorithm != ''
    AND host_key_fingerprint IS NOT NULL
    AND host_key_fingerprint != ''
    AND last_success_at IS NOT NULL;
