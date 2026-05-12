CREATE INDEX IF NOT EXISTS idx_host_service_observation_fingerprint_success
  ON host_service_observation(banner_fingerprint, last_success_at DESC, host)
  WHERE banner_fingerprint IS NOT NULL
    AND banner_fingerprint != ''
    AND last_success_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_host_http_observation_header_fingerprint_success
  ON host_http_observation(header_fingerprint, last_success_at DESC, host)
  WHERE header_fingerprint IS NOT NULL
    AND header_fingerprint != ''
    AND last_success_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_host_http_observation_favicon_hash_success
  ON host_http_observation(favicon_hash, last_success_at DESC, host)
  WHERE favicon_hash IS NOT NULL
    AND favicon_hash != ''
    AND last_success_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_page_scan_link_scan_id_desc
  ON page_scan_link(scan_id DESC, target_host);

CREATE INDEX IF NOT EXISTS idx_page_scan_email_scan_id_desc
  ON page_scan_email(scan_id DESC, email);

CREATE INDEX IF NOT EXISTS idx_page_scan_crypto_scan_id_desc
  ON page_scan_crypto(scan_id DESC, asset_type, reference);

CREATE INDEX IF NOT EXISTS idx_site_profile_source_page_id
  ON site_profile(source_page_id);
