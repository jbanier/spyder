CREATE TABLE host_ssh_observation(
  id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
  host VARCHAR NOT NULL,
  port INTEGER NOT NULL,
  status VARCHAR NOT NULL,
  host_key_algorithm VARCHAR,
  host_key VARCHAR,
  host_key_fingerprint VARCHAR,
  server_banner VARCHAR,
  last_error VARCHAR,
  last_attempt_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_success_at VARCHAR,
  created_at VARCHAR NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(host, port)
);

CREATE INDEX idx_host_ssh_observation_host ON host_ssh_observation(host);
CREATE INDEX idx_host_ssh_observation_status ON host_ssh_observation(status);
CREATE INDEX idx_host_ssh_observation_fingerprint ON host_ssh_observation(host_key_fingerprint);
CREATE INDEX idx_host_ssh_observation_last_success_at ON host_ssh_observation(last_success_at);
