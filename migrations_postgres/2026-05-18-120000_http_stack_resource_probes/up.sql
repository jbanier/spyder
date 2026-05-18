ALTER TABLE host_http_observation
  ADD COLUMN stack_versions TEXT,
  ADD COLUMN exposed_resources TEXT;
