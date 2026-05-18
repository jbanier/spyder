ALTER TABLE host_http_observation
  DROP COLUMN IF EXISTS exposed_resources,
  DROP COLUMN IF EXISTS stack_versions;
