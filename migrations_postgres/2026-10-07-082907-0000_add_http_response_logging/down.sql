-- Remove HTTP response logging fields
DROP INDEX IF EXISTS idx_page_detected_technologies;
DROP INDEX IF EXISTS idx_page_response_headers;
DROP INDEX IF EXISTS idx_page_server_software;
DROP INDEX IF EXISTS idx_page_status_code;

ALTER TABLE page DROP COLUMN IF EXISTS detected_technologies;
ALTER TABLE page DROP COLUMN IF EXISTS server_software;
ALTER TABLE page DROP COLUMN IF EXISTS redirect_chain;
ALTER TABLE page DROP COLUMN IF EXISTS cookies;
ALTER TABLE page DROP COLUMN IF EXISTS request_headers;
ALTER TABLE page DROP COLUMN IF EXISTS response_headers;
ALTER TABLE page DROP COLUMN IF EXISTS response_time_ms;
ALTER TABLE page DROP COLUMN IF EXISTS status_code;
