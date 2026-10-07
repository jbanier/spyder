-- Add HTTP response logging fields to page table
ALTER TABLE page ADD COLUMN status_code INTEGER;
ALTER TABLE page ADD COLUMN response_time_ms INTEGER;
ALTER TABLE page ADD COLUMN response_headers JSONB;
ALTER TABLE page ADD COLUMN request_headers JSONB;
ALTER TABLE page ADD COLUMN cookies JSONB;
ALTER TABLE page ADD COLUMN redirect_chain JSONB;
ALTER TABLE page ADD COLUMN server_software VARCHAR;
ALTER TABLE page ADD COLUMN detected_technologies JSONB;

-- Create indexes for common queries
CREATE INDEX idx_page_status_code ON page(status_code) WHERE status_code IS NOT NULL;
CREATE INDEX idx_page_server_software ON page(server_software) WHERE server_software IS NOT NULL;

-- Add GIN index for JSONB searches
CREATE INDEX idx_page_response_headers ON page USING GIN (response_headers) WHERE response_headers IS NOT NULL;
CREATE INDEX idx_page_detected_technologies ON page USING GIN (detected_technologies) WHERE detected_technologies IS NOT NULL;
