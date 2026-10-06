-- Add network field to page table
ALTER TABLE page ADD COLUMN network VARCHAR(16) DEFAULT 'clearnet' NOT NULL;

-- Create indexes for network-based queries
CREATE INDEX idx_page_network ON page(network);
CREATE INDEX idx_page_network_url ON page(network, url);

-- Add check constraint to validate network values
ALTER TABLE page ADD CONSTRAINT page_network_check
  CHECK (network IN ('clearnet', 'tor', 'i2p'));
