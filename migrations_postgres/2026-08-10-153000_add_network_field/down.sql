-- Remove network field and associated objects
ALTER TABLE page DROP CONSTRAINT page_network_check;
DROP INDEX idx_page_network_url;
DROP INDEX idx_page_network;
ALTER TABLE page DROP COLUMN network;
