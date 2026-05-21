DROP INDEX IF EXISTS idx_auto_blacklist_event_created_at;
DROP INDEX IF EXISTS idx_auto_blacklist_event_domain;
DROP INDEX IF EXISTS idx_auto_blacklist_event_unique_page;
DROP TABLE IF EXISTS auto_blacklist_event;

DROP INDEX IF EXISTS idx_auto_blacklist_rule_enabled_type;
DROP INDEX IF EXISTS idx_auto_blacklist_rule_type_value;
DROP TABLE IF EXISTS auto_blacklist_rule;
