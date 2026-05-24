CREATE INDEX IF NOT EXISTS idx_page_link_source_page_id
  ON page_link(source_page_id);

CREATE INDEX IF NOT EXISTS idx_page_email_page_id
  ON page_email(page_id);

CREATE INDEX IF NOT EXISTS idx_page_crypto_page_id
  ON page_crypto(page_id);

CREATE INDEX IF NOT EXISTS idx_site_profile_category_created_host
  ON site_profile(category, created_at, host)
  WHERE category <> '';

CREATE INDEX IF NOT EXISTS idx_site_profile_forum_host
  ON site_profile(host)
  WHERE category = 'forum';

CREATE INDEX IF NOT EXISTS idx_page_keyword_tag_keyword_page_created
  ON page_keyword_tag(tag, page_id, created_at)
  WHERE tag LIKE 'keyword:%';

CREATE INDEX IF NOT EXISTS idx_page_topic_tag_created_topic_page
  ON page_topic_tag(created_at, topic, page_id)
  WHERE topic <> '';

CREATE INDEX IF NOT EXISTS idx_page_url_host_without_port
  ON page(
    lower(split_part(
      CASE
        WHEN position('://' IN url) > 0 THEN split_part(split_part(url, '://', 2), '/', 1)
        ELSE ''
      END,
      ':',
      1
    ))
  );
