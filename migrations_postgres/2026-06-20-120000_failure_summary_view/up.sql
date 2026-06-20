CREATE MATERIALIZED VIEW work_unit_failure_summary AS
SELECT
  failure_category,
  COUNT(*) as count,
  ARRAY(
    SELECT url
    FROM work_unit w2
    WHERE w2.failure_category = w1.failure_category
    ORDER BY w2.last_attempt_at DESC
    LIMIT 10
  ) as sample_urls,
  MAX(last_attempt_at) as last_failure_at
FROM work_unit w1
WHERE status IN ('failed', 'pending')
  AND failure_category IS NOT NULL
GROUP BY failure_category;

CREATE INDEX idx_work_failure_summary_category
  ON work_unit_failure_summary(failure_category);
