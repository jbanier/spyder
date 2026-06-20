ALTER TABLE work_unit
  ADD COLUMN url_discovery_id INTEGER REFERENCES url_discovery(id),
  ADD COLUMN failure_category TEXT;

CREATE INDEX idx_work_unit_discovery ON work_unit(url_discovery_id);
CREATE INDEX idx_work_unit_failure_category ON work_unit(failure_category);
