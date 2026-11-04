-- Run in an explicitly selected sandbox catalog/schema with CREATE TABLE privilege.
-- Use these fully qualified names in serverless-smoke.yaml before running the Job.
CREATE OR REPLACE TABLE pairity_equal_baseline USING DELTA AS
SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS t(id, value);
CREATE OR REPLACE TABLE pairity_equal_candidate USING DELTA AS
SELECT * FROM VALUES (3, 'c'), (1, 'a'), (2, 'b') AS t(id, value);
CREATE OR REPLACE TABLE pairity_changed_baseline USING DELTA AS
SELECT * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS t(id, value);
CREATE OR REPLACE TABLE pairity_changed_candidate USING DELTA AS
SELECT * FROM VALUES (1, 'a'), (2, 'CHANGED'), (4, 'd') AS t(id, value);
-- Expected: equal pair passes; changed pair matched=2, changed=1, missing=1, extra=1.
-- The overall Job must fail after persisting both pair reports and the suite report.
