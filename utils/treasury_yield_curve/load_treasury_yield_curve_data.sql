CREATE TEMP TABLE temp (
    like visuals.treasury_yield_curve
    including defaults
    including indexes
);

COPY temp (date, instrument, yield)
FROM '/home/brendan/Github/python-airflow-personal-blog-data-pipeline/{}'
DELIMITER ',' CSV HEADER;

INSERT INTO visuals.treasury_yield_curve
SELECT * FROM temp
ON CONFLICT (date, instrument)
DO
    UPDATE SET yield = EXCLUDED.yield, created_at = EXCLUDED.created_at;
