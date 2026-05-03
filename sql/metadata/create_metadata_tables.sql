-- =============================================================
-- Operational Metadata DDL
-- File: sql/metadata/create_metadata_tables.sql
-- Purpose: Track the execution, row counts, and health of 
--          the ETL pipeline. This is 'Operational Metadata'.
-- =============================================================

CREATE TABLE IF NOT EXISTS etl_job_execution (
    job_id          SERIAL PRIMARY KEY,
    job_name        VARCHAR(100) NOT NULL,
    start_time      TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    end_time        TIMESTAMP,
    status          VARCHAR(20)  NOT NULL, -- RUNNING, SUCCESS, FAILED
    records_read    INTEGER,
    records_written INTEGER,
    error_message   TEXT
);

CREATE TABLE IF NOT EXISTS data_quality_log (
    log_id          SERIAL PRIMARY KEY,
    job_id          INTEGER      NOT NULL REFERENCES etl_job_execution(job_id),
    table_name      VARCHAR(100) NOT NULL,
    check_name      VARCHAR(255) NOT NULL,
    failed_rows     INTEGER      NOT NULL,
    logged_at       TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ── Analytical Views for Metadata ────────────────────────────

-- Track pipeline health over time
CREATE OR REPLACE VIEW v_pipeline_health AS
SELECT 
    job_name,
    COUNT(*)               AS run_count,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS success_count,
    SUM(CASE WHEN status = 'FAILED'  THEN 1 ELSE 0 END) AS fail_count,
    AVG(EXTRACT(EPOCH FROM (end_time - start_time)))    AS avg_duration_seconds,
    SUM(records_written)   AS total_rows_processed
FROM etl_job_execution
GROUP BY job_name
ORDER BY run_count DESC;
