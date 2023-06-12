
-- RETENTION POLICIES : SHOW THE SCHEDULED JOBS FOR THE ACTIVE CONNECTION
--https://docs.timescale.com/latest/using-timescaledb/data-retention
--https://docs.timescale.com/latest/api#automation-policies

-- show job schedules
SELECT * FROM timescaledb_information.job_stats;

-- show the jobs
SELECT * FROM timescaledb_information.jobs;

--create extension timescaledb

-- Get list of chunks associated with a hypertable.
select show_chunks ('"Monitoring"');






