
--I didn't run create extension, just add to shared_libraries following instructions:
--http://www.postgresql.org/docs/9.2/static/pgstatstatements.html

select pg_reload_conf()
SELECT * FROM pg_available_extensions WHERE name = 'pg_stat_statements';
CREATE EXTENSION pg_stat_statements;

--select pg_stat_statements_reset()

select * from pg_stat_statements;

select * from tbl_boolean tb 

--https://blog.crunchydata.com/blog/tentative-smarter-query-optimization-in-postgres-starts-with-pg_stat_statements

SELECT 
  (total_exec_time / 1000 / 60) as total_min, 
  mean_exec_time as avg_ms,
  calls, 
  query 
FROM pg_stat_statements 
ORDER BY 1 DESC 
LIMIT 500;


select * from general order by time desc limit 10



