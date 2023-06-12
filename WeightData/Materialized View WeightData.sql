-- helper function : 
/*
CREATE OR REPLACE FUNCTION public.pattyn_getbatchstart(varrecipebatch character varying)
 RETURNS timestamp
LANGUAGE plpgsql
AS $$
declare 
 batchstart timestamp;
BEGIN
--select first(time, time) into batchstart from weightdata_v1 where recipebatch =  varrecipebatch;
-- or get the batch start directly from the materialized view !!
select first(startstamp, startstamp ) into batchstart from weightdata_summary where recipebatch =  varrecipebatch;
return batchstart;
end;
$$
;
*/

--select * from pattyn_getbatchstart('106') as batchstart
--select * from testplctimescale.weightdata_v1 where recipebatch = '106'
--select * from testplctimescale.weightdata_summary where recipebatch = '106'


--2021-03-23 11:52:24

-- create a materialized view whithout time buckets for weight data : make sure the correct schema is selected !!
-- https://docs.timescale.com/latest/using-timescaledb/continuous-aggregates#real-time-aggregates
-- refreshing should be done over a specific time bucket
-- the materialized view 


----------------------------------------------------------------------------------------------------
--drop materialized view weightdata_hourly
--drop materialized view weightdata_summary


CREATE MATERIALIZED VIEW testplctimescale.weightdata_daily -- materialized view is schema specific, use the right connection or mention the schema! 
WITH (timescaledb.continuous) AS 
SELECT 
machine 
, machineserial
, recipe
, recipebatch
, loggeractivationuid
, recipientweight_sunitid
, recipientweight_eweightresult
, time_bucket(INTERVAL '1 day', time) AS bucket  
-- these are all the needed seperate fields. Now let's aggregate...
,count(recipientweight_eweightresult) AS nrofweightresult
,max(recipientweight_fsetpoint) AS setpoint
,max(recipientweight_fmaxoverweight) as maxoverweight
,max(recipientweight_fmaxunderweight) as maxunderweight
,count(recipientweight_fnetweight) as nrofboxes
,sum(recipientweight_fnetweight) as totalweight 
,min(recipientweight_fnetweight) as minboxweight
,max(recipientweight_fnetweight) as maxboxweight
,avg(recipientweight_fnetweight) as avgboxweight
,stddev(recipientweight_fnetweight) as stdevboxweight --not correct on time buckets ?
,first(time, recipientweight_fnetweight) as fillingfirsttime
,first(time, recipientweight_eweightresult) as fillingfirsttime1
,last(time, recipientweight_fnetweight) as fillinglasttime
, first(time,time )as bucketfirsttime
, last(time,time )as bucketlasttime
,avg(recipientweight_ftareweight) as avgtareweight
--,avg(recipientweight_fgrossweight) as avggrossweight
FROM testplctimescale.weightdata_v1
where recipientweight_fnetweight is not null
GROUP BY machine ,machineserial , recipe, recipebatch, loggeractivationuid, recipientweight_sunitid, recipientweight_eweightresult, bucket
with no data -- to prevent aggregating the entire materialized view on creation...


-- nieuwe database !! 15/04/2021
CREATE MATERIALIZED VIEW ppltesttslite2.weightdata_v2_hourly -- materialized view is schema specific, use the right connection or mention the schema!
WITH (timescaledb.continuous) AS
SELECT
machine
, machineserial
, recipe
, recipebatch
, loggeractivationuid
, recipientweight_imoduleIndex
, recipientweight_eweightresult
, time_bucket(INTERVAL '1 hour', time) AS bucket
-- these are all the needed seperate fields. Now let's aggregate...
,count(recipientweight_eweightresult) AS nrofweightresult
,max(recipientweight_fsetpoint) AS setpoint
,max(recipientweight_fmaxoverweight) as maxoverweight
,max(recipientweight_fmaxunderweight) as maxunderweight
,count(recipientweight_fnetweight) as nrofboxes
,sum(recipientweight_fnetweight) as totalweight
,min(recipientweight_fnetweight) as minboxweight
,max(recipientweight_fnetweight) as maxboxweight
,avg(recipientweight_fnetweight) as avgboxweight
,stddev(recipientweight_fnetweight) as stdevboxweight --not correct on time buckets ?
,first(time, recipientweight_fnetweight) as fillingfirsttime
,first(time, recipientweight_eweightresult) as fillingfirsttime1
,last(time, recipientweight_fnetweight) as fillinglasttime
, first(time,time )as bucketfirsttime
, last(time,time )as bucketlasttime
,avg(recipientweight_ftareweight) as avgtareweight
,last(time,recipientweight_smodulekey) as skey
--,avg(recipientweight_fgrossweight) as avggrossweight
FROM ppltesttslite2.weightdata_v2
where recipientweight_fnetweight is not null
GROUP BY machine ,machineserial ,loggeractivationuid, recipe, recipebatch, recipientweight_imoduleIndex, recipientweight_eweightresult, bucket
with no data -- to prevent aggregating the entire materialized view on creation...


select * from ppltesttslite.weightdata_v2_hourly



---- View on ppltesttslite4 
drop materialized view ppltesttslite4.weightdata_hourly

-- MV VOOR TEST4 weightdata 
CREATE MATERIALIZED VIEW ppltesttslite4.weightdata_hourly -- materialized view is schema specific, use the right connection or mention the schema!
WITH (timescaledb.continuous) AS
 SELECT weightdata.loggeractivationuid,
    weightdata.machine,
    weightdata.machineserial,
    weightdata.recipe,
    weightdata.recipeloadcounter,
    weightdata.recipientweight_imoduleindex,
    weightdata.recipientweight_eweightresult,
    (weightdata.recipientweight_istructversion)::integer AS recipientweight_istructversion,
    time_bucket('01:00:00'::interval, weightdata."time") AS bucket,
    count(weightdata.recipientweight_eweightresult) AS nrofweightresult,
    max(weightdata.recipientweight_fsetpoint) AS setpoint,
    max(weightdata.recipientweight_fmaxoverweight) AS maxoverweight,
    max(weightdata.recipientweight_fmaxunderweight) AS maxunderweight,
    count(weightdata.recipientweight_fnetweight) AS nrofboxes,
    sum(weightdata.recipientweight_fnetweight) AS totalweight,
    min(weightdata.recipientweight_fnetweight) AS minboxweight,
    max(weightdata.recipientweight_fnetweight) AS maxboxweight,
    avg(weightdata.recipientweight_fnetweight) AS avgboxweight,
    stddev(weightdata.recipientweight_fnetweight) AS stdevboxweight,
    first(weightdata."time", weightdata.recipientweight_fnetweight) AS fillingfirsttime,
    last(weightdata."time", weightdata.recipientweight_fnetweight) AS fillinglasttime,
    first(weightdata."time", weightdata."time") AS bucketfirsttime,
    last(weightdata."time", weightdata."time") AS bucketlasttime,
    avg(weightdata.recipientweight_ftareweight) AS avgtareweight,
    max((weightdata.recipientweight_smodulekey)::text) AS skey
   FROM ppltesttslite4.weightdata
  WHERE (weightdata.recipientweight_fnetweight IS NOT NULL)
  GROUP BY weightdata.recipientweight_istructversion, weightdata.machine, weightdata.machineserial, weightdata.loggeractivationuid, weightdata.recipe, weightdata.recipeloadcounter, weightdata.recipientweight_imoduleindex, weightdata.recipientweight_eweightresult, (time_bucket('01:00:00'::interval, weightdata."time"))
with no data

 
SELECT add_continuous_aggregate_policy('weightdata_hourly',
    start_offset => INTERVAL '10 day', --setting this to null will also remove the deleted chunk data !!
    end_offset => INTERVAL '24 h',
    schedule_interval => INTERVAL '1 h');
   
-- change owner !!   
select * from timescaledb_information.continuous_aggregates
ALTER MATERIALIZED VIEW ppltesttslite4.weightdata_hourly owner to testcompany


--,( select * from pattyn_getbatchstart('106') as batchstart ) as batchstart ==> this is not working, so change the function
----------------------------------------------------------------------------------------------------
-- function get the batchstart from the materialized view...2021-03-23 11:52:24
select * from pattyn_getbatchstart('106') as batchstart
select * from pattyn_getbatchstart() as batchstart

select distinct recipe from testplctimescale.weightdata_v1 where recipe is not null
select distinct recipe from testplctimescale.weightdata_hourly where recipe is not null
select recipe from testplctimescale.weightdata_v1 where recipe is not null group by recipe
select recipe from testplctimescale.weightdata_hourly where recipe is not null group by recipe

select count(recipe), recipe from testplctimescale.weightdata_v1 where time >( now() - interval '10d') group by recipe
select count(recipe), recipe from testplctimescale.weightdata_hourly where bucket >( now() - interval '10d') group by recipe


-- dit was het origineel 
select time_from from public.pattyn_getrecipeperiods('$Machine', (now()- interval '1 day')::timestamp, now()::timestamp)
		order by time_from desc limit 1

-- dit gaat stukken sneller ? ==> maar selecteert de verkeerde recipebatch (volgens bucket !!)

select first(fillingfirsttime, fillingfirsttime ) from weightdata_hourly where recipebatch::int in 
( select max(recipebatch::int) FROM testplctimescale.weightdata_v1 where time >( now() - interval '1d')  limit 1);

select * from testplctimescale.weightdata_v1 order by time desc limit 1000

select recipebatch::int FROM testplctimescale.weightdata_v1 order by 1 desc

select recipebatch::int FROM testplctimescale.weightdata_hourly order by 1 desc

select count(recipebatch::int) FROM testplctimescale.weightdata_v1

with recipebatch as (
select recipebatch::int FROM testplctimescale.weightdata_hourly order by bucket desc limit 1) as a

select * from pattyn_getbatchstart(a.recipebatch) as batchstart


select count(*) from weightdata_v1; --2070, 2079,
select * from weightdata_v1 order by time asc limit 20

select count(*) from weightdata_hourly; --2070, 2079, 
select * from weightdata_hourly order by bucket desc limit 10

select bucket,
bucketfirsttime as time, recipe AS metric,
recipientweight_sunitid, nrofboxes AS nrofboxes,recipebatch
FROM weightdata_hourly
WHERE
recipientweight_eweightresult = 10
GROUP BY 1,2,3,4, 6,5
ORDER BY 1 desc ,2,3 limit 20


select recipe, recipebatch, recipientweight_sunitid, recipientweight_eweightresult , bucket,nrofweightresult,setpoint,
maxoverweight,nrofboxes,totalweight
from weightdata_hourly order by bucket desc limit 10

explain (analyze, costs off)
select * from weightdata_hourly where --recipebatch = '106' order by startstamp desc  --2021-03-23 13:41:51
--recipe = 'J01' and 
recipientweight_eweightresult = 10 and
recipientweight_sunitid = 'W1' order by bucket desc, recipebatch

----------------------------------------------------------------------------------------------------
-- add a policy to update the MV every xx hours
SELECT add_continuous_aggregate_policy('weightdata_hourly',
    start_offset => INTERVAL '10 day', --setting this to null will also remove the deleted chunk data !!
    end_offset => INTERVAL '1 h',
    schedule_interval => INTERVAL '1 h');
   
 SELECT add_continuous_aggregate_policy('testplctimescale.weightdata_daily',
    start_offset => INTERVAL '10 day', --setting this to null will also remove the deleted chunk data !!
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 h');

   SELECT add_continuous_aggregate_policy('ppltesttslite2.weightdata_v2_hourly',
    start_offset => INTERVAL '5 day', --setting this to null will also remove the deleted chunk data !!
    end_offset => INTERVAL '1 day',
    schedule_interval => INTERVAL '1 h');
  
   -- can I put retention on a materialized view ?
   SELECT remove_retention_policy('ppltesttslite2.weightdata_v2_hourly');
SELECT add_retention_policy('ppltesttslite2.weightdata_v2_hourly', INTERVAL '51 days'); --120
SELECT add_retention_policy('testplctimescale.weightdata_hourly', INTERVAL '151 days'); --120

 -- perform a manual update with the refresh_continuous_aggregate
-- call refresh_continuous_aggregate('weightdata_summary', '2021-03-01', '2021-03-20');
-- call refresh_continuous_aggregate('weightdata_summary', NULL, NULL); -- REFRESH EVERYTHING e.g. at startup ???
 
select * from timescaledb_information.continuous_aggregates


select * from timescaledb_information.jobs
select * from timescaledb_information.job_stats -- show jobs/policies


--SELECT * FROM timescaledb_information.jobs where application_name like 'User-Define%';

SELECT  js.*, cagg.* FROM
  timescaledb_information.job_stats js, timescaledb_information.continuous_aggregates cagg
  WHERE 
  cagg.view_schema = 'pattyntestplcts'
  cagg.view_name = 'weightdata_hourly' 
  and cagg.materialization_hypertable_name = js.hypertable_name;


select * from timescaledb_information.chunks where hypertable_name like '%weight%'

select * from timescaledb_information.compression_settings cs 

select * from 
SELECT alter_job(<job_id>, scheduled => false);



-- hypertable size ??
select * from timescaledb_information.chunks where hypertable_name like '%weight%'
select hypertable_schema, hypertable_name, chunk_name, range_start, range_end
from timescaledb_information.chunks where hypertable_name ='weightdata_v1'



select hypertable_size('pattyntestplcts.weightdata_hourly_uuid')
SELECT * FROM hypertable_detailed_size('testplctimescale.weightdata_v1') ORDER BY node_name;
SELECT * FROM chunks_detailed_size('testplctimescale.weightdata_v1') ORDER BY chunk_name, node_name;


select hypertable_size('testplctimescale.weightdata_v1')/(8*1024*1024) as MB
select hypertable_size('testplctimescale.argocheck_datas_pbd')/(8*1024*1024) as MB
select hypertable_size('testplctimescale.checkweigher_datas_pbd')/(8*1024*1024) as MB

-- 
--select hypertable_index_size('testplctimescale.weightdata_v1')
SELECT * FROM show_tablespaces('testplctimescale.weightdata_v1');

--- Real Time Aggregation
-- https://blog.timescale.com/blog/achieving-the-best-of-both-worlds-ensuring-up-to-date-results-with-real-time-aggregation/
    


-- change the owner of the aggretate view ? 
select * from timescaledb_information.continuous_aggregates
ALTER MATERIALIZED VIEW testplctimescale.weightdata_hourly owner to testcompany

--

--explain --analyse
select 
recipe, 
recipebatch,
min(bucketfirsttime) as batchstart,
max(bucketlasttime) as batchfinish,
max(bucketlasttime)-min(bucketfirsttime) as recipeduration,
(extract(epoch from max(bucketlasttime)-min(bucketfirsttime))/3600) as hours,
sum(nrofweightresult) as boxes,
round((sum(totalweight)/1000)::numeric,3) as filledTon,
round(((sum(totalweight)/1000) / (extract(epoch from max(bucketlasttime)-min(bucketfirsttime))/3600))::numeric, 2) as tonperhour,
round(((sum(nrofweightresult)) / (extract(epoch from max(bucketlasttime)-min(bucketfirsttime))/3600))::numeric, 0) as boxesperhour,
round(((sum(nrofweightresult)) / (extract(epoch from max(bucketlasttime)-min(bucketfirsttime))/3600*60))::numeric, 0) as boxesperminute
from testplctimescale.weightdata_hourly group by recipe, recipebatch
order by recipebatch::numeric desc

select * from  weightdata_v1 order by time desc limit 100

select * from  weightdata_v1  where time > '2021-04-04 00:00:00' and time < '2021-04-05 00:00:00' limit 100
order by time  

select count(*) from  weightdata_v1  where time > '2021-04-04 01:59:59' and time < '2021-04-04 02:00:00'
  








-- add a policy
SELECT add_drop_chunks_policy('objects', INTERVAL '48 hours');
-- quickly remove some stuff
SELECT drop_chunks('objects', INTERVAL '24 hours');


SELECT remove_retention_policy('objects');
SELECT add_retention_policy('objects', INTERVAL '48 hours');

SELECT remove_retention_policy('alarms');
SELECT add_retention_policy('alarms', INTERVAL '48 hours');

SELECT remove_retention_policy('general');
SELECT add_retention_policy('general', INTERVAL '48 hours');

SELECT remove_retention_policy('pplsoftwarets.monitoring');
SELECT add_retention_policy('pplsoftwarets.monitoring', INTERVAL '48 hours');

SELECT remove_retention_policy('parameters');
SELECT add_retention_policy('parameters', INTERVAL '48 hours');

SELECT remove_retention_policy('objects');
SELECT add_retention_policy('objects', INTERVAL '48 hours');

SELECT remove_retention_policy('testplctimescale.weightdata_v1');
SELECT add_retention_policy('testplctimescale.weightdata_v1', INTERVAL '48 hours');

SELECT remove_retention_policy('testplctimescale.argocheck_datas_pbd');
SELECT add_retention_policy('testplctimescale.argocheck_datas_pbd', INTERVAL '4 hours');

SELECT remove_retention_policy('testplctimescale.checkweigher_datas_pbd');
SELECT add_retention_policy('testplctimescale.checkweigher_datas_pbd', INTERVAL '4 hours');

SELECT remove_retention_policy('testplctimescale.objects');
SELECT add_retention_policy('testplctimescale.objects', INTERVAL '4 hours');

SELECT remove_retention_policy('ppltesttslite2.objects');
SELECT add_retention_policy('ppltesttslite2.objects', INTERVAL '1 hours');
SELECT add_retention_policy('ppltesttslite2.objects', INTERVAL '30 days');

select * from public.mytable m  where relname = 'objects' order by time desc
select public.pattyn_dba.tablesizes()




