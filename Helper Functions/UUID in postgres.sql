
--https://dba.stackexchange.com/questions/122623/default-value-for-uuid-column-in-postgres

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


CREATE EXTENSION if not exists "pgcrypto";

select gen_random_uuid()

-- default gen_random_uuid()

ALTER TABLE pattyntestplcts.weightdata ADD uuid uuid NOT NULL DEFAULT gen_random_uuid();

alter table public.tbl_stdtest add primary key (uuid);

pattyntestplcts.weightdata

CREATE TABLE public.tbl_stdtest1 (
"uuid" uuid not null default gen_random_uuid(),
	ncount int4 NULL,
	stddeviation float8 NULL,
	batch varchar NULL,
	mean float8 null,
	CONSTRAINT tbl_stdtest1_pkey PRIMARY KEY (uuid)
);



CREATE TABLE public.tbl_stdtest (
	uid int4 NOT NULL,
	ncount int4 NULL,
	stddeviation float8 NULL,
	batch varchar NULL,
	mean float8 NULL,
	uuid uuid NOT NULL DEFAULT gen_random_uuid(),
	CONSTRAINT tbl_stdtest_pkey PRIMARY KEY (uuid)
);


-- Permissions

ALTER TABLE public.tbl_stdtest OWNER TO "PattynAdmin";
GRANT ALL ON TABLE public.tbl_stdtest TO PattynAdmin;


select * from public.tbl_stdtest


INSERT INTO public.tbl_stdtest (uid, ncount, mean, stddeviation, batch) values (1,50,25.4564206184922,0.255170987233015,'ALL');


select uuid, * from pattyntestplcts.weightdata 
order by time desc
limit 10

select count(time) from pattyntestplcts.weightdata

-- can a guid be added to a MV ?

 -- get MV definition
select view_definition 
--, * 
from timescaledb_information.continuous_aggregates MV  where view_name  like '%weight%'

CREATE MATERIALIZED VIEW pattyntestplcts.weightdata_hourly_uuid 
WITH (timescaledb.continuous) AS      
    SELECT 
    first(weightdata.uuid, weightdata."time") AS uuid,
    --gen_random_uuid() as uuid,
    weightdata.loggeractivationuid,
    weightdata.machine,
    weightdata.machineserial,
    weightdata.recipe,
    weightdata.recipeloadcounter,
    weightdata.recipientweight_imoduleindex,
    weightdata.recipientweight_eweightresult,
    weightdata.recipientweight_scustomfield1,
    weightdata.recipientweight_scustomfield2,
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
   FROM weightdata
  WHERE (weightdata.recipientweight_fnetweight IS NOT NULL)
    GROUP BY weightdata.recipientweight_istructversion, 
 weightdata.recipientweight_scustomfield1, 
weightdata.recipientweight_scustomfield2, 
weightdata.machine, 
weightdata.machineserial, 
weightdata.loggeractivationuid, 
weightdata.recipe, 
weightdata.recipeloadcounter, 
weightdata.recipientweight_imoduleindex, 
weightdata.recipientweight_eweightresult, 
(time_bucket('01:00:00'::interval, weightdata."time")); 

; 




drop materialized view pattyntestplcts.weightdata_hourly_uuid

select * from weightdata
order by time
limit 10 -- 2021-05-18 07:44:58

select * from weightdata_hourly
order by bucket
limit 10 -- 2021-05-18 07:00:00

SELECT add_retention_policy('pattyntestplcts.weightdata', INTERVAL '90 days'); --2118

select * from weightdata
order by time
limit 10 -- 2021-06-15 02:00:00

select bucket, * from weightdata_hourly
order by bucket
limit 15 -- 2021-05-18 07:00:00

select bucket, * from weightdata_hourly_uuid
order by bucket
limit 15 -- 2021-05-18 07:00:00






select uuid, time, count(*)
from weightdata
group by uuid, time
HAVING count(*) > 1


select gen_random_uuid() as uuid,
--SQL Error [0A000]: ERROR: only immutable functions supported in continuous aggregate view