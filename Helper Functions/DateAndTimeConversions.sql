-- function UTC2EPOCH
-- function EPOCH2UTC

CREATE OR REPLACE FUNCTION public.pattyn_utc2epoch (UTC_time timestamp without time zone DEFAULT CURRENT_TIMESTAMP)
RETURNS FLOAT
 LANGUAGE plpgsql
AS $function$

/*
 * Created 	 : 20210129
 * CreatedBy : SSAV
 * ----------------------------------------------------------------------------------
 * Purpose : input UTC timestamp and convert to epoch time
 * 
 * ----------------------------------------------------------------------------------
 *
 * Inputs  : 
 * - UTC timestamp, or none for Current Time (default)
 * 
 * Output  : 
 * - epoch time (float)
 * 
 * Used By : 
 * - General Helper function 
 *   
 * ----------------------------------------------------------------------------------
 * Changes:
 * 20210129 SSAV - added comments for function
 * 
 * ---------------------------------------------------------------------------------- 
 * How to Use : 
 * 	select * from pattyn_utc2epoch() -- 1611906149810.539
 * 	select * from pattyn_utc2epoch('2021-01-29T03:45:34.303Z') -- 1611906072.000
 * ----------------------------------------------------------------------------------
 */ 

BEGIN
		return date_part('epoch', UTC_time) *1000 AS epoch_time;  
end;
$function$;




select current_timestamp as UTCnow, now() as timeNow, date_part('epoch', current_timestamp)*1000 as EpochNow -- 2021-01-29 07:41:12

select time zone

-- TIMESTAMPS ARE INTERNALLY STORED AS UTC TIMES
-- SET THE TIME ZONE FOR THIS SESSION
set time zone UTC
-- READ DATE WITHOUT TIMEZONE, READ DATE WITH SERVER SYSTEM TIMEZONE
select now()::timestamp, now()::timestampTZ;


select * from pattyn_utc2epoch() -- 1611906149810.539
select * from pattyn_utc2epoch('2021-01-29T03:45:34.303Z') -- 1611906072.000

select where  "time" BETWEEN '2021-01-29T04:29:00.786Z' AND '2021-01-29T07:29:00.786Z' and alarm_backnowledged=1
-- from:"1611894540786" to:"1611905340786"

select * from pattyn_utc2epoch('2021-01-29T04:29:00.786Z') --1611894540786
select * from pattyn_utc2epoch('2021-01-29T04:29:00.786Z') --1611894540786

select * from pattyn_utc2epoch(now()::timestamp) --1611894540786

select * from  pattyn_getrecipeperiods_epoch ('1002', (now()- interval '1 day')::timestamp, now()::timestamp) 
order by recipe_epoch_time_from desc limit 1

select stmachine_emachinepowerstate ,* from general  where time >  to_timestamp(1615450278590/1000) limit 100


/*
SELECT  * from pattyn_getrecipeperiods_epoch ('1002', '2021-01-29T03:45:34.303Z', '2021-01-29T06:45:34.303Z')

J01	1611899123645	1611899131985
J10	1611892427209	1611899091745
J01	1611888346521	1611892427049

*/
