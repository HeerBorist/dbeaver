
--DROP FUNCTION pattyn_weight_details(INTERVAL) 
CREATE OR REPLACE FUNCTION public.pattyn_weight_details(v_interval interval DEFAULT '10 min'::INTERVAL)

RETURNS TABLE(
"time" timestamp with time zone,
machine character varying,
machineserial character varying,
	recipe character varying,
	id varchar(255),
	"Target_weight" float8, 
	"Netto_weight" float8, 
	"Filling head" float8, 
	recipientweight_icustomerfield float8, 
	recipientweight_scustomerfield varchar(255))
	
 LANGUAGE plpgsql

 AS $function$

	 
	
/* FUNCTION COMMENT : 
 * 
 * Created 	 : 20210723
 * CreatedBy : SSAV
 * 
 ----------------------------------------------------------------------------------
 * Change History : 
 *
 * 
 * 
 * To do : 
 * 
 * ----------------------------------------------------------------------------------
 *  *  
 * Purpose : return weightdata details per recipeloadcounter for the last $1 minutes
 * returns part of the raw data.
 * can be used to return data to customer ! Just don't know how to activate yet ?? How to keep track of what has been returned ? 
 * Includes a test to find out which function is being performed ! This can be usefull for logging user activity !!
 * 
 * Inputs  : 
 * - a time interval. Max 1 hour, or limit to 10000 records.
 * 
 *
 * Output  : 
 * - table  
 *

 * 
 *  
 * Used By : 
 * - testing the reports...  
 * - testing API data export
 *   
 * ----------------------------------------------------------------------------------
  
 * Example of how to Use the function : 
 *
 * select * from public.pattyn_weight_details() -- returns 10 min of data
 * select * from public.pattyn_weight_details('1 hour') 
 * select * from public.pattyn_weight_details('3 hour') ==> truncated to 1 hour

 * ----------------------------------------------------------------------------------
* END OF FUNCTION COMMENT
 */ 
		
-- The function : 	
	
	begin	
		
	
		-- try to write some logging information in the database (on error do nothing !)   

       perform pattyn_dba_functionlogger('function','pattyn_weight_details internal');
		
	
		if $1 > interval '1 hour' then 
				raise notice 'interval % is larger than 1 hour ! Will be reduced to 1 hour', $1;
--				raise notice 'current user is % and the session user is %',c_user,s_user; 
				$1 = interval '1 hour';
			end if;
		

		if $1 is not null then

		return query

		SELECT 
		wd."time", 
		--line,
		wd.machine, 
		wd.machineserial, 
		wd.recipe, 
		wd.recipeloadcounter as Id, 
		--loggeractivationuid, 
		--loggeridentifier, 
		--recipientweight_eunit, 
		--recipientweight_eweightresult, 
		--recipientweight_fgrossweight, 
		--recipientweight_fmaxoverweight, 
		--recipientweight_fmaxunderweight, 
		wd.recipientweight_fsetpoint as "Target weight",
		wd.recipientweight_fnetweight  as "Netto weight",
		--recipientweight_ftareweight, 
		--recipientweight_icountinactiverecipe, 
		wd.recipientweight_imoduleindex as "Filling head", 
		--recipientweight_istructversion, 
		--recipientweight_itimeofweightms, 
		--recipientweight_smodulekey, 
		--recipientweight_srecipientid, 
		--bnewrecipientweight, 
		--** future fields ! 
		wd.recipientweight_icustomerfield, 
		wd.recipientweight_scustomerfield 
		--recipientweight_scustomfield1, 
		--recipientweight_scustomfield2
		-- ** Loders Specific fields
		--loderscais as "Loders cais code",
		--lodersbatch as "Loders batch code"
		FROM weightdata wd
		--where recipeloadcounter = $1
		where wd.time > now() - $1
		--where wd.time > now() - interval '10 min'
		order by wd.time asc
		limit 10
		;
		raise notice 'Query will return now data for last %', $1;
	else
		raise notice 'empty input parameter detected :  %', $1;
	end if;

	end;
$function$
;



select * from pattyn_weight_details('3 hour')
select * from pattyn_weight_details('55 min')
select * from pattyn_weight_details()

select * from dba_log 

--select * from pg_stat_activity

select * from weightdata w where time > (now() - interval '10 min')
select now() - interval '10 min'