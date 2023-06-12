CREATE OR REPLACE FUNCTION public.pattyn_getrecipeperiods(machine_name character varying, from_time timestamp without time zone, to_time timestamp without time zone DEFAULT CURRENT_TIMESTAMP, line character varying DEFAULT 1)
 RETURNS TABLE(recipe character varying, time_from timestamp with time zone, time_to timestamp with time zone)
 LANGUAGE plpgsql
AS $function$

--declare 
BEGIN

	return QUERY
--------------------------------------------------------------------------------
-- use grafana 'williamvenner-timepickerbuttons-panel ?
-- works on table general .
-- add parameter : machine, line !!
-- add parameter : start - finish !!


  WITH grouped_recipes AS (
  SELECT g."recipe",
         g."time",
         g."machine",
--         g."Line",
         (
           g."recipe",
           -- There is a subtraction below, don't be fooled by the formatting
--           DENSE_RANK() OVER (ORDER BY g."line", g."machine", date_part('epoch', g."time")) 
--         - DENSE_RANK() OVER (PARTITION BY g."Recipe" ORDER BY g."Line" , g."Machine" ,date_part('epoch', g."time"))
           DENSE_RANK() OVER (ORDER BY  g."machine", date_part('epoch', g."time")) 
         - DENSE_RANK() OVER (PARTITION BY g."recipe" ORDER BY g."machine" ,date_part('epoch', g."time"))
         ) AS recipe_group
  FROM public."general" g where g."time"> from_time and g."time" < to_time
  and g."machine"  = machine_name --'%AVL 24%'  -- machines, lijnen, etc...op voorhand uitsplitsen !!!
  -- and g.'line' = line
  )

 
		SELECT  --Min(gr."Line") Line,
				--MIN(gr."machine") Machine, 
				MIN(gr."recipe")::varchar as recipe, 
		--     COUNT(1) AS count, 
		       MIN(time) AS time_from,
		       MAX(time) AS time_to
		--       date_part('epoch',MIN(time)) AS time_from,
		--       date_part('epoch',MAX(time)) AS time_to
		FROM grouped_recipes gr
		GROUP BY gr."machine",  gr.recipe_group
		HAVING COUNT(1) > 1
		ORDER BY MIN(time) desc;



end;
$function$
;

CREATE OR REPLACE FUNCTION public.pattyn_getrecipeperiods_epoch(machine_name character varying, from_time timestamp without time zone, to_time timestamp without time zone DEFAULT CURRENT_TIMESTAMP, line character varying DEFAULT 1)
 RETURNS TABLE(recipe character varying, recipe_epoch_time_from double precision, recipe_epoch_time_to double precision)
 LANGUAGE plpgsql
AS $function$

--declare 
BEGIN

	return QUERY
--------------------------------------------------------------------------------
-- use grafana 'williamvenner-timepickerbuttons-panel ?
-- works on table general .
-- add parameter : machine, line !!
-- add parameter : start - finish !!


  WITH grouped_recipes AS (
  SELECT g."recipe",
         g."time",
         g."machine",
--         g."Line",
         (
           g."recipe",
           -- There is a subtraction below, don't be fooled by the formatting
--           DENSE_RANK() OVER (ORDER BY g."line", g."machine", date_part('epoch', g."time")) 
--         - DENSE_RANK() OVER (PARTITION BY g."Recipe" ORDER BY g."Line" , g."Machine" ,date_part('epoch', g."time"))
           DENSE_RANK() OVER (ORDER BY  g."machine", date_part('epoch', g."time")) 
         - DENSE_RANK() OVER (PARTITION BY g."recipe" ORDER BY g."machine" ,date_part('epoch', g."time"))
         ) AS recipe_group
  FROM public."general" g where g."time"> from_time and g."time" < to_time
  and g."machine"  = machine_name --'%AVL 24%'  -- machines, lijnen, etc...op voorhand uitsplitsen !!!
  -- and g.'line' = line
  )

 
		SELECT  --Min(gr."Line") Line,
				--MIN(gr."machine") Machine, 
				MIN(gr."recipe")::varchar as recipe, 
		--     COUNT(1) AS count, 
		--       MIN(time) AS time_from,
		--       MAX(time) AS time_to
		       date_part('epoch',MIN(time)) *1000 AS recipe_epoch_time_from,
		       date_part('epoch',MAX(time)) *1000 AS recipe_epoch_time_to
		FROM grouped_recipes gr
		GROUP BY gr."machine",  gr.recipe_group
		HAVING COUNT(1) > 1
		ORDER BY MIN(time) desc;

	-- use :
	--select * from  pattyn_getrecipeperiods_epoch ('1002', '2021-01-20T04:49:49.815Z', '2021-01-20T07:49:49.815Z');

end;
$function$
;

CREATE OR REPLACE FUNCTION public.pattyn_utc2epoch(utc_time timestamp without time zone DEFAULT CURRENT_TIMESTAMP)
 RETURNS double precision
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
$function$
;


CREATE OR REPLACE FUNCTION public.pattyn_transskey(skey character varying, typeid integer, lang character varying)
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$

-- SSAV 13/01/2020
-- first test on timescale
--
declare 
	S ALIAS for skey;
	T ALIAS for typeid;
	L ALIAS for lang; 
    generalSkey varchar(14);
    dLanguage varchar(5);
--    uKey varchar(25);
begin
	-- transform skey to general skey
	select into generalSkey '_'||split_part(S,'_',2)||'_1_'||split_part(S,'_',4)||'_'||split_part(S,'_',5); --in : _29_2_39_1 ==> _29_1_39_1
	-- check language variable, change to english if nok
	select into dLanguage case when (select c.language from capturetranslations c  where c.language = L limit(1)) is null
		then 'en_US'
		else (select c.language from capturetranslations c  where c.language = L limit(1))
		end;
	-- generate uKey 
	-- select into uKey dLanguage ||'_'||T||generalSkey;
	--get and return the translation
--	return c.translation from capturetranslations c where c.uniquekey = uKey limit(1);
	return c.translation from capturetranslations c where c.uniquekey = dLanguage ||'_'||T||generalSkey limit(1);

 
/*
usage :
select Pattyn_Transskey('_29_1_39_1','6','en_US') --Emergency stop 
select Pattyn_Transskey('_29_3_39_1','6','pl_PL') --Wylacznik awaryjny ==> let op de '3'
select Pattyn_Transskey('_29_1_39_1','6','da_DD') -- Emergency stop ==> taal bestaat niet ==> engels
select Pattyn_Transskey('_29_1_39_1','6','nl_BE') --Noodstop
*/

end;
$function$
;
