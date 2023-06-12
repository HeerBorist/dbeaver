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




select sum(Fouten.fout) from (
	select count(recipientweight_fnetweight) as fout from weightdata_v1 wv 
	where recipientweight_fnetweight > 20.5 and recipe = 'J10'
	union 
	select count(recipientweight_fnetweight) as fout from weightdata_v1 wv 
	where recipientweight_fnetweight < 19.5 and recipe = 'J10') as Fouten

 select fout / count(recipientweight_fnetweight) 

 with fouten as (
 	select count(recipientweight_fnetweight) as fout from weightdata_v1 wv 
		where (recipientweight_fnetweight > 20.5 or recipientweight_fnetweight < 19.5) and recipe = 'J10') 
 with alles as (
 	select count(recipientweight_fnetweight) as all from weightdata_v1 wv 
		where 
		--(recipientweight_fnetweight > 20.5 or recipientweight_fnetweight < 19.5) and 
		recipe = 'J10')

		select fout from fouten

		
		select fout / count(recipientweight_fnetweight) from weightdata_v1 wv 
	where recipe = 'J10'

with numbers as (
	select (	 	select count(recipientweight_fnetweight) as fout from weightdata_v1 wv 
		where (recipientweight_fnetweight > 20.5 and recipe = 'J10') or (recipientweight_fnetweight < 19.6 and recipe = 'J01'))  as fouten,
		( 	select count(recipientweight_fnetweight) as all from weightdata_v1 wv where recipe = 'J01') as totaal
		)
		select fouten, totaal, totaal-fouten, (fouten::decimal / totaal::decimal) as mypercent  from numbers
		

		select count(recipientweight_fnetweight) as fout from weightdata_v1 wv 
		where (recipientweight_fnetweight > 25 and recipe = 'J10') 
		select count(recipientweight_fnetweight) as fout from weightdata_v1 wv 
		where (recipientweight_fnetweight < 15 and recipe = 'J10') or (recipientweight_fnetweight < 19.6 and recipe = 'J01')
		

		
-- poging om accuracy te berekenen (source = dieter)		
with cijfers as (select 
		(
		select count(recipientweight_fnetweight) as totalok
        from weightdata_v1
        WHERE
            time>('2021-03-09T08:42:53.535Z')
            and recipe in ('J01')
            and recipientweight_sunitId in ('W1')
            and recipientweight_fnetweight > (  SELECT recipientweight_fsetpoint-recipientweight_fmaxunderweight
                                                FROM weightdata_v1 
                                                ORDER BY time desc
                                            limit 1)
            and recipientweight_fnetweight < ( SELECT recipientweight_fsetpoint+recipientweight_fmaxoverweight
                                                FROM weightdata_v1
                                                ORDER BY time desc
                                                limit 1)
		) as fouten,
		(select count(recipientweight_fnetweight) as total 
		from weightdata_v1
        WHERE
            time>('2021-03-09T08:42:53.535Z')
            and recipe in ('J01')
            and recipientweight_sunitId in ('W1') ) as totaal
		)
		
		select cijfers.totaal, cijfers.fouten , (fouten::decimal/totaal::decimal) as mynumber from cijfers

/*
|totaal|fouten|mynumber              |
|------|------|----------------------|
|3955  |2726  |0.68925410872313527181|
*/
		
		select * from weightdata_v1 wv order by time desc limit 100
		{"RecipeBatch": "191", "bRecipientWeightUpdated": 0.0}
		