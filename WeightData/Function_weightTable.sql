DROP FUNCTION pattyn_weight_summary(character varying)
CREATE OR REPLACE FUNCTION public.pattyn_weight_summary(recipeselection varchar DEFAULT '%')

RETURNS table (
		Id int4,
		Recipe_name varchar,
		batch_start timestamp with time zone,
		batch_end timestamp with time zone,
		Duration float8,
		Target_weight float8,
		Total_weight float8,
		Speed float8,
		Total_boxes int4,
		Avg_weight float8
)
 LANGUAGE plpgsql
AS $function$

	 
	
/*
 * Created 	 : 20210129
 * CreatedBy : SSAV
 * ----------------------------------------------------------------------------------
 * Purpose : return weightdata per batch
 * 
 * ----------------------------------------------------------------------------------
 *
 * Inputs  : 
 * - none or one specific recipe...
 * 
 * Output  : 
 * - table with last xx batches 
 * 
 * Used By : 
 * - testing the reports... and report parameters (recipe in this case???)
 *   
 * ----------------------------------------------------------------------------------
 * Changes:
 * 20210531 SSAV - first creation
 * 
 * ---------------------------------------------------------------------------------- 
 * How to Use : 
 * 	select * from public.pattyn_weight_summary()
 * select * from public.pattyn_weight_summary('%cal%')
 * select * from public.pattyn_weight_summary('J10') -- mooie kolommen
 * select public.pattyn_weight_summary() -- als 1 veld !!

 * ----------------------------------------------------------------------------------
 */ 
		

-- Then function : 		
	
	begin	
		
		
		return query
		
		SELECT
  			cast(RecipeLoadCounter as int) as "Id", 
  			mode() WITHIN GROUP (ORDER BY Recipe) as "Recipe_name",
  			min(fillingfirsttime) as "batch_start",
  			max(fillinglasttime) as "batch_end",
  			(extract(epoch from max(fillinglasttime)) - extract(epoch from min(fillingfirsttime)))::float8 as "Duration", 
  			-- array_agg(distinct(setpoint)) as setpoint -- in case want to visualize if setpoint changed 
  			mode() WITHIN GROUP (ORDER BY setpoint)::float8 as "Target weight", -- show target weight? 
  			sum(totalweight)/1000::float8 as "Total weight",
  			(sum(totalweight)*3.6)/(extract(epoch from max(fillinglasttime))-extract(epoch from min(fillingfirsttime)))::float8 as "Speed", 
  			-- could use fillingfirsttime and fillinglasttime instead, than performance will be higher
  			sum(nrofboxes)::int4 as "Total boxes",
  			--avg(stdevboxweight)*1000 as "Standard deviation"
  			round((sum(totalweight)/sum(nrofboxes))::numeric,2)::float8 as "Avg weight"

			FROM pattyntestplcts.weightdata_hourly wh 
			WHERE 
			 lower(recipe) like lower(recipeselection)
			 --recipe ilike ('%')
			 --recipe ~* 'Bar'
		  	--RecipeLoadCounter in ($RecipeIdx)
			GROUP BY RecipeLoadCounter
			ORDER BY RecipeLoadCounter desc
			limit 20;
		
	end;
$function$
;

select * from pattyn_getbatchstart()

select * from pattyn_weight_summary()

select id, total_boxes from pattyn_weight_summary()

SELECT (extract(epoch from batch_start))*1000, total_boxes FROM pattyn_weight_summary()

SELECT batch_start AS time, total_boxes FROM pattyn_weight_summary()


select sum(total_weight) from pattyn_weight_summary()

select * from public.pattyn_weight_summary('%cal%')
select * from public.pattyn_weight_summary('J10') -- mooie kolommen
select public.pattyn_weight_summary() -- als 1 veld !!


SELECT
  RecipeLoadCounter as "Id", 
  mode() WITHIN GROUP (ORDER BY Recipe) as "Recipe",
  min(fillingfirsttime) as "Start",
  max(fillinglasttime) as "End",
  (extract(epoch from max(fillinglasttime)) - extract(epoch from min(fillingfirsttime))) as "Duration", 
  -- array_agg(distinct(setpoint)) as setpoint -- in case want to visualize if setpoint changed 
  mode() WITHIN GROUP (ORDER BY setpoint) as "Target weight", -- show target weight? 
  sum(totalweight)/1000 as "Total weight",
  (sum(totalweight)*3.6)/(extract(epoch from max(fillinglasttime))-extract(epoch from min(fillingfirsttime))) as "Speed", 
  -- could use fillingfirsttime and fillinglasttime instead, than performance will be higher
  sum(nrofboxes) as "Total boxes",
  --avg(stdevboxweight)*1000 as "Standard deviation"
  round((sum(totalweight)/sum(nrofboxes))::numeric,2) as "Avg weight"
  
FROM pattyntestplcts.weightdata_hourly wh 
--WHERE 
--  recipe in ($Recipe) and 
  
--  fillingfirsttime >= $__timeFrom() 
--  and fillinglasttime <= $__timeTo()
  --RecipeLoadCounter in ($RecipeIdx)
GROUP BY RecipeLoadCounter
ORDER BY RecipeLoadCounter desc
limit 50


Id double,
Recipe varchar,
start timestamp,
end timestamp,
Duration float,
Target weight float,
Total weight float,
Speed float,
Total boxes double,
Avg weight float

select * from general limit 100
select now()

select * from pattyn_getrecipeperiods('1002', '2021-05-01 15:33:56','2021-06-01 15:33:56', '1' )




  
