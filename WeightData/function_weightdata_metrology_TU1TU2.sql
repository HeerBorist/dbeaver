-- weightdata metrology function 
--DROP FUNCTION pattyn_weightdata_metrology(character varying)

create or replace function public.pattyn_weightdata_metrology_speedtest (in_recipeloadcounter varchar)


RETURNS TABLE(
	"time" timestamp with time zone,
	endtime timestamp with time zone,
	line character varying, 
	machine character varying, 
	machineserial character varying, 
	recipe character varying, 
	recipeloadcounter character varying, 
--	"Filling head" double precision, --?? could be nice to find anomalies ?? 
	"Target_weight" double precision, 
	box_amount integer,
	avg_weight  double precision, --in gram
	stddev_gram double precision, --in gram
	giveaway_percentage  double precision,
	recipientweight_eunit  double precision, 
	unit_factor double precision, -- te berekenen
	TU1_gram_value double precision, --in gram
	TU2_gram_value double precision, --in gram
	minTU2  double precision, --in gram
	minTU1  double precision, --in gram
	QN  double precision, --in gram
	maxTO1  double precision, --in gram
	maxTO2  double precision, --in gram
	LESS_THAN_TU2 double precision,
	LESS_THAN_TU1 double precision,
	LESS_THAN_QN double precision,
	LESS_THAN_TO1 double precision,
	LESS_THAN_TO2 double precision,
	more_THAN_TO2 double precision,
	LESS_THAN_TU2_perc double precision,
	LESS_THAN_TU1_perc double precision,
	LESS_THAN_QN_perc double precision,
	LESS_THAN_TO1_perc double precision,
	LESS_THAN_TO2_perc double precision,
	more_THAN_TO2_perc double precision
 )

 LANGUAGE plpgsql
	AS $function$

	/* FUNCTION COMMENT : 
	 * 
	 * Created 	 : 20220513
	 * CreatedBy : SSAV
	 * Function returns the TU1/TU2 weight batch data according to European Regulations
	 * 
	 ----------------------------------------------------------------------------------
	 * Change History : 
	 * ----------------------------------------------------------------------------------
	 *  *  
	 * Purpose : return weightdata details per recipeloadcounter  -- 
	 * 		details need to be provided per recipeloadcounter (batch ??)
	 * 
	 * Inputs  : 
	 * - a recipeloadcounter
	 * 
	 * Checks : 
	 *  - does the batch exist ? 
	 *  - limit number of records to 30.000 , else return 1 record
	 * - is the batch terminated : no check. This check needs to be performed elsewhere !
	 * 
	 * Output  : 
	 * - table  
	 * Used By : 
	 * - testing the reports...  
	 * ----------------------------------------------------------------------------------
	  
	 * Example of how to Use the function : 
	 * select * from pattyn_weightdata_metrology('526700') -- returns 
	 * ----------------------------------------------------------------------------------
	 * 
	 * What should still be added : 
	 * TU2 reject below option (true, false) ?
	 * TO2 reject above (true, false)
	 * both 
	 * TU1 reject below (2.5%) (true, false)
	 * 
	 * business logic : tha average weight of a batch has to be equer or superior to the target weight. Only 2.5% of the TU2 products are accepted in a batch.
	 * Thus : below TU2 should be rejected,
	 * only first 2.5% below TU1 are accepted, consequent below TU1 values should be rejected...
	 * 
	* END OF FUNCTION COMMENT
	 */ 
		
	-- The function : 	
	
Declare  
		batch_exists boolean;
		max_records integer;
		v_time timestamp with time zone;
		v_endtime timestamp with time zone;
		v_line character varying;
		v_machine character varying;
		v_machineserial character varying;
		v_recipe character varying;
		v_recipeloadcounter character varying;
		v_Target_weight double precision;
		v_box_amount integer;
		v_avg_weight  double precision; --in gram
		v_stddev double precision; --in gram
		v_giveaway_percentage  double precision;
		v_recipientweight_eunit  double precision;
		v_unit_factor double precision; -- te berekenen
		
		v_TU1_gram_value double precision; --in gram
		v_TU2_gram_value double precision; --in gram
		v_minTU2  double precision; --in gram
		v_minTU1  double precision; --in gram
		v_QN  double precision; --in gram
		v_maxTO1  double precision; --in gram
		v_maxTO2  double precision; --in gram
		v_LESS_THAN_TU2 double precision; 
		v_LESS_THAN_TU1 double precision; 
		v_LESS_THAN_QN double precision; 
		v_LESS_THAN_TO1 double precision; 
		v_LESS_THAN_TO2 double precision; 
		v_more_THAN_TO2 double precision; 
		v_LESS_THAN_TU2_perc double precision;
		v_LESS_THAN_TU1_perc double precision;
		v_LESS_THAN_QN_perc double precision;
		v_LESS_THAN_TO1_perc double precision;
		v_LESS_THAN_TO2_perc double precision;
		v_more_THAN_TO2_perc double precision;
	
	
begin
		-- try to write some logging information in the database (on error do nothing !)   
--       perform pattyn_dba_functionlogger('function','public.pattyn_weightdata_metrology internal');
      -- check if the batchnumber exist

      --SELECT exists (SELECT 1 FROM bungelodersnlts.weightdata wd WHERE wd.recipeloadcounter  = $1 LIMIT 1);

	select 0 into batch_exists;

--	test1
     IF exists (SELECT 1 FROM weightdata w WHERE w.recipeloadcounter  = $1 LIMIT 1)
     then 
     	-- figure out what to do : the recipeloadcounter parameter has been found !!
--         	raise notice 'do something in the future with this batch : %', $1;
            select 1 into batch_exists;
--            raise notice 'batch exists : %', batch_exists ;

           -- count the maximum of records for this batch and adjust if necessary
           select count(recipientweight_fnetweight) into max_records from weightdata wd where wd.recipeloadcounter  = $1;
--           raise notice 'number of records found : %', max_records;
           if max_records > 30000 then 
          		select 30000 into max_records;
          		end if;
--           raise notice 'corrected number of records found : %', max_records ; --kan in de limit komen
        else 
--         	raise notice 'this batch has not been found : %', $1;
         	$1 = '%';
    	    max_records = 1;
     end if;



	raise notice 'start : %', clock_timestamp();

	if $1 is not null then
		-- unit factor to put everything in gram		
		select case 
			when w.recipientweight_eunit = 0  then	1000 --kg ==> gram
			when w.recipientweight_eunit = 1  then	1 --gr ==> gr
			when w.recipientweight_eunit = 2  then	1000*1000 --ton ==> gram
			when w.recipientweight_eunit = 3  then	453.59237 --lbs ==> gram
			end  conversion_factor
			from weightdata W where w.recipeloadcounter = $1
			order by W.time
			limit 1 offset 3 --take te third record only, instead of selecting e.g. the min of the complete batch
			into v_unit_factor;
		
--		raise notice 'v_unit_factor : %', v_unit_factor;
--		raise notice 'v_unit_factor : %', clock_timestamp();
		
		-- get the basic values for the batch : 
		select 
		count(W.recipientweight_fnetweight),				--	v_box_amount
		--min(W.recipientweight_fsetpoint) * v_unit_factor,  --  v_Target_weight (gram)
		mode() within group  (order by  W.recipientweight_fsetpoint)  * v_unit_factor, --  v_Target_weight (gram)
		avg(W.recipientweight_fnetweight )* v_unit_factor, --	v_avg_weight (gram)
		stddev(w.recipientweight_fnetweight)* v_unit_factor ,  -- standaard deviatie
		(avg(W.recipientweight_fnetweight)-min(W.recipientweight_fsetpoint))/min(W.recipientweight_fsetpoint) * 100, -- giveaway_percentage
		min(W.recipientweight_eunit), --v_recipientweight_eunit
		first(w.time,w.time),
		last(w.time,w.time),
		min(W.machine),
		min(W.machineserial), 
		min(W.line),
		min(W.recipe)
		from weightdata W where W.recipeloadcounter = $1
		into v_box_amount,	v_Target_weight, v_avg_weight,	v_stddev ,v_giveaway_percentage, v_recipientweight_eunit, v_time, v_endtime, 
		v_machine ,v_machineserial ,v_line,   v_recipe ;
		
--		raise notice 'boxes : %', v_box_amount;
	
		--calculate the TU1 value
		select case 
		 when v_Target_weight > 5 	and v_Target_weight <= 50 then v_Target_weight * 0.09
		 when v_Target_weight > 50  and v_Target_weight <= 100 then 4.5
		 when v_Target_weight > 100	and v_Target_weight <= 200 then v_Target_weight * 0.045
		 when v_Target_weight > 200	and v_Target_weight <= 300 then 9
		 when v_Target_weight > 300	and v_Target_weight <= 500 then v_Target_weight * 0.03
	     when v_Target_weight > 500	and v_Target_weight <= 1000 then 15
	 	 when v_Target_weight > 1000 and v_Target_weight <= 10000 then v_Target_weight * 0.015
		 when v_Target_weight > 10000 and v_Target_weight <= 15000 then 150
		 when v_Target_weight > 15000 then v_Target_weight * 0.01 -- 1% volgens franse uitleg , 1.5% volgens uitleg PattynLog
		--else 0 
		end	TU1_gr 	
		--from weightdata where recipeloadcounter = $1
		into v_TU1_gram_value;
	
	--https://www.stevenstraceability.com/average-weight-explained/

	--  calculate the boundaries
	select 
	v_TU1_gram_value * 2,					 --v_TU2_gram_value
	v_Target_weight - v_TU1_gram_value * 2, --v_minTU2
	v_Target_weight - v_TU1_gram_value  , 	--v_minTU1
	v_Target_weight, 						--v_QN
	v_Target_weight + v_TU1_gram_value  , --v_maxTO1
	v_Target_weight + v_TU1_gram_value * 2 --v_maxTO2
	into v_TU2_gram_value, v_minTU2, v_minTU1, v_QN, v_maxTO1, v_maxTO2; 
	
--	raise notice 'TU values : % %',v_TU1_gram_value,v_QN;



-- hier ergens de volledige lijst in een temptabel stoppen en veld 'isrejected' updaten ifv TU1 / TU2 ???
-- maar enkel als er een 'isrejected op true staat ??? '


	-- fill the histogram buckets with values 
	select 
	 count(*) filter (where W.recipientweight_fnetweight*v_unit_factor <  v_minTU2)  --as LESS_THAN_TU2
	,count(*) filter (where W.recipientweight_fnetweight*v_unit_factor >= v_minTU2 and W.recipientweight_fnetweight*v_unit_factor < v_minTU1)--as LESS_THAN_TU1
	,count(*) filter (where W.recipientweight_fnetweight*v_unit_factor >= v_minTU1 and W.recipientweight_fnetweight*v_unit_factor <  v_QN)  --as LESS_THAN_QN
	,count(*) filter (where W.recipientweight_fnetweight*v_unit_factor >= v_QN   and W.recipientweight_fnetweight*v_unit_factor <  v_maxTO1) --as LESS_THAN_TO1
	,count(*) filter (where W.recipientweight_fnetweight*v_unit_factor >= v_maxTO1 and W.recipientweight_fnetweight*v_unit_factor <  v_maxTO2)  --as LESS_THAN_TO2
	,count(*) filter (where W.recipientweight_fnetweight*v_unit_factor >= v_maxTO2)  --as more_THAN_TO2
--	, LESS_THAN_TU2 / count(*) as percentage
	into v_LESS_THAN_TU2 , v_LESS_THAN_TU1 , v_LESS_THAN_QN , v_LESS_THAN_TO1 , v_LESS_THAN_TO2 , v_more_THAN_TO2  
	from weightdata W where W.recipeloadcounter = $1;

	-- calculate the histogram buckets percentages of the population
	select 
	 (v_LESS_THAN_TU2 / v_box_amount) * 100
	,(v_LESS_THAN_TU1 / v_box_amount) *100
	, (v_LESS_THAN_QN / v_box_amount)*100
	, (v_LESS_THAN_TO1 / v_box_amount)*100
	, (v_LESS_THAN_TO2 / v_box_amount) *100
	, (v_more_THAN_TO2 / v_box_amount) *100
	into v_LESS_THAN_TU2_perc , v_LESS_THAN_TU1_perc , v_LESS_THAN_QN_perc , v_LESS_THAN_TO1_perc , v_LESS_THAN_TO2_perc , v_more_THAN_TO2_perc ;

		-- the final result : 
		return query
		SELECT 
			v_time as "time"
			,v_endtime as endtime
			,v_line as line 
			,v_machine as machine 
			,v_machineserial as machineserial 
			,v_recipe as recipe  
			,$1 as recipeloadcounter  
			,v_Target_weight as "Target_weight"  --in gram
			,v_box_amount as box_amount 
			,v_avg_weight as avg_weight --in gram
			,v_stddev as stddev_gram --in gram
			,v_giveaway_percentage as giveaway_percentage  
			,v_recipientweight_eunit as recipientweight_eunit 
			,v_unit_factor as unit_factor 
			,v_TU1_gram_value as TU1_gram_value --in gram
			,v_TU2_gram_value as TU2_gram_value  --in gram
			,v_minTU2 as minTU2   --in gram
			,v_minTU1 as minTU1  --in gram
			,v_QN as QN  --in gram
			,v_maxTO1 as maxTO1   --in gram
			,v_maxTO2 as maxTO2  --in gram
			,v_LESS_THAN_TU2 as LESS_THAN_TU2 
			,v_LESS_THAN_TU1 as LESS_THAN_TU1 
			,v_LESS_THAN_QN as LESS_THAN_QN 
			,v_LESS_THAN_TO1 as LESS_THAN_TO1 
			,v_LESS_THAN_TO2 as LESS_THAN_TO2 
			,v_more_THAN_TO2 as more_THAN_TO2 
			,v_LESS_THAN_TU2_perc as LESS_THAN_TU2_perc 
			,v_LESS_THAN_TU1_perc as LESS_THAN_TU1_perc 
			,v_LESS_THAN_QN_perc as LESS_THAN_QN_perc 
			,v_LESS_THAN_TO1_perc as LESS_THAN_TO1_perc
			,v_LESS_THAN_TO2_perc as LESS_THAN_TO2_perc
			,v_more_THAN_TO2_perc as more_THAN_TO2_perc
		;
		raise notice 'done with parameter :   %', $1;
	else
		raise notice 'empty input parameter detected :  %', $1;
	end if;




	end;
$function$
;

--select * from pattyn_weightdata_metrology('1753') -- returns
--	select * from pattyn_weightdata_metrology_speedtest('1753') -- returns