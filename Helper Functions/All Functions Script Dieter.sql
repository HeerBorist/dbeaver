CREATE OR REPLACE FUNCTION public.pattyn_dtou_batch_dynqarr(q_inputs character varying[], from_time timestamp with time zone, to_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP)
 RETURNS TABLE(recipex character varying, starttime timestamp with time zone, endtime timestamp with time zone, resarr numeric[], qarr character varying[])
 LANGUAGE plpgsql
AS $function$
declare
    r record;
   qr record;  
begin
for r in SELECT recipe, time_from, time_to 
	from pattyn_getrecipeperiods ('1002', '2021-01-19T23:00:00Z', '2021-01-20T22:59:59.999Z')
loop
	recipex := r.recipe; 
	starttime := r.time_from; 
	endtime := r.time_to; 
	select array_agg(res) as resarr, array_agg(q) as qarr into qr from pattyn_dtou_dynqArr(q_inputs, r.time_from, r.time_to);
	resarr := qr.resarr; 
	qarr := qr.qarr;
	return next;
end loop;

end;
$function$
;


CREATE OR REPLACE FUNCTION public.pattyn_dtou_batch_dynqarr_trsps(q_inputs character varying[], from_time timestamp with time zone, to_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP)
 RETURNS TABLE(recipe character varying, starttime timestamp with time zone, endtime timestamp with time zone, d1 numeric, d2 numeric, d3 numeric, d4 numeric, d5 numeric, d6 numeric, d7 numeric)
 LANGUAGE plpgsql
AS $function$
declare
	r_batch record;	--	result of fn to gather batch info. will be used to loop over results 
   	r_q record;  
 	i_cols integer; -- number of cols from column for each batch
	
	q text; 		-- compose query to gather data for each batch. query is build so it return a text which is another query
					-- this query must be executed and transposes the data: row <-> cols   
	q2 text;		-- result of above record in text, which is a query that can be executed, result is transposed data 	
	res record; 	-- result of query to transpose, contains actual data  
begin
	
	select array_length(q_inputs,1) into i_cols; 
	
for r_batch in SELECT * 
	from pattyn_dtou_getrecipeperiods('1002', from_time, to_time)
loop
	recipe := r_batch.recipe; 
	starttime := r_batch.time_from; 
	endtime := r_batch.time_to; 

	q:=  $$SELECT 'SELECT * FROM unnest(
	  ''{q, res}''::text[], ' || string_agg(quote_literal(ARRAY[idx::text, res::text]) || '::text[]', E'\n, ') || E') \n 
		AS t(col,' || string_agg('d' || idx, ',') || ')' AS qry$$; 
	
	q:= q || ' from pattyn_dtou_dynqArr_idx($1,$2,$3)';
	execute q into r_q using q_inputs, starttime, endtime;

	q2 := r_q.qry || 'order by 1 desc LIMIT 1';
	execute q2 into res;

	case 
		when i_cols = 1 then
			d1 := res.d1;
			d2 := 0;			
			d3 := 0;			
			d4 := 0;			
			d5 := 0;			
			d6 := 0;			
			d7 := 0; 
		when i_cols = 2 then
			d1 := res.d1;			
			d2 := res.d2;			
			d3 := 0;			
			d4 := 0;			
			d5 := 0;			
			d6 := 0;			
			d7 := 0;
		when i_cols = 3 then
			d1 := res.d1;			
			d2 := res.d2;			
			d3 := res.d3;			
			d4 := 0;			
			d5 := 0;			
			d6 := 0;			
			d7 := 0;		
		when i_cols = 4 then
			d1 := res.d1;			
			d2 := res.d2;			
			d3 := res.d3;			
			d4 := res.d4;			
			d5 := 0;			
			d6 := 0;			
			d7 := 0;		
		when i_cols = 5 then
			d1 := res.d1;			
			d2 := res.d2;			
			d3 := res.d3;			
			d4 := res.d4;			
			d5 := res.d5;			
			d6 := 0;			
			d7 := 0;
		when i_cols = 6 then
			d1 := res.d1;			
			d2 := res.d2;			
			d3 := res.d3;			
			d4 := res.d4;			
			d5 := res.d5;			
			d6 := res.d6;			
			d7 := 0;		
		when i_cols = 7 then
			d1 := res.d1;			
			d2 := res.d2;			
			d3 := res.d3;			
			d4 := res.d4;			
			d5 := res.d5;			
			d6 := res.d6;			
			d7 := res.d7;
		else 
			d1 := 0;			
			d2 := 0;			
			d3 := 0;			
			d4 := 0;			
			d5 := 0;			
			d6 := 0;			
			d7 := 0;
		end case; 
		
	return next;
end loop;
end;
$function$
;

CREATE OR REPLACE FUNCTION public.pattyn_dtou_batch_dynqarr_trsps2(q_inputs character varying[], from_time timestamp without time zone, to_time timestamp without time zone DEFAULT CURRENT_TIMESTAMP)
 RETURNS TABLE(recipex character varying, starttime timestamp with time zone, endtime timestamp with time zone, i_cols numeric)
 LANGUAGE plpgsql
AS $function$
declare
    r record;
   qr record;  
	r_cols record; -- result of query to get number of cols for each batch
  
  
begin
for r in SELECT recipe, time_from, time_to 
	from pattyn_getrecipeperiods ('1002', from_time::timestamp, to_time::timestamp)
loop
	recipex := r.recipe; 
	starttime := r.time_from; 
	endtime := r.time_to; 

	select idx into r_cols from pattyn_dtou_dynqArr_idx(q_inputs, from_time::timestamp, to_time::timestamp)
	order by 1 desc
	limit 1;  

	i_cols := r_cols.idx;

	return next;
end loop;

end;
$function$
;

CREATE OR REPLACE FUNCTION public.pattyn_dtou_batch_dynqarr_trsps2(q_inputs character varying[], from_time timestamp with time zone, to_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP)
 RETURNS TABLE(recipe character varying, starttime timestamp with time zone, endtime timestamp with time zone, d1 numeric)
 LANGUAGE plpgsql
AS $function$
declare
	r_batch record;	--	result of fn to gather batch info. will be used to loop over results 
   	r_q record;  
 	i_cols integer; -- number of cols from column for each batch
	
	q text; 		-- compose query to gather data for each batch. query is build so it return a text which is another query
					-- this query must be executed and transposes the data: row <-> cols   
	q2 text;		-- result of above record in text, which is a query that can be executed, result is transposed data 	
	res record; 	-- result of query to transpose, contains actual data  
begin
	
	select array_length(q_inputs,1) into i_cols; 
	
for r_batch in SELECT * 
	from pattyn_dtou_getrecipeperiods ('1002', from_time, to_time)
loop
	recipe := r_batch.recipe; 
	starttime := r_batch.time_from; 
	endtime := r_batch.time_to; 

	q:=  $$SELECT 'SELECT * FROM unnest(
	  ''{q, res}''::text[], ' || string_agg(quote_literal(ARRAY[idx::text, res::text]) || '::text[]', E'\n, ') || E') \n 
		AS t(col,' || string_agg('d' || idx, ',') || ')' AS qry$$; 
	
	q:= q || ' from pattyn_dtou_dynqArr_idx($1,$2,$3)';
	execute q into r_q using q_inputs, starttime, endtime;

	q2 := r_q.qry || 'order by 1 desc LIMIT 1';
	execute q2 into res;

	raise notice '%', q; 

	d1 := res.d1;

	return next;
end loop;
end;
$function$
;


CREATE OR REPLACE FUNCTION public.pattyn_dtou_dynq(q_input character varying, from_time timestamp without time zone, to_time timestamp without time zone DEFAULT CURRENT_TIMESTAMP)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$

declare
--	example_input varchar := 'weightdata_v1|recipientweight_eweightresult|avg';
    t varchar; -- table
   col varchar; -- column 
  aggr varchar;  -- aggregation over column 
   res numeric; 
  q text; -- query build 
  
--  from_time timestamp with time zone:= '2021-01-20T14:39:55.245Z'; 
-- to_time timestamp with time zone:= '2021-01-20T14:39:55.245Z'; 
begin
	
	select into t split_part(q_input,'|',1);
	select into col split_part(q_input,'|',2);
	select into aggr split_part(q_input,'|',3);

	q := 'SELECT ' || aggr ||'(' || col || ') as g from ' || quote_ident(t) || ' WHERE "time" between ' || quote_literal(from_time) ||' and ' || quote_literal(to_time);

   execute q into res;
  
  return res; 

end;
$function$
;


CREATE OR REPLACE FUNCTION public.pattyn_dtou_dynq(q_input character varying, from_time timestamp with time zone, to_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$

declare
--	example_input varchar := 'weightdata_v1|recipientweight_eweightresult|avg';
    t varchar; -- table
   col varchar; -- column 
  aggr varchar;  -- aggregation over column 
    filt varchar; -- filter 
   res numeric; 
  q text; -- query build 
  
--  from_time timestamp with time zone:= '2021-01-20T14:39:55.245Z'; 
-- to_time timestamp with time zone:= '2021-01-20T14:54:55.245Z'; 
begin
	
	select into t split_part(q_input,'|',1);
	select into col split_part(q_input,'|',2);
	select into aggr split_part(q_input,'|',3);
	select into filt trim(split_part(q_input,'|',4));

	if (filt = '')
	then 
		filt = true; 
	end if; 

	q := 'SELECT ' || aggr ||'(' || col || ') as g from ' || quote_ident(t) ||
		' WHERE "time" between ' || quote_literal(from_time) ||' and ' || quote_literal(to_time) ||' and ' || filt ;

   execute q into res;
  
  return res; 

end;
$function$
;


CREATE OR REPLACE FUNCTION public.pattyn_dtou_dynqarr(q_inputs character varying[], from_time timestamp with time zone, to_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP)
 RETURNS TABLE(q character varying, res numeric)
 LANGUAGE plpgsql
AS $function$

declare 
helpq varchar; 

begin
	
	foreach helpq in array q_inputs
	loop 
		select pattyn_dtou_dynq(helpq,from_time,to_time) into res;
		q := split_part(helpq,'|',3) || '_' || split_part(helpq,'|',2);
 		return next; 
	end loop; 

end;
$function$
;

CREATE OR REPLACE FUNCTION public.pattyn_dtou_dynqarr_idx(q_inputs character varying[], from_time timestamp without time zone, to_time timestamp without time zone DEFAULT CURRENT_TIMESTAMP)
 RETURNS TABLE(idx integer, res numeric)
 LANGUAGE plpgsql
AS $function$

declare 
helpq varchar; 
i integer := 1; 
begin
	
	foreach helpq in array q_inputs
	loop 
		select pattyn_dtou_dynq(helpq,from_time,to_time) into res;
		idx := i; 
 		return next;
 		i := i + 1; 
	end loop; 

end;
$function$
;

CREATE OR REPLACE FUNCTION public.pattyn_dtou_dynqarr_idx(q_inputs character varying[], from_time timestamp with time zone, to_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP)
 RETURNS TABLE(idx integer, res numeric)
 LANGUAGE plpgsql
AS $function$

declare 
helpq varchar; 
i integer := 1; 
begin
	
	foreach helpq in array q_inputs
	loop 
		select pattyn_dtou_dynq(helpq,from_time,to_time) into res;
		idx := i; 
 		return next;
 		i := i + 1; 
	end loop; 

end;
$function$
;

CREATE OR REPLACE FUNCTION public.pattyn_dtou_getrecipeperiods(machine_name character varying, from_time timestamp with time zone, to_time timestamp with time zone DEFAULT CURRENT_TIMESTAMP, line character varying DEFAULT 1)
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
