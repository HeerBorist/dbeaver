

--DROP FUNCTION public.pattyn_combined_stddev(_float8,_float8);
 
CREATE OR REPLACE FUNCTION public.pattyn_combined_stddev(std_mean_n1 float8 array[3], std_mean_n2 float8 array[3])
returns float8 array[3]
 -- array : [1] = stddeviation, [2] = mean or average, [3] = population or number of items
 
	IMMUTABLE
	returns null on null input 
	LANGUAGE plpgsql

	AS $function$


/* FUNCTION COMMENT : 
 * 
 * Created 	 : 20210906
 * CreatedBy : SSAV
 * 
 ----------------------------------------------------------------------------------
 * Change History : 
 *
 
 * if input is NULL, the function will not be called and return NULL
 * 
 * ----------------------------------------------------------------------------------
 *  
 * Purpose : return the combined standard deviation for a set of 2 standard deviations
 * Example : raw data has been divided into buckets of e.g. 1 hour by using a materialized view. 
 * 	To compose the batch-information, several buckets need to be 'reassembled' per batch. 
 *  This function composes two standard deviations into one combined standard deviation 
 *  
 *  By defining an aggregate function that uses this function, will allow for more than two (2) records to be combined.
 * 
 * Inputs  : 
 * 2 times : array float8[3] : [1] = stddeviation, [2] = mean or average, [3] = population or number of items
 * 
 * Output  : 
 * array float8[3] : [1] = stddeviation, [2] = mean or average, [3] = population or number of items  
 *
 * Used By : ?
 *  
 * 
 * References : 
 * -- combined mean, combined variance :
 * -- https://www.emathzone.com/tutorials/basic-statistics/combined-variance.html
 * 
 * 
 * CREATION OF AN AGGREGATE FOR THIS FUNCTION : 
 * 
 * 
 * --drop aggregate public.pattyn_combined_stddev_agg(float8[3])
	create or replace aggregate public.pattyn_combined_stddev_agg(float8 array[3])
	(
	initcond = '{0,0,0}',
	stype = float8 array[3], 	
	sfunc = public.pattyn_combined_stddev
	--finalfunc = xxx
	)

	--REFERENCES FOR AGGREGATE FUNCTION : 
	-- https://www.postgresql.org/docs/9.6/sql-createaggregate.html
	-- https://www.cybertec-postgresql.com/en/writing-your-own-aggregation-functions/
	-- https://www.postgresql.org/docs/9.6/xaggr.html
	-- https://stackoverflow.com/questions/46812478/custom-aggregate-in-postgres-using-multiple-columns
	-- https://stackoverflow.com/questions/4547672/return-multiple-fields-as-a-record-in-postgresql-with-pl-pgsql


-- use the function :
	select * from pattyn_combined_stddev('{9,63,50}','{6,54,40}') --==> {9.0,59.0,90.0}
	select *, a[1] as stdev, a[2] as mean, a[3] as n from pattyn_combined_stddev('{9,63,50}','{6,54,40}') as a
	

-- use the aggregation :
	select  batch,pattyn_combined_stddev_agg(array[stddeviation::float8, mean::float8, ncount::float8]) as ArrResult,
	(pattyn_combined_stddev_agg(array[stddeviation::float8, mean::float8, ncount::float8]))[1] as stdev,
	(pattyn_combined_stddev_agg(array[stddeviation::float8, mean::float8, ncount::float8]))[2] as mean,
	(pattyn_combined_stddev_agg(array[stddeviation::float8, mean::float8, ncount::float8]))[3] as n
	from tbl_stdtest tg
	group by batch
	order by batch
 * 
 * 
-- setup some testdata : 
-- drop table public.tbl_stdtest

	CREATE TABLE public.tbl_stdtest (
		uid int NOT NULL,
		ncount int NULL,
	    mean float8 NULL;
		stddeviation float8 null
	);


INSERT INTO public.tbl_stdtest (uid, ncount, mean, stddeviation, batch) values (1,50,25.4564206184922,0.255170987233015,'ALL');
INSERT INTO public.tbl_stdtest (uid, ncount, mean, stddeviation, batch) values (2,15,25.4692477338015,0.301928894939703,'FOUR');
INSERT INTO public.tbl_stdtest (uid, ncount, mean, stddeviation, batch) values (3,10,25.4528930477491,0.212813907625396,'FOUR');
INSERT INTO public.tbl_stdtest (uid, ncount, mean, stddeviation, batch) values (4,10,25.451703890749,0.211326367088071,'FOUR');
INSERT INTO public.tbl_stdtest (uid, ncount, mean, stddeviation, batch) values (5,15,25.4490897021739,0.256266628618023,'FOUR');
INSERT INTO public.tbl_stdtest (uid, ncount, mean, stddeviation, batch) values (6,30,25.4591687179877,0.28021136278023,'TWO');
INSERT INTO public.tbl_stdtest (uid, ncount, mean, stddeviation, batch) values (7,20,25.452298469249,0.212072275124094,'TWO');

	--delete from tbl_stdtest
	select * from public.tbl_stdtest

* ----------------------------------------------------------------------------------
* END OF FUNCTION COMMENT
		
-- The basic function : 	

*/

declare 

-- outputs :
 std_total float8; 
 mean_total float8;
 n_total float8;
 std_mean_n float8 array[3];
--inputs 1
 std1 constant float8 := std_mean_n1[1];
 mean1 constant float8 := std_mean_n1[2];
 n1 constant float8 := std_mean_n1[3];
--inputs 2
 std2 constant float8 := std_mean_n2[1];
 mean2 constant float8 := std_mean_n2[2];
 n2 constant float8 := std_mean_n2[3];
-- 
a float8;
b float8;

begin	
	--raise info 'inputs1 :  std1 : % , mean1 : % , population1: % ',  std1, mean1, n1;
	--raise info 'inputs2 :  std2 : % , mean2 : % , population2: % ',  std2, mean2, n2;
	-- what about null values ? 

	if (std1 = 0) and (mean1 = 0) and (n1 = 0)  -- initial condition for aggregation
	then 
		--raise info 'initial aggregate condition found !';
		std_total := std2;
		mean_total := mean2;
		n_total := n2 ;
	else
		n_total := n1 + n2;
		mean_total := ((n1*mean1)+(n2*mean2))/(n_total);
		a:=	n1*((std1^2)+(mean1 - mean_total)^2);
		b:= n2*((std2^2)+(mean2 - mean_total)^2);
		std_total := sqrt((a+b)/n_total)::float8;
	end if; 
		std_mean_n[1] := std_total;
		std_mean_n[2] := mean_total;
		std_mean_n[3] := n_total;
		--raise info 'outputs : std_total : % , mean_total : % ,  n_total : % ', std_mean_n[1] ,std_mean_n[2] , std_mean_n[3] ;
	return std_mean_n;

end; 
$function$
;



select *, a[1] as stdev, a[2] as mean, a[3] as n from pattyn_combined_stddev('{9,63,50}','{6,54,40}') as a

