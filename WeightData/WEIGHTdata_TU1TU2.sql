select * from pattyntestplcts.weightdata where recipeloadcounter = '1753' order by time 

select distinct recipeloadcounter::integer from weightdata 
order by recipeloadcounter::integer desc  
limit 10

select
recipeloadcounter,
count(recipientweight_fnetweight), 
avg(recipientweight_fnetweight) as avg_net,  
min(recipientweight_fsetpoint) as min_setting,   
max(recipientweight_fsetpoint) as max_setting,
mode() within group  (order by  recipientweight_fsetpoint)   as mode_setting,
min( recipientweight_eunit) as min_eunit, 
max(recipientweight_eunit)  as max_eunit--eunit 0 = kg ???,
from weightdata where recipeloadcounter = '1330'
group by recipeloadcounter
order by recipeloadcounter::integer desc

select * from weightdata where recipeloadcounter = '1347' order by time asc
select * from weightdata where recipeloadcounter = '1347' order by time desc




---- zoek de normale verdeling

with batchconstants as 
	( select
		count(recipientweight_fnetweight) as box_amount, 
		avg(recipientweight_fnetweight)*1000 as avg_weight,    
		case -- define the weight_unit factor
			when min( recipientweight_eunit) = 0  then	min(recipientweight_fsetpoint) *1000 --kg ==> gram
			when min( recipientweight_eunit) = 1  then	min(recipientweight_fsetpoint) *1 --gr ==> gr
			when min( recipientweight_eunit) = 2  then	min(recipientweight_fsetpoint) *1000*1000 --ton ==> gram
			when min( recipientweight_eunit) = 2  then	min(recipientweight_fsetpoint) *453.59237 --lbs ==> gram
			end  nominal_weight,
		
		case -- assume setpoint in gram !!! (use first a conversion function if necessary) !!
		 when min(recipientweight_fsetpoint)*1000 > 5 and min(recipientweight_fsetpoint)*1000 <= 50 then min(recipientweight_fsetpoint)*1000*0.09
		 when min(recipientweight_fsetpoint)*1000 > 50 and min(recipientweight_fsetpoint)*1000 <= 100 then 4.5
	 	 when min(recipientweight_fsetpoint)*1000 > 100 and min(recipientweight_fsetpoint)*1000 <= 200 then min(recipientweight_fsetpoint)*1000*0.045
	 	 when min(recipientweight_fsetpoint)*1000 > 200 and min(recipientweight_fsetpoint)*1000 <= 300 then 9
		 when min(recipientweight_fsetpoint)*1000 > 300 and min(recipientweight_fsetpoint)*1000 <= 500 then min(recipientweight_fsetpoint)*1000*0.03 
		 when min(recipientweight_fsetpoint)*1000 > 500 and min(recipientweight_fsetpoint)*1000 <= 1000 then 15
		 when min(recipientweight_fsetpoint)*1000 > 1000 and min(recipientweight_fsetpoint)*1000 <= 10000 then min(recipientweight_fsetpoint)*1000*0.015 
		 when min(recipientweight_fsetpoint)*1000 > 10000 and min(recipientweight_fsetpoint)*1000 <= 15000 then 150
		 when min(recipientweight_fsetpoint)*1000 > 15000 then min(recipientweight_fsetpoint)*1000*0.01 -- 1% volgens franse uitleg
		 --else 150 
		end TU1_gr
	from weightdata where recipeloadcounter = '1303'
	) ,

 batchborders as 
	(
	select box_amount,TU1_gr as TU1, TU1_gr*2 as TU2, 
--	nominal_weight - TU1_gr*3 as lowerborder,
	nominal_weight - TU1_gr*2 as minTU2,
	nominal_weight - TU1_gr as minTU1,
	nominal_weight as QN, 
	nominal_weight + TU1_gr as maxTO1,
	nominal_weight + TU1_gr*2 as maxTO2,
--	nominal_weight + TU1_gr*3 as upperborder,
	avg_weight, 
	((avg_weight-nominal_weight)/nominal_weight) * 100 as giveaway_percentage
	from batchconstants
	)

	select * into temp table temp_batchborders from batchborders ;
	--	create temp table temp_batchborders as 	select * from batchborders;
	--select * from temp_batchborders
	--drop table temp_batchborders
	
-- put all the values in the correct metrology bucket !!
select 
 count(*) filter (where W.recipientweight_fnetweight*1000 <  T.minTU2) as LESS_THAN_TU2
,count(*) filter (where W.recipientweight_fnetweight*1000 >= T.minTU2 and W.recipientweight_fnetweight*1000 < T.minTU1) as LESS_THAN_TU1
,count(*) filter (where W.recipientweight_fnetweight*1000 >= T.minTU1 and W.recipientweight_fnetweight*1000 <  T.QN) as LESS_THAN_QN
,count(*) filter (where W.recipientweight_fnetweight*1000 >= T.QN     and W.recipientweight_fnetweight*1000 <  T.maxTO1) as LESS_THAN_TO1
,count(*) filter (where W.recipientweight_fnetweight*1000 >= T.maxTO1 and W.recipientweight_fnetweight*1000 <  T.maxTO2) as LESS_THAN_TO2
,count(*) filter (where W.recipientweight_fnetweight*1000 >= T.maxTO2) as more_THAN_TO2
, LESS_THAN_TU2 / count(*) as percentage
into temp_buckets 
from weightdata W , temp_batchborders T
	where W.recipeloadcounter = '1303';

select * from temp_buckets;

drop table if exists temp_batchborders;
drop table if exists temp_buckets;

--"less_than_tu2","less_than_tu1","less_than_qn","less_than_maxto1","less_than_maxto2","more_than_maxto2"
-- 398           ,246             ,2384         ,2429              ,247                ,406





	
	histogram as 
	(
	select width_bucket (recipientweight_fnetweight*1000, lowerborder,upperborder,6) as bucket,
--	int8range(lowerborder, upperborder, '[]') as range,
	numrange(min(recipientweight_fnetweight*1000)::numeric, max(recipientweight_fnetweight*1000)::numeric, '[]') as range,
	count(*) as freq
	from weightdata W , batchborders 
	where W.recipeloadcounter = '1303'
	group by bucket order by bucket
	)

	select bucket, 
	lowerborder + bucket * TU1 as floor,
	lowerborder + (bucket +1) * TU1 as ceiling, * 
	from histogram, batchborders

	
	select bucket--
	, range
	, freq 
	, repeat ('■',
               (   freq::float
                 / max(freq) over()
                 * 30
               )::int
        ) as bar
   from histogram;
  
min, max, tu 150, 15000.0
14550.0 14700 14850 15000 15150 15300 15450
  
select s.d, 10 * s.d from generate_series(14550, 15450,150) s(d)
/*
select 10 * s.d, count(t.age)
from generate_series(0, 10) s(d)
left outer join thing t on s.d = floor(t.age / 10)
group by s.d
order by s.d
*/




select min(recipientweight_fnetweight) , max (recipientweight_fnetweight) from weightdata where recipeloadcounter = '1303'

select 
sum(recipientweight_fnetweight),  
min(recipientweight_fsetpoint)*count(recipientweight_fnetweight) as nominal,
avg(recipientweight_fnetweight) as average,
count(recipientweight_fnetweight) as box_amount
from weightdata where recipeloadcounter = '1303'


select * from pattyntestplcts.alarms_consolidated order by time desc limit 5

select mqttcustomfield1 ,* from weightdata order by time desc  limit 10 --2022-05-11 09:44:49.898 +0200
select * from general order by time desc  limit 10  --2022-05-10 19:24:23.013 +0200
select * from alarms order by time desc  limit 10 --2022-05-10 19:24:23.065 +0200

