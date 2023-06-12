-- select data from two schemes !! yes !!

--update ppltesttslite4.general set machine = '1005'

(select time, machine, recipe  from pattyntestplcts.general order by time DESC limit 1) --1002  2021-06-16 15:16:48
UNION
(select time, machine, recipe  from ppltesttslite4.general order by time ASC limit 1) --1005 2021-05-04 14:31:07
order by time 


-- find all schemes in database pattyn : 

 
select * from public.dba_show_shemes dss 
select * from public.dba_table_details dtd where relname like '%general%'


