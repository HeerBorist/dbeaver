

-- lees alle tabelnamen , let op de aanhalingstekens !

SELECT 
  table_name 
FROM 
  information_schema.tables 
WHERE 
  lower(table_name) like 'weight%';


-- lees alle kolomnamen , let op de aanhalingstekens !

SELECT 
  table_name , *
FROM 
  information_schema.tables 
WHERE 
  lower(table_name) like 'weight%';
 
 
 

SELECT 
  column_name 
FROM 
  information_schema.columns 
WHERE 
  table_name = 'weightdata_v1';
  
 
-- Kan een normale user dit uitvoeren ? 
-- Blijft hij binnen zijn schema / search path ? 

 
 SELECT 
  column_name 
FROM 
  information_schema.columns 
WHERE 
  table_name in (SELECT   table_name FROM   information_schema.tables WHERE   lower(table_name) like 'weight%') and lower(column_name) like 'recipe%'
  
 