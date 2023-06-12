WITH grouped_recipes AS (
  SELECT g."recipe",
         g."time",
         g."machine",
         (
           g."recipe",
           -- There is a subtraction below, don't be fooled by the formatting
           DENSE_RANK() OVER (ORDER BY  g."machine", date_part('epoch', g."time")) 
         - DENSE_RANK() OVER (PARTITION BY g."recipe" ORDER BY g."machine" ,date_part('epoch', g."time")) 
          ) AS recipe_group
  FROM public."general" g where g."time"> '2021-01-19 13:26:49' and g."time" < '2021-02-03 13:26:49'
  --and g."machine"  = machine_name --'%AVL 24%'  -- machines, lijnen, etc...op voorhand uitsplitsen !!!
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
