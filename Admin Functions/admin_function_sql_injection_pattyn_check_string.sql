
--drop function public.pattyn_check_string ( VARCHAR)
CREATE OR REPLACE FUNCTION public.pattyn_check_string(in_string character varying)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$ 

 
DECLARE ret_val INTEGER := 1;

/* FUNCTION COMMENT : 
 * 
 * Created 	 : 20210728
 * CreatedBy : SSAV
 * 
 ----------------------------------------------------------------------------------
 * Change History : 
 *
 * 20220331  added escape characters for '_' as "%xp\_%', to avoid fail on e.g. 'xpr', but not on 'xp_'
 * ----------------------------------------------------------------------------------
 * 				Purpose : 
 * ----------------------------------------------------------------------------------
 * 	check (function) variables for dangerous patterns to avoid some basic "sql injection" issues
 *   
 * 
 * ----------------------------------------------------------------------------------
 * 				Inputs  : 
 * ----------------------------------------------------------------------------------
 * a text variable
 * 
 *
 * ----------------------------------------------------------------------------------
 * 				Output  : 
 * ----------------------------------------------------------------------------------
 * returns 1 if input string is OK, 0 otherwise

* ----------------------------------------------------------------------------------
* 
* Example of how to Use the function : 
*
* select pattyn_check_string('this is a dangerous text !;--')
* select pattyn_check_string('this is not a dangerous text !')
* 
* <in function example :> 
* if 	(pattyn_check_string(v_logcategory) = 0) 
		then raise exception 'input did not pass the SQL injection security test !'
				using hint = 'check the input variables for words like -end- -begin- -drop- ...';
		else
			-- do something here ;
		end if;
* 
* 
* see e.g. public.pattyn_dba_functionlogger() to see this function in action !
* ----------------------------------------------------------------------------------
* 
* 
* END OF FUNCTION COMMENT
*/ 
		

BEGIN
    ---assume ret_val=1;  
   
    IF (in_string like '%''%') then ret_val:=0;
    ELSEIF (in_string like '%--%') then ret_val:=0;
    ELSEIF (in_string like '%/*%') then ret_val:=0;
    ELSEIF (in_string like '%*/%') then ret_val:=0;
    ELSEIF (in_string like '%@') then ret_val:=0;
    ELSEIF (in_string like '%@@%') then ret_val:=0;
    ELSEIF (in_string like '%char%') then ret_val:=0;
    ELSEIF (in_string like '%nchar%') then ret_val:=0;
    ELSEIF (in_string like '%varchar%') then ret_val:=0;
    ELSEIF (in_string like '%nvarchar%') then ret_val:=0;
    
    ELSEIF (in_string like '%select%') then ret_val:=0;
    ELSEIF (in_string like '%insert%') then ret_val:=0;
    ELSEIF (in_string like '%update%') then ret_val:=0;
    ELSEIF (in_string like '%delete%') then ret_val:=0;
    ELSEIF (in_string like '%from%') then ret_val:=0;
    ELSEIF (in_string like '%table%') then ret_val:=0;
 
    ELSEIF (in_string like '%drop%') then ret_val:=0;
    ELSEIF (in_string like '%create%') then ret_val:=0;
    ELSEIF (in_string like '%alter%') then ret_val:=0;
 
    ELSEIF (in_string like '%begin%') then ret_val:=0;
    ELSEIF (in_string like '%end%') then ret_val:=0; --risky ? 
 
    ELSEIF (in_string like '%grant%') then ret_val:=0;
    ELSEIF (in_string like '%deny%') then ret_val:=0;
 
    ELSEIF (in_string like '%exec%') then ret_val:=0;
    ELSEIF (in_string like '%sp\_%') then ret_val:=0;  --the _ is used as a match for one character, so need to escape here to avoid 'thisisnospy' to fail... !!
    ELSEIF (in_string like '%xp\_%') then ret_val:=0;  --the _ is used as a match for one character, so need to escape here to avoid 'boxpro'  to fail... !!
 
    ELSEIF (in_string like '%cursor%') then ret_val:=0;
    ELSEIF (in_string like '%fetch%') then ret_val:=0;
 
    ELSEIF (in_string like '%kill%') then ret_val:=0;
    ELSEIF (in_string like '%open%') then ret_val:=0;
 
    ELSEIF (in_string like '%sysobjects%') then ret_val:=0;
    ELSEIF (in_string like '%syscolumns%') then ret_val:=0;
    ELSEIF  (in_string like '%sys%') then ret_val:=0;
    end if;
    
 
    RETURN (ret_val);
 
END;
$function$
;

-- Permissions

ALTER FUNCTION public.pattyn_check_string(varchar) OWNER TO "PattynAdmin";
GRANT ALL ON FUNCTION public.pattyn_check_string(varchar) TO public;
GRANT ALL ON FUNCTION public.pattyn_check_string(varchar) TO "PattynAdmin";


select pattyn_check_string('this is a dangerous text !;--')
select pattyn_check_string('this is not a dangerous text !')
