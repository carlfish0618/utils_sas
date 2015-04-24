%MACRO adjust_weight(my_library, stock_pool, max_weight, is_other_adjust, stock_pool_adjust_weight);
	%IF &is_other_adjust = 0 %THEN %DO;
		DATA &my_library..&stock_pool_adjust_weight;
			SET &my_library..&stock_pool;
			IF weight > &max_weight THEN weight = &max_weight;
		RUN;
	%END;
%MEND adjust_weight;

/*%ELSE %DO;
		%LOCAL big_nums = 0;
		DATA &my_library..stock_pool_adjust_weight;
			SET &my_library..&stock_pool;
		RUN;

		PROC SQL NOPRINT;
			SELECT stock_code,weight,count(*) into:big_ones_code separated by ' ',:big_ones_weight
									separated by ' ',:big_nums 
			FROM &my_library..stock_pool_adjust_weight
			WHERE  weight > &max_weight
		QUIT;

		%DO %WHILE (%SYSEVALF(&big_nun));
			PROC SQL NOPRINT;
				SELECT stock_code,weight,count(*) into:big_ones_code separated by ' ',:big_ones_weight
									separated by ' ',:big_nums 
				FROM &my_library..stock_pool_adjust_weight
				WHERE  weight > &max_weight;

				SELECT stock_code,weight,count(*),sum(weight) into:small_ones_code separated by ' ',:small_ones_weight
									separated by ' ',:small_nums,:sum_of_small_weight 
            	FROM &my_library..stock_pool_adjust_weight
				WHERE  weight < &max_weight;
       		QUTI;

	  
			%LET Large_part=0;
				%DO big_nums_index=1 %TO &big_nums;
						%LET Big=%SCAN(&big_ones_weight,&big_nums_index,' ');
						%LET Large_part=%SYSEVALF(&Large_part + &Big - &max_weight);
				%END;
				DATA &my_library..stock_pool_adjust_weight;
	   				MODIFY &my_library..stock_pool_adjust_weight;
					IF weight > &max_weight THEN weight = &max_weight;
					IF weight < &max_weight then weight = weight +(weight/&sum_of_small_weight) * &Large_part;
				RUN;
		%END;
	%END;
%MEND adjust_weight;

*/

/* module 1: normalized the weight */
/* Input: 
	(1) my_library
	(2) stock_pool: dataset
	(3) is_fixed_size: 1-> refer to channel, each stock in the pool shares one channel (weight = 1)
	(4) size: numeric (only valid when is_fixed_size = 1) 
	(5) stock_pool_norm_weight: (output) datasets*/
/* Output:
	(1) stock_pool_norm_weight: datasets */
/* Datasets Detail:
	(1) (input) stock_pool: date, stock_code, weight (and other columns)
	(2) (output) stock_pool_norm_weight: stock_pool with normalized weight */

%MACRO norm_weight(my_library, stock_pool, is_fixed_size, size , stock_pool_norm_weight);
	%IF &is_fixed_size = 1 %THEN %DO;  /* channel */
		DATA &my_library..&stock_pool_norm_weight;
			SET &my_library..&stock_pool;
			weight = round(weight/&size,0.000001);  /* weight = 1 */
		RUN;
	%END;
	%ELSE %DO;
		PROC SQL NOPRINT;  /* ps: is_sell = 1 & is_buy = 0 => weight = 0 */
			CREATE TABLE &my_library..tmp1 AS
			SELECT date, sum(weight) as t_weight
			FROM &my_library..&stock_pool
			GROUP BY date;
		QUIT;

		PROC SORT DATA = &my_library..&stock_pool OUT = &my_library..stock_pool_tmp;
			BY date;
		RUN;

		DATA &my_library..tmp1;
			MERGE &my_library..tmp1(in = a) &my_library..stock_pool_tmp(in = b);
			BY date;
			IF b;
		RUN;

		DATA &my_library..&stock_pool_norm_weight;
			SET &my_library..tmp1;
			IF t_weight ~= 0 THEN weight = round(weight/t_weight,0.000001);
			ELSE weight = 0;
			DROP t_weight;
		RUN;
		
		PROC SQL;
			DROP TABLE &my_library..stock_pool_tmp, &my_library..tmp1;
		QUIT;
	%END;

%MEND norm_weight;

%LET stock_pool = all_sample_pool;
%LET stock_sector_mapping = stock_sector_mapping;
%LET ind_max_weight = 0.05;
%LET all_max_weight = 0.8;
%LET start_date = &min_busday.;
%LET edit_stock_pool = &eventName._pool_edit;






