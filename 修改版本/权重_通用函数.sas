/** ======================说明=================================================**/

/*** 函数功能：提供与计算权重相关的通用函数 **** /

/**** 函数列表:
(1) neutralize_weight:  将weight进行标准化处理
(2) limit_adjust_stock_only: 根据个股权重(上/下限)进行调整
(3) limit_adjust: 根据子行业(上/下限)和个股权重(上限)调整行业权重，及个股在行业中的权重
(4) indus_netrual: 行业等权，个股维持其原本在行业内的权重
(5) adjust_to_sector_neutral: 相对某一指数，构建行业中性策略。行业内个股保持，组合中原有的权重
(6) fill_in_index: 根据仓位要求配置股票
****/ 
/** =======================================================================**/




/*** 模块1: 将weight进行标准化处理 **/
/** 输入: 
(1) stock_pool: end_date /stock_code/ weight
/** 输出:
(1) output_stock_pool: end_date/stock_code/weight(调整后) /其他
**/

%MACRO neutralize_weight(stock_pool, output_stock_pool, col=weight);
	PROC SQL NOPRINT;	
		CREATE TABLE tmp AS
		SELECT A.*, B.t_&col.
		FROM &stock_pool. A LEFT JOIN 
		(
		SELECT end_date, sum(&col.) AS t_&col.
		FROM &stock_pool.
		GROUP BY end_date
		)B
		ON A.end_date = B.end_date;
	QUIT;

	DATA &output_stock_pool.(drop = t_&col.);
		SET tmp;
		IF t_&col. ~= 0 THEN &col. = round(&col./t_&col.,0.00001);  
		ELSE &col. = 0;
	RUN;
	PROC SQL;
		DROP TABLE tmp;
	QUIT;
%MEND neutralize_weight;


/***** 模块2: 设定个股权重 **/
/** 输入: 
(1) stock_pool: end_date / stock_code/ weight 
(2)  stock_upper(numeric): 个股上限
(3) stock_lower(numeric): 个股下限
/** 输出:
(1) output_stock_pool: end_date/stock_code/weight(调整后)/其他
**/

/** 注意: 如果上限和下限设定不合理导致始终无法调整到满足条件，则程序仍能正常输出。但是调整的weight是错误的 */
/** 程序优先保证上限不会被触碰。这与优先保证下限不会被触碰的结果是不同的 */

%MACRO limit_adjust_stock_only(stock_pool, stock_upper, stock_lower, output_stock_pool);
	/** 限定个股权重 **/
	PROC SQL;
		CREATE TABLE indus_info AS
		SELECT *
		FROM &stock_pool.;
	QUIT;

	PROC SQL NOPRINT;
		SELECT distinct end_date, count(distinct end_date)
		INTO :date_list separated by ' ',
			 :date_nobs
		FROM indus_info;
	QUIT;
	
	/** 每天处理 */
	%DO date_index = 1 %TO &date_nobs;
		%LET curdate = %SCAN(&date_list., &date_index., ' ');
		
		/* Step1：降低触上限个股 */
		/** 超过上限的个股 **/
		%LET big_nobs = 0;
		%LET big_wt = 0;
		PROC SQL NOPRINT;
			SELECT end_date, sum(weight), count(*)
			INTO :end_date,
				 :big_wt,
				 :big_nobs
			FROM indus_info
			WHERE end_date = input("&curdate.", mmddyy10.) AND weight > &stock_upper.
			GROUP BY end_date;
		QUIT;

		/** 低于上限的个股 **/ 
		%LET small_nobs = 0;
		%LET small_wt = 0;
		PROC SQL NOPRINT;
				SELECT end_date, sum(weight),count(*)
				INTO :end_date,
				 	 :small_wt,
					 :small_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND weight < &stock_upper.
				GROUP BY end_date;
			QUIT;
	
		%DO %WHILE (%SYSEVALF( &big_nobs. AND &small_nobs.));
			DATA indus_info;
				MODIFY indus_info;
				IF weight > &stock_upper. AND end_date = input("&curdate.", mmddyy10.) THEN DO;
					weight = &stock_upper.;
				END;
				ELSE IF weight < &stock_upper. AND end_date =input("&curdate.", mmddyy10.) THEN DO;
					%LET large_part = %SYSEVALF(&big_wt. - &stock_upper. * &big_nobs.);
					weight = weight + (weight / &small_wt.) * &large_part.;
				END;
			RUN;

			/** 超过上限的个股 **/
			%LET big_nobs = 0;
			%LET big_wt = 0;
			PROC SQL NOPRINT;
				SELECT end_date, sum(weight), count(*)
				INTO :end_date,
				 :big_wt,
				 :big_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND weight > &stock_upper.
				GROUP BY end_date;
			QUIT;

			/** 低于下限的个股 **/ 
			%LET small_nobs = 0;
			%LET small_wt = 0;
			PROC SQL NOPRINT;
				/* 低于限制的 */
				SELECT end_date, sum(weight),count(*)
				INTO :end_date,
				 	 :small_wt,
					 :small_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND weight < &stock_upper.
				GROUP BY end_date;
			QUIT;
		%END;


		/** Step2: 增加，触下限个股 */
		/** 超过下限的个股 **/
		%LET big_nobs = 0;
		%LET big_wt = 0;
		PROC SQL NOPRINT;
			SELECT end_date, sum(weight), count(*)
			INTO :end_date,
				 :big_wt,
				 :big_nobs
			FROM indus_info
			WHERE end_date = input("&curdate.", mmddyy10.) AND weight > &stock_lower.
			GROUP BY end_date;
		QUIT;

		/** 低于下限的行业 **/ 
		%LET small_nobs = 0;
		%LET small_wt = 0;
		PROC SQL NOPRINT;
				SELECT end_date, sum(weight),count(*)
				INTO :end_date,
				 	 :small_wt,
					 :small_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND weight < &stock_lower.
				GROUP BY end_date;
			QUIT;

	
		%DO %WHILE (%SYSEVALF( &big_nobs. AND &small_nobs.));
			DATA indus_info;
				MODIFY indus_info;
				IF weight < &stock_lower. AND end_date = input("&curdate.", mmddyy10.) THEN DO;
					weight = &stock_lower.;
				END;
				ELSE IF weight > &stock_lower. AND end_date =input("&curdate.", mmddyy10.) THEN DO;
					%LET small_part = %SYSEVALF(&stock_lower. * &small_nobs. - &small_wt.);
					weight = weight - (weight / &big_wt.) * &small_part.;
				END;
			RUN;

			/** 超过下限的个股 **/
			%LET big_nobs = 0;
			%LET big_wt = 0;
			PROC SQL NOPRINT;
				SELECT end_date, sum(weight), count(*)
				INTO :end_date,
				 :big_wt,
				 :big_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND weight > &stock_lower.
				GROUP BY end_date;
			QUIT;

			/** 低于下限的行业 **/ 
			%LET small_nobs = 0;
			%LET small_wt = 0;
			PROC SQL NOPRINT;
				SELECT end_date, sum(weight),count(*)
				INTO :end_date,
				 	 :small_wt,
					 :small_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND weight < &stock_lower.
				GROUP BY end_date;
			QUIT;
		%END;

	%END;

	DATA &output_stock_pool;
		SET indus_info;
	RUN;
	PROC SQL;
		DROP TABLE indus_info;
	QUIT;

%MEND limit_adjust_stock_only;


/****** 模块3: 设定子行业及个股偏离最大值 **/
/** 输入: 
(1) stock_pool: end_date / stock_code/ weight /indus_code/其他
(2) indus_upper(numeric): 单个行业在组合中的最大权重
(3) indus_lower(numeric): 单个行业在组合中最小权重
(4) stock_limit(numeric): 个股在其归属行业中的最大权重
/** 输出:
(1) output_stock_pool: end_date/stock_code/weight(调整后)/indus_code/其他  **/

%MACRO limit_adjust(stock_pool, indus_upper, indus_lower, stock_limit, output_stock_pool);
	/** 限定行业权重 **/
	PROC SQL;
		CREATE TABLE indus_info AS
		SELECT end_date, indus_code, sum(weight) AS indus_weight
		FROM &stock_pool.
		GROUP BY end_date, indus_code;
	QUIT;

	PROC SQL NOPRINT;
		SELECT distinct end_date, count(distinct end_date)
		INTO :date_list separated by ' ',
			 :date_nobs
		FROM indus_info;
	QUIT;
	
	/** 每天处理 */
	%DO date_index = 1 %TO &date_nobs;
		%LET curdate = %SCAN(&date_list., &date_index., ' ');
		
		/* Step1：降低触上限行业 */
		/** 超过上限的行业 **/
		%LET big_nobs = 0;
		%LET big_wt = 0;
		PROC SQL NOPRINT;
			SELECT end_date, sum(indus_weight), count(*)
			INTO :end_date,
				 :big_wt,
				 :big_nobs
			FROM indus_info
			WHERE end_date = input("&curdate.", mmddyy10.) AND indus_weight > &indus_upper.
			GROUP BY end_date;
		QUIT;

		/** 低于上限的行业 **/ 
		%LET small_nobs = 0;
		%LET small_wt = 0;
		PROC SQL NOPRINT;
				SELECT end_date, sum(indus_weight),count(*)
				INTO :end_date,
				 	 :small_wt,
					 :small_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND indus_weight < &indus_upper.
				GROUP BY end_date;
			QUIT;

	
		%DO %WHILE (%SYSEVALF( &big_nobs. AND &small_nobs.));
			DATA indus_info;
				MODIFY indus_info;
				IF indus_weight > &indus_upper. AND end_date = input("&curdate.", mmddyy10.) THEN DO;
					indus_weight = &indus_upper.;
				END;
				ELSE IF indus_weight < &indus_upper. AND end_date =input("&curdate.", mmddyy10.) THEN DO;
					%LET large_part = %SYSEVALF(&big_wt. - &indus_upper. * &big_nobs.);
					indus_weight = indus_weight + (indus_weight / &small_wt.) * &large_part.;
				END;
			RUN;

			/** 超过上限的行业 **/
			%LET big_nobs = 0;
			%LET big_wt = 0;
			PROC SQL NOPRINT;
				SELECT end_date, sum(indus_weight), count(*)
				INTO :end_date,
				 :big_wt,
				 :big_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND indus_weight > &indus_upper.
				GROUP BY end_date;
			QUIT;

			/** 低于下限的行业 **/ 
			%LET small_nobs = 0;
			%LET small_wt = 0;
			PROC SQL NOPRINT;
				/* 低于限制的 */
				SELECT end_date, sum(indus_weight),count(*)
				INTO :end_date,
				 	 :small_wt,
					 :small_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND indus_weight < &indus_upper.
				GROUP BY end_date;
			QUIT;
		%END;


		/** Step2: 增加，触下限行业 */
		/** 超过下限的行业 **/
		%LET big_nobs = 0;
		%LET big_wt = 0;
		PROC SQL NOPRINT;
			SELECT end_date, sum(indus_weight), count(*)
			INTO :end_date,
				 :big_wt,
				 :big_nobs
			FROM indus_info
			WHERE end_date = input("&curdate.", mmddyy10.) AND indus_weight > &indus_lower.
			GROUP BY end_date;
		QUIT;

		/** 低于上限的行业 **/ 
		%LET small_nobs = 0;
		%LET small_wt = 0;
		PROC SQL NOPRINT;
				/* 低于限制的 */
				SELECT end_date, sum(indus_weight),count(*)
				INTO :end_date,
				 	 :small_wt,
					 :small_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND indus_weight < &indus_lower.
				GROUP BY end_date;
			QUIT;

	
		%DO %WHILE (%SYSEVALF( &big_nobs. AND &small_nobs.));
			DATA indus_info;
				MODIFY indus_info;
				IF indus_weight < &indus_lower. AND end_date = input("&curdate.", mmddyy10.) THEN DO;
					indus_weight = &indus_lower.;
				END;
				ELSE IF indus_weight > &indus_lower. AND end_date =input("&curdate.", mmddyy10.) THEN DO;
					%LET small_part = %SYSEVALF(&indus_lower. * &small_nobs. - &small_wt.);
					indus_weight = indus_weight - (indus_weight / &big_wt.) * &small_part.;
				END;
			RUN;

			/** 超过下限的行业 **/
			%LET big_nobs = 0;
			%LET big_wt = 0;
			PROC SQL NOPRINT;
				SELECT end_date, sum(indus_weight), count(*)
				INTO :end_date,
				 :big_wt,
				 :big_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND indus_weight > &indus_lower.
				GROUP BY end_date;
			QUIT;

			/** 低于下限的行业 **/ 
			%LET small_nobs = 0;
			%LET small_wt = 0;
			PROC SQL NOPRINT;
				SELECT end_date, sum(indus_weight),count(*)
				INTO :end_date,
				 	 :small_wt,
					 :small_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND indus_weight < &indus_lower.
				GROUP BY end_date;
			QUIT;
		%END;

	%END;
	
	/** 把每个行业的权重，匹配到个股。要求个股权重若超过设定权重，则在其归属的行业内部调整 **/
	PROC SQL;
		CREATE TABLE stock_info AS
		SELECT A.*, B.indus_weight, C.indus_weight_raw
		FROM &stock_pool. A LEFT JOIN indus_info B
		ON A.end_date = B.end_date AND A.indus_code = B.indus_code
		LEFT JOIN 
		(SELECT end_date, indus_code, sum(weight) AS indus_weight_raw
		FROM &stock_pool.
		GROUP BY end_date, indus_code ) C
		ON A.end_date = C.end_date AND A.indus_code = C.indus_code
		ORDER BY A.end_date, A.indus_code;
	QUIT;
	/* 按照调整后的行业权重，重新分配行业内个股权重 */
	DATA stock_info(drop = indus_weight_raw);
		SET stock_info;
		weight = weight * indus_weight / indus_weight_raw;
	RUN;
	

	/** 要求个股的最大权重不能超过其所处行业权重的一定比例 */
	/** 每天，分行业处理 */

	%DO date_index = 1 %TO &date_nobs;
		%LET curdate = %SCAN(&date_list., &date_index., ' ');
		/* 取行业，分别处理 */

		PROC SQL NOPRINT;
			SELECT distinct indus_code, count(distinct indus_code)
				INTO :indus_list separated by ' ',
					: indus_nobs
			FROM stock_info
			WHERE end_date = input("&curdate.", mmddyy10.);
		QUIT;


		%DO indus_index = 1 %TO &indus_nobs.;
			%LET cur_indus = %SCAN(&indus_list, &indus_index., ' ');

			%LET big_nobs = 0;
			/** 超过限制的个股 **/
			PROC SQL NOPRINT;
				SELECT end_date, indus_code, sum(weight), count(*), mean(indus_weight)
				INTO :end_date,
					 :indus_code,
					 :big_wt,
				 	 :big_nobs,
					 :indus_wt
				FROM stock_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND indus_code = "&cur_indus." 
					AND weight > &stock_limit.
				GROUP BY end_date, indus_code;
			QUIT;

			%LET small_wt = 0;
			%LET small_nobs = 0;
			PROC SQL NOPRINT;
				SELECT end_date, indus_code, sum(weight), count(*)
				INTO :end_date,
						 :indus_code,
					 	:small_wt,
				 	 	:small_nobs
				FROM stock_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND indus_code = "&cur_indus." 
					AND weight < &stock_limit.
				GROUP BY end_date, indus_code;
			QUIT;

		
			%DO %WHILE (%SYSEVALF(&big_nobs. AND &small_nobs.));
				/** 低于限制的个股 **/
				DATA stock_info;
					MODIFY stock_info;
					IF end_date = input("&curdate.", mmddyy10.) AND indus_code = "&cur_indus." 
						AND weight > &stock_limit. THEN DO;
						weight = &stock_limit.;
					END;
					ELSE IF end_date = input("&curdate.", mmddyy10.) AND indus_code = "&cur_indus." 
						AND weight < &stock_limit. THEN DO;
							%LET large_part = %SYSEVALF(&big_wt. - &stock_limit. * &big_nobs.);
							weight = weight + (weight / &small_wt.) * &large_part.;
					END;
				RUN;

				/** 检验是否还有超过限制的个股 */
				%LET big_nobs = 0;
				PROC SQL NOPRINT;
					SELECT end_date, indus_code, sum(weight), count(*), mean(indus_weight)
					INTO :end_date,
						 :indus_code,
					 	:big_wt,
				 	 	:big_nobs,
					 	:indus_wt
					FROM stock_info
					WHERE end_date = &curdate. AND indus_code = "&cur_indus." 
						AND weight >  &stock_limit.
					GROUP BY end_date, indus_code;
				QUIT;

				%LET small_nobs = 0;
				%LET small_wt = 0;
				PROC SQL NOPRINT;
					SELECT end_date, indus_code, sum(weight), count(*)
					INTO :end_date,
						 :indus_code,
					 	:small_wt,
				 	 	:small_nobs
					FROM stock_info
					WHERE end_date = input("&curdate.", mmddyy10.) AND indus_code = "&cur_indus." 
						AND weight < &stock_limit.
					GROUP BY end_date, indus_code;
				QUIT;

			%END;
		%END;
	%END;

	DATA &output_stock_pool;
		SET stock_info(drop = indus_weight);
	RUN;
	PROC SQL;
		DROP TABLE stock_info, indus_info;
	QUIT;

%MEND limit_adjust;



/** 模块4: 行业等权，个股维持其原本在行业内的权重 */
/** 输入: 
(1) stock_pool: end_date / stock_code/ weight /indus_code/其他

/** 输出:
(1) output_stock_pool: end_date/stock_code/weight(调整后)/indus_code/其他 */

%MACRO indus_netrual(stock_pool, output_stock_pool);
	PROC SQL;
		CREATE TABLE tt_pool AS 
		SELECT A.*, B.indus_nobs, C.indus_wt
		FROM &stock_pool. A LEFT JOIN
		(SELECT end_date, count(distinct indus_code) AS indus_nobs
		FROM &stock_pool.
		GROUP BY end_date) B 
		ON A.end_date = B.end_date
		LEFT JOIN
		(SELECT end_date, indus_code, sum(weight) AS indus_wt
		FROM &stock_pool.
		GROUP BY end_date, indus_code) C
		ON A.end_date = C.end_date AND A.indus_code = C.indus_code
		ORDER BY A.end_date, A.stock_code;
	QUIT;
	DATA &output_stock_pool.(drop = indus_wt indus_nobs);
		SET tt_pool;
		weight = (1/indus_nobs)*(weight/indus_wt);
	RUN;
%MEND indus_netrual;


/*** 模块5: 行业中性策略 **/

/** 输入: 
(1) stock_pool: end_date / stock_code/ weight /indus_code/其他
(2) upper_limit(numeric): 个股在行业中最大权重(如果无法满足，则要求多出的权重，由"行业"权重或是"行业"中的个股权重补充)
(3) index_component_table: end_date/stock_code/o_code/其他

/** 输出:
(1) output_stock_pool: end_date/stock_code/weight(调整后)/indus_code/type/其他 
其中:
type: 标注位，是否为指数代码(0- 个股/1-基准指数/2-行业指数)
**/

%MACRO adjust_to_sector_neutral(stock_pool, upper_limit, index_component_table, output_stock_pool);
	/* Step1:计算调整日，基准的行业权重 */
	PROC SQL;
		CREATE TABLE tt_index_info AS
		SELECT end_date, indus_code, sum(weight)/100 AS sector_weight
		FROM &index_component_table.
		WHERE end_date IN
		(SELECT end_date FROM &stock_pool.)
		GROUP BY end_date, indus_code;
	QUIT;

	/* Step2: 计算个股权重 */
	PROC SQL NOPRINT;
		CREATE TABLE tt_stock_pool AS
		SELECT A.*, B.sector_weight, C.t_indus_weight  
		FROM &stock_pool. A 
		LEFT JOIN 
		(
			SELECT end_date, indus_code, sum(weight)/100 AS sector_weight /* 取行业在基准中的权重*/
			FROM tt_index_info
			GROUP BY end_date, indus_code;
		) B
		ON A.end_date = B.end_date AND A.indus_code = B.indus_code 
		LEFT JOIN
		(
			SELECT end_date, indus_code, sum(weight)/100 AS t_indus_weight
			FROM &stock_pool.
			GROUP BY end_date, indus_code
		) C 
		ON A.end_date = C.end_date AND A.indus_code = C.indus_code 
		ORDER BY A.end_date, A.indus_code;
	QUIT;
	
	
	DATA tt_stock_pool(drop = t_indus_weight);
		SET  tt_stock_pool;
		IF missing(sector_weight) OR round(sector_weight,0.0001) = 0 THEN weight = 0; /* 如果当日基准中没有该行业，则设定该行业的所有个股权重为0 */
		ELSE IF t_indus_weight ~= 0 THEN weight = round(sector_weight*round(weight/t_indus_weight,0.0001),0.00001);
		ELSE weight = 0;
		IF weight > &upper_limit. * sector_weight THEN weight = &upper_limit. * sector_weight;  /* 行业内有个股上限 */
		type = 0 ; /* 标注个股 */
	RUN;
 

	/* Step3: 调整完个股权重后，重新计算行业加总权重。必要时，增加相应的行业指数 */
	PROC SQL;
		CREATE TABLE tt_indus_weight AS
		SELECT A.*, B.t_indus_weight
		FROM tt_index_info A LEFT JOIN
		(
			SELECT end_date, indus_code, sum(weight) AS t_indus_weight
			FROM tt_stock_pool
			GROUP BY end_date, indus_code
		) B
		ON A.end_date = B.end_date AND A.indus_code = B.indus_code
		WHERE NOT missing(sector_weight)  /* 行业权重可能缺失 */
		ORDER BY A.end_date, A.o_code;
	QUIT;
 
	DATA tt_indus_weight(keep = end_date indus_code add_indus type rename = (indus_code = stock_code add_indus = weight));
		SET tt_indus_weight;
		IF missing(t_indus_weight) OR round(t_indus_weight, 0.0001) = 0 THEN DO;
			add_indus = sector_weight;
		END;
		ELSE IF abs(sector_weight-t_indus_weight)> 0.001 AND round(sector_weight-t_indus_weight,0.001)>0 THEN DO;
			add_indus = round(sector_weight - t_indus_weight,0.0001);
		END;
		ELSE DO;
			add_indus = 0;
		END;
		type = 2;
		/* 处理精度 */
		IF abs(add_indus) <= 0.001 THEN add_indus = 0;
	RUN;
	
	/* Step4: 完善指数的股票池 */
	DATA &output_stock_pool.;
		SET tt_ind_weight tt_indus_weight;
		IF weight > 0;
	RUN;
	
	PROC SORT DATA = &output_stock_pool.;
		BY end_date descending type stock_code;
	RUN;

	PROC SQL;
		DROP TABLE  tt_indus_weight, tt_index_info, tt_stock_pool;
	QUIT;
%MEND adjust_to_sector_neutral;


/*** 模块5: 根据仓位要求配置股票 **/

/** 输入: 
(1) stock_pool: end_date / stock_code/ weight /其他
(2) index_code(characters): 指数代码
(3) ind_max_weight(numeric): 个股在组合中中最大权重(如果无法满足，则要求多出的权重，由指数中的个股或指数本身权重补充)
(4) all_max_weight(numeric): 仓位最大值


/** 输出:
(1) output_stock_pool: end_date/stock_code/weight(调整后)/type/其他 
其中:
&mark.: 标注位，是否为指数代码(0- 个股/1-基准指数/2-行业指数)
**/

/** 要求：原始权重已经标准化了 */

%MACRO fill_in_index(stock_pool,all_max_weight, ind_max_weight, output_stock_pool,index_code, mark=type);
	/* Step1: 计算个股权重 */
	DATA tt_ind_pool;
		SET &stock_pool.;
		IF weight > &ind_max_weight. THEN weight = &ind_max_weight;  /* 有个股上限 */
		&mark. = 0;
	RUN;
	
	/* Step2: 调整完个股权重后，重新计算组合加总权重。针对所有调整日，必要时，增加相应的基准指数 */
	PROC SQL;
		CREATE TABLE tt_info AS
		SELECT end_date, sum(weight) AS t_weight
		FROM tt_ind_pool
		GROUP BY end_date
		ORDER BY end_date;
	QUIT;

	DATA tt_info;
		SET tt_info;
		IF missing(t_weight) OR round(t_weight,0.0001) = 0 THEN DO;
			add_bm_weight = 1;  /* 仓位全为指数 */
			multiplier = 0;
		END;
		ELSE IF abs(t_weight-&all_max_weight.)>=0.0001 AND round(t_weight-&all_max_weight.,0.0001)>=0 THEN DO;  /* 超过设定的权重 */
			add_bm_weight = 1-&all_max_weight.;
			multiplier = round(&all_max_weight./t_weight,0.001);  /* 仓位等比例降低 */
		END;
		ELSE DO;
			add_bm_weight = 1-t_weight;
			multiplier = 1;
		END;
		/* 精度问题 */
		IF abs(add_bm_weight)<=0.001 THEN DO;
			add_bm_weight = 0;
			multiplier = 1;
		END;
	RUN;

	/* Step3: 重新调整个股权重 */
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.multiplier
		FROM tt_ind_pool A LEFT JOIN tt_info B
		ON A.end_date = B.end_date;
	QUIT;

	DATA tt_ind_pool;
		SET tmp;
		weight = weight * multiplier;
	RUN;

	/* Step4: 完善指数的股票池，新增列: type+stock_code*/
	DATA tt_info;
		SET tt_info(keep = end_date add_bm_weight rename = (add_bm_weight = weight));
		stock_code = "&index_code.";
		&mark. = 1; 
		IF weight > 0;
	RUN;

	DATA &output_stock_pool.;
		SET tt_ind_pool tt_info;
/*		IF weight > 0;*/
	RUN;
	
	PROC SORT DATA = &output_stock_pool.;
		BY end_date descending &mark. stock_code;
	RUN;

	PROC SQL;
		DROP TABLE tmp,tt_info, tt_ind_pool;
	QUIT;

%MEND fill_in_index;


/** 模块6: 把指数替换为对应成分股 (原始函数在 "组合构建_绩效分析_2.0.sas"中。暂时不用该模块)**/

