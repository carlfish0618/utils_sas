/*** 测试因子有效性 **/

/**** 函数列表:
	(1) cal_intval_return: 计算单区间或累计区间收益
	****/ 


/*** 模块1: 计算单区间或累计区间收益**/
/** 要求：start_intval <= end_intval */
/** 输入:
(1) raw_table(交易日列表): 包含end_date, &group_name, &price_name及其他
(2) group_name(character): 可以是stock_code/indus_code等，任何标识主体的列名
(3) price_name: 收盘点位
(4) is_single: 单区间或累计区间(1-单区间 / 0-累计区间)
(5) date_table: end_date/date_i(f/b)（不允许包含其他列） --> 函数get_date_windows的运行结果
**/

/**　输出：
(1) output_table: end_date/group_name/ret_(f/b)_i
另外：如果end_date未出现在date_table中，则匹配结果缺失。
**/


%MACRO cal_intval_return(raw_table, group_name, price_name, date_table, output_table, is_single = 1);

	/** Step1: 取所需的点位数据 */
	PROC SQL;
		CREATE TABLE indus_close_sub AS
		SELECT end_date, &group_name., &price_name.
		FROM &raw_table.
		WHERE end_date IN
		(SELECT end_date FROM &date_table.)
		ORDER BY &group_name., end_date;
	QUIT;

	/** Step2: 获取date_table变量名 */
	PROC CONTENTS DATA = &date_table. OUT = var_list(keep = name) NOPRINT;
	RUN;
	DATA var_list;
		SET var_list;
		IF upcase(name) NOT IN ("END_DATE");
	RUN;
	PROC SQL NOPRINT;
		SELECT name, count(1) 
		INTO :name_list SEPARATED BY ' ',
			 :nvars
		FROM var_list
		ORDER BY name;
	QUIT;
	
	/* 循环计算累计收益率 */
	%IF %SYSEVALF(&is_single. = 0) %THEN %DO;
		%DO index = 1 %TO &nvars.;
			%LET var_i = %scan(&name_list., &index., ' ');
			%LET var_i = %substr(&var_i,%length(date_)+1,%length(&var_i)-%length(date_));
			%IF %sysevalf(&var_i. ~= f0)  %THEN %DO;
			PROC SQL;
				CREATE TABLE tt_accum AS
				SELECT A.end_date, A.&group_name., A.&price_name., C.&price_name. AS &price_name._&var_i. LABEL "&price_name._&var_i."
				FROM indus_close_sub A LEFT JOIN &date_table. B
				ON A.end_date = B.end_date
				LEFT JOIN 
				(SELECT end_date, &group_name., &price_name. FROM indus_close_sub) C
				ON A.&group_name. = C.&group_name. AND B.date_&var_i. = C.end_date
				ORDER BY A.&group_name., A.end_date;
			QUIT;
		
			/* 计算累计收益率 */
			DATA tt_accum(drop = &price_name._&var_i.);
				SET tt_accum;
				IF not missing(&price_name.) AND not missing(&price_name._&var_i.) THEN DO; /* 不同的顺序 */
					IF substr("&var_i.",1,1) = 'f' THEN ret_&var_i. = (&price_name._&var_i.-&price_name.)/&price_name. * 100; 
					ELSE ret_&var_i. = (&price_name.-&price_name._&var_i.)/&price_name._&var_i. * 100;
				END;
			RUN;
			PROC SQL;
				CREATE TABLE tmp AS
				SELECT A.*, B.ret_&var_i.
				FROM indus_close_sub A LEFT JOIN tt_accum B
				ON A.&group_name. = B.&group_name. AND A.end_date = B.end_date;
			QUIT;
			DATA indus_close_sub;
				SET tmp;
			RUN;
			PROC SQL;
				DROP TABLE tt_accum, tmp;
			QUIT;
		%END;
		%END;
	%END;
	
	
	%IF %SYSEVALF(&is_single. = 1) %THEN %DO;
		DATA tt_accum;
			SET indus_close_sub;
		RUN;
		%DO index = 1 %TO &nvars.;
			%LET var_i = %scan(&name_list., &index., ' ');
			%LET var_i = %substr(&var_i,%length(date_)+1,%length(&var_i)-%length(date_));
			PROC SQL;
				CREATE TABLE tmp AS
				SELECT A.*, C.&price_name. AS &price_name._&var_i. LABEL "&price_name._&var_i."
				FROM tt_accum A LEFT JOIN &date_table. B
				ON A.end_date = B.end_date
				LEFT JOIN 
				(SELECT end_date, &group_name., &price_name. FROM indus_close_sub) C
				ON A.&group_name. = C.&group_name. AND B.date_&var_i. = C.end_date
				ORDER BY A.&group_name., A.end_date;
			QUIT;
			DATA tt_accum;
				SET tmp;
			RUN;
		%END;
		/* 计算单期收益率 */
		%DO index = 2 %TO &nvars.;  /* 从第二个开始 */
			%LET var_i = %scan(&name_list., &index., ' ');
			%LET var_i_b = %scan(&name_list., &index.-1, ' ');
			%LET var_i = %substr(&var_i,%length(date_)+1,%length(&var_i.)-%length(date_));
			%LET var_i_b = %substr(&var_i_b,%length(date_)+1,%length(&var_i_b.)-%length(date_));
/*			%put &var_i.;*/
/*			%put &var_i_b.;*/
			%IF %substr(&var_i.,1,1) = f AND %substr(&var_i_b.,1,1) = f %THEN %DO;
				%LET single_name =ret_&var_i; 
				DATA tt_accum;
					SET tt_accum;
					ret_&var_i = (&price_name._&var_i.-&price_name._&var_i_b.)/&price_name._&var_i_b. * 100; 
				RUN;
			%END;
			%ELSE %IF %substr(&var_i.,1,1) = b AND %substr(&var_i_b.,1,1) = b %THEN %DO;  /** 对应的如：date_b2,date_b1 */
				%LET single_name =ret_&var_i; 
				DATA tt_accum;
					SET tt_accum;
					ret_&var_i. = (&price_name._&var_i_b.-&price_name._&var_i.)/&price_name._&var_i. * 100;
				RUN;
			%END;
			%ELSE %IF %substr(&var_i.,1,1) = f AND %substr(&var_i_b.,1,1) = b %THEN %DO;/** 对应的是: date_f0, date_b(*) */
				%LET single_name = ret_b1; 
				DATA tt_accum;
					SET tt_accum;
					ret_b1 = (&price_name._f0-&price_name._b1)/&price_name._b1 * 100;
				RUN;
			%END;
			PROC SQL;
				CREATE TABLE tmp AS
				SELECT A.*, B.&single_name.
				FROM indus_close_sub A LEFT JOIN tt_accum B
				ON A.&group_name. = B.&group_name. AND A.end_date = B.end_date;
			QUIT;
			DATA indus_close_sub;
				SET tmp;
			RUN;
		%END;
		PROC SQL;
			DROP TABLE tt_accum, tmp;
		QUIT;

	%END;
	DATA &output_table.;
		SET indus_close_sub;
	RUN;
	PROC SQL;
		DROP TABLE indus_close_sub, var_list;
	QUIT;

%MEND;


/*** 模块2: 计算单因子的IC和胜率**/
/** 输入:
(1) factor_table(交易日列表): 包含end_date, group_name, factor_name
(2) return_table(收益率表): 包含end_date, group_name及收益率列 ---> cal_intval_return()的结果
(3) group_name(character): 可以是stock_code/indus_code等，任何标识主体的列名
(4) fname: 收盘点位
**/

/**　输出：
(1) &fname._stat: end_date/n_obs_f1/p_ic_f1/s_ic_f1 (日期列表以factor_table中的end_date为准)
**/

%MACRO single_factor_ic(factor_table, return_table, group_name, fname, output_table);
	DATA &fname._t;
		SET &factor_table.(keep = end_date &group_name. &fname.);
		IF not missing(&fname.);
	RUN;
	PROC SQL;
		CREATE TABLE &fname._stat AS
		SELECT distinct end_date 
		FROM &fname._t
		ORDER BY end_date;
	QUIT;

	/* 循环针对未来N个月的累计或单月收益率 */
	PROC CONTENTS DATA = &return_table. OUT = var_list(keep = name) NOPRINT;
	RUN;
	DATA var_list;
		SET var_list;
		IF index(name, "ret") = 1;
	RUN;
	PROC SQL NOPRINT;
		SELECT name, count(1) 
		INTO :name_list SEPARATED BY ' ',
			 :nvars
		FROM var_list;
	QUIT;
	
	%DO index = 1 %TO &nvars.;
		%LET var_i = %scan(&name_list., &index., ' ');
		%LET var_i = %substr(&var_i.,%length(ret_)+1,%length(&var_i.)-%length(ret_));
		PROC SQL;
			CREATE TABLE tmp AS
			SELECT A.*, B.ret_&var_i. AS &var_i.
			FROM &fname._t A JOIN &return_table. B  /** 取交集，只考虑那些同时出现在得分表和因子表中的记录，但允许他们有缺失 */
			ON A.&group_name. = B.&group_name. AND A.end_date = B.end_date
			ORDER BY A.end_date, A.&fname.;
		QUIT;

		/* 计算相关系数，去除缺失值 */
		DATA &fname._t;
			SET tmp;
			IF not missing(&var_i.) AND not missing(&fname.);
		RUN;
		PROC CORR DATA = &fname._t pearson spearman OUTP = corr_p OUTS = corr_s NOPRINT;
			BY end_date;
			VAR &fname. &var_i.;
		RUN;
		/** pearson */
		DATA corr_p(keep = end_date _TYPE_ &var_i.);
			SET corr_p;
			IF upcase(_NAME_) = upcase("&fname.") OR _TYPE_ = "N";
		RUN;
		PROC TRANSPOSE DATA = corr_p OUT = corr_p(keep = end_date corr N rename = (N = nobs_&var_i. corr = p_ic_&var_i.));
			BY end_date;
			VAR &var_i.;
			ID  _TYPE_;
		RUN;
		/* spearman */
		DATA corr_s(keep = end_date _TYPE_ &var_i.);
			SET corr_s;
			IF upcase(_NAME_) = upcase("&fname.") OR _TYPE_ = "N";
		RUN;
		PROC TRANSPOSE DATA = corr_s OUT = corr_s(keep = end_date corr N rename = (N = nobs_&var_i. corr = s_ic_&var_i.));
			BY end_date;
			VAR &var_i.;
			ID  _TYPE_;
		RUN;

		/* 进行汇总 */
		PROC SQL;
			CREATE TABLE tmp AS
			SELECT A.*, B.nobs_&var_i., B.p_ic_&var_i., C.s_ic_&var_i.
			FROM  &fname._stat A 
			LEFT JOIN corr_p B
			ON A.end_date = B.end_date
			LEFT JOIN corr_s C
			ON A.end_date = C.end_date
			ORDER BY A.end_date;
		QUIT;
		DATA &fname._stat;
			SET tmp;
		RUN;
	%END;

	PROC SQL;
		DROP TABLE tmp, corr_p, corr_s, &fname._t, var_list;
	QUIT;
%MEND single_factor_ic;


/*** 模块3: 计算多个因子的IC和胜率**/
/** 需调用：single_factor_ic() 函数 */

/** 输入:
(1) factor_table(交易日列表): 包含end_date, group_name, factor_name
(2) return_table(收益率表): 包含end_date, group_name及收益率列 ---> cal_intval_return()的结果
(3) group_name(character): 可以是stock_code/indus_code等，任何标识主体的列名
(4) fname: 收盘点位
**/

/**　输出(每个因子)：
(1) &fname._stat: end_date/n_obs_f1/p_ic_f1/s_ic_f1
**/


%MACRO loop_factor_ic(factor_table, return_table, group_name);
	/** 生成因子列表 */
	PROC CONTENTS DATA = &factor_table. OUT = factor_list(keep = name) NOPRINT;
	RUN;
	%LET group_name = %upcase(&group_name.);
	DATA factor_list;
		SET factor_list;
		IF upcase(name) not IN ("END_DATE", "&group_name." );  /* 注意大小写 */
	RUN;
	PROC SQL NOPRINT;   /** 相互调用的关系，不要与single_factor_ic中的宏有重复 */
		SELECT name, count(1) 
		INTO :factor_list SEPARATED BY ' ',
			 :nfactors
		FROM factor_list;
	QUIT;
			
	%DO i = 1 %TO &nfactors.;
		%LET fname =  %scan(&factor_list.,&i., ' ');
		%single_factor_ic(&factor_table., &return_table., &group_name., &fname.);
	%END;
	PROC SQL;
		DROP TABLE factor_list;
	QUIT;
		
%MEND;

