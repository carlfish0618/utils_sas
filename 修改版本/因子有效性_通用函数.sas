/*** 测试因子有效性 **/

/**** 函数列表:
	(1) cal_intval_return: 计算单区间或累计区间收益
	(2) single_factor_ic: 计算单因子的IC和胜率
	(3) loop_factor_ic: 计算多个因子的IC和胜率
	(4) single_factor_score: 根据因子得分，排序并打分
	(5) single_score_ret： 根据分组统计收益
	(6) test_single_factor_ic: 在single_factor_ic基础上引入cover，同时仅计算spearman_ic，并默认输出
	(7) test_multiple_factor_ic: 与loog_factor_ic相似，但此处调用test_single_factor_ic 
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

/** 注：要求至少date_table中需要包含date_f0!!! 即get_date_windows中设定的start_win <=0并且end_win>=0 **/

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
					IF substr("&var_i.",1,1) = 'f' AND &price_name. ~= 0 THEN DO;
						ret_&var_i. = (&price_name._&var_i.-&price_name.)/&price_name. * 100; 
					END;
					ELSE IF substr("&var_i.",1,1) = 'f' THEN ret_&var_i. = .;
					ELSE IF &price_name._&var_i. ~= 0 THEN ret_&var_i. = (&price_name.-&price_name._&var_i.)/&price_name._&var_i. * 100;
					ELSE ret_&var_i. = .;
				END;
				ELSE ret_&var_i. = .;
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
			%LET var_i = %substr(&var_i., %length(date_)+1,%length(&var_i.)-%length(date_));
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
		%DO index = 1 %TO &nvars.;
			%LET var_i = %scan(&name_list., &index., ' ');
			%LET var_i = %substr(&var_i., %length(date_)+1,%length(&var_i.)-%length(date_));

			%IF &var_i. = b1 %THEN %DO;
				%put 1!!;
				%LET single_name = ret_b1;
				%put &single_name.;
				DATA tt_accum;
					SET tt_accum;
					IF &price_name._b1 ~=0 AND not missing(&price_name._b1) AND not missing(&price_name._f0) THEN DO;
						&single_name. = (&price_name._f0-&price_name._b1)/&price_name._b1 * 100;
					END;
					ELSE &single_name. = .;
				RUN;
			%END;
			%ELSE %IF %substr(&var_i.,1,1) = b %THEN %DO; 
				%put 2!!;
				%LET single_name =ret_&var_i; 
			    %put &single_name.;

				%LET number = %substr(&var_i.,2,%length(&var_i.)-1);
				%LET number_b = %sysevalf(&number.-1);
				DATA tt_accum;
					SET tt_accum;
					IF &price_name._&var_i.~=0 AND not missing(&price_name._&var_i.) AND not missing(&price_name._b&number_b.) THEN DO;
						&single_name. = (&price_name._b&number_b.-&price_name._&var_i.)/&price_name._&var_i. * 100;
					END;
					ELSE &single_name. = .;
				RUN;
			%END;
			%ELSE %IF %substr(&var_i.,1,1) = f AND &var_i. ~= f0 %THEN %DO;
				%put 3!!;
				%LET single_name =ret_&var_i; 
				%put &single_name.;
				%LET number = %substr(&var_i.,2,%length(&var_i.)-1);
				%LET number_b = %sysevalf(&number.-1);
				DATA tt_accum;
					SET tt_accum;
					IF &price_name._f&number_b.~=0 AND not missing(&price_name._&var_i.) AND not missing(&price_name._f&number_b.) THEN DO; 
						&single_name. = (&price_name._&var_i.-&price_name._f&number_b.)/&price_name._f&number_b. * 100; 
					END;
					ELSE &single_name. = .;
				RUN;
			%END;
			%IF &var_i. ~= f0 %THEN %DO; /** f0时候应跨过该循环 */
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
(4) fname: 因子名称
(5) type：控制输出数据，1- 全部输出 2- p_ic 3- s_ic 4- n_obs
**/

/**　输出：
(1) output_table: end_date/n_obs_f1/p_ic_f1/s_ic_f1 (日期列表以factor_table中的end_date为准)
**/

%MACRO single_factor_ic(factor_table, return_table, group_name, fname, output_table, type=1);
	DATA &fname._t_raw;
		SET &factor_table.(keep = end_date &group_name. &fname.);
		IF not missing(&fname.);
	RUN;
	/** 修改：2016-1-15，要求end_date要在return_table的end_date中。这样就不用要求factor_table和return_table频率需要一致*/
	PROC SQL;
		CREATE TABLE &output_table. AS
		SELECT distinct end_date 
		FROM &fname._t_raw
		WHERE end_date IN (SELECT end_date FROM &return_table.)
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
			FROM &fname._t_raw A JOIN &return_table. B  /** 取交集，只考虑那些同时出现在因子表和收益表中的记录，但允许他们有缺失 */
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
		%IF %SYSEVALF(&type. = 1) %THEN %DO;
			PROC SQL;
				CREATE TABLE tmp AS
				SELECT A.*, B.nobs_&var_i., B.p_ic_&var_i., C.s_ic_&var_i.
				FROM  &output_table. A 
				LEFT JOIN corr_p B
				ON A.end_date = B.end_date
				LEFT JOIN corr_s C
				ON A.end_date = C.end_date
				ORDER BY A.end_date;
			QUIT;
			DATA &output_table.;
				SET tmp;
			RUN;
		%END;
		%IF %SYSEVALF(&type. = 2) %THEN %DO;
			PROC SQL;
				CREATE TABLE tmp AS
				SELECT A.*,  B.p_ic_&var_i.
				FROM  &output_table. A 
				LEFT JOIN corr_p B
				ON A.end_date = B.end_date
				ORDER BY A.end_date;
			QUIT;
			DATA &output_table.;
				SET tmp;
			RUN;
		%END;
		%IF %SYSEVALF(&type. = 3) %THEN %DO;
			PROC SQL;
				CREATE TABLE tmp AS
				SELECT A.*, C.s_ic_&var_i.
				FROM  &output_table. A 
				LEFT JOIN corr_s C
				ON A.end_date = C.end_date
				ORDER BY A.end_date;
			QUIT;
			DATA &output_table.;
				SET tmp;
			RUN;
		%END;
		%IF %SYSEVALF(&type. = 4) %THEN %DO;
			PROC SQL;
				CREATE TABLE tmp AS
				SELECT A.*, B.nobs_&var_i.
				FROM  &output_table. A 
				LEFT JOIN corr_p B
				ON A.end_date = B.end_date
				ORDER BY A.end_date;
			QUIT;
			DATA &output_table.;
				SET tmp;
			RUN;
		%END;
	%END;
	PROC SQL;
		DROP TABLE tmp, corr_p, corr_s, &fname._t, var_list,&fname._t_raw;
	QUIT;
%MEND single_factor_ic;


/*** 模块3: 计算多个因子的IC和胜率**/
/** 需调用：single_factor_ic() 函数 */

/** 输入:
(1) factor_table(交易日列表): 包含end_date, group_name, factor_name
(2) return_table(收益率表): 包含end_date, group_name及收益率列 ---> cal_intval_return()的结果
(3) group_name(character): 可以是stock_code/indus_code等，任何标识主体的列名
(4) fname: 因子名称
(5) type: 控制输出数据，1- 全部输出 2- p_ic 3- s_ic 4- n_obs
(6) exclude_list: 要求大写，即剔除不测试的列
**/

/**　输出(每个因子)：
(1) &fname._ic: end_date/n_obs_f1/p_ic_f1/s_ic_f1
**/


%MACRO loop_factor_ic(factor_table, return_table, group_name, type=1, exclude_list=(''));
	/** 生成因子列表 */
	PROC CONTENTS DATA = &factor_table. OUT = factor_list(keep = name) NOPRINT;
	RUN;
	%LET group_name = %upcase(&group_name.);
	DATA factor_list;
		SET factor_list;
		IF upcase(name) not IN ("END_DATE", "&group_name." );  /* 注意大小写 */
		IF upcase(name) NOT IN &exclude_list.;
	RUN;
	PROC SQL NOPRINT;   /** 相互调用的关系，不要与single_factor_ic中的宏有重复 */
		SELECT name, count(1) 
		INTO :factor_list SEPARATED BY ' ',
			 :nfactors
		FROM factor_list;
	QUIT;
			
	%DO i = 1 %TO &nfactors.;
		%LET fname =  %scan(&factor_list.,&i., ' ');
		%single_factor_ic(&factor_table., &return_table., &group_name., &fname.,&fname._ic, type=&type.);
	%END;
	PROC SQL;
		DROP TABLE factor_list;
	QUIT;
		
%MEND;


/*** 模块4：根据因子得分，排序并打分 */
/** 输入:
(1) raw_table(交易日列表): 包含end_date, &identity, &factor_name及其他
(2) identity(character): 可以是stock_code/indus_code等，任何标识主体的列名
(3) factor_name: 因子
(4) is_increase: 1-升序 0-降序
(5) group_num: 分组的个数

**/

/**　输出：
(1) output_table: 原有的raw_table中加一列 &factor_name._score
**/

%MACRO single_factor_score(raw_table, identity, factor_name,output_table, is_increase = 1, group_num = 5);
	
	/** 统计每天的样本量 */
	PROC SQL;
		CREATE TABLE tt_rank AS
		SELECT end_date, count(1) AS nobs
		FROM &raw_table.
		WHERE not missing(&factor_name.)   /** 把没有因子值的剔除 */
		GROUP BY end_date;
	QUIT;
	DATA tt_rank;
		SET tt_rank;
		part = floor(nobs/&group_num.);
		res = nobs - part * &group_num.;
	RUN;
	
	%IF %SYSEVALF(&is_increase.=1) %THEN %DO;
		PROC SQL;
		CREATE TABLE tt_raw_table AS
			SELECT A.end_date, A.&identity., A.&factor_name., B.part, B.res
			FROM &raw_table. A LEFT JOIN tt_rank B
		 	ON A.end_date = B.end_date
			WHERE not missing(&factor_name.)
			ORDER BY A.end_date, A.&factor_name.;
		QUIT;
	%END;
	%ELSE %DO;
		PROC SQL;
		CREATE TABLE tt_raw_table AS
			SELECT A.end_date, A.&identity., A.&factor_name., B.part, B.res
			FROM &raw_table. A LEFT JOIN tt_rank B
		 	ON A.end_date = B.end_date
			WHERE not missing(&factor_name.)
			ORDER BY A.end_date, A.&factor_name. desc;
		QUIT;
	%END;
			
	DATA tt_raw_table;
		SET tt_raw_table;
		BY end_date;
		RETAIN group 1;
		RETAIN within_rank 1;
		IF first.end_date THEN DO;
			group = 1;
			within_rank = 1;
		END;
		rank = within_rank;
		score = group;
		/** 判断明天分组情况 */
		IF score <= res THEN DO;  /* 在前N组 */
			IF rank + 1 <= part + 1 THEN DO;
				within_rank + 1;
			END;
			ELSE DO; /* 新的一组 */
				group + 1;  
				within_rank = 1;
			END;
		END;
		ELSE DO;
			IF rank + 1 <= part THEN DO;
				within_rank + 1;
			END;
			ELSE DO;
				group + 1;
				within_rank = 1;
			END;
		END;
	RUN;
	
	%IF %SYSEVALF(&is_increase.=1) %THEN %DO;
		PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.score AS &factor_name._score
		FROM &raw_table. A LEFT JOIN tt_raw_table B
		ON A.end_date = B.end_date AND A.&identity. = B.&identity.
		ORDER BY A.end_date, A.&factor_name.;
	QUIT;
	%END;
	%ELSE %DO;
		PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.score AS &factor_name._score
		FROM &raw_table. A LEFT JOIN tt_raw_table B
		ON A.end_date = B.end_date AND A.&identity. = B.&identity.
		ORDER BY A.end_date, A.&factor_name. desc;
	QUIT;
	%END;
			

	DATA &output_table.; /** 对于因子值缺失的股票，则没有对应因子的得分 */
		SET tmp;
	RUN;
	PROC SQL;
		DROP TABLE tmp, tt_rank, tt_raw_table;
	QUIT;
%MEND single_factor_score;


/*** 模块5: 分组统计收益率**/
/** 输入:
(1) score_table(交易日列表): 包含end_date, identity, score_name
(2) return_table(收益率表): 包含end_date, identity及收益率列 ---> cal_intval_return()的结果
(3) identity(character): 可以是stock_code/indus_code等，任何标识主体的列名
(4) score_name: 排序的得分名称
(5) type：控制输出数据，1- 全部输出 2- ret only 3- n_obs only 4-std only
(6) ret_column: 缺失(.)-针对所有的return table中的ret列。也可以指定为某一列。
(7) is_transpose: 是否按照分组，重排结果。（只有当ret_column为特定一列的时候，该参数才有效)
	
**/

/**　输出：
(1) &score_name._ret: end_date/n_obs_f1/ret_f1(日期列表以score_table中的end_date为准)
**/

%MACRO single_score_ret(score_table, return_table, identity, score_name, ret_column =., is_transpose = 0, type=2);
	DATA &score_name._t;
		SET &score_table.(keep = end_date &identity. &score_name.);
		IF not missing(&score_name.);  /* 没有因子得分的，不进行之后分组收益率的计算 */
	RUN;


	/* 循环针对未来N个月的累计或单月收益率 */
	PROC CONTENTS DATA = &return_table. OUT = var_list(keep = name) NOPRINT;
	RUN;
	%IF %SYSEVALF(&ret_column. = .) %THEN %DO;
		DATA var_list;
			SET var_list;
			IF index(name, "ret") = 1;
		RUN;
	%END;
	%ELSE %DO;
		DATA var_list;
			SET var_list;
			IF upcase(name) = upcase("&ret_column.");
		RUN;
	%END;
		
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
			CREATE TABLE tt_result AS
			SELECT A.end_date, A.&score_name., mean(B.ret_&var_i.) AS &var_i., 
				std(B.ret_&var_i.) AS std_&var_i., 
				count(1) AS nobs_&var_i.
			FROM &score_name._t A JOIN &return_table. B  /** 取交集，只考虑那些同时出现在得分表和收益表中的记录，但允许他们有缺失 */
			ON A.&identity. = B.&identity. AND A.end_date = B.end_date
			GROUP BY A.end_date, A.&score_name.
			ORDER BY A.end_date, A.&score_name.;
		QUIT;
		%IF %SYSEVALF(&index. = 1) %THEN %DO;
				PROC SQL;
					CREATE TABLE &score_name._stat AS
					SELECT distinct end_date, &score_name.
					FROM &score_name._t
					ORDER BY end_date, &score_name.;
				QUIT;
		%END;

		/* 进行汇总 */
		%IF %SYSEVALF(&type. = 1) %THEN %DO;
			PROC SQL;
				CREATE TABLE tmp AS
				SELECT A.*, B.nobs_&var_i., B.&var_i., B.std_&var_i.
				FROM  &score_name._stat A
				LEFT JOIN tt_result B
				ON A.end_date = B.end_date AND A.&score_name. = B.&score_name.
				ORDER BY A.end_date;
			QUIT;
			DATA &score_name._stat;
				SET tmp;
			RUN;
		%END;
		%IF %SYSEVALF(&type. = 2) %THEN %DO;
			PROC SQL;
				CREATE TABLE tmp AS
				SELECT A.*, B.&var_i.
				FROM  &score_name._stat A
				LEFT JOIN tt_result B
				ON A.end_date = B.end_date AND A.&score_name. = B.&score_name.
				ORDER BY A.end_date;
			QUIT;
			DATA &score_name._stat;
				SET tmp;
			RUN;
		%END;
		%IF %SYSEVALF(&type. = 3) %THEN %DO;
			PROC SQL;
				CREATE TABLE tmp AS
				SELECT A.*, B.nobs_&var_i.
				FROM  &score_name._stat A
				LEFT JOIN tt_result B
				ON A.end_date = B.end_date AND A.&score_name. = B.&score_name.
				ORDER BY A.end_date;
			QUIT;
			DATA &score_name._stat;
				SET tmp;
			RUN;
		%END;
		%IF %SYSEVALF(&type. = 4) %THEN %DO;
			PROC SQL;
				CREATE TABLE tmp AS
				SELECT A.*, B.std_&var_i.
				FROM  &score_name._stat A
				LEFT JOIN tt_result B
				ON A.end_date = B.end_date AND A.&score_name. = B.&score_name.
				ORDER BY A.end_date;
			QUIT;
			DATA &score_name._stat;
				SET tmp;
			RUN;
		%END;
	%END;
	%IF %SYSEVALF(&ret_column. ~= . AND &is_transpose. = 1) %THEN %DO;
		PROC TRANSPOSE DATA = &score_name._stat prefix = g OUT =  &score_name._stat;
			BY end_date;
			ID &score_name.;
		RUN;
	%END;

	PROC SQL;
		DROP TABLE tmp,&score_name._t, tt_result, var_list;
	QUIT;
%MEND single_score_ret;


/*** 模块6：利用single_factor_ic计算spearman_ic，并同时计算cover */
/** 默认输出：
(1) &fname._cover
(2) &fname._ic
（3） &fname._dist
**/



%MACRO test_single_factor_ic(factor_table, return_table, group_name, fname);
	/** 1- 因子覆盖度 */
	DATA tt_test_pool(keep = end_date stock_code &fname.);
		SET &factor_table.;
	RUN;
	PROC SQL;
		CREATE TABLE &fname._cover AS
		SELECT end_date, sum(not missing(&fname.))/count(1) AS pct
		FROM tt_test_pool
		GROUP BY end_date;
	QUIT;

	/** Step2: 因子IC **/
	%single_factor_ic(factor_table=tt_test_pool, return_table=&return_table., group_name=&group_name., 
			fname=&fname., output_table=&fname._ic, type=3);

	/** Step3: 因子分布情况 */
	%cal_dist(input_table=tt_test_pool, by_var=end_date, cal_var=&fname., out_table=stat);
	PROC SQL;
		CREATE TABLE &fname._dist AS
		SELECT sum(obs) AS nobs,
		mean(mean) AS mean,
		mean(std) AS std,
		mean(p100) AS p100,
		mean(p90) AS p90,
		mean(p75) AS p75,
		mean(p50) AS p50,
		mean(p25) AS p25,
		mean(p10) AS p10,
		mean(p0) AS p0
	FROM stat;
	QUIT;

	PROC SQL;
		DROP TABLE tt_test_pool, stat;
	QUIT;
%MEND test_single_factor_ic;

/*** 模块7：调用test_factor_ic，而非single_factor_ic分析多个因子*/
%MACRO 	test_multiple_factor_ic(factor_table, return_table, group_name, exclude_list=(''));
	PROC CONTENTS DATA = &factor_table. OUT = tt_varlist2(keep = name) NOPRINT;
	RUN;
	DATA tt_varlist2;
		SET tt_varlist2;
		IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE");
		IF upcase(name) NOT IN &exclude_list.;
	RUN;
	
	PROC SQL NOPRINT;
		SELECT name, count(1)
		 INTO :name_list2 SEPARATED BY ' ',
               :nfactors2
          FROM tt_varlist2;
     QUIT;
           
     %DO i = 1 %TO &nfactors2.;
          %LET fname =  %scan(&name_list2.,&i., ' ');
		  %test_single_factor_ic(factor_table=&factor_table., return_table=&return_table., 
								group_name=&group_name., fname=&fname.);
     %END;
	PROC SQL;
		DROP TABLE tt_varlist2;
	QUIT;
%MEND test_multiple_factor_ic;

