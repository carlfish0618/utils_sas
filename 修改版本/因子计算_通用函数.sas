/*** 因子计算 **/

/**** 函数列表:
	(1) cal_percentitle: 计算某一列得分的分位点
	(2) normalize_single_score: 标准化单因子得分
	(3) normalize_multi_score: 标准化多个因子得分
	(4) winsorize_single_score: winsorize单因子得分
	(5) winsorize_multi_score: winsorize多个因子得分
	(6) weighted_single_factor: 计算单个因子加权得分
	(7) weighted_multi_factor: 计算多个因子加权得分
	(8) neutralize_single_score：中性化因子得分
	(9) neutralize_multi_score: 中性化多个因子得分
	(10) cut_subset: 根据因子得分，取某个分位点或绝对值下的分组
	****/ 

/** 注意：因子计算一般有两类步骤
(1) normalize --〉winsorize
(2) normalize --> neutralize --> winsorize (即在进行行业中性化时，先不用对normaliz的结果进行winsorize，而是留待最后 
另外，normalize和neutralize一般都选用fmv_sqr作为权重计算sample average，但前者计算z-score,且标准差是"等权"标准差（所以normalize之后的和并不会为0)。
后者计算的是deviation(即与行业均值的偏离)，而不是z-score，类似的，因为均值计算方法的原因后者的和也不会为0。
*****************/


/*** 模块1: 计算某一列得分的分位点 **/
/** 输入:
(1) input_table: end_date / stock_code / &colname.
(2) colname(character): 列名
(3) pct: 需要计算的百分位点(如: 50/90等)

/**　输出：
(1) output_table: end_date, pct&pct.
**/

/** 说明: 方法说明在: http://en.wikipedia.org/wiki/Percentile 中Excel的方法 **/

%MACRO cal_percentitle(input_table, colname, pct, output_table);
	PROC SORT DATA = &input_table.;
		BY end_date &colname.;
	RUN;
	/** Step1: 计算每天非空的样本数 */
	PROC SQL;
		CREATE TABLE tt_intval AS
		SELECT end_date, count(1) AS nobs
		FROM &input_table.
		WHERE not missing(&colname.)
		GROUP BY end_date;
	QUIT;
	DATA tt_intval;
		SET tt_intval;
		nn = &pct./100 * (nobs-1)+1;
		nk = floor(nn);
		nd = nn - nk;
	RUN;
	/** Step2: 给原始数据排序 */
	DATA tt_nomissing;
		SET &input_table.(keep = end_date stock_code &colname.);
		IF not missing(&colname.);
	RUN;
	PROC SORT DATA = tt_nomissing;
		BY end_Date &colname.;
	RUN;
	DATA tt_nomissing;
		SET tt_nomissing;
		BY end_Date;
		RETAIN rank 0;
		IF first.end_date THEN rank = 0;
		rank + 1;
	RUN;
	PROC SQL;
		CREATE TABLE tt_intval2 AS
		SELECT A.*, B.&colname. AS v_1, C.&colname. AS v_n, D.&colname. AS v_k, E.&colname. AS v_k1
		FROM tt_intval A LEFT JOIN tt_nomissing B
		ON A.end_date = B.end_date AND B.rank = 1
		LEFT JOIN tt_nomissing C
		ON A.end_Date = C.end_date AND C.rank = A.nobs
		LEFT JOIN tt_nomissing D
		ON A.end_Date = D.end_Date AND D.rank = A.nk
		LEFT JOIN tt_nomissing E
		ON A.end_date = E.end_date AND E.rank = A.nk+1;
	QUIT;
	DATA tt_intval(keep = end_date pct&pct.);
		SET tt_intval2;
		IF nk = 0 THEN pct&pct. = v_1;
		ELSE IF nk = nobs THEN pct&pct. = v_n;
		ELSE pct&pct. = v_k + nd*(v_k1-v_k);
	RUN;
	DATA &output_table.;
		SET tt_intval;
	RUN;
	PROC SQL;
		DROP TABLE tt_intval, tt_intval2, tt_nomissing;
	QUIT;

%MEND cal_percentitle;


/*** 模块2: 标准化单因子得分 **/
/** 输入:
(1) input_table: end_date / stock_code / &colname./fmv_sqr (流通市值的平方根)
(2) colname(character): 因子名
(3) is_replace: 是否将原始因子替换为标准化后的结果。1-替换 0- 生成新列:&colname._mdf

/**　输出：
(1) output_table: input_table + &colname._mdf(is_replace=0)
**/

%MACRO normalize_single_score(input_table, colname, output_table, is_replace = 1);
	%cal_percentitle(&input_table., &colname., 1, pct1);
	%cal_percentitle(&input_table., &colname., 99, pct99);
	PROC SQL;
		CREATE TABLE tt_raw AS
		SELECT A.end_date, A.stock_code, A.&colname., A.fmv_sqr, B.pct1, C.pct99
		FROM &input_table. A LEFT JOIN pct1 B
		ON A.end_date = B.end_date
		LEFT JOIN pct99 C
		ON A.end_Date = C.end_date
		ORDER BY A.end_date;
	QUIT;
	/* winsorize */
	DATA tt_raw;
		SET tt_raw;
		IF not missing(&colname.) AND &colname. > pct99 THEN &colname._mdf = pct99;
		ELSE IF not missing(&colname.) AND &colname. < pct1 THEN &colname._mdf = pct1;
		ELSE IF not missing(&colname.) THEN &colname._mdf = &colname.;
		ELSE &colname._mdf = .;
	RUN;
	/* normalized */
	PROC SQL;
		CREATE TABLE tmp_std AS
		SELECT end_date, std(&colname._mdf) AS std, sum(&colname._mdf * fmv_sqr) AS sum_wt
		FROM tt_raw
		GROUP BY end_date;
	QUIT;
	PROC SQL;
		CREATE TABLE tmp_fmv AS
		SELECT end_date, sum(fmv_sqr) AS sum_fmv
		FROM tt_raw
		WHERE not missing(&colname._mdf) AND not missing(fmv_sqr)
		GROUP BY end_Date;
	QUIT;
	PROC SQL;
		CREATE TABLE tmp_normal AS
		SELECT A.end_date, A.stock_code, A.&colname._mdf, B.std, B.sum_wt, C.sum_fmv
		FROM tt_raw A LEFT JOIN tmp_std B
		ON A.end_date = B.end_date
		LEFT JOIN tmp_fmv C
		ON A.end_date = C.end_date
		ORDER BY A.end_date, A.stock_code;
	QUIT;

	DATA tmp_normal(drop = std sum_wt sum_fmv);
		SET tmp_normal;
		IF not missing(&colname._mdf) THEN &colname._mdf = (&colname._mdf - sum_wt/sum_fmv)/std;
	RUN;
	%IF %SYSEVALF(&is_replace.) %THEN %DO;   /* 替换初始值 */
		DATA tmp_normal(drop = &colname._mdf);
			SET tmp_normal;
			&colname = &colname._mdf;
		RUN;
	%END;
	PROC SORT DATA = &input_table.;
		BY end_date stock_code;
	RUN;
	PROC SORT DATA = tmp_normal;
		BY end_date stock_code;
	RUN;
	DATA &output_table.;
		UPDATE &input_table. tmp_normal;
		BY end_date stock_code;
	RUN;
	PROC SQL;
		DROP TABLE tt_raw, tmp_std, tmp_fmv, tmp_normal, pct1, pct99;
	QUIT;
%MEND  normalize_single_score;


/*** 模块3: 标准化多个因子得分 **/
/** 输入:
(1) input_table: end_date / stock_code / fmv_sqr (流通市值的平方根) / 其余各列都视为因子得分
(2) exclude_list: 剔除非因子的列，如("TOT", "F1")等。记得需要带括号。
(3) type: 1-exclude_list生效，2-include_list生效, 3-二者都生效 4-二者都无效

/**　输出：
(1) output_table: 因子列都由标准化后得分替换。
**/

%MACRO 	normalize_multi_score(input_table, output_table, exclude_list=(), include_list=(), type=1);
	DATA rr_result;
		SET &input_table.;
	RUN;

	PROC CONTENTS DATA = &input_table. OUT = tt_varlist(keep = name) NOPRINT;
	RUN;
	%IF %SYSEVALF(&type.=1) %THEN %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE","FMV_SQR");
			IF upcase(name) NOT IN &exclude_list.;
		RUN;
	%END;
	%ELSE %IF %SYSEVALF(&type.=2) %THEN %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) IN &include_list.;
		RUN;
	%END;
	%ELSE %IF %SYSEVALF(&type.=3) %THEN %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) IN &include_list. AND upcase(name) NOT IN &exclude_list.;
		RUN;
	%END;
	%ELSE %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE","FMV_SQR");
		RUN;
	%END;
		

	PROC SQL NOPRINT;
		SELECT name, count(1)
		 INTO :name_list SEPARATED BY ' ',
               :nfactors
          FROM tt_varlist;
     QUIT;
              
     %DO i = 1 %TO &nfactors.;
	 	 %put total &nfactors.;
	 	 %put NO &i. factor;
          %LET fname =  %scan(&name_list.,&i., ' ');
          %normalize_single_score(rr_result, &fname., rr_result);
		  %put ending...;
     %END;
	 DATA &output_table.;
	 	SET rr_result;
	RUN;
	PROC SQL;
		DROP TABLE rr_result, tt_varlist;
	QUIT;
%MEND normalize_multi_score;


/*** 模块4: winsorize单因子得分 **/
/** 输入:
(1) input_table: end_date / stock_code / &colname.
(2) colname(character): 因子名
(3) is_replace: 是否将原始因子替换为标准化后的结果。1-替换 0- 生成新列:&colname._mdf
(4) upper(标准化上界) 
(5) lower(标准化下界)

/**　输出：
(1) output_table: input_table + &colname._mdf(is_replace=0)
**/
%MACRO winsorize_single_score(input_table, colname, output_table, upper=3, lower = -3, is_replace = 1);
	/* winsorize */
	DATA tt_raw;
		SET &input_table.;
		IF not missing(&colname.) AND &colname. > &upper. THEN &colname._mdf = &upper.;
		ELSE IF not missing(&colname.) AND &colname. < &lower. THEN &colname._mdf = &lower.;
		ELSE IF not missing(&colname.) THEN &colname._mdf = &colname.;
		ELSE &colname._mdf = .;
	RUN;

	%IF %SYSEVALF(&is_replace.) %THEN %DO;   /* 替换初始值 */
		DATA tt_raw(drop = &colname._mdf);
			SET tt_raw;
			&colname = &colname._mdf;
		RUN;
	%END;
	PROC SORT DATA = &input_table.;
		BY end_date stock_code;
	RUN;
	PROC SORT DATA = tt_raw;
		BY end_date stock_code;
	RUN;
	DATA &output_table.;
		UPDATE &input_table. tt_raw;
		BY end_date stock_code;
	RUN;
	PROC SQL;
		DROP TABLE tt_raw;
	QUIT;
%MEND  winsorize_single_score;


/*** 模块5: winsorize多个因子得分 **/
/** 输入:
(1) input_table: end_date / stock_code / &colname.
(2) upper(标准化上界) 
(3) lower(标准化下界)
(4) exclude_list: 剔除非因子的列，如(TOT F1)等。记得需要带括号。
(5) type: 1-exclude_list生效，2-include_list生效, 3-二者都生效 4-二者都无效


/**　输出：
(1) output_table: 因子列都由winsorize后得分替换。
**/

%MACRO 	winsorize_multi_score(input_table, output_table, exclude_list=(), include_list=(), type=1, upper=3, lower = -3);
	DATA rr_result;
		SET &input_table.;
	RUN;

	PROC CONTENTS DATA = &input_table. OUT = tt_varlist(keep = name) NOPRINT;
	RUN;
	%IF %SYSEVALF(&type.=1) %THEN %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE","FMV_SQR");
			IF upcase(name) NOT IN &exclude_list.;
		RUN;
	%END;
	%ELSE %IF %SYSEVALF(&type.=2) %THEN %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) IN &include_list.;
		RUN;
	%END;
	%ELSE %IF %SYSEVALF(&type.=3) %THEN %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) IN &include_list. AND upcase(name) NOT IN &exclude_list.;
		RUN;
	%END;
	%ELSE %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE","FMV_SQR");
		RUN;
	%END;
		
	PROC SQL NOPRINT;
		SELECT name, count(1)
		 INTO :name_list SEPARATED BY ' ',
               :nfactors
          FROM tt_varlist;
     QUIT;
              
     %DO i = 1 %TO &nfactors.;
          %LET fname =  %scan(&name_list.,&i., ' ');
          %winsorize_single_score(rr_result, &fname., rr_result, upper=&upper., lower = &lower., is_replace = 1);
     %END;
	 DATA &output_table.;
	 	SET rr_result;
	RUN;
	PROC SQL;
		DROP TABLE rr_result, tt_varlist;
	QUIT;
%MEND winsorize_multi_score;



/*** 模块6: 计算因子加权得分(即: 因子值*该因子的权重) **/
/** 允许将个股分为不同的组，不同组享受不一样的因子权重。若拥有相同的权重则group_id应提前设定为单一的值 */

/** 输入:
(1) input_table: end_date / stock_code / &colname. /group_id(组别)
(2) colname(character): 因子名
(3) is_replace: 是否将原始因子替换为标准化后的结果。1-替换 0- 生成新列:&colname._mdf
(4) weight_table: end_date/ factor_name / weight(因子的权重) / group_id


/**　输出：
(1) output_table: input_table + &colname._mdf(is_replace=0)
**/
%MACRO weighted_single_factor(input_table, colname, weight_table, output_table, is_replace=1);

	PROC SQL;
		CREATE TABLE tt_date AS
		SELECT A.end_date, B.end_date AS effective_date
		FROM 
		(SELECT distinct(end_date) FROM &input_table.) A
		LEFT JOIN
		(SELECT distinct(end_date) FROM &weight_table.) B
		ON A.end_date >= B.end_date
		GROUP BY A.end_date
		HAVING B.end_date = max(B.end_date)
		ORDER BY A.end_Date;
	QUIT;
	
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.end_date, A.stock_code, A.&colname., B.weight
		FROM &input_table. A LEFT JOIN tt_date C
		ON A.end_date = C.end_date
		LEFT JOIN &weight_table. B
		ON B.end_date = C.effective_date AND upcase(A.factor_name) = upcase("&colname.") AND A.group_id = B.group_id
		ORDER BY A.end_date;
	QUIT;
	DATA tmp(drop = weight);
		SET tmp;
		IF not missing(weight) THEN &colname._mdf = weight * &colname.;
		ELSE &colname._mdf = 0;
	RUN;
	%IF %SYSEVALF(&is_replace.=1) %THEN %DO;   /* 替换初始值 */
		DATA tmp(drop = &colname._mdf);
			SET tmp;
			&colname. = &colname._mdf;
		RUN;
	%END;
	PROC SORT DATA = &input_table.;
		BY end_date stock_code;
	RUN;
	PROC SORT DATA = tmp;
		BY end_date stock_code;
	RUN;
	DATA &output_table.;
		UPDATE &input_table. tmp;
		BY end_date stock_code;
	RUN;
	PROC SQL;
		DROP TABLE tmp, tt_date;
	QUIT;
%MEND weighted_single_factor;


/*** 模块7: 计算多个因子加权得分(即: 因子值*该因子的权重) **/
/** 允许将个股分为不同的组，不同组享受不一样的因子权重。若拥有相同的权重则group_id应提前设定为单一的值 */

/** 输入:
(1) input_table: end_date / stock_code / &colname. /group_id(组别)
(2) exclude_list: 剔除非因子的列，如(TOT F1)等。记得需要带括号。
(3) weight_table: end_date/ factor_name / weight(因子的权重) / group_id
(4) type: 1-exclude_list生效，2-include_list生效, 3-二者都生效 4-二者都无效


/**　输出：
(1) output_table: input_table + &colname._mdf(is_replace=0)
**/
%MACRO weighted_multi_factor(input_table, weight_table, output_table, exclude_list=(), include_list=(), type=1);
	DATA rr_result;
		SET &input_table.;
	RUN;

	PROC CONTENTS DATA = &input_table. OUT = tt_varlist(keep = name) NOPRINT;
	RUN;
	%IF %SYSEVALF(&type.=1) %THEN %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE","FMV_SQR");
			IF upcase(name) NOT IN &exclude_list.;
		RUN;
	%END;
	%ELSE %IF %SYSEVALF(&type.=2) %THEN %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) IN &include_list.;
		RUN;
	%END;
	%ELSE %IF %SYSEVALF(&type.=3) %THEN %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) IN &include_list. AND upcase(name) NOT IN &exclude_list.;
		RUN;
	%END;
	%ELSE %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE","FMV_SQR");
		RUN;
	%END;
	PROC SQL NOPRINT;
		SELECT name, count(1)
		 INTO :name_list SEPARATED BY ' ',
               :nfactors
          FROM tt_varlist;
     QUIT;
	 
	 /** 确保weight_table中一天内，每组的所有因子和为1 */
	 PROC SQL;	
		CREATE TABLE tmp AS
		SELECT A.*, B.t_weight
		FROM &weight_table. A LEFT JOIN 
		(
		SELECT end_date, group_id, sum(weight) AS t_weight
		FROM &weight_table.
		GROUP BY end_date, group_id
		)B
		ON A.end_date = B.end_date AND A.group_id = B.group_id
	QUIT;

	DATA tt_weight(drop = t_weight);
		SET tmp;
		IF t_weight ~= 0 THEN weight = round(weight/t_weight,0.00001);  
		ELSE weight = 0;
	RUN;
              
     %DO i = 1 %TO &nfactors.;
          %LET fname =  %scan(&name_list.,&i., ' ');
          %weighted_single_factor(input_table=rr_result, colname=&fname., weight_table = tt_weight, output_table = rr_result);
     %END;
	 DATA &output_table.;
	 	SET rr_result;
	RUN;
	PROC SQL;
		DROP TABLE rr_result, tt_varlist, tt_weight;
	QUIT;
%MEND weighted_multi_factor;
	

/*** 模块7: 中性化因子得分 **/
/** 允许将个股分为不同的组，不同组内进行中性化 */
/** 如：常规因子在一级行业内做中性化。估值因子一般在二级行业做中性化 **/

/** 输入:
(1) input_table: end_date / stock_code / &colname. /fmv_sqr / group_id(组别)
(2) colname(character): 因子名
(3) group_name(character): 分组标记(中性化在分组内进行)
(4) is_replace: 是否将原始因子替换为标准化后的结果。1-替换 0- 生成新列:&colname._mdf


/**　输出：
(1) output_table: input_table + &colname._mdf(is_replace=0)
**/


%MACRO neutralize_single_score(input_table, colname, group_name, output_table, is_replace=1);

	PROC SQL;
		CREATE TABLE tmp_std AS
		SELECT end_date, &group_name., sum(&colname. * fmv_sqr) AS sum_wt
		FROM &input_table.
		GROUP BY end_date, &group_name.;
	QUIT;
	PROC SQL;
		CREATE TABLE tmp_fmv AS
		SELECT end_date, &group_name., sum(fmv_sqr) AS sum_fmv
		FROM &input_table.
		WHERE not missing(&colname.) AND not missing(fmv_sqr)
		GROUP BY end_date, &group_name.;
	QUIT;
	PROC SQL;
		CREATE TABLE tmp_normal AS
		SELECT A.end_date, A.stock_code, A.&colname.,B.sum_wt, C.sum_fmv
		FROM &input_table. A LEFT JOIN tmp_std B
		ON A.end_date = B.end_date AND A.&group_name. = B.&group_name.
		LEFT JOIN tmp_fmv C
		ON A.end_date = C.end_date AND A.&group_name. = C.&group_name.
		ORDER BY A.end_date, A.stock_code;
	QUIT;

	DATA tmp_normal(drop = sum_wt sum_fmv);
		SET tmp_normal;
		IF not missing(&colname.) THEN &colname._mdf = &colname. - sum_wt/sum_fmv;
		ELSE &colname._mdf = .;
	RUN;
	%IF %SYSEVALF(&is_replace.) %THEN %DO;   /* 替换初始值 */
		DATA tmp_normal(drop = &colname._mdf);
			SET tmp_normal;
			&colname = &colname._mdf;
		RUN;
	%END;
	PROC SORT DATA = &input_table.;
		BY end_date stock_code;
	RUN;
	PROC SORT DATA = tmp_normal;
		BY end_date stock_code;
	RUN;
	DATA &output_table.;
		UPDATE &input_table. tmp_normal;
		BY end_date stock_code;
	RUN;
	PROC SQL;
		DROP TABLE tmp_std, tmp_fmv, tmp_normal;
	QUIT;

%MEND  neutralize_single_score;


/*** 模块8: 中性化多个因子得分 **/
/** 输入:
(1) input_table: end_date / stock_code / fmv_sqr (流通市值的平方根) / 其余各列都视为因子得分
(2) exclude_list: 剔除非因子的列，如(TOT F1)等。记得需要带括号。
(3) group_name(character): 分组标记(中性化在分组内进行)
(4) type: 1-exclude_list生效，2-include_list生效, 3-二者都生效 4-二者都无效


/**　输出：
(1) output_table: 因子列都由标准化后得分替换。
**/

/** 注: exclude_list中需要把&group_name等非因子列的剔除 */
%MACRO 	neutralize_multi_score(input_table, output_table,group_name, exclude_list=(), include_list=(),type=1);
	DATA rr_result;
		SET &input_table.;
	RUN;

	PROC CONTENTS DATA = &input_table. OUT = tt_varlist(keep = name) NOPRINT;
	RUN;
	%IF %SYSEVALF(&type.=1) %THEN %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE","FMV_SQR");
			IF upcase(name) NOT IN &exclude_list.;
		RUN;
	%END;
	%ELSE %IF %SYSEVALF(&type.=2) %THEN %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) IN &include_list.;
		RUN;
	%END;
	%ELSE %IF %SYSEVALF(&type.=3) %THEN %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) IN &include_list. AND upcase(name) NOT IN &exclude_list.;
		RUN;
	%END;
	%ELSE %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE","FMV_SQR");
		RUN;
	%END;
	PROC SQL NOPRINT;
		SELECT name, count(1)
		 INTO :name_list SEPARATED BY ' ',
               :nfactors
          FROM tt_varlist;
     QUIT;
              
     %DO i = 1 %TO &nfactors.;
	 	 %put NO &i. factor!!!!;
          %LET fname =  %scan(&name_list.,&i., ' ');
          %neutralize_single_score(rr_result, &fname., &group_name.,  rr_result);
     %END;
	 DATA &output_table.;
	 	SET rr_result;
	RUN;
	PROC SQL;
		DROP TABLE rr_result, tt_varlist;
	QUIT;
%MEND neutralize_multi_score;


/*** 模块9: 根据因子得分，取某个分位点或绝对值下的分组 **/
/** 允许

/** 输入:
(1) input_table: end_date / stock_code / &colname.
(2) colname(character): 因子名
(3) type: 1-根据百分点进行分组(适合连续型因子) 2- 根据绝对值进行分组(适合离散型因子)
(4) threshold: type=1时，表示需要划分的百分位点(如: 50/90等); type=2时，表示绝对值
(4) is_decrease: 0-选小,1-选大,2-恰好相等
(5) is_cut: 1-输出只选取最终的子集。 0-保留全样本，但新增字段cut_mark(1-required 0-filter)


/**　输出：
(1) output_table: input_table(is_cut=1:只保留符合要求的子集) + cut_mark(is_cut=0)
**/

%MACRO cut_subset(input_table, colname, output_table, type=1, threshold=50, is_decrease=1, is_cut=1);
	%IF %SYSEVALF(&type.=1) %THEN %DO;	
		%cal_percentitle(&input_table., &colname., &threshold., pct_table);
		DATA pct_table;
			SET pct_table(rename = (pct&threshold = threshold));
		RUN;
	%END;
	%ELSE %DO;
		PROC SQL;
			CREATE TABLE pct_table AS  /* 构造一样的形式 */
			SELECT distinct end_date, &threshold. AS threshold
			FROM &input_table.
			ORDER BY end_Date;
		QUIT;
	%END; 
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.threshold 
		FROM &input_table. A LEFT JOIN pct_table B
		ON A.end_date = B.end_date
		ORDER BY A.end_date, A.&colname.;
	QUIT;
 	%IF %SYSEVALF(&is_decrease.=1) %THEN %DO;
		DATA &output_table.(drop = threshold);
			SET tmp;
			IF &colname. >= threshold THEN cut_mark = 1;
			ELSE cut_mark = 0;
		RUN;
	%END;
	%ELSE %IF %SYSEVALF(&is_decrease.=0) %THEN %DO;;
		DATA &output_table.(drop = threshold);
			SET tmp;
			IF &colname. <= threshold THEN cut_mark = 1;
			ELSE cut_mark = 0;
		RUN;
	%END;
	%ELSE %DO;;
		DATA &output_table.(drop = threshold);
			SET tmp;
			IF &colname. = threshold THEN cut_mark = 1;
			ELSE cut_mark = 0;
		RUN;
	%END;
	 %IF %SYSEVALF(&is_cut.=1) %THEN %DO;
		DATA &output_table.(drop = cut_mark);
			SET &output_table.;
			IF cut_mark = 1;
		RUN;
	%END;

	PROC SQL;
		DROP TABLE tmp, pct_table;
	QUIT;
%MEND cut_subset;

