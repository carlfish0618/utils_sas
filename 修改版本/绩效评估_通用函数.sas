/** ======================说明=================================================**/


/**** 函数列表:
(1) eval_pfmance


**/


/** =======================================================================**/



/***  模块0: 完善主动调仓记录，生成end_date,effective_date **/
/** 全局表: 
(1) busday **/
/** 输入: 
(1) index_pool: 待比较组合，包含:date/&index_ret. (日收益率)
(2) bm_pool: 基准组合，包含:date/&bm_ret. (日收益率)
(3) index_ret: 待比较组合日收益率列名
(4) bm_ret: 基准组合日收益率列名
(5) start_date: 比较区间开始端
(6) end_date: 比较区间结束端
(7) type: 1- 只分析alpha / 2- 只分析index_pool中的绝对收益
(8) output_table: year/ret/std/ir/hit_ratio/index_draw (yea=0表示在整个区间内，数据经过年化)

注：如果两个组合index_pool和bm_pool覆盖的范围不同，start_date和end_date将自动调整到保证都在二者的覆盖去见。否则在计算alpha时容易出错。
***/
/**** (全样本) ***/
/** 胜率为月度胜率 */
%MACRO eval_pfmance(index_pool, bm_pool, index_ret, bm_ret, start_date, end_date, type, output_table);
	%IF %SYSEVALF(&type. = 1) %THEN %DO;
		PROC SQL;
			CREATE TABLE tt_summary_day AS
			SELECT A.date, coalesce(A.&index_ret.,0)-coalesce(B.&bm_ret.,0) AS ret
			FROM &index_pool. A LEFT JOIN &bm_pool. B
			ON A.date = B.date 
			WHERE A.date >= (SELECT min(date) FROM &bm_pool.) 
			AND A.date <= (SELECT max(date) FROM &bm_pool.)
			AND A.date >= "&start_date."d
			AND A.date <= "&end_date."d
			ORDER BY A.date;
		QUIT;
	%END;
	%ELSE %IF %SYSEVALF(&type.= 2) %THEN %DO;
		PROC SQL;
			CREATE TABLE tt_summary_day AS
			SELECT A.date, coalesce(A.&index_ret.,0) AS ret
			FROM &index_pool. A
			WHERE A.date >= "&start_date."d
			AND A.date <= "&end_date."d
			ORDER BY A.date;
		QUIT;
	%END;
	/** 计算指数 */
	DATA tt_summary_day;
		SET tt_summary_day;
		RETAIN accum_ret 0;
		accum_ret = ((accum_ret/100 + 1)*(1+ret/100)-1)*100; /* 以复权因子计算 */
		index = 1000 * (1+accum_ret/100);
		year = year(date);
		month = month(date);
	RUN;

	/* Step1: 按年度统计 */
	/* Step1-1:按年度计算最大回撤，累计收益(未年化) */
	DATA tt_summary_day(drop = max_index);
		SET tt_summary_day;
		BY year;
		RETAIN max_index .;
		RETAIN accum_ret_year 0;
		IF first.year THEN DO;
			max_index = .;
			accum_ret_year = 0;
		END;
		IF index >= max_index THEN max_index = index;
		index_draw_year = (index - max_index)/max_index *100;
		accum_ret_year  = ((1+accum_ret_year/100)*(1+ret/100)-1)*100;
	RUN;

	DATA tt_stat1(rename = (accum_ret_year = accum_ret));
		SET tt_summary_day(keep = year accum_ret_year) ;
		BY year;
		IF last.year;
	RUN;
	/* Step1-2: 分年度：收益率+ 波动率 + IR + 胜率 + 最大回撤(%) */
	PROC SQL;
		CREATE TABLE tt_stat2 AS
		SELECT year, 
		sqrt(var(ret))*sqrt(250) AS sd,
		sum(ret)*sqrt(250)/(count(1)*sqrt(var(ret))) AS ir,
		sum(ret>0)/count(1) AS hit_ratio,
		min(index_draw_year) AS index_draw
		FROM tt_summary_day
		GROUP BY year;
	QUIT;
	/** Step1-3: 分年度：月度胜率 */
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT year, month, sum(ret) AS m_ret
		FROM tt_summary_day
		GROUP BY year, month;
	QUIT;
	PROC SQL;
		CREATE TABLE tt_stat3 AS
		SELECT year, sum(m_ret>0)/count(1) AS hit_ratio_m
		FROM tmp
		GROUP BY year;
	QUIT;

	PROC SQL;
		CREATE TABLE tt_summary_stat1 AS
		SELECT A.*, B.*, C.hit_ratio_m
		FROM tt_stat1 A JOIN tt_stat2 B
		ON A.year = B.year
		JOIN tt_stat3 C
		ON A.year = C.year
		ORDER BY A.year;
	QUIT;
	

	/* Step2: 整个回测区间数据 */
	/* Step2-1: 收益率+ 波动率 + IR + 胜率 + 最大回撤(%) */
	DATA tt_summary_day(drop = max_index);
		SET tt_summary_day;
		BY year;
		RETAIN max_index .;
		IF index >= max_index THEN max_index = index;
		index_draw = (index - max_index)/max_index *100;
	RUN;

	/** Step2-2: 分年度：月度胜率 */
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT year, month, sum(ret) AS m_ret
		FROM tt_summary_day
		GROUP BY year, month;
	QUIT;
	PROC SQL;
		CREATE TABLE tt_stat3 AS
		SELECT 0 AS year, sum(m_ret>0)/count(1) AS hit_ratio_m
		FROM tmp
	QUIT;

	PROC SQL;
		CREATE TABLE tt_summary_stat2 AS
		SELECT A.*, B.hit_ratio_m 
		FROM
		(SELECT 0 AS year,
		mean(ret)*250 AS accum_ret,
		sqrt(var(ret))*sqrt(250) AS sd,
		sum(ret)*sqrt(250)/(count(1)*sqrt(var(ret))) AS ir,
		sum(ret>0)/count(1) AS hit_ratio,
		min(index_draw) AS index_draw
		FROM tt_summary_day) A LEFT JOIN tt_stat3 B
		ON A.year = B.year;
	QUIT;

	DATA &output_table.;
		SET tt_summary_stat2 tt_summary_stat1;
	RUN;
	PROC SQL;
		DROP TABLE tmp, tt_summary_stat1, tt_summary_stat2, tt_stat1, tt_stat2, tt_stat3, tt_summary_day;
	QUIT;
%MEND eval_pfmance;
