/*** 创建指数 **/
/*** 函数列表：
(1) construct_index: 创建指数，并统计绝对收益或alpha
(2) construct_index_neat: 仅生成指数收益

***/

/** 模块1：创建指数*/
/** 输入：
(1) test_pool: end_date/stock_code/weight
(2) adjust_date: end_date
(3) test_date: date
(4) bm_index_table; date/daily_ret 
	(当index_result_type=2时，强制将该表设定为output_index_table，即自身指数表。因此当index_result_type=1时，该表可以为缺失)
(5) output_index_table: 输出的指数文件，包含date/
(6) output_stat_table: 输出的与基准的比较结果
(7) output_trade_table: 输出的换手率
(8) excel_path: 输出的excel表，仅is_output=1时有效
(9) sheet_name_index/sheet_name_stat/sheet_name_trade:分别对应以上三张表的sheet名
(10) start_date: 基准比较开始的日期
(11) end_date: 基准比较结束的日期
(12) index_result_type: 1- 分析alpha, 2-分析绝对收益
(13) is_output: 是否输出 
(14) annualizd_factor: 计算换手率年化时需要用的factor
***/


%MACRO construct_index(test_pool, adjust_date, test_date,bm_index_table,
			output_index_table, output_stat_table, output_trade_table, 
			excel_path, sheet_name_index, sheet_name_stat, sheet_name_trade, 
			start_date, end_date, 
			index_result_type = 2, is_output=0, annualized_factor=12);
	%neutralize_weight(stock_pool=&test_pool., output_stock_pool=&test_pool._copy,col=weight);
	%gen_daily_pool(stock_pool=&test_pool._copy, test_period_table=test_busdate, 
		adjust_date_table=adjust_busdate, output_stock_pool=&test_pool._copy);
	%cal_stock_wt_ret(daily_stock_pool=&test_pool._copy, adjust_date_table=adjust_busdate, output_stock_pool=&test_pool._copy);
	%cal_portfolio_ret(daily_stock_pool=&test_pool._copy, output_daily_summary=&output_index_table., type=1); /* 基于前收计算 */
	%IF %SYSEVALF(&index_result_type.=1) %THEN %DO;
		%eval_pfmance(index_pool=&output_index_table., bm_pool=&bm_index_table., index_ret=daily_ret, 
			bm_ret=daily_ret, start_date=&start_date., end_date=&end_date., type=1, 
				output_table=&output_stat_table.);
	%END;
	%ELSE %IF %SYSEVALF(&index_result_type.=2) %THEN %DO;
		%eval_pfmance(index_pool=&output_index_table., bm_pool=., index_ret=daily_ret, 
			bm_ret=daily_ret, start_date=&start_date., end_date=&end_date., type=2, 
				output_table=&output_stat_table.);
	%END;
	/** 统计换手率 */
	/** 函数结果是双边的 */
	%trading_summary(daily_stock_pool=&test_pool._copy, adjust_date_table=adjust_busdate,
		output_stock_trading=tt, output_daily_trading=&output_trade_table., trans_cost = 0.0035, type = 1); /* 基于前收计算 */
	DATA &output_trade_table.;
		SET &output_trade_table.;
		year = year(date);
	RUN;
	/** 这里调整为单边 */
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT 0 AS year, sum(sell_wt)/(count(sell_wt)-1)*&annualized_factor. AS turnover /* 最开始的卖出为0，不予计算 */
		FROM &output_trade_table.;

		CREATE TABLE tmp2 AS
		SELECT year, sum(sell_wt) AS turnover
		FROM &output_trade_table.
		GROUP BY year;
	QUIT;
	DATA tmp;
		SET tmp tmp2;
	RUN;
	PROC SQL;
		CREATE TABLE tmp2 AS
		SELECT A.*, B.turnover AS turnover_uni  /* 单边 */
		FROM &output_stat_table. A LEFT JOIN tmp B
		ON A.year = B.year
		ORDER BY A.year;
	QUIT;
	DATA &output_stat_table.;
		SET tmp2;
	RUN;
	/** 统计股票数量*/
	DATA &output_index_table.;
		SET &output_index_table.;
		year = year(date);
	RUN;
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT 0 AS year, mean(nstock) AS nstock /* 最开始的卖出为0，不予计算 */
		FROM &output_index_table.;

		CREATE TABLE tmp2 AS
		SELECT year, mean(nstock) AS nstock
		FROM &output_index_table.
		GROUP BY year;
	QUIT;
	DATA tmp;
		SET tmp tmp2;
	RUN;
	PROC SQL;
		CREATE TABLE tmp2 AS
		SELECT A.*, B.nstock
		FROM &output_stat_table. A LEFT JOIN tmp B
		ON A.year = B.year
		ORDER BY A.year;
	QUIT;
	DATA &output_stat_table.;
		SET tmp2;
	RUN;
	/** 输出*/
	%IF %SYSEVALF(&is_output.=1) %THEN %DO;
		%output_to_excel(excel_path=&excel_path., input_table=&output_index_table., sheet_name = &sheet_name_index.);
		%output_to_excel(excel_path=&excel_path., input_table=&output_stat_table., sheet_name = &sheet_name_stat.);
		%output_to_excel(excel_path=&excel_path., input_table=&output_trade_table., sheet_name = &sheet_name_trade.);
	%END;
	PROC SQL;
		DROP TABLE tt, tmp, tmp2;
	QUIT;

%MEND construct_index;


/** 模块1：仅生成指数收益文件*/
/** 输入：
(1) test_pool: end_date/stock_code/weight
(2) adjust_date: end_date
(3) test_date: date
(4) output_index_table: 输出的指数文件，包含date/
(5) excel_path: 输出的excel表，仅is_output=1时有效
(6) is_output: 是否输出 
***/

%MACRO construct_index_neat(test_pool, adjust_date, test_date,output_index_table, excel_path, is_output=0);
	%neutralize_weight(stock_pool=&test_pool., output_stock_pool=&test_pool._copy,col=weight);
	%gen_daily_pool(stock_pool=&test_pool._copy, test_period_table=test_busdate, 
		adjust_date_table=adjust_busdate, output_stock_pool=&test_pool._copy);
	%cal_stock_wt_ret(daily_stock_pool=&test_pool._copy, adjust_date_table=adjust_busdate, output_stock_pool=&test_pool._copy);
	%cal_portfolio_ret(daily_stock_pool=&test_pool._copy, output_daily_summary=&output_index_table., type=1); /* 基于前收计算 */

	/** 输出*/
	%IF %SYSEVALF(&is_output.=1) %THEN %DO;
		%output_to_excel(excel_path=&excel_path., input_table=&output_index_table., sheet_name = index);
	%END;
	PROC SQL;
		DROP TABLE &test_pool._copy;
	QUIT;
%MEND construct_index_neat;
