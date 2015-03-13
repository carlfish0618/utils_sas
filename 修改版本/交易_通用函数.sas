/** ======================说明=================================================**/

/*** 函数功能：提供与交易相关的通用函数 **** /
/**** 函数列表:
(1) gen_adjust_pool: 完善主动调仓记录 --> !!需要用到外部表
(2) gen_daily_pool: 根据调仓数据生成每天的股票池 --> !!需要用到外部表
(3) cal_stock_wt_ret: 根据收益率确定组合每天个股准确的weight和单日收益（该版本无需每天迭代计算) --> !!需要用到外部表
****/ 

/** =======================================================================**/

/** 调用辅助函数: */
%LET code_dir = F:\Research\GIT_BACKUP\utils\修改版本;
%INCLUDE "&code_dir.\其他_通用函数.sas";
%INCLUDE "&code_dir.\日期_通用函数.sas";


/** 模块1: 生成交易清单 */
/** 输入: 
(1) daily_stock_pool: date/pre_date/adjust_date/stock_code/
		open_wt(open_wt_c)/close_wt(close_wt_c)/pre_close_wt(pre_close_wt_c)/after_close_wt(after_close_wt_c)/其他
(2) adjust_date_table: end_date
(3) type(logical): 1- 以“基于前收盘价”计算的权重为基准计算; 0- 以“基于复权因子”计算的权重为基准计算
***/ 

/** 生成两张表: 认为交易发生在date的收盘后
(1) 股票交易清单(output_stock_trading); 包括date/stock_code/initial_wt/traded_wt/final_wt/status/trade_type/trans_cost(暂定为单边0.35%)
(2) 每天交易清单(output_daily_trading): 包括date/delete_assets/added_assets/sell_wt/buy_wt/buy_cost/sell_cost/turnover(双边和)

其中:
(a) status(调整后状态): 0-继续持有; -1-卖出; 1-买入
(b) trade_type(调整后状态): 0-不变; 1-增持; -1-减持
(c) traded_wt: 换手权重
(d) initial_wt: 调整前权重
(e) final_wt: 调整后权重  

***/



%MACRO trading_summary(daily_stock_pool, adjust_date_table, output_stock_trading, output_daily_trading, trans_cost = 0.0035, type = 1);
	
	/* Step1: 只考虑调仓日和调仓日之后的一天 */
	%IF %SYSEVALF(&type. = 1) %THEN %DO;
		PROC SQL;
			CREATE TABLE tt_stock_pool AS
			SELECT date, pre_date, stock_code, 
			close_wt_c AS close_wt, 
			open_wt_c AS open_wt,
			pre_close_wt_c AS pre_close_wt, 
			after_close_wt_c AS after_close_wt
			FROM &daily_stock_pool.
			WHERE date IN  (SELECT end_date FROM &adjust_date_table.) 
			OR pre_date IN (SELECT end_date FROM &adjust_date_table.);
		QUIT;
	%END;
	%ELSE %DO;
		PROC SQL;
			CREATE TABLE tt_stock_pool AS
			SELECT date, pre_date, stock_code, 
			close_wt, 
			open_wt,
			pre_close_wt, 
			after_close_wt
			FROM &daily_stock_pool.
			WHERE date IN  (SELECT end_date FROM &adjust_date_table.) 
			OR pre_date IN (SELECT end_date FROM &adjust_date_table.);
		QUIT;
	%END;
		
	/* Step2: 确定股票变动*/
	DATA tt_stock_pool;
		SET tt_stock_pool;
		/* 针对调仓日(可以直接获取，继续持有或者卖出股票的信息) */
		IF after_close_wt = 0 THEN status = -1;  /* 表示剔除 */
		ELSE status = 0; /* 保留 */
		initial_wt = close_wt;
		traded_wt = after_close_wt - close_wt;
		final_wt = after_close_wt;
		IF after_close_wt - close_wt > 0 THEN trade_type = 1;
		ELSE IF after_close_wt = close_wt THEN trade_type = 0;
		ELSE trade_type = -1;
		/* 针对调仓日之后的一天(为了获取，新买入的股票信息)*/
		IF pre_close_wt = 0 THEN status_p = 1;
		ELSE status_p = 0;
		initial_wt_p = pre_close_wt;
		traded_wt_p = open_wt - pre_close_wt;
		final_wt_p = open_wt;
		IF open_wt - pre_close_wt > 0 THEN trade_type_p = 1;
		ELSE IF open_wt = pre_close_wt  THEN trade_type_p = 0;
		ELSE trade_type_p = -1;
	RUN;

	/* Step3：把所有的调仓都认为发生在收盘后 */
	/* 确定测试期内所有的调仓日 */
	PROC SQL;
		CREATE TABLE tt_adjust AS
		SELECT distinct adjust_date AS date
		FROM &daily_stock_pool.;
	QUIT;
		
	PROC SQL;
		CREATE TABLE tmp1 AS
		SELECT A.date, B.stock_code,
		B.status, B.trade_type, B.initial_wt, B.traded_wt, B.final_wt
		FROM tt_adjust A LEFT JOIN tt_stock_pool B
		ON A.date = B.date /* 在第一次调仓时, stock_code_b可能为空 */
		WHERE not missing(stock_code)
		ORDER BY A.date;

		CREATE TABLE tmp2 AS
		SELECT A.date, B.stock_code, B.status_p, B.trade_type_p, 
		B.initial_wt_p, B.traded_wt_p, B.final_wt_p
		FROM tt_adjust A LEFT JOIN tt_stock_pool B
		ON A.date = B.pre_date
		WHERE not missing(stock_code)
		ORDER BY A.date;

		CREATE TABLE tt_stock_pool AS  /* 取并集，股票如果没有调入调出的，在tmp1和tmp2中是一样的 */
		SELECT *
		FROM tmp1 UNION
		(SELECT date, stock_code, status_p AS status, trade_type_p AS trade_type,
		initial_wt_p AS initial_wt, traded_wt_p AS traded_wt, final_wt_p AS final_wt
		FROM tmp2) 
		ORDER BY date, stock_code;
	QUIT;
	DATA &output_stock_trading.;
		SET tt_stock_pool;
		trans_cost = abs(traded_wt) * &trans_cost.;
	RUN;

	/* Step4: 统计每天的情况 */
	PROC SQL;
		CREATE TABLE &output_daily_trading. AS
		SELECT date, sum(status=1) AS added_assets, sum(status=-1) AS deleted_assets,
			sum(traded_wt *(traded_wt>0)) AS buy_wt, 
			- sum(traded_wt * (traded_wt<0)) AS sell_wt,
			sum(trans_cost *(traded_wt>0))  AS buy_cost,
			sum(trans_cost * (traded_wt<0)) AS sell_cost,
			sum(traded_wt *(traded_wt>0)) - sum(traded_wt * (traded_wt<0)) AS turnover
		FROM &output_stock_trading.
		GROUP BY date;
	QUIT;
	
	PROC SQL;
		DROP TABLE tt_stock_pool, tmp1, tmp2, tt_adjust;
	QUIT;
%MEND trading_summary;
