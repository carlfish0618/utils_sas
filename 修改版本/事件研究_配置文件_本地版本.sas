/** 事件研究-配置文件-本地版本*/

/** ！！外部变量: env_start_date **/

/*%LET env_start_date = 15dec2011;*/


/*** 事件研究_配置文件 **/

/** 1- A股行情 **/
PROC SQL;
	CREATE TABLE hqinfo AS
	SELECT datepart(end_Date) AS end_date FORMAT yymmdd10.,
	stock_code, close, high, low, open, vol, value, istrade, pre_close, factor
	FROM database.hqinfo
	WHERE type = "A" AND "&env_start_date."d <= datepart(end_Date)
	ORDER BY end_date, stock_code;
QUIT;

/** 1-2 指数行情 **/
PROC SQL;
	CREATE TABLE index_hqinfo AS
	SELECT datepart(end_Date) AS end_date FORMAT yymmdd10.,
	stock_code, close, pre_close
	FROM database.hqinfo
	WHERE type = "S" AND "&env_start_date."d <= datepart(end_Date)
	AND stock_code IN ("000905","000300", "399102", "399101","000001","000906")
	ORDER BY end_date, stock_code;
QUIT;

/** 2- 交易日 **/
PROC SQL;
	CREATE TABLE busday AS
	SELECT distinct end_date AS date
	FROM hqinfo
	ORDER BY end_date;
QUIT;

PROC SQL;
	CREATE TABLE busday2 AS
	SELECT A.*, B.date AS pre_date
	FROM busday A LEFT JOIN busday B
	ON A.date >  B.date
	GROUP BY A.date
	HAVING B.date = max(B.date)
	ORDER BY A.date;
QUIT;

/** 3- 个股行情表简化 */
/** 有两种算法：一种用复权因子，一种用前收。对比后发现除了少数天数少数个股外，二者计算出的结果是基本一致的 */
/** 
(1) end_Date
(2) stock_code
(3) price
(4) pre_price
**/

/** 利用复权因子计算的收益 */
/*PROC SQL;*/
/*	CREATE TABLE stock_hqinfo AS*/
/*	SELECT A.end_date, A.stock_code, A.close*A.factor AS price, C.close*C.factor AS pre_price*/
/*	FROM hqinfo A LEFT JOIN busday2 B*/
/*	ON A.end_date = B.date*/
/*	LEFT JOIN hqinfo C*/
/*	ON B.pre_date = C.end_date AND A.stock_code = C.stock_code*/
/*	ORDER BY A.end_date, A.stock_code;*/
/*QUIT;*/

/** 利用前收计算收益 */
PROC SQL;
	CREATE TABLE stock_hqinfo AS
	SELECT A.end_date, A.stock_code, A.close AS price, A.pre_close AS pre_price
	FROM hqinfo A
	ORDER BY A.end_date, A.stock_code;
QUIT;

/** 4- 股票信息表 **/
/** 
(1) stock_code
(2) stock_name
(3) list_date
(4) delist_date
(5) is_delist
(6) bk
***/

PROC SQL;
	CREATE TABLE stock_info_table AS
	SELECT *
	FROM database.stock_info_table;
QUIT;


/* 5- 特殊交易日表 */
/* 
(1) end_date
(2) stock_code
(3) is_halt: 是否停牌
(4) is_limit: 1- 一字涨停 2- 一字跌停 3- 超涨幅限制 4- 超跌幅限制 5- 正常
(5) is_resumption: 是否复牌
(6) last_no_halt_date：上一个停牌日期(is_halt=1)。缺失有两种可能：1- 上一个停牌日期在一年以上 2- is_halt = 0
(7) halt_days: 停牌交易日天数(is_halt=1时，有意义)，10000：表示超过一年(自然日)
**/ 
PROC SQL;
	CREATE TABLE market_table AS
	SELECT A.*, B.is_st, 0 AS is_halt, 0 AS is_limit
	FROM hqinfo A LEFT JOIN stock_info_table B
	ON A.stock_code = B.stock_code
	ORDER BY A.stock_code, A.end_date;
QUIT;

PROC SQL;
	UPDATE market_table 
		SET is_halt = 1 WHERE missing(vol) OR vol = 0 OR (not missing (vol) AND vol ~= 0 AND istrade = 0);  /* 是否停牌*/
	UPDATE market_table    /* 涨跌停标志 */
		SET is_limit = CASE 
							WHEN close = high AND close = low AND close = open AND close > pre_close THEN 1
							WHEN close = high AND close = low AND close = open AND close < pre_close THEN 2
			 				WHEN (close >= round(pre_close * 1.15,0.01) AND is_st = 0 )  /* 放宽：1.1 -> 1.15 */
							OR ( close >= round(pre_close * 1.09,0.01) AND is_st = 1) THEN 3   /* 放宽：1.05 -> 1.09 */
							WHEN (close <= round(pre_close * 0.85,0.01) AND is_st = 0 )  /* 放宽：0.9 -> 0.85 */
							OR ( close <= round(pre_close * 0.91,0.01) AND is_st = 1) THEN 4  /* 放宽：0.95 -> 0.91*/
							ELSE 0
						END;
QUIT;



/* 是否复牌 */
DATA  market_table(keep = end_date stock_code is_halt is_limit is_resumption);
	SET  market_table;
	BY stock_code;
	last_is_halt = lag(is_halt);
	IF first.stock_code THEN last_is_halt = .;
	IF last_is_halt = 1 AND is_halt = 0 THEN is_resumption = 1;
	ELSE is_resumption = 0;
RUN;

/** 对于停牌日期，寻找最近的非停牌日期(最远365天) **/
PROC SQL;
	CREATE TABLE market_data_append AS
	SELECT A.stock_code, A.end_date, B.end_date AS last_no_halt_date
	FROM 
	(
	SELECT stock_code, end_date
	FROM market_table
	WHERE is_halt = 1
	) A LEFT JOIN
	(SELECT stock_code, end_date
	FROM market_table
	WHERE is_halt = 0 )B
	ON B.end_date + 365 >=  A.end_date > B.end_date AND A.stock_code = B.stock_code
	GROUP BY A.stock_code, A.end_date
	HAVING B.end_date = max(B.end_date)
	ORDER BY A.end_date, A.stock_code;
QUIT;

%map_date_to_index(busday_table=busday, raw_table=market_data_append, date_col_name=end_date, raw_table_edit=market_data_append2);
DATA market_data_append2;
	SET market_data_append2(rename = (date_index = end_date_index));
RUN;
%map_date_to_index(busday_table=busday, raw_table=market_data_append2, date_col_name=last_no_halt_date, raw_table_edit=market_data_append2);
DATA market_data_append2(drop = end_date_index date_index last_no_halt_date);
	SET market_data_append2;
	IF missing(last_no_halt_date) THEN halt_days = 1000000;
	ELSE halt_days = end_date_index - date_index;
RUN;
PROC SORT DATA = market_table;
	BY end_date stock_code;
RUN;
PROC SORT DATA = market_data_append2;
	BY end_date stock_code;
RUN;
DATA market_table;
	UPDATE market_table market_data_append2;
	BY end_date stock_code;
RUN;
DATA market_table;
	SET market_table;
	IF is_halt = 0 THEN halt_days = .;
RUN;



/*** 自由流通市值表 */
PROC SQL;
	CREATe TABLE fg_wind_freeshare AS
	SELECT stock_code, datepart(end_date) AS end_date FORMAT yymmdd10.,
		freeshare, total_share, a_share, liqa_share
	FROM database.fg_wind_freeshare
	WHERE datepart(end_date) >= "&env_start_date."d
	ORDER BY end_date, stock_code;
QUIT;

/** 行业信息表 */
PROC SQL;
	CREATe TABLE fg_wind_sector AS
	SELECT stock_code, datepart(end_date) AS end_date FORMAT yymmdd10.,
		o_code AS indus_code, o_name AS indus_name
	FROM database.fg_wind_sector
	WHERE datepart(end_date) >= "&env_start_date."d
	ORDER BY end_date, stock_code;
QUIT;


/** 构建富国股票池2 */
PROC SQL;
	CREATE TABLE fg_stock_pool AS
	(
		SELECT stock_code, datepart(end_date) AS end_date FORMAT yymmdd10.
		FROM database.fg_eps_info
		WHERE cnum >= 4 AND datepart(end_date) >= "&env_start_date."d AND stock_code NOT IN ("600837", "000166")
	) 
	ORDER BY end_date;
QUIT;

/** 取6月末的最后一天，认为是股票池 */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT *, year(end_date) AS year
	FROM fg_stock_pool
	WHERE month(end_date) = 6 
	GROUP BY year(end_date), month(end_date)
	HAVING end_date = max(end_date)
	ORDER BY end_date, stock_code;
QUIT;
DATA fg_stock_pool;
	SET tmp;
RUN;
/** 因为股票池的日期，有的不是交易日需要进行调整 */
%adjust_date_modify(busday_table=busday , raw_table=fg_stock_pool ,colname=end_date,  
	output_table=fg_stock_pool, is_forward = 0 );
DATA fg_stock_pool(drop = adj_end_date end_date_is_busday);
	SET fg_stock_pool;
	end_date = adj_end_date;
RUN;
