/* environment setting */

/* 外部设置 */
/*%LET env_start_date = 20dec2005;*/  
/* %LET index_code = 000906; */


/* my written module */
%INCLUDE  "D:\Research\CODE\sascode\event\完整范例\date_macro.sas";

/* 生成的表格包括：
(1)busday: 
		date
(2)my_hqinfo:  
		stock_code, date, price, last_price, next_price, ret, istrade, vol, close, high, low, open, pre_close
(3)benchmark_info:  
		stock_code, date, price, last_price, next_price, ret
(4)my_hqinfo_with_bm: 
		date, stock_code, price, ret, bm_price, bm_ret
(5)sector_hqinfo:
		stock_code, date, price, last_price, next_price, ret
(6)stock_sector_mapping:
		stock_code, date, o_code, o_name
(7) my_hqinfo_with_indus: 
		date, stock_code, price, ret, bm_price, bm_ret
(8) stock_info_table: 
		stock_code, stock_name, is_delist, list_date, delist_date, is_st, bk
(9) market_table: 
		stock_code date is_halt is_limit is_in_pool
(10) a_stock_list: 
		stock_code
*/

/* Step1 :交易日 */
/* date */
PROC SQL;
	CREATE TABLE &my_library..busday AS
	SELECT DISTINCT datadate AS end_date
	FROM test.fg_index_dailyreturn
/*	WHERE datepart(datadate)>= "&env_start_date."d */
	ORDER BY datadate;
QUIT;

DATA &my_library..busday(drop = end_date);
	SET &my_library..busday;
	date = datepart(end_date);
	FORMAT date mmddyy10.;
RUN;

PROC SQL NOPRINT;
	SELECT max(date),min(date) into: max_busday, :min_busday
	FROM &my_library..busday;
QUIT;



/* Step 2: 个股行情表*/
/* columns: stock_code, date, price, last_price, next_price, ret, istrade, vol, close, high, low, open, pre_close*/
PROC SQL;
	CREATE TABLE &my_library..my_hqinfo AS
		SELECT end_date, stock_code, close * factor AS price, istrade, vol, close, high, low, open, pre_close
		FROM hq.hqinfo
		WHERE type = 'A' AND datepart(end_date)>= "&env_start_date."d /* A share only */
		ORDER BY stock_code, end_date;
QUIT;

DATA &my_library..my_hqinfo(drop = end_date);
	SET &my_library..my_hqinfo;
	date = datepart(end_date);
	FORMAT date mmddyy10.;
	IF price <= 0 THEN price = .;  /* before ipo, some prices are set to zero, eg, 603167 */
RUN;

PROC SORT DATA = &my_library..my_hqinfo;
	BY stock_code date;
RUN;

DATA &my_library..my_hqinfo;
	SET &my_library..my_hqinfo;
	BY stock_code;
	last_price = lag(price);
	IF first.stock_code THEN last_price = .;
	ret = (price - last_price)/last_price * 100;
RUN;

PROC SORT DATA = &my_library..my_hqinfo;
	BY stock_code DESCENDING date;
RUN;
	
DATA &my_library..my_hqinfo;
	SET &my_library..my_hqinfo;
	BY stock_code;
	next_price = lag(price);
	IF first.stock_code THEN next_price = .;
RUN;

PROC SORT DATA = &my_library..my_hqinfo;
	BY stock_code date;
RUN;



/* Step 3:  基准指数行情表*/
/* columns: stock_code, date, price, last_price, next_price, ret */ 
PROC SQL;
	CREATE TABLE &my_library..benchmark_hqinfo AS
		SELECT end_date, stock_code, close AS price
		FROM hq.hqinfo
		WHERE stock_code = "&index_code"  AND TYPE = 'S'  AND datepart(end_date)>= "&env_start_date."d /* default: hs300 */
		ORDER BY stock_code, end_date;
QUIT;

DATA &my_library..benchmark_hqinfo (drop = end_date);
	SET &my_library..benchmark_hqinfo;
	date = datepart(end_date);
	FORMAT date mmddyy10.;
	IF price <= 0 THEN price = .;  /* before ipo, some prices are set to zero, eg, 603167 */
RUN;

PROC SORT DATA = &my_library..benchmark_hqinfo ;
	BY stock_code date;
RUN;

DATA &my_library..benchmark_hqinfo ;
	SET &my_library..benchmark_hqinfo ;
	BY stock_code;
	last_price = lag(price);
	IF first.stock_code THEN last_price = .;
	ret = (price - last_price)/last_price * 100;
RUN;

PROC SORT DATA = &my_library..benchmark_hqinfo ;
	BY stock_code DESCENDING date;
RUN;
	
DATA &my_library..benchmark_hqinfo;
	SET &my_library..benchmark_hqinfo ;
	BY stock_code;
	next_price = lag(price);
	IF first.stock_code THEN next_price = .;
RUN;

PROC SORT DATA = &my_library..benchmark_hqinfo ;
	BY date;
RUN;


/* Step 4: 个股+基准指数行情表 */
/* columns: date, stock_code, price, ret, bm_price, bm_ret */
PROC SQL;
	CREATE TABLE &my_library..my_hqinfo_with_bm  AS
	SELECT A.date, A.stock_code, A.price, A.ret, A.last_price, B.price AS bm_price, B.ret AS bm_ret, B.last_price AS bm_last_price
	FROM &my_library..my_hqinfo  A LEFT JOIN &my_library..benchmark_hqinfo  B
	ON A.date = B.date
	ORDER BY stock_code, date;
QUIT;

/* Step 5: 行业指数行情表 */
/* stock_code, date, price, last_price, next_price, ret*/
DATA &my_library..sector_hqinfo;
	SET test.fg_index_dailyreturn;
	IF indexcode = "&index_code." AND sectortype = "一级行业" AND datepart(datadate)>= "&env_start_date."d;  /* 000905: CSI500, 000906:800一级行业*/
RUN;


/* 等权行业指数 */
DATA &my_library..sector_hqinfo(DROP =  datadate);
	SET &my_library..sector_hqinfo(keep =  datadate sectorcode averagereturn averageclose);
	date = datepart(datadate);
	FORMAT date mmddyy10.;
	RENAME sectorcode = stock_code averagereturn = ret averageclose = price;
RUN;

/* 加权行业指数 */
/*DATA &my_library..sector_hqinfo(DROP =  datadate);;*/
/*	SET &my_library..sector_hqinfo(keep =  datadate sectorcode weightedreturn close);*/
/*	date = datepart(datadate);*/
/*	FORMAT date mmddyy10.;*/
/*	RENAME sectorcode = stock_code weightedreturn = ret close = price;*/
/*RUN;*/

PROC SORT DATA =  &my_library..sector_hqinfo;
	BY stock_code date;
RUN;


DATA &my_library..sector_hqinfo ;
	SET &my_library..sector_hqinfo ;
	BY stock_code;
	last_price = lag(price);
	IF first.stock_code THEN last_price = .;
RUN;

PROC SORT DATA = &my_library..sector_hqinfo ;
	BY stock_code DESCENDING date;
RUN;
	
DATA &my_library..sector_hqinfo;
	SET &my_library..sector_hqinfo ;
	BY stock_code;
	next_price = lag(price);
	IF first.stock_code THEN next_price = .;
	ret = ret * 100;
RUN;


/* Step6: 个股->行业映射表 */
/* stock_code, date, o_code, o_name */
DATA  fg_wind_sector;
	SET bk.fg_wind_sector;
	IF datepart(end_date)>= "&env_start_date."d - 40; /* 向前调整最多1个月 */
	date = datepart(end_date);
	FORMAT date mmddyy10.;
RUN; 

/* 时间没有连续，补全时间 */
PROC SQL;
	CREATE TABLE stock_list_from_sector AS
	SELECT DISTINCT stock_code
	FROM fg_wind_sector;
QUIT;

PROC SQL;
	CREATE TABLE &my_library..stock_sector_mapping AS
	SELECT stock_code, date 
	FROM stock_list_from_sector, &my_library..busday;
QUIT;

PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.o_code, B.o_name
	FROM &my_library..stock_sector_mapping  A LEFT JOIN fg_wind_sector B
	ON A.date = B.date AND A.stock_code = B.stock_code
	ORDER BY A.stock_code, A.date;
QUIT;

DATA &my_library..stock_sector_mapping(drop =  o_code o_name rename = (o_code_2 = o_code o_name_2 = o_name));
	SET tmp;
	LENGTH o_code_2 $ 16;
	LENGTH o_name_2 $ 16;
	BY stock_code;
	RETAIN o_code_2 '';
	RETAIN o_name_2 '';
	IF first.stock_code THEN DO;
		o_code_2 = '';
		o_name_2 = '';
	END;
	IF NOT missing(o_code) THEN DO;
		o_code_2 = o_code;
		o_name_2 = o_name;
	END;
RUN;


/* 新增一列: 该行业当日在指数中的权重(sector_weight) */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.sectorweight AS sector_weight
	FROM &my_library..stock_sector_mapping A LEFT JOIN test.fg_index_dailyreturn B
	ON A.date = datepart(B.datadate) AND A.o_code = B.sectorcode AND indexcode = "&index_code."
	ORDER BY A.stock_code, A.date;
QUIT;

DATA  &my_library..stock_sector_mapping;
	SET tmp;
RUN;

PROC SQL;
	DROP TABLE fg_wind_sector, stock_list_from_sector, tmp;
QUIT;


/* Step7: 个股+行业指数行情表 */
/* date, stock_code, price, ret, bm_price, bm_ret */
PROC SQL;
	CREATE TABLE &my_library..my_hqinfo_with_indus  AS
	SELECT A.date, A.stock_code, A.price, A.ret, A.last_price, B.o_code
	FROM 
	 &my_library..my_hqinfo  A LEFT JOIN &my_library..stock_sector_mapping B
	 ON A.date = B.date AND A.stock_code = B.stock_code
	ORDER BY A.stock_code, A.date;
QUIT;

PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.price AS bm_price, B.ret AS bm_ret, B.last_price AS bm_last_price
	FROM &my_library..my_hqinfo_with_indus  A LEFT JOIN &my_library..sector_hqinfo B
	ON A.o_code = B.stock_code AND A.date = B.date
	ORDER BY A.stock_code, A.date;
QUIT;

DATA &my_library..my_hqinfo_with_indus(drop =  o_code);
	SET tmp;
RUN;

PROC SQL;
	DROP TABLE tmp;
QUIT;



/* Step8: 构建股票信息表 */
/* stock_code, stock_name, is_delist, list_date, delist_date, is_st, bk */
PROC SQL;
	CREATE TABLE &my_library..stock_info_table AS
	SELECT F16_1090 AS stock_code, OB_OBJECT_NAME_1090 AS stock_name,  F17_1090, F18_1090, F19_1090 AS is_delist, F6_1090 AS bk
	FROM locwind.tb_object_1090
	WHERE F4_1090 = 'A';
QUIT;

DATA &my_library..stock_info_table(drop = F17_1090 F18_1090);
	SET &my_library..stock_info_table;
	list_date = input(F17_1090,yymmdd8.);
	delist_date = input(F18_1090,yymmdd8.);
	IF index(stock_name,'ST') THEN is_st = 1;
	ELSE is_st = 0;
	FORMAT list_date delist_date mmddyy10.;
RUN;


/* Step 9: 构造特殊交易日表 */
/* stock_code date is_halt is_limit is_in_pool */
PROC SQL;
	CREATE TABLE &my_library..market_table AS
	SELECT A.*, B.is_st, 0 AS is_halt, 0 AS is_limit
	FROM &my_library..my_hqinfo A LEFT JOIN &my_library..stock_info_table B
	ON A.stock_code = B.stock_code
	ORDER BY A.stock_code, A.date;
QUIT;

PROC SQL;
	UPDATE &my_library..market_table 
		SET is_halt = 1 WHERE missing(vol) OR vol = 0 OR (not missing (vol) AND vol ~= 0 AND istrade = 0);  /* 是否停牌*/
	UPDATE &my_library..market_table    /* 涨跌停标志 */
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
DATA  &my_library..market_table;
	SET  &my_library..market_table(keep = stock_code date is_halt is_limit);
	BY stock_code;
	last_is_halt = lag(is_halt);
	IF first.stock_code THEN last_is_halt = .;
	IF last_is_halt = 1 AND is_halt = 0 THEN is_resumption = 1;
	ELSE is_resumption = 0;
RUN;

/* 是否在我们的股票池中*/
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.stock_code AS stock_code_b
	FROM &my_library..market_table A LEFT JOIN score.fg_hs300_factor B
	ON A.stock_code = B.stock_code AND A.date = datepart(B.end_date)
	ORDER BY A.stock_code, A.date;
QUIT;

DATA  &my_library..market_table(drop = stock_code_b);
	SET tmp;
	IF NOT missing(stock_code_b) THEN is_in_pool = 1;
	ELSE is_in_pool = 0;
RUN;

PROC SQL;
	DROP TABLE tmp;
QUIT;

 /* Step10: 构造所有A股股票 */
/* stock_code */
PROC SQL;
	CREATE TABLE &my_library..a_stock_list AS
	SELECT distinct stock_code
	FROM &my_library..my_hqinfo
	ORDER BY stock_code;
QUIT;
