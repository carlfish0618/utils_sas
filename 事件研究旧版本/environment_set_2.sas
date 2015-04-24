/* environment setting */

options mprint;   /* nomprint */
options symbolgen;  /* nosymbolgen */


/************************************************** 参数设置 ********************************************/
/*  设置路径，保证对应的文件夹存在*/

%LET my_dir = D:\Research\DATA;  
%LET output_dir = &my_dir.\output_data;   /* 结果输出路径 */
%LET input_dir = &my_dir.\input_data;   /* 外部文件输入路径 */


/* global macro */
%LET my_library = work;
%LET eventName = event;
%LET benchmark_code = 000300;

/********************************************************** 结束参数设置 ***************************************/

/* my written module */
%INCLUDE  "D:\Research\CODE\sascode\event\完整范例\date_macro.sas";
%INCLUDE  "D:\Research\CODE\sascode\event\完整范例\impact_macro_2.sas";
%INCLUDE  "D:\Research\CODE\sascode\event\完整范例\equally_weight_macro.sas";
%INCLUDE  "D:\Research\CODE\sascode\event\完整范例\capital_allocation_macro.sas";
%INCLUDE  "D:\Research\CODE\sascode\event\完整范例\adjust_weight_macro.sas";

/* 生成的表格包括：
(1) busday
(2) my_hqinfo
(3) benchmark_hqinfo
(4) my_hqinfo_with_bm
(5) sector_hqinfo
(6) stock_sector_mapping
(7) my_hqinfo_with_indus
(8) a_stock_list
(9) trading_table
(10) list_delist_table
(11) invest_pool
(12) stock_bk_mapping
*/


/* Step1 :交易日 */
PROC SQL;
	CREATE TABLE &my_library..busday AS
	SELECT DISTINCT end_date
	FROM hq.hqinfo
	ORDER BY end_date;
QUIT;

DATA &my_library..busday(drop = end_date);
	SET &my_library..busday;
	date = datepart(end_date);
	FORMAT date mmddyy10.;
RUN;


/* Step 2: 个股行情表*/
/* columns: stock_code, date, price, last_price, next_price, ret，istrade*/
PROC SQL;
	CREATE TABLE &my_library..my_hqinfo AS
		SELECT end_date, stock_code, close * factor AS price, istrade
		FROM hq.hqinfo
		WHERE type = 'A'   /* A share only */
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
		SELECT end_date, stock_code, close * factor AS price
		FROM hq.hqinfo
		WHERE stock_code = "&benchmark_code"  AND TYPE = 'S'  /* default: hs300 */
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
	SELECT A.date, A.stock_code, A.price, A.ret, B.price AS bm_price, B.ret AS bm_ret
	FROM &my_library..my_hqinfo  A LEFT JOIN &my_library..benchmark_hqinfo  B
	ON A.date = B.date
	ORDER BY stock_code, date;
QUIT;

/* Step 5: 行业指数行情表 */
DATA &my_library..sector_hqinfo;
	SET test.fg_index_dailyreturn;
	IF indexcode = "000906" AND sectortype = "一级行业";  /* 800一级行业*/
RUN;


DATA &my_library..sector_hqinfo(DROP =  datadate);;
	SET &my_library..sector_hqinfo(keep =  datadate sectorcode weightedreturn close);
	date = datepart(datadate);
	FORMAT date mmddyy10.;
	RENAME sectorcode = stock_code weightedreturn = ret close = price;
RUN;

PROC SORT DATA =  &my_library..sector_hqinfo;
	BY stock_code date;
RUN;

/* !!! test */
/* FG16 (保险行业)从2007年1月23日开始才具有数据，这之前的个股在做事件研究时必须予以剔除 */
/*PROC SQL;*/
/*	CREATE TABLE tmp AS*/
/*	SELECT **/
/*	FROM &my_library..sector_hqinfo*/
/*	WHERE ret = .;*/
/*QUIT;*/
/* !!! end test */

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
PROC SORT DATA =  &my_library..sector_hqinfo;
	BY stock_code date;
RUN;

/* Step6: 个股->行业映射表 */

DATA  fg_wind_sector;
	SET bk.fg_wind_sector;
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
RUN;

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

PROC SQL;
	DROP TABLE fg_wind_sector, stock_list_from_sector, tmp;
QUIT;





/* Step7: 个股+行业指数行情表 */
PROC SQL;
	CREATE TABLE &my_library..my_hqinfo_with_indus  AS
	SELECT A.date, A.stock_code, A.price, A.ret, B.o_code
	FROM 
	 &my_library..my_hqinfo  A LEFT JOIN &my_library..stock_sector_mapping B
	 ON A.date = B.date AND A.stock_code = B.stock_code
	ORDER BY A.stock_code, A.date;
QUIT;

PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.price AS bm_price, B.ret AS bm_ret
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


/* Step8: 构造所有A股股票 */
PROC SQL;
	CREATE TABLE &my_library..a_stock_list AS
	SELECT distinct stock_code
	FROM &my_library..my_hqinfo
	ORDER BY stock_code;
QUIT;


/* Step9: 构建涨跌停表 */
DATA &my_library..trading_table;
	SET &my_library..my_hqinfo(keep = stock_code date istrade);
RUN;


/* Step10: 构建股票上市及可以交易表 */
PROC SQL;
	CREATE TABLE &my_library..list_delist_table AS
	SELECT F16_1090 AS stock_code, F17_1090, F18_1090, F19_1090 AS is_delist
	FROM locwind.tb_object_1090
	WHERE F4_1090 = 'A';
QUIT;

DATA &my_library..list_delist_table(drop = F17_1090 F18_1090);
	SET &my_library..list_delist_table;
	list_date = input(F17_1090,yymmdd8.);
	delist_date = input(F18_1090,yymmdd8.);
	FORMAT list_date delist_date mmddyy10.;
RUN;

	

/* Step11 : 构造可投资股票池 */
DATA &my_library..invest_pool(drop =  end_date);
	SET score.fg_hs300_factor(keep =  end_date stock_code);
	date = datepart(end_date);
	FORMAT date mmddyy10.;
RUN;

/* Step 12: 板块映射 */
PROC SQL;
	CREATE TABLE &my_library..stock_bk_mapping AS
	SELECT F16_1090 AS stock_code, F6_1090 AS bk
	FROM locwind.tb_object_1090
	WHERE F4_1090 = 'A';
QUIT;
