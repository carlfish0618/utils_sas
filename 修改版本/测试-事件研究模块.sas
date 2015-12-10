/** 测试程序 */

%LET sz_path = D:\Research\GIT-BACKUP\sz_research\sz_research\20151030;


LIBNAME sz "D:\Research\GIT-BACKUP\sz_research\sas_data";
LIBNAME database "D:\Research\数据库\通用";
%LET utils_dir = D:\Research\GIT-BACKUP\utils\SAS\修改版本; 

%INCLUDE "&utils_dir.\日期_通用函数.sas";
%INCLUDE "&utils_dir.\权重_通用函数.sas";



%LET env_start_date = 15jun2013;


PROC SQL;
	CREATE TABLE event_table AS
	SELECT sname, end_date, stock_code
	FROM sz.sz_opt_portfolio
	WHERE upcase(sname) = "S1_W_B_NR";
QUIT;
PROC SORT DATa = event_table NODUPKEY;
	BY stock_code;
RUN;
DATA event_table;
	SET event_table;
	event_date = end_date;
	event_id = _N_;
	DROP sname end_date;
	FORMAT event_date yymmdd10.;
RUN;

PROC SQL;
	CREATE TABLE hqinfo AS
	SELECT datepart(end_Date) AS end_date FORMAT yymmdd10.,
	stock_code, close, high, low, open, vol, value, istrade, pre_close, factor
	FROM database.hqinfo
	WHERE type = "A" AND "&env_start_date."d <= datepart(end_Date)
	ORDER BY end_date, stock_code;
QUIT;

PROC SQL;
	CREATE TABLE busday AS
	SELECT distinct end_date AS date
	FROM hqinfo
	ORDER BY end_date;
QUIT;

/** 1-2 指数行情 **/
PROC SQL;
	CREATE TABLE index_hqinfo AS
	SELECT datepart(end_Date) AS end_date FORMAT yymmdd10.,
	stock_code, close, pre_close, factor
	FROM database.hqinfo
	WHERE type = "S" AND "&env_start_date."d <= datepart(end_Date)
	AND stock_code IN ("000905")
	ORDER BY end_date, stock_code;
QUIT;
PROC SQL;
	CREATE TABLE bm_hqinfo AS
	SELECT A.end_date, A.stock_code, B.close, B.pre_close, B.factor
	FROM hqinfo A LEFT JOIN index_hqinfo B
	ON A.end_date = B.end_date;
QUIT;


%event_gen_windows(event_table=event_table, start_win=-20, end_win=20, busday_table = busday, output_table=sz_win);
%event_get_marketdata(win_table=sz_win, buy_win=0, hq_table=hqinfo, bm_table=bm_hqinfo, busday_table=busday, output_table=sz_rtn);
%event_mark_win(rtn_table=sz_rtn, stock_info_table=database.stock_info_table, output_table=sz_rtn);
%event_cal_accum_alpha(rtn_table=sz_rtn,buy_win=0, output_table=sz_rtn);
%event_smooth_rtn(rtn_table=sz_rtn, buy_win=0, suffix=sm, output_table=sz_rtn, filter_set=(1,2));

DATA sz_rtn;
	SET sz_rtn;
	year = year(event_date);
	month = month(event_date);
RUN;

%event_cal_stat(rtn_table=sz_rtn, rtn_var=rel_rtn_sm, output_table=stat1, filter_set=(.), group_var=);
%event_addindex_stat(stat_table=stat1, rtn_var=rel_rtn_sm, buy_win=0, output_table=stat1, group_var=);

%event_stack_stat(stat_table=stat1, output_table=stat1_s, group_var=);
%event_mdf_stat(stat_table=stat1, rtn_var=rel_rtn_sm, output_table=sta1_n,group_var=, win_set=(-60,-40,-20,-10,-5,0,5,10,20,40,60,120));
PROC SQL;
	CREATE TABLE stat As
	SELECT count(1) AS nobs,
		sum(filter=1) AS n1,
		sum(filter=2) AS n2,
		sum(filter=3) AS n3,
		sum(filter=4) AS n4,
		sum(filter=5) AS n5
	FROM sz_rtn1;
QUIT;
