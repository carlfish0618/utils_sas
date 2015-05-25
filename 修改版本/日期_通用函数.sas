
/*** 函数列表：
(1) adjust_date_modify: 将非交易日调整为交易日(或者其他指定的非连续的日期)
(2) get_date_windows: 提取日期的窗口[start_intval, end_intval]
(3) get_month_date:  提取月末日期(交易日)

***/


/*** 模块1: 将非交易日调整为交易日(或者其他指定的非连续的日期) **/
/** 针对旧版本中的 adjust_date进行了修改 */
/** 输入:
(1) busday_table(交易日列表): date
(2) raw_table: 待调整的表格
(3) colname(character): 在待调整表格中的日期列名称
(4) is_forward(numeric): 1- 往未来调整 0- 往过去调整
**/

/**　输出：output_table: 原有的列 + adj_&colname, &colname._is_busday两列。前者为调整后的日期，后者标注原始日期是否为交易日 */
%MACRO adjust_date_modify(busday_table , raw_table ,colname,  output_table, is_forward = 1 );  /* busday_table: date */
	PROC SQL;
		CREATE TABLE teventday AS
			SELECT DISTINCT &colname 
			FROM &raw_table
		QUIT;
	QUIT;
	%IF %SYSEVALF(&is_forward. =1) %THEN %DO;
		PROC SQL;
			CREATE TABLE tmp AS
			SELECT A.&colname., B.date AS adj_&colname.
			FROM teventday A LEFT JOIN &busday_table. B
			ON A.&colname.<= B.date 
			GROUP BY A.&colname.
			HAVING B.date = min(B.date)
			ORDER BY A.&colname.;
		QUIT;
	%END;
	%ELSE %DO;
		PROC SQL;
			CREATE TABLE tmp AS
			SELECT A.&colname., B.date AS adj_&colname.
			FROM teventday A LEFT JOIN &busday_table. B
			ON A.&colname.>= B.date 
			GROUP BY A.&colname.
			HAVING B.date = max(B.date)
			ORDER BY A.&colname.;
		QUIT;
	%END;
		
	DATA tmp;
		SET tmp;
		IF &colname. = adj_&colname. THEN &colname._is_busday =1;
		ELSE &colname._is_busday = 0;
	RUN;
	PROC SQL;
		CREATE TABLE tmp2 AS
		SELECT A.*, B.adj_&colname., B.&colname._is_busday
		FROM &raw_table. A LEFT JOIN tmp B
		ON A.&colname. = B.&colname.
		ORDER BY A.&colname.;
	QUIT;
	DATA &output_table.;
		SET tmp2;
	RUN;
	PROC SQL;
		DROP TABLE tmp, tmp2, teventday;
	QUIT;
%MEND adjust_date_modify;


/*** 模块2: 提取日期的窗口[start_intval, end_intval]**/
/** 要求：start_intval <= end_intval */
/** 输入:
(1) raw_table(交易日列表): 包含colname(与日期相关)
(2) colname(character): 在待调整表格中的日期列名称
(3) start_intval: 负数表示往前推
(4) end_intval: 负数表示往前推
**/

/**　输出：
(1) output_table: 原有的列 + &colname._i(b/f) (i为窗口距离。如果是往前的时间窗口，则结尾为b,否则为f)
**/

%MACRO get_date_windows(raw_table, colname, output_table, start_intval = 1, end_intval = 12);
	PROC SQL;
		CREATE TABLE tt_date AS
		SELECT distinct &colname. AS date_bb
		FROM &raw_table.
		ORDER BY &colname.;
	QUIT;

	DATA tt_date;
		SET tt_date;
		id = _N_;
	RUN;

	%DO i = &start_intval. %TO &end_intval. %BY 1;
		%IF %SYSEVALF(&i.<0) %THEN %LET iname = b%sysevalf(-&i.);
		%ELSE %LET iname = f&i.; 
		PROC SQL;
			CREATE TABLE tmp AS
			SELECT A.*, B.date_bb AS date_&iname LABEL "date_&iname."
			FROM tt_date A LEFT JOIN tt_date B
			ON B.id = A.id + (&i.)
			ORDER BY A.date_bb;
		QUIT;
		DATA tt_date;
			SET tmp;
		RUN;
	%END;
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.*
		FROM &raw_table. A LEFT JOIN tt_date B
		ON A.&colname. = B.date_bb
		ORDER BY A.&colname.;
	QUIT;

	DATA &output_table.(drop = date_bb id);
		SET tmp;
	RUN;
	PROC SQL;
		DROP TABLE tt_date, tmp;
	QUIT;
%MEND get_date_windows;

/*** 模块3: 提取月末日期(交易日) **/
/** 输入:
(1) busday_table(交易日列表): date
(2) start_date: 开始日期
(3) end_date: 结束日期
**/
/**　输出：
(1) output_table: date
**/

%MACRO get_month_date(busday_table, start_date, end_date, rename, output_table);
	PROC SQL;
		CREATE TABLE &output_table. AS
		SELECT date AS &rename. LABEL "end_date"
		FROM &busday_table.
		GROUP BY year(date), month(date)
		HAVING date = max(date);
	QUIT;
	DATA &output_table.;
		SET &output_table.;
		IF "&start_date"d <= end_date <= "&end_date."d;
	RUN;
%MEND get_month_date;









/* module 2: create a subsets and new global macro */
/* Input: 
	(1) raw_table: datasets
	(2) busday_table: datasets 
	(3) interval: move forward/backward days for expanding the range
	(4) is_global_macro: 1-> create or replace global macro for max_day and min_day
	(5) macro_max_day: macro name 
	(6) macro_min_day: macro name
/* Output:
	(1) subset_busday_table: datasets */

/* Datasets Detail:
	(1) (input) raw_table: date and other columns
	(2) (input) busday_table: date 
	(3) (output) subset_busday_table: date  */

%MACRO date_subset(raw_table, busday_table, interval, is_global_macro, macro_max_day, macro_min_day, subset_busday_table);
	PROC MEANS DATA = &raw_table NOPRINT;
		VAR date;
		OUTPUT OUT = ttmp min = min_date max = max_date;
	RUN;

	%IF &is_global_macro = 1 %THEN %LET field = G;
	%ELSE %LET field = L;

	DATA _null_;
		SET ttmp;
		call symputx("&macro_max_day", put(max_date, mmddyy10.), "&field");
		call symputx("&macro_min_day", put(min_date, mmddyy10.), "&field");
	RUN;


	DATA &subset_busday_table;
		SET &busday_table;
		IF intnx('day', input("&&&macro_min_day", mmddyy10.),  -&interval) <= date <= intnx('day', input("&&&macro_max_day", mmddyy10.), &interval);
	RUN; 

	PROC SQL;
		DROP TABLE ttmp;
	QUIT;

%MEND date_subset;


/* module 3: mapping date to its order number */
/* Input:
	(1) busday_table: datasets
	(2) raw_table: datasets 
	(3) date_col_name: colname for the date in raw_table 
	(4) raw_table_edit: datasets*/
/* Output:
	(1) raw_table_edit: raw_table with one column date_index added, can be replaced raw one*/
/* Datasets:
	(1) (input) busday_table: date
	(2) (input) raw_table: &date_col_name and other columns */

%MACRO map_date_to_index(busday_table, raw_table, date_col_name, raw_table_edit);
	
	PROC SORT DATA = &busday_table;
		BY date;
	RUN;

	DATA tbusday;
		SET &busday_table;
		index = _N_;
	RUN;

	PROC SQL;
		CREATE TABLE ttmp AS
		SELECT A.*, B.index AS date_index
		FROM &raw_table. A LEFT JOIN tbusday B
		ON A.&date_col_name = B.date;
	QUIT;

	DATA &raw_table_edit;
		SET ttmp;
	RUN;

	PROC SQL;
		DROP TABLE ttmp, tbusday;
	QUIT;

%MEND map_date_to_index;

/* 模块3: 根据起始日和持有日，计算卖出日期；如果是超过当前日期，则标注，暂不处理 */
/* 要求event_table需要有的列，包括stock_code, date, max_day */

%MACRO cal_ineffective_date(event_table, busday_table, end_date,  output_table);
	%map_date_to_index(busday_table=&busday_table., raw_table=&busday_table., date_col_name=date, raw_table_edit=m_busday);
	%map_date_to_index(busday_table=&busday_table., raw_table=&event_table., date_col_name=date, raw_table_edit=m_event);

	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.date AS cal_ineffective_date
		FROM m_event A LEFT JOIN m_busday B
		ON A.date_index + max_day = B.date_index
		ORDER BY A.stock_code, A.date;
	QUIT;

	DATA &output_table.(drop = cal_ineffective_date);
		SET tmp;
		IF missing(cal_ineffective_date) THEN DO;
			ineffective_date = &end_date.;
			is_to_end = 1;
		END;
		ELSE DO;
			ineffective_date = cal_ineffective_date;
			is_to_end = 0;
		END;
	RUN;
	PROC SQL;
		DROP TABLE tmp, m_busday, m_event;
	QUIT;

%MEND cal_ineffective_date;



/* 模块3: 调整日期到最近的，每周指定的一个日期，如: 每周五 */
/* event_table中需要有列: stock_code, date(或其他指定的日期列) */

%MACRO adjust_to_week(event_table, busday_table, trade_day, column, end_date, output_table);  /* trade_day = 1: Sunday; trade_day = 6: Friday */
	DATA tt_busday;
		SET &busday_table;
		wd = weekday(date);
		IF wd = &trade_day.;
	RUN;

	DATA tt_busday;
		SET tt_busday;
		pre_busday = lag(date);
		FORMAT pre_busday mmddyy10.;
	RUN;

	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.date AS bb_date
		FROM &event_table A LEFT JOIN tt_busday B
		ON pre_busday < A.&column. <= B.date
		ORDER BY stock_code, A.&column.;
	QUIT;

	DATA &output_table.(drop = &column. rename = (bb_date = &column.));
		SET tmp;
		BY stock_code;
		IF missing(bb_date) THEN DO;
			bb_date = &end_date.;
		END;
	RUN;

	PROC SQL;
		DROP TABLE tt_busday, tmp;
	QUIT;

%MEND;


