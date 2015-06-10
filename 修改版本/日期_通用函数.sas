
/*** 函数列表：
(1) adjust_date_modify: 将非交易日调整为交易日(或者其他指定的非连续的日期)
(2) get_date_windows: 提取日期的窗口[start_intval, end_intval]
(3) get_month_date:  提取月末或月初日期(交易日)
(4) get_weekday_date: 提取每周某一特定日期(如周一/周二等)，或第N个交易日
(5) get_daily_date: 提取每天日期
(6) gen_test_busdate: 生成回测日期
(7) gen_adjust_busdate：生成调仓日期（可以区分日频/月频/周频(允许周中某一天)
(8) adjust_date_to_mapdate: 根据mapdate_table将rawdate_table对应到最近的一个日期(可以往前或者往后，包含或者不包含map_busdate当日)

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

/*** 模块3: 提取月末或月初日期(交易日) **/
/** 输入:
(1) busday_table(交易日列表): date
(2) start_date: 开始日期
(3) end_date: 结束日期
(4) rename: 是否进行重命名
(5) type: 1-月末（默认) 2-月初
**/
/**　输出：
(1) output_table: &rename.
**/
/** 注意：月末会自动将busday_table中的最后一天加入。月初会自动将busday_table中的第一天加入 */

%MACRO get_month_date(busday_table, start_date, end_date, rename, output_table, type=1);
	%IF %SYSEVALF(&type.=1) %THEN %DO;
		PROC SQL;
			CREATE TABLE &output_table. AS
			SELECT date AS &rename. LABEL "end_date"
			FROM &busday_table.
			GROUP BY year(date), month(date)
			HAVING date = max(date);
		QUIT;
	%END;
	%ELSE %DO;
		PROC SQL;
			CREATE TABLE &output_table. AS
			SELECT date AS &rename. LABEL "end_date"
			FROM &busday_table.
			GROUP BY year(date), month(date)
			HAVING date = min(date);
		QUIT;
	%END;
	DATA &output_table.;
		SET &output_table.;
		IF "&start_date"d <= &rename. <= "&end_date."d;
	RUN;
%MEND get_month_date;


/*** 模块4: 提取每周某一特定日期(如周一/周二等)，或第N个交易日(若N>周交易日个数的最大值，则取最后一个交易日。N>0)**/
/** 特别的：另N=7则必然选择最后一个交易日 */
/** 细节：week和weekday函数都认为每一周的开始都是周日 */

/** 输入:
(1) busday_table(交易日列表): date
(2) start_date: 开始日期
(3) end_date: 结束日期
(4) rename: 是否进行重命名
(5) type: 1- 特定日期 2- 第N个交易日
(6) trade_day: 当type=1时，trade_day=1表示周日，2表示周一，以此类推（取值范围：0-6)。当type=2时，trade_day=1表示每周第一个交易日(取值范围：>0)。
**/
/**　输出：
(1) output_table: &rename.
**/

	
%MACRO get_weekday_date(busday_table, start_date, end_date, rename, type, trade_day, output_table);
	DATA tt_busdate;
		SET &busday_table.(keep = date);
		week = week(date); /* 在每一年中的第几周。从0开始 */
		wd = weekday(date);
		year = year(date);
	RUN;
	%IF %SYSEVALF(&type.=1) %THEN %DO;
		DATA &output_table.(keep = date rename = (date = &rename.));
			SET tt_busdate;
			IF wd = &trade_day.;
			IF "&start_date."d <= date <= "&end_date."d;
		RUN;
	%END;
	%ELSE %DO;
		PROC SORT DATA = tt_busdate;
			BY year week date;
		RUN;
		DATA tt_busdate;
			SET tt_busdate;
			BY year week;
			RETAIN rank 0;
			IF first.week THEN rank = 0;
			rank + 1;
		RUN;

		PROC SQL;
			CREATE TABLE &output_table. AS
			SELECT date AS &rename. 
			FROM tt_busdate
			WHERE rank <= &trade_day.
			GROUP BY year(date), week
			HAVING date = max(date);
		QUIT; 
		DATA &output_table.;
			SET &output_table.;
			IF "&start_date"d <= &rename. <= "&end_date."d;
		RUN;
	%END;
	PROC SQL;
		DROP TABLE tt_busdate;
	RUN;
%MEND get_weekday_date;

/** 模块5：生成每日日期 **/
/** 输入:
(1) busday_table(交易日列表): date
(2) start_date: 开始日期
(3) end_date: 结束日期
(4) rename: 是否进行重命名(默认为date，与以前习惯一致)
**/
/**　输出：
(1) output_table: &rename.
**/

%MACRO get_daily_date(busday_table, start_date, end_date, rename, output_table);
	DATA &output_table.(keep = &rename.);
		SET &busday_table.(keep = date);
		IF "&start_date."d <= date <= "&end_date."d;
		&rename. = date;
		FORMAT &rename. yymmdd10.;
	RUN;
%MEND get_daily_date;

 
		

/** 模块6：生成回测日期(日频率) */
/** 输入:
(1) busday_table(交易日列表): date
(2) start_date: 开始日期
(3) end_date: 结束日期
(4) rename: 是否进行重命名(默认为date，与以前习惯一致)
**/
/**　输出：
(1) output_table: &rename.
**/

%MACRO gen_test_busdate(busday_table, start_date, end_date, rename=date, output_table=test_busdate);
	%get_daily_date(busday_table=&busday_table., start_date=&start_date., end_date=&end_date., rename=&rename., output_table=&output_table.);
%MEND gen_test_busdate;

/** 模块7：生成调仓日期(支持各种频率) **/
/** 输入:
(1) busday_table(交易日列表): date
(2) start_date: 开始日期
(3) end_date: 结束日期
(4) rename: 是否进行重命名(默认为date，与以前习惯一致)
(5) freq: 调仓频率 1-每日 2-每月 3-每周
(6) type: 只有当feq=2/3时才生效。当freq=2时,1-月末 2-月初。当freq=3时，1-特定日期 2-第N个交易日（这两个参数与get_month_date和get_weekday_date中参数一致)
(7) trade_day: 只有当freq=3时才生效。参数定义与get_weekday_date中参数一致
**/
/**　输出：
(1) output_table: &rename.
**/

%MACRO gen_adjust_busdate(busday_table, start_date, end_date, rename=end_date, freq=2, type=1, trade_day=., output_table=adjust_busdate);
	%IF %SYSEVALF(&freq.=1) %THEN %DO;
		%get_daily_date(busday_table=&busday_table., start_date=&start_date., end_date=&end_date., rename=&rename., output_table=&output_table.);
	%END;
	%ELSE %IF %SYSEVALF(&freq.=2) %THEN %DO;
		%get_month_date(busday_table=&busday_table., start_date=&start_date., end_date=&end_date., 
				rename=&rename., output_table=&output_table., type=&type.);
	%END;
	%ELSE %IF %SYSEVALF(&freq.=3) %THEN %DO;
		%get_weekday_date(busday_table=&busday_table., start_date=&start_date., end_date=&end_date., rename=&rename.,
							type=&type., trade_day=&trade_day., output_table=&output_table.);
	%END;
%MEND gen_adjust_busdate;


/** 模块8：根据mapdate_table将rawdate_table对应到最近的一个日期(可以往前或者往后，包含或者不包含map_busdate当日) **/
/** 输入:
(1) rawdate_table: 包含日期列及其他列
(2) mapdate_table: 包含日期列和其他列
(3) raw_colname: rawdate_table中日期列的名称
(4) map_colname: mapdate_table中日期列的名称
(5) is_backward: 1-rawdate_table中出现了mapdate_table中没有的日期，map向前最近的日期。(更为常见)
				 0-map向后最近的日期
(6) is_included: 
			(a) 当is_backward=1时，0-如果rawdate_table中出现了mapdate_table中同样出现的日期，则往前取最近日期。1- 就取当前的日期。
			(b) 当is_backward=0时，0-如果rawdate_table中出现了mapdate_table中同样出现的日期，则往后取最近日期。1- 就取当前的日期。
注：is_included=0主要情况是：mapdate_table中的信息如果生效日为下一天，则mapdate当天用到的其实是前一天的信息。
**/
/**　输出：
(1) output_table: rawdate_table中的原始列+map_&raw_colname.
**/

/** 注：对于无法寻找到合适匹配的，则设定为缺失 */
/** 注2-1：当is_backward=0时，mapdate_table的起始时间要求超过rawdate_table中的起始时间。否则，之前无法匹配的设定为缺失。*/
/** 注2-2：当is_backward=1时，mapdate_table的结束时间要求超过rawdate_table中的结束时间。否则，之后无法匹配的都统一设定为mapdate_table的结束时间。*/


%MACRO adjust_date_to_mapdate(rawdate_table, mapdate_table, raw_colname, map_colname, output_table,is_backward=1, is_included=0);
	PROC SQL;
		CREATE TABLE tt_mapdate AS
		SELECT A.&map_colname., 
			min(B.&map_colname.) AS next_&map_colname. FORMAT yymmdd10. 
		FROM &mapdate_table. A LEFT JOIN &mapdate_table. B
		ON A.&map_colname. < B.&map_colname.
		GROUP BY A.&map_colname.
		ORDER BY A.&map_colname.;
	QUIT;

	%IF %SYSEVALF(&is_backward.=1) %THEN %DO;
		%IF %SYSEVALF(&is_included. = 0) %THEN %DO;
			PROC SQL;
				CREATE TABLE tt_output AS
				SELECT A.*, B.&map_colname. AS map_&raw_colname.
				FROM &rawdate_table. A LEFT JOIN tt_mapdate B
				ON B.&map_colname. < A.&raw_colname. <= B.next_&map_colname.
				ORDER BY A.&raw_colname.;
			QUIT;
			PROC SQL;
				UPDATE tt_output
				SET map_&raw_colname. = (SELECT max(&map_colname.) FROM tt_mapdate)
				WHERE &raw_colname. > (SELECT max(&map_colname.) FROM tt_mapdate);
			QUIT;
		%END;
		%ELSE %DO;
			PROC SQL;
				CREATE TABLE tt_output AS
				SELECT A.*, B.&map_colname. AS map_&raw_colname.
				FROM &rawdate_table. A LEFT JOIN tt_mapdate B
				ON B.&map_colname. <= A.&raw_colname. < B.next_&map_colname.
				ORDER BY A.&raw_colname.;
			QUIT;
			PROC SQL;
				UPDATE tt_output
				SET map_&raw_colname. = (SELECT max(&map_colname.) FROM tt_mapdate)
				WHERE &raw_colname. >= (SELECT max(&map_colname.) FROM tt_mapdate);
			QUIT;
		%END;
	%END;
	%ELSE %DO;
		%IF %SYSEVALF(&is_included. = 0) %THEN %DO;
			PROC SQL;
				CREATE TABLE tt_output AS
				SELECT A.*, B.next_&map_colname. AS map_&raw_colname.
				FROM &rawdate_table. A LEFT JOIN tt_mapdate B
				ON B.&map_colname. <= A.&raw_colname. < B.next_&map_colname.
				ORDER BY A.&raw_colname.;
			QUIT;
		%END;
		%ELSE %DO;
			PROC SQL;
				CREATE TABLE tt_output AS
				SELECT A.*, B.next_&map_colname. AS map_&raw_colname.
				FROM &rawdate_table. A LEFT JOIN tt_mapdate B
				ON B.&map_colname. < A.&raw_colname. <= B.next_&map_colname.
				ORDER BY A.&raw_colname.;
			QUIT;
			/** 当mapdate_table中的最小值大于rawdate_table中的最小值的时候，需要将mapdate_table中的最小值补上 */
			PROC SQL;
				UPDATE tt_output
				SET map_&raw_colname. = &raw_colname. WHERE &raw_colname. =
				(SELECT min(&map_colname.) FROM tt_mapdate);
			QUIT;
		%END;
	%END;
	DATA &output_table.;
		SET tt_output;
	RUN;
	PROC SQL;
		DROP TABLE tt_output, tt_mapdate;
	QUIT;
%MEND adjust_date_to_mapdate;


			






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


