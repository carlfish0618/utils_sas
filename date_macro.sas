/* module 1:  adjust date to business date */
/* another algorithmn: join event_day with busday on (event_day)<= busday, pick up the minimum one */

/* Input: (1) busday_table: dataset (business date)
		  (2) raw_table: dataset 
		  (3) colname : initial row name for the date in raw_table(character) */

/* Output: updated raw_table  */

/* Dataset Detail:
	(1) (input) busday_table: date
	(2) (input) raw_table: &colname and other columns
	(3) (output) updated raw_table (added 2 columns: adj_&colname, &colname._is_busday ) */

%MACRO adjust_date(busday_table = , raw_table = ,colname = );  /* busday_table: date */
	PROC SQL;
		CREATE TABLE teventday AS
			SELECT DISTINCT &colname 
			FROM &raw_table
		QUIT;

	/* merge with busday*/
	PROC SQL;
		CREATE TABLE tall_day AS
		SELECT A.date AS busday_date, B.*
		FROM  &busday_table A FULL JOIN teventday B
		ON A.date = B.&colname 
		ORDER BY A.date;
	RUN;

	DATA tall_day(keep = busday_date &colname._is_busday);
		SET tall_day;
		IF busday_date =. THEN &colname._is_busday = 0;
		ELSE &colname._is_busday = 1;

		IF busday_date = . THEN busday_date = &colname;
	RUN;

	PROC SORT DATA = tall_day;
		BY descending busday_date;  /* descending */
	RUN;

	/* adjust non-business day for nearest coming business day*/
	DATA tall_day;
		SET tall_day;
		RETAIN adj_&colname;
		IF _N_ = 1 and &colname._is_busday = 0 THEN adj_&colname = .;
		IF &colname._is_busday = 1 THEN adj_&colname = busday_date;
		FORMAT adj_&colname mmddyy10.;
	RUN;

	PROC SQL;
		CREATE TABLE ttmp AS
		SELECT A.*, B.adj_&colname, B.&colname._is_busday
		FROM &raw_table A LEFT JOIN tall_day B
		ON A.&colname = B.busday_date
		ORDER BY A.&colname;
	QUIT;

	DATA &raw_table;
		SET ttmp;
		IF missing(&colname.) THEN DO;
			adj_&colname = . ;
			&colname._is_busday = .;
		END;
	RUN;

	PROC SQL;
		DROP TABLE ttmp, teventday, tall_day;
	QUIT;
%MEND adjust_date;



	

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
