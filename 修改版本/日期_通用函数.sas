
/*** �����б�
(1) adjust_date_modify: ���ǽ����յ���Ϊ������(��������ָ���ķ�����������)
(2) get_date_windows: ��ȡ���ڵĴ���[start_intval, end_intval]
(3) get_month_date:  ��ȡ��ĩ���³�����(������)
(4) get_weekday_date: ��ȡÿ��ĳһ�ض�����(����һ/�ܶ���)�����N��������
(5) get_daily_date: ��ȡÿ������
(6) gen_test_busdate: ���ɻز�����
(7) gen_adjust_busdate�����ɵ������ڣ�����������Ƶ/��Ƶ/��Ƶ(��������ĳһ��)
(8) adjust_date_to_mapdate: ����mapdate_table��rawdate_table��Ӧ�������һ������(������ǰ�������󣬰������߲�����map_busdate����)

***/


/*** ģ��1: ���ǽ����յ���Ϊ������(��������ָ���ķ�����������) **/
/** ��Ծɰ汾�е� adjust_date�������޸� */
/** ����:
(1) busday_table(�������б�): date
(2) raw_table: �������ı��
(3) colname(character): �ڴ���������е�����������
(4) is_forward(numeric): 1- ��δ������ 0- ����ȥ����
**/

/**�������output_table: ԭ�е��� + adj_&colname, &colname._is_busday���С�ǰ��Ϊ����������ڣ����߱�עԭʼ�����Ƿ�Ϊ������ */
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


/*** ģ��2: ��ȡ���ڵĴ���[start_intval, end_intval]**/
/** Ҫ��start_intval <= end_intval */
/** ����:
(1) raw_table(�������б�): ����colname(���������)
(2) colname(character): �ڴ���������е�����������
(3) start_intval: ������ʾ��ǰ��
(4) end_intval: ������ʾ��ǰ��
**/

/**�������
(1) output_table: ԭ�е��� + &colname._i(b/f) (iΪ���ھ��롣�������ǰ��ʱ�䴰�ڣ����βΪb,����Ϊf)
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

/*** ģ��3: ��ȡ��ĩ���³�����(������) **/
/** ����:
(1) busday_table(�������б�): date
(2) start_date: ��ʼ����
(3) end_date: ��������
(4) rename: �Ƿ����������
(5) type: 1-��ĩ��Ĭ��) 2-�³�
**/
/**�������
(1) output_table: &rename.
**/
/** ע�⣺��ĩ���Զ���busday_table�е����һ����롣�³����Զ���busday_table�еĵ�һ����� */

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


/*** ģ��4: ��ȡÿ��ĳһ�ض�����(����һ/�ܶ���)�����N��������(��N>�ܽ����ո��������ֵ����ȡ���һ�������ա�N>0)**/
/** �ر�ģ���N=7���Ȼѡ�����һ�������� */
/** ϸ�ڣ�week��weekday��������Ϊÿһ�ܵĿ�ʼ�������� */

/** ����:
(1) busday_table(�������б�): date
(2) start_date: ��ʼ����
(3) end_date: ��������
(4) rename: �Ƿ����������
(5) type: 1- �ض����� 2- ��N��������
(6) trade_day: ��type=1ʱ��trade_day=1��ʾ���գ�2��ʾ��һ���Դ����ƣ�ȡֵ��Χ��0-6)����type=2ʱ��trade_day=1��ʾÿ�ܵ�һ��������(ȡֵ��Χ��>0)��
**/
/**�������
(1) output_table: &rename.
**/

	
%MACRO get_weekday_date(busday_table, start_date, end_date, rename, type, trade_day, output_table);
	DATA tt_busdate;
		SET &busday_table.(keep = date);
		week = week(date); /* ��ÿһ���еĵڼ��ܡ���0��ʼ */
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

/** ģ��5������ÿ������ **/
/** ����:
(1) busday_table(�������б�): date
(2) start_date: ��ʼ����
(3) end_date: ��������
(4) rename: �Ƿ����������(Ĭ��Ϊdate������ǰϰ��һ��)
**/
/**�������
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

 
		

/** ģ��6�����ɻز�����(��Ƶ��) */
/** ����:
(1) busday_table(�������б�): date
(2) start_date: ��ʼ����
(3) end_date: ��������
(4) rename: �Ƿ����������(Ĭ��Ϊdate������ǰϰ��һ��)
**/
/**�������
(1) output_table: &rename.
**/

%MACRO gen_test_busdate(busday_table, start_date, end_date, rename=date, output_table=test_busdate);
	%get_daily_date(busday_table=&busday_table., start_date=&start_date., end_date=&end_date., rename=&rename., output_table=&output_table.);
%MEND gen_test_busdate;

/** ģ��7�����ɵ�������(֧�ָ���Ƶ��) **/
/** ����:
(1) busday_table(�������б�): date
(2) start_date: ��ʼ����
(3) end_date: ��������
(4) rename: �Ƿ����������(Ĭ��Ϊdate������ǰϰ��һ��)
(5) freq: ����Ƶ�� 1-ÿ�� 2-ÿ�� 3-ÿ��
(6) type: ֻ�е�feq=2/3ʱ����Ч����freq=2ʱ,1-��ĩ 2-�³�����freq=3ʱ��1-�ض����� 2-��N�������գ�������������get_month_date��get_weekday_date�в���һ��)
(7) trade_day: ֻ�е�freq=3ʱ����Ч������������get_weekday_date�в���һ��
**/
/**�������
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


/** ģ��8������mapdate_table��rawdate_table��Ӧ�������һ������(������ǰ�������󣬰������߲�����map_busdate����) **/
/** ����:
(1) rawdate_table: ���������м�������
(2) mapdate_table: ���������к�������
(3) raw_colname: rawdate_table�������е�����
(4) map_colname: mapdate_table�������е�����
(5) is_backward: 1-rawdate_table�г�����mapdate_table��û�е����ڣ�map��ǰ��������ڡ�(��Ϊ����)
				 0-map������������
(6) is_included: 
			(a) ��is_backward=1ʱ��0-���rawdate_table�г�����mapdate_table��ͬ�����ֵ����ڣ�����ǰȡ������ڡ�1- ��ȡ��ǰ�����ڡ�
			(b) ��is_backward=0ʱ��0-���rawdate_table�г�����mapdate_table��ͬ�����ֵ����ڣ�������ȡ������ڡ�1- ��ȡ��ǰ�����ڡ�
ע��is_included=0��Ҫ����ǣ�mapdate_table�е���Ϣ�����Ч��Ϊ��һ�죬��mapdate�����õ�����ʵ��ǰһ�����Ϣ��
**/
/**�������
(1) output_table: rawdate_table�е�ԭʼ��+map_&raw_colname.
**/

/** ע�������޷�Ѱ�ҵ�����ƥ��ģ����趨Ϊȱʧ */
/** ע2-1����is_backward=0ʱ��mapdate_table����ʼʱ��Ҫ�󳬹�rawdate_table�е���ʼʱ�䡣����֮ǰ�޷�ƥ����趨Ϊȱʧ��*/
/** ע2-2����is_backward=1ʱ��mapdate_table�Ľ���ʱ��Ҫ�󳬹�rawdate_table�еĽ���ʱ�䡣����֮���޷�ƥ��Ķ�ͳһ�趨Ϊmapdate_table�Ľ���ʱ�䡣*/


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
			/** ��mapdate_table�е���Сֵ����rawdate_table�е���Сֵ��ʱ����Ҫ��mapdate_table�е���Сֵ���� */
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

/* ģ��3: ������ʼ�պͳ����գ������������ڣ�����ǳ�����ǰ���ڣ����ע���ݲ����� */
/* Ҫ��event_table��Ҫ�е��У�����stock_code, date, max_day */

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



/* ģ��3: �������ڵ�����ģ�ÿ��ָ����һ�����ڣ���: ÿ���� */
/* event_table����Ҫ����: stock_code, date(������ָ����������) */

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


