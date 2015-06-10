/** ����Fullgoal��ҵָ�� **/
/** ÿ�ն�������ͨ��ֵ��Ȩ�ķ���������ָ�� */
/** ���ս������: indus_index�� **/

%LET pfolio_env_start_date = 15dec2013;  /* ����"��Ϲ���_�����ļ�"�� */
%LET indus_start_date = 1jan2014;
%LET indus_end_date = 31may2015;



%LET utils_dir = F:\Research\GIT_BACKUP\utils\SAS\�޸İ汾; 
%INCLUDE "&utils_dir.\����_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\����_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\Ȩ��_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\��Ϲ���_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\����_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\�¼��о�_ͨ�ú���.sas";

options validvarname=any; /* ֧�����ı����� */

/*** step0: ������Ϲ���_�����ļ� **/
%INCLUDE "&utils_dir.\��Ϲ���_�����ļ�.sas";

/** step1: ����ÿ�ո�����ҵ�ĳɷֹ� */
%get_daily_date(busday_table=busday, start_date=&indus_start_date., end_date=&indus_end_date., rename=date, output_table=date_list);

PROC SQL;
	CREATE TABLE indus_mapdate AS
	SELECT distinct end_date FORMAT yymmdd10.
	FROM
	(SELECT datepart(end_date) AS end_date FROM bk.fg_wind_sector)
	WHERE end_date >= "&pfolio_env_start_date."d
	ORDER BY end_date;
QUIT;

%adjust_date_to_mapdate(rawdate_table=date_list, mapdate_table=indus_mapdate, raw_colname=date, map_colname=end_date, 
		output_table=date_list,is_backward=1, is_included=1);
PROC SQL;
	CREATE TABLE index_stock_pool AS
	SELECT A.date AS end_date, B.o_code, B.o_name, B.stock_code, B.stock_name
	FROM date_list A LEFT JOIN bk.fg_wind_sector B
	ON A.map_date = datepart(B.end_date)
	ORDER BY A.date, B.o_code, B.stock_code;
QUIT;

/** Step2: ȡ������ͨ��ֵ 
(1) stock_table: stock_code/end_date/����
(2) index(numeric): ����ȡ 1-freeshare 2-a_share 3-total_share��һ�ߡ�
(3) info_table: stock_code/end_date/close
(4) share_table: stock_code/end_date/freeshare(a_share,total_share��) 

**/

PROC SQL;
	CREATE TABLE share_table AS
	SELECT stock_code, datepart(end_date) AS end_date FORMAT yymmdd10., freeshare, a_share, total_share
	FROM tinysoft.fg_wind_freeshare
	WHERE end_date >= "&pfolio_env_start_date."d 
	ORDER bY end_date, stock_code;
QUIT;
/** 2015��4��14�գ�fg_wind_freeshareȱʧ���� */
/** ��ʱ��2015-4-13�����ݲ�ȫ */
DATA share_table_append;
	SET share_table;
	IF end_date = "13apr2015"d;
	end_date = "14apr2015"d;
RUN;
DATA share_table;
	SET share_table share_table_append;
RUN;


%get_stock_size(stock_table=index_stock_pool, info_table=hqinfo, share_table=share_table,output_table=index_stock_pool2, index = 1);

DATA index_stock_pool2;
	SET index_stock_pool2;
	IF not missing(value);
RUN;



/** Step3: ��Ϲ��� */
/** ˵����������ͨ��ֵ��Ȩ��*/
%MACRO single_indus_index(o_code);
	DATA test_stock_pool(keep = end_date stock_code weight);
		SET index_stock_pool2;
		IF o_code = "FG01";
		weight = value;
	RUN;
	%neutralize_weight(stock_pool=test_stock_pool, output_stock_pool=test_stock_pool);
	%gen_daily_pool(stock_pool=test_stock_pool, test_period_table=test_busdate, 
		adjust_date_table=adjust_busdate, output_stock_pool=test_stock_pool);
	%cal_stock_wt_ret(daily_stock_pool=test_stock_pool, adjust_date_table=adjust_busdate, output_stock_pool=test_stock_pool);
	%cal_portfolio_ret(daily_stock_pool=test_stock_pool, output_daily_summary=index_info);

	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, index AS &o_code.
		FROM indus_index A LEFT JOIN index_info B
		ON A.date = B.date
		ORDER BY A.date;
	QUIT;
	DATA indus_index;
		SET tmp;
		IF date = &min_date. THEN &o_code. = 1000;  /* ��ȫ���� */
	RUN;
	PROC SQL;
		DROP TABLE tmp, index_info;
	QUIT;
%MEND single_indus_index;

%MACRO all_indus_index();
	%gen_test_busdate(busday_table=busday, start_date=&indus_start_date., end_date=&indus_end_date., rename=date, output_table=test_busdate);
	%gen_adjust_busdate(busday_table=busday, start_date=&indus_start_date., end_date=&indus_end_date., rename=end_date, freq=1, output_table=adjust_busdate);
	DATA indus_index;
		SET test_busdate;
	RUN;
	PROC SQL NOPRINT;
		SELECT distinct o_code, count(distinct o_code)
		INTO :indus_list SEPARATED BY ' ',
			 :nindus
		FROM bk.fg_wind_sector
		ORDER BY o_code;
	QUIT;
	PROC SQL NOPRINT;
		SELECT min(date)
		INTO :min_date
		FROM indus_index;
	QUIT;

	%DO i = 1 %TO &nindus.;
		%LET cur_code = %SCAN(&indus_list., &i., ' ');
		%single_indus_index(&cur_code.);
	%END;
%MEND all_indus_index;

%all_indus_index();

	
