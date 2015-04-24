
/*** �¼��о�_�����ļ� **/
%LET env_start_date = 15dec2010;

/** 1- A������ **/
PROC SQL;
	CREATE TABLE hqinfo AS
	SELECT datepart(end_Date) AS end_date FORMAT yymmdd10.,
	stock_code, close, high, low, open, vol, istrade, pre_close, factor
	FROM hq.hqinfo
	WHERE type = "A" AND "&env_start_date."d <= datepart(end_Date)
	ORDER BY end_date, stock_code;
QUIT;

/** 2- ������ **/
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

/** 3- ���������� */
/** �������㷨��һ���ø�Ȩ���ӣ�һ����ǰ�ա��ԱȺ��ֳ��������������������⣬���߼�����Ľ���ǻ���һ�µ� */
/** 
(1) end_Date
(2) stock_code
(3) price
(4) pre_price
**/

/** ���ø�Ȩ���Ӽ�������� */
/*PROC SQL;*/
/*	CREATE TABLE stock_hqinfo AS*/
/*	SELECT A.end_date, A.stock_code, A.close*A.factor AS price, C.close*C.factor AS pre_price*/
/*	FROM hqinfo A LEFT JOIN busday2 B*/
/*	ON A.end_date = B.date*/
/*	LEFT JOIN hqinfo C*/
/*	ON B.pre_date = C.end_date AND A.stock_code = C.stock_code*/
/*	ORDER BY A.end_date, A.stock_code;*/
/*QUIT;*/

/** ����ǰ�ռ������� */
PROC SQL;
	CREATE TABLE stock_hqinfo AS
	SELECT A.end_date, A.stock_code, A.close AS price, A.pre_close AS pre_price
	FROM hqinfo A
	ORDER BY A.end_date, A.stock_code;
QUIT;

/** 4- ��Ʊ��Ϣ�� **/
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
	SELECT F16_1090 AS stock_code, OB_OBJECT_NAME_1090 AS stock_name,  F17_1090, F18_1090, F19_1090 AS is_delist, F6_1090 AS bk
	FROM locwind.tb_object_1090
	WHERE F4_1090 = 'A';
QUIT;

DATA stock_info_table(drop = F17_1090 F18_1090);
	SET stock_info_table;
	list_date = input(F17_1090,yymmdd8.);
	delist_date = input(F18_1090,yymmdd8.);
	IF index(stock_name,'ST') THEN is_st = 1;
	ELSE is_st = 0;
	FORMAT list_date delist_date mmddyy10.;
RUN;


/* 5- ���⽻���ձ� */
/* 
(1) end_date
(2) stock_code
(3) is_halt: �Ƿ�ͣ��
(4) is_limit: 1- һ����ͣ 2- һ�ֵ�ͣ 3- ���Ƿ����� 4- ���������� 5- ����
(5) is_resumption: �Ƿ���
(6) last_no_halt_date����һ��ͣ������(is_halt=1)��ȱʧ�����ֿ��ܣ�1- ��һ��ͣ��������һ������ 2- is_halt = 0
(7) halt_days: ͣ�ƽ���������(is_halt=1ʱ��������)��10000����ʾ����һ��(��Ȼ��)
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
		SET is_halt = 1 WHERE missing(vol) OR vol = 0 OR (not missing (vol) AND vol ~= 0 AND istrade = 0);  /* �Ƿ�ͣ��*/
	UPDATE market_table    /* �ǵ�ͣ��־ */
		SET is_limit = CASE 
							WHEN close = high AND close = low AND close = open AND close > pre_close THEN 1
							WHEN close = high AND close = low AND close = open AND close < pre_close THEN 2
			 				WHEN (close >= round(pre_close * 1.15,0.01) AND is_st = 0 )  /* �ſ�1.1 -> 1.15 */
							OR ( close >= round(pre_close * 1.09,0.01) AND is_st = 1) THEN 3   /* �ſ�1.05 -> 1.09 */
							WHEN (close <= round(pre_close * 0.85,0.01) AND is_st = 0 )  /* �ſ�0.9 -> 0.85 */
							OR ( close <= round(pre_close * 0.91,0.01) AND is_st = 1) THEN 4  /* �ſ�0.95 -> 0.91*/
							ELSE 0
						END;
QUIT;



/* �Ƿ��� */
DATA  market_table(keep = end_date stock_code is_halt is_limit is_resumption);
	SET  market_table;
	BY stock_code;
	last_is_halt = lag(is_halt);
	IF first.stock_code THEN last_is_halt = .;
	IF last_is_halt = 1 AND is_halt = 0 THEN is_resumption = 1;
	ELSE is_resumption = 0;
RUN;

/** ����ͣ�����ڣ�Ѱ������ķ�ͣ������(��Զ365��) **/
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



