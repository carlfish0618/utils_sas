/** ��Ϲ����������ļ� **/

/** ���ܣ�������Ϲ���������ⲿ���ݱ�
(1) busday: date
(2) hqinfo: end_date/stock_code/pre_close/close/factor (����ֻ����A�ɹ�Ʊ) **/

/** �ⲿ���� */
/*%LET pfolio_env_start_date = 15dec2013;*/

PROC SQL;
	CREATE TABLE hqinfo AS
	SELECT datepart(end_Date) AS end_date FORMAT yymmdd10.,
	stock_code, close, pre_close, factor, 
    high, low, open, vol, value
	FROM database.hqinfo
	WHERE type = "A" AND "&pfolio_env_start_date."d <= datepart(end_Date)
	ORDER BY end_date, stock_code;
QUIT;

/** 2- ������ **/
PROC SQL;
	CREATE TABLE busday AS
	SELECT distinct end_date AS date
	FROM hqinfo
	ORDER BY end_date;
QUIT;

/** 3-�ɱ��� */
/* ���ڹ����Ȩ��� */
PROC SQL;
	CREATE TABLE fg_wind_freeshare AS
	SELECT datepart(end_date) As end_date, stock_code,
	freeshare,
	a_share,
	total_share,
	liqa_share
	FROM database.fg_wind_freeshare
	WHERE "&pfolio_env_start_date."d <= datepart(end_Date)
	ORDER BY end_date, stock_code;
QUIT;

/** ��ҵ�� */
PROC SQL;
	CREATE TABLE fg_wind_sector AS
	SELECT datepart(end_date) AS end_Date FORMAT yymmdd10.,
		stock_code,o_code,o_name,v_code,v_name
	FROM database.fg_wind_sector
	WHERE "&pfolio_env_start_date."d <= datepart(end_Date)
	ORDER BY end_date, stock_code;
QUIT;
