/** ��Ϲ����������ļ� **/

/** ���ܣ�������Ϲ���������ⲿ���ݱ�
(1) busday: date
(2) hqinfo: end_date/stock_code/pre_close/close/factor (����ֻ����A�ɹ�Ʊ) **/

/** �ⲿ���� */
/*%LET pfolio_env_start_date = 15dec2013;*/

PROC SQL;
	CREATE TABLE hqinfo AS
	SELECT datepart(end_Date) AS end_date FORMAT yymmdd10.,
	stock_code, close, pre_close, factor
	FROM hq.hqinfo
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
