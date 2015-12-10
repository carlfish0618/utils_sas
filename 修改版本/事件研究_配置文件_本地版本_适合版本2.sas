/** �¼��о�-�����ļ�-���ذ汾*/

/** �����ⲿ����:
	env_start_date
	index_code
**/


/*%LET env_start_date = 15dec2011;*/
/*%LET index_code = 000300;*/


/*** �¼��о�_�����ļ� **/

/** 1- A������ **/
PROC SQL;
	CREATE TABLE hqinfo AS
	SELECT datepart(end_Date) AS end_date FORMAT yymmdd10.,
	stock_code, close, high, low, open, vol, value, istrade, pre_close, factor
	FROM database.hqinfo
	WHERE type = "A" AND "&env_start_date."d <= datepart(end_Date)
	ORDER BY end_date, stock_code;
QUIT;

/** 1-2 ָ������ **/
PROC SQL;
	CREATE TABLE index_hqinfo AS
	SELECT datepart(end_Date) AS end_date FORMAT yymmdd10.,
	stock_code, close, pre_close, factor
	FROM database.hqinfo
	WHERE type = "S" AND "&env_start_date."d <= datepart(end_Date)
	AND stock_code IN ("000300", "399102", "399101","000001","000906","000905")
	ORDER BY end_date, stock_code;
QUIT;

/** 1-3����׼ָ��(��Ϊ�ǻ���300) */
PROC SQL;
	CREATE TABLE bm_hqinfo AS
	SELECT A.end_date, A.stock_code, B.close, B.pre_close, B.factor
	FROM hqinfo A LEFT JOIN 
	(SELECT * FROM index_hqinfo WHERE stock_code = "&index_code.") B
	ON A.end_date = B.end_date;
QUIT;


/** 2- ������ **/
PROC SQL;
	CREATE TABLE busday AS
	SELECT distinct end_date AS date FORMAT yymmdd10.
	FROM hqinfo
	ORDER BY date;
QUIT;


/** 3- ��Ʊ��Ϣ�� **/
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
	SELECT *
	FROM database.stock_info_table;
QUIT;


