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
	AND substr(stock_code,1,1) in ('0','3','6')
	ORDER BY end_date, stock_code;
QUIT;

/** 1-2 ָ������ **/
/** 399982: 500��Ȩ��399984:300��Ȩ */
PROC SQL;
	CREATE TABLE index_hqinfo AS
	SELECT datepart(end_Date) AS end_date FORMAT yymmdd10.,
	stock_code, close, pre_close, factor
	FROM database.hqinfo
	WHERE type = "S" AND "&env_start_date."d <= datepart(end_Date)
	AND stock_code IN ("000300", "399102", "399101","000001","000906","000905","399982","399984")
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

/** 4-�ɱ���Ϣ */
PROC SQL;
	CREATE TABLE fg_wind_freeshare AS
	SELECT datepart(end_date) As end_date, stock_code,
	freeshare,
	a_share,
	total_share,
	liqa_share
	FROM database.fg_wind_freeshare
	WHERE "&env_start_date."d <= datepart(end_Date)
	ORDER BY end_date, stock_code;
QUIT;


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

/** Ѱ������һ��ͣ�����ڣ������գ������ж�ͣ�Ƹ�������� */
PROC SORT DATA = market_table;
	BY stock_code end_date;
RUN;
DATA market_table(drop = r_last_halt r_last_trade);
	SET market_table;
	BY stock_code;
	RETAIN r_last_halt .;
	RETAIN r_last_trade .;
	IF first.stock_code THEN DO;
		r_last_halt = .;
		r_last_trade = .;
	END;
	last_halt = r_last_halt;
	last_trade = r_last_trade;
	IF is_halt = 1 THEN r_last_halt = end_date;
	IF is_halt = 0 THEN r_last_trade = end_date;
	FORMAT last_halt last_trade  yymmdd10.;
RUN;
%cal_date_intval(busday_table=busday, input_table=market_table, date1=last_halt, date2=end_date, 
						intval_col=halt_days0, output_table=market_table);
%cal_date_intval(busday_table=busday, input_table=market_table, date1=last_trade, date2=end_date, 
						intval_col=trade_days0, output_table=market_table);
/** ���¶��� */
/** (1) is_halt = 1�� halt_days = trade_days0 ����ͣ������(������)
	(2) is_halt= 0: trade_days=halt_days0������һ��ͣ�ƿ�ʼ����N����������(������)�������ƺ��N�졣���ֶζ���is_halt=1������
					ͨ�����ո�����trade_days=1��(��Ϊ����ֵ�Ŀ����ǣ��м�����������ȱʧ��һ������ʱ����)
	(3) �������ո��ƣ�����halt_days = trade_days0-1 ����ͣ��N�պ���(���첻����ͣ�������ļ���)�� 
**/
DATA market_table(drop = halt_days0 trade_days0);
	SET market_table;
	IF is_halt = 1 THEN halt_days = trade_days0;
	IF is_halt = 0 THEN trade_days = halt_days0;
	IF is_resumption = 1 THEN DO;
		halt_days = trade_days0 - 1;
	END;
RUN;
