/** ======================˵��=================================================**/

/*** �������ܣ��ṩ�뽻����ص�ͨ�ú��� **** /
/**** �����б�:
(1) gen_adjust_pool: �����������ּ�¼ --> !!��Ҫ�õ��ⲿ��
(2) gen_daily_pool: ���ݵ�����������ÿ��Ĺ�Ʊ�� --> !!��Ҫ�õ��ⲿ��
(3) cal_stock_wt_ret: ����������ȷ�����ÿ�����׼ȷ��weight�͵������棨�ð汾����ÿ���������) --> !!��Ҫ�õ��ⲿ��
****/ 

/** =======================================================================**/

/** ���ø�������: */
%LET code_dir = F:\Research\GIT_BACKUP\utils\�޸İ汾;
%INCLUDE "&code_dir.\����_ͨ�ú���.sas";
%INCLUDE "&code_dir.\����_ͨ�ú���.sas";


/** ģ��1: ���ɽ����嵥 */
/** ����: 
(1) daily_stock_pool: date/pre_date/adjust_date/stock_code/
		open_wt(open_wt_c)/close_wt(close_wt_c)/pre_close_wt(pre_close_wt_c)/after_close_wt(after_close_wt_c)/����
(2) adjust_date_table: end_date
(3) type(logical): 1- �ԡ�����ǰ���̼ۡ������Ȩ��Ϊ��׼����; 0- �ԡ����ڸ�Ȩ���ӡ������Ȩ��Ϊ��׼����
***/ 

/** �������ű�: ��Ϊ���׷�����date�����̺�
(1) ��Ʊ�����嵥(output_stock_trading); ����date/stock_code/initial_wt/traded_wt/final_wt/status/trade_type/trans_cost(�ݶ�Ϊ����0.35%)
(2) ÿ�콻���嵥(output_daily_trading): ����date/delete_assets/added_assets/sell_wt/buy_wt/buy_cost/sell_cost/turnover(˫�ߺ�)

����:
(a) status(������״̬): 0-��������; -1-����; 1-����
(b) trade_type(������״̬): 0-����; 1-����; -1-����
(c) traded_wt: ����Ȩ��
(d) initial_wt: ����ǰȨ��
(e) final_wt: ������Ȩ��  

***/



%MACRO trading_summary(daily_stock_pool, adjust_date_table, output_stock_trading, output_daily_trading, trans_cost = 0.0035, type = 1);
	
	/* Step1: ֻ���ǵ����պ͵�����֮���һ�� */
	%IF %SYSEVALF(&type. = 1) %THEN %DO;
		PROC SQL;
			CREATE TABLE tt_stock_pool AS
			SELECT date, pre_date, stock_code, 
			close_wt_c AS close_wt, 
			open_wt_c AS open_wt,
			pre_close_wt_c AS pre_close_wt, 
			after_close_wt_c AS after_close_wt
			FROM &daily_stock_pool.
			WHERE date IN  (SELECT end_date FROM &adjust_date_table.) 
			OR pre_date IN (SELECT end_date FROM &adjust_date_table.);
		QUIT;
	%END;
	%ELSE %DO;
		PROC SQL;
			CREATE TABLE tt_stock_pool AS
			SELECT date, pre_date, stock_code, 
			close_wt, 
			open_wt,
			pre_close_wt, 
			after_close_wt
			FROM &daily_stock_pool.
			WHERE date IN  (SELECT end_date FROM &adjust_date_table.) 
			OR pre_date IN (SELECT end_date FROM &adjust_date_table.);
		QUIT;
	%END;
		
	/* Step2: ȷ����Ʊ�䶯*/
	DATA tt_stock_pool;
		SET tt_stock_pool;
		/* ��Ե�����(����ֱ�ӻ�ȡ���������л���������Ʊ����Ϣ) */
		IF after_close_wt = 0 THEN status = -1;  /* ��ʾ�޳� */
		ELSE status = 0; /* ���� */
		initial_wt = close_wt;
		traded_wt = after_close_wt - close_wt;
		final_wt = after_close_wt;
		IF after_close_wt - close_wt > 0 THEN trade_type = 1;
		ELSE IF after_close_wt = close_wt THEN trade_type = 0;
		ELSE trade_type = -1;
		/* ��Ե�����֮���һ��(Ϊ�˻�ȡ��������Ĺ�Ʊ��Ϣ)*/
		IF pre_close_wt = 0 THEN status_p = 1;
		ELSE status_p = 0;
		initial_wt_p = pre_close_wt;
		traded_wt_p = open_wt - pre_close_wt;
		final_wt_p = open_wt;
		IF open_wt - pre_close_wt > 0 THEN trade_type_p = 1;
		ELSE IF open_wt = pre_close_wt  THEN trade_type_p = 0;
		ELSE trade_type_p = -1;
	RUN;

	/* Step3�������еĵ��ֶ���Ϊ���������̺� */
	/* ȷ�������������еĵ����� */
	PROC SQL;
		CREATE TABLE tt_adjust AS
		SELECT distinct adjust_date AS date
		FROM &daily_stock_pool.;
	QUIT;
		
	PROC SQL;
		CREATE TABLE tmp1 AS
		SELECT A.date, B.stock_code,
		B.status, B.trade_type, B.initial_wt, B.traded_wt, B.final_wt
		FROM tt_adjust A LEFT JOIN tt_stock_pool B
		ON A.date = B.date /* �ڵ�һ�ε���ʱ, stock_code_b����Ϊ�� */
		WHERE not missing(stock_code)
		ORDER BY A.date;

		CREATE TABLE tmp2 AS
		SELECT A.date, B.stock_code, B.status_p, B.trade_type_p, 
		B.initial_wt_p, B.traded_wt_p, B.final_wt_p
		FROM tt_adjust A LEFT JOIN tt_stock_pool B
		ON A.date = B.pre_date
		WHERE not missing(stock_code)
		ORDER BY A.date;

		CREATE TABLE tt_stock_pool AS  /* ȡ��������Ʊ���û�е�������ģ���tmp1��tmp2����һ���� */
		SELECT *
		FROM tmp1 UNION
		(SELECT date, stock_code, status_p AS status, trade_type_p AS trade_type,
		initial_wt_p AS initial_wt, traded_wt_p AS traded_wt, final_wt_p AS final_wt
		FROM tmp2) 
		ORDER BY date, stock_code;
	QUIT;
	DATA &output_stock_trading.;
		SET tt_stock_pool;
		trans_cost = abs(traded_wt) * &trans_cost.;
	RUN;

	/* Step4: ͳ��ÿ������ */
	PROC SQL;
		CREATE TABLE &output_daily_trading. AS
		SELECT date, sum(status=1) AS added_assets, sum(status=-1) AS deleted_assets,
			sum(traded_wt *(traded_wt>0)) AS buy_wt, 
			- sum(traded_wt * (traded_wt<0)) AS sell_wt,
			sum(trans_cost *(traded_wt>0))  AS buy_cost,
			sum(trans_cost * (traded_wt<0)) AS sell_cost,
			sum(traded_wt *(traded_wt>0)) - sum(traded_wt * (traded_wt<0)) AS turnover
		FROM &output_stock_trading.
		GROUP BY date;
	QUIT;
	
	PROC SQL;
		DROP TABLE tt_stock_pool, tmp1, tmp2, tt_adjust;
	QUIT;
%MEND trading_summary;
