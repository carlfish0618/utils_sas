/*** ����ָ�� **/
/*** �����б�
(1) construct_index: ����ָ������ͳ�ƾ��������alpha
(2) construct_index_neat: ������ָ������

***/

/** ģ��1������ָ��*/
/** ���룺
(1) test_pool: end_date/stock_code/weight
(2) adjust_date: end_date
(3) test_date: date
(4) bm_index_table; date/daily_ret 
	(��index_result_type=2ʱ��ǿ�ƽ��ñ��趨Ϊoutput_index_table��������ָ������˵�index_result_type=1ʱ���ñ����Ϊȱʧ)
(5) output_index_table: �����ָ���ļ�������date/
(6) output_stat_table: ��������׼�ıȽϽ��
(7) output_trade_table: ����Ļ�����
(8) excel_path: �����excel����is_output=1ʱ��Ч
(9) sheet_name_index/sheet_name_stat/sheet_name_trade:�ֱ��Ӧ�������ű��sheet��
(10) start_date: ��׼�ȽϿ�ʼ������
(11) end_date: ��׼�ȽϽ���������
(12) index_result_type: 1- ����alpha, 2-������������
(13) is_output: �Ƿ���� 
(14) annualizd_factor: ���㻻�����껯ʱ��Ҫ�õ�factor
***/


%MACRO construct_index(test_pool, adjust_date, test_date,bm_index_table,
			output_index_table, output_stat_table, output_trade_table, 
			excel_path, sheet_name_index, sheet_name_stat, sheet_name_trade, 
			start_date, end_date, 
			index_result_type = 2, is_output=0, annualized_factor=12);
	%neutralize_weight(stock_pool=&test_pool., output_stock_pool=&test_pool._copy,col=weight);
	%gen_daily_pool(stock_pool=&test_pool._copy, test_period_table=test_busdate, 
		adjust_date_table=adjust_busdate, output_stock_pool=&test_pool._copy);
	%cal_stock_wt_ret(daily_stock_pool=&test_pool._copy, adjust_date_table=adjust_busdate, output_stock_pool=&test_pool._copy);
	%cal_portfolio_ret(daily_stock_pool=&test_pool._copy, output_daily_summary=&output_index_table., type=1); /* ����ǰ�ռ��� */
	%IF %SYSEVALF(&index_result_type.=1) %THEN %DO;
		%eval_pfmance(index_pool=&output_index_table., bm_pool=&bm_index_table., index_ret=daily_ret, 
			bm_ret=daily_ret, start_date=&start_date., end_date=&end_date., type=1, 
				output_table=&output_stat_table.);
	%END;
	%ELSE %IF %SYSEVALF(&index_result_type.=2) %THEN %DO;
		%eval_pfmance(index_pool=&output_index_table., bm_pool=., index_ret=daily_ret, 
			bm_ret=daily_ret, start_date=&start_date., end_date=&end_date., type=2, 
				output_table=&output_stat_table.);
	%END;
	/** ͳ�ƻ����� */
	/** ���������˫�ߵ� */
	%trading_summary(daily_stock_pool=&test_pool._copy, adjust_date_table=adjust_busdate,
		output_stock_trading=tt, output_daily_trading=&output_trade_table., trans_cost = 0.0035, type = 1); /* ����ǰ�ռ��� */
	DATA &output_trade_table.;
		SET &output_trade_table.;
		year = year(date);
	RUN;
	/** �������Ϊ���� */
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT 0 AS year, sum(sell_wt)/(count(sell_wt)-1)*&annualized_factor. AS turnover /* �ʼ������Ϊ0��������� */
		FROM &output_trade_table.;

		CREATE TABLE tmp2 AS
		SELECT year, sum(sell_wt) AS turnover
		FROM &output_trade_table.
		GROUP BY year;
	QUIT;
	DATA tmp;
		SET tmp tmp2;
	RUN;
	PROC SQL;
		CREATE TABLE tmp2 AS
		SELECT A.*, B.turnover AS turnover_uni  /* ���� */
		FROM &output_stat_table. A LEFT JOIN tmp B
		ON A.year = B.year
		ORDER BY A.year;
	QUIT;
	DATA &output_stat_table.;
		SET tmp2;
	RUN;
	/** ͳ�ƹ�Ʊ����*/
	DATA &output_index_table.;
		SET &output_index_table.;
		year = year(date);
	RUN;
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT 0 AS year, mean(nstock) AS nstock /* �ʼ������Ϊ0��������� */
		FROM &output_index_table.;

		CREATE TABLE tmp2 AS
		SELECT year, mean(nstock) AS nstock
		FROM &output_index_table.
		GROUP BY year;
	QUIT;
	DATA tmp;
		SET tmp tmp2;
	RUN;
	PROC SQL;
		CREATE TABLE tmp2 AS
		SELECT A.*, B.nstock
		FROM &output_stat_table. A LEFT JOIN tmp B
		ON A.year = B.year
		ORDER BY A.year;
	QUIT;
	DATA &output_stat_table.;
		SET tmp2;
	RUN;
	/** ���*/
	%IF %SYSEVALF(&is_output.=1) %THEN %DO;
		%output_to_excel(excel_path=&excel_path., input_table=&output_index_table., sheet_name = &sheet_name_index.);
		%output_to_excel(excel_path=&excel_path., input_table=&output_stat_table., sheet_name = &sheet_name_stat.);
		%output_to_excel(excel_path=&excel_path., input_table=&output_trade_table., sheet_name = &sheet_name_trade.);
	%END;
	PROC SQL;
		DROP TABLE tt, tmp, tmp2;
	QUIT;

%MEND construct_index;


/** ģ��1��������ָ�������ļ�*/
/** ���룺
(1) test_pool: end_date/stock_code/weight
(2) adjust_date: end_date
(3) test_date: date
(4) output_index_table: �����ָ���ļ�������date/
(5) excel_path: �����excel����is_output=1ʱ��Ч
(6) is_output: �Ƿ���� 
***/

%MACRO construct_index_neat(test_pool, adjust_date, test_date,output_index_table, excel_path, is_output=0);
	%neutralize_weight(stock_pool=&test_pool., output_stock_pool=&test_pool._copy,col=weight);
	%gen_daily_pool(stock_pool=&test_pool._copy, test_period_table=test_busdate, 
		adjust_date_table=adjust_busdate, output_stock_pool=&test_pool._copy);
	%cal_stock_wt_ret(daily_stock_pool=&test_pool._copy, adjust_date_table=adjust_busdate, output_stock_pool=&test_pool._copy);
	%cal_portfolio_ret(daily_stock_pool=&test_pool._copy, output_daily_summary=&output_index_table., type=1); /* ����ǰ�ռ��� */

	/** ���*/
	%IF %SYSEVALF(&is_output.=1) %THEN %DO;
		%output_to_excel(excel_path=&excel_path., input_table=&output_index_table., sheet_name = index);
	%END;
	PROC SQL;
		DROP TABLE &test_pool._copy;
	QUIT;
%MEND construct_index_neat;
