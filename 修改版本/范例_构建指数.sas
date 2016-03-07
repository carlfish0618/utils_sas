/** ָ���������� **/
%LET utils_dir = D:\Research\GIT-BACKUP\utils\SAS\�޸İ汾; 
LIBNAME database "D:\Research\���ݿ�\ͨ��";
%LET excel_path = D:\test.xls;


/** Step1: ���������ļ� */
%LET pfolio_env_start_date = 15dec2012;
%INCLUDE "&utils_dir.\��Ϲ���_�����ļ�_���ذ汾.sas";

/*** Step2: �����������׼�� */
%LET adjust_start_date = 31dec2014;   
%LET adjust_end_date = 31dec2100;
%LET test_start_date = 1jan2015;   
%LET test_end_date = 31dec2015;


/* (1) ��������: ÿ������ĩ */
%get_month_date(busday_table=busday, start_date=&adjust_start_date., end_date=&adjust_end_date., 
					rename=end_date, output_table=adjust_busdate, type=1);
/* (2) �ز����ڣ�ÿ�� */
%get_daily_date(busday_table=busday, start_date=&test_start_date., end_date=&test_end_date., 
					rename=date, output_table=test_busdate);

/** ��(3) ��������ɷֹ��ļ��� */
/* Ҫ���ֶΣ�end_date, stock_code, weight */
PROC SQL;
	CREATE TABLE stock_pool AS
	SELECT end_date AS date, stock_code, 1 AS weight
	FROM hqinfo
	WHERE substr(stock_code,1,3) = "300";
QUIT;
/**ע�⣺����move_date_forward =0.���Զ���ʹ�õ�adjust_busdate��һ���ġ�
�������move_date_forward=1��gen_adjust_pool���adjust_date_tableʵ����effective_date_table*/
%gen_adjust_pool(stock_pool=stock_pool, adjust_date_table=adjust_busdate, move_date_forward=0, output_stock_pool=stock_pool, busday_table = busday);
%adjust_pool_mdf(stock_pool=stock_pool, hq_table=hqinfo, output_table=stock_pool, threshold_rtn=0.095);

PROC SQL;
	CREATE TABLE index_pool AS
	SELECT end_date AS date, stock_code, 1 AS weight
	FROM hqinfo
	WHERE substr(stock_code,1,3) = "002";
QUIT;
%gen_adjust_pool(stock_pool=index_pool, adjust_date_table=adjust_busdate, move_date_forward=0, output_stock_pool=index_pool);
%adjust_pool_mdf(stock_pool=index_pool, hq_table=hqinfo, output_table=index_pool, threshold_rtn=0.095);



/***** Step2: ����ָ���ļ� */
/** ��Example1���������桿 */
/** �ؼ���index_result_type=2, ����bm_index_table������ */
%construct_index(test_pool=stock_pool, adjust_date=adjust_busdate, test_date=test_busdate,
			bm_index_table=.,   
			output_index_table=bm_index, output_stat_table=bm_stat, output_trade_table=bm_trade, 
			excel_path=., sheet_name_index=., sheet_name_stat=., sheet_name_trade=., 
			start_date=&test_start_date., end_date=&test_end_date., 
			index_result_type = 2,
			is_output=0, annualized_factor=12);

/** ������� */
%construct_index(test_pool=stock_pool, adjust_date=adjust_busdate, test_date=test_busdate,
			bm_index_table=.,   
			output_index_table=bm_index, output_stat_table=bm_stat, output_trade_table=bm_trade, 
			excel_path=&excel_path., sheet_name_index=index, sheet_name_stat=stat, sheet_name_trade=trade, 
			start_date=&test_start_date., end_date=&test_end_date., 
			index_result_type = 2,
			is_output=1, annualized_factor=12);

/** ��Example2��������桿 */

%construct_index(test_pool=index_pool, adjust_date=adjust_busdate, test_date=test_busdate,
			bm_index_table=bm_index,   
			output_index_table=zxb_index, output_stat_table=zxb_stat, output_trade_table=zxb_trade, 
			excel_path=., sheet_name_index=., sheet_name_stat=., sheet_name_trade=., 
			start_date=&test_start_date., end_date=&test_end_date., 
			index_result_type = 1,
			is_output=0, annualized_factor=12);

/** ��Example3: ��ָ���򻯰桿*/
%construct_index_neat(test_pool=index_pool, adjust_date=adjust_busdate, test_date=test_busdate,
		output_index_table=bm_index, excel_path=., is_output=0);
%construct_index_neat(test_pool=index_pool, adjust_date=adjust_busdate, test_date=test_busdate,
		output_index_table=bm_index, excel_path=&excel_path., is_output=1);
