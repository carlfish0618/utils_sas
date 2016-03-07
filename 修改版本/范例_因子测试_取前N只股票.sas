/*** ���Ӳ��Է�����ȡ������ߵ�min(N,half(nstock))���ֱ𹹽���Ȩ�͵÷ּ�Ȩ��� **/
%LET utils_dir = D:\Research\GIT-BACKUP\utils\SAS\�޸İ汾; 
LIBNAME database "D:\Research\���ݿ�\ͨ��";
%LET excel_path = D:\test.xls;


/** Step1: ���������ļ�(����Ϲ����������ļ���ͬ) */
%LET pfolio_env_start_date = 15dec2012;
%INCLUDE "&utils_dir.\������Ч��_�����ļ�_���ذ汾.sas";

%INCLUDE "&utils_dir.\����_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\Ȩ��_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\����_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\����_ͨ�ú���.sas";

%INCLUDE "&utils_dir.\����_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\��Ч����_ͨ�ú���.sas";

%INCLUDE "&utils_dir.\��Ϲ���_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\�ۺϹ���ָ��_ͨ�ú���.sas";

%INCLUDE "&utils_dir.\���Ӽ���_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\������Ч��_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\������Ч���ۺϲ���_ͨ�ú���.sas";

;

/*** Step2: �����������׼�� */
%LET adjust_start_date = 31dec2014;   
%LET adjust_end_date = 31dec2100;
%LET test_start_date = 1jan2015;   
%LET test_end_date = 31dec2015;

%LET fname = vol;

/** ��Step3: �����������ӡ� */
/* Ҫ���ֶΣ�end_date, stock_code, ����*/
%LET fname = vol;
PROC SQL;
	CREATE TABLE score_pool AS
	SELECT end_date, stock_code, vol,value, input(substr(stock_code,1,1),8.) AS bmark
	FROM hqinfo
	WHERE substr(stock_code,1,3) in( "300","002");
QUIT;

/*** ��Step4: ��������universe�� */
/** ��Ȩ��� */
PROC SQL;
	CREATE TABLE bm_equal_pool AS
	SELECT end_date, stock_code, 1 AS weight
	FROM score_pool;
QUIT;

/** ��Ȩ��� */
PROC SQL;
	CREATE TABLE bm_weight_pool AS
	SELECT end_date, stock_code
	FROM score_pool;
QUIT;
%get_stock_size(stock_table=bm_weight_pool, info_table=hqinfo, share_table=fg_wind_freeshare,
	output_table=bm_weight_pool, colname=weight, index = 1);



/*********************************** ִ�н׶� ********************/
/*** ��Step1��: ���ɻ�׼�ļ� **/
/* ��������: ÿ������ĩ */
%get_month_date(busday_table=busday, start_date=&adjust_start_date., end_date=&adjust_end_date., 
					rename=end_date, output_table=adjust_busdate, type=1);
/* �ز����ڣ�ÿ�� */
%get_daily_date(busday_table=busday, start_date=&test_start_date., end_date=&test_end_date., 
					rename=date, output_table=test_busdate);
/** ������Ȩ��׼ָ��*/
%construct_index_neat(test_pool=bm_equal_pool, adjust_date=adjust_busdate, test_date=test_busdate,
		output_index_table=bm_equal_index, excel_path=., is_output=0);
/** �����Ȩ��׼���*/
%construct_index_neat(test_pool=bm_weight_pool, adjust_date=adjust_busdate, test_date=test_busdate,
		output_index_table=bm_weight_index, excel_path=., is_output=0);


/*** ��Step2��: ȥ�÷���ߵ�100ֻ��Ʊ���ԱȲ�ͬ����ϻ�׼��alpha */
/** ��Example1: ��������������alpha��*/
%test_single_cfactor_higher_ret(factor_table=score_pool, fname=&fname., 
				bm_index=bm_equal_index, index_result_type=1,
				adjust_date=adjust_busdate, test_date=test_busdate,
				start_date=&test_start_date., end_date=&test_end_date., 
				annualized_factor=12,
				nstock=100);


/** ��Example2: ��������������������桿*/
%test_single_cfactor_higher_ret(factor_table=score_pool, fname=&fname., 
				bm_index=., index_result_type=2,
				adjust_date=adjust_busdate, test_date=test_busdate,
				start_date=&test_start_date., end_date=&test_end_date., 
				annualized_factor=12,
				nstock=20);


/** ��Example3: ��ɢ����������alpha��*/
%test_single_dfactor_higher_ret(factor_table=score_pool, fname=bmark, fname_value=0, 
				bm_index=bm_equal_index, index_result_type=1,
				adjust_date=adjust_busdate, test_date=test_busdate,
				start_date=&test_start_date., end_date=&test_end_date., 
				annualized_factor=12);


/** ��Example4: ��ɢ����������������桿*/
%test_single_dfactor_higher_ret(factor_table=score_pool, fname=bmark, fname_value=0, 
				bm_index=., index_result_type=2,
				adjust_date=adjust_busdate, test_date=test_busdate,
				start_date=&test_start_date., end_date=&test_end_date., 
				annualized_factor=12);

/** ��Example5: ����������������ӣ�����alpha��*/
%test_multiple_factor_higher_ret(factor_table=score_pool,
				bm_index=bm_equal_index, index_result_type=1,
				adjust_date=adjust_busdate, test_date=test_busdate,
				start_date=&test_start_date., end_date=&test_end_date., 
				annualized_factor=12,
				var=100,
				is_continuous=1,
				exclude_list=('BMARK'));

/* ����ݺͱ������ܽ�������м�Ȩ��Ͻ������������ */
%merge_result_higher_total(factor_table=score_pool, suffix=100_stat_w, 
			merge_var=accum_ret, output_table=top_stat_w, exclude_list=('BMARK'));
%merge_result_higher_total(factor_table=score_pool, suffix=100_stat_e, 
			merge_var=accum_ret, output_table=top_stat_e, exclude_list=('BMARK'));


/** ��Example5: ��ɢ������������ӣ�����alpha��*/
%test_multiple_factor_higher_ret(factor_table=score_pool,
				bm_index=bm_equal_index, index_result_type=1,
				adjust_date=adjust_busdate, test_date=test_busdate,
				start_date=&test_start_date., end_date=&test_end_date., 
				annualized_factor=12,
				var=0,
				is_continuous=0,
				exclude_list=('VOL','VALUE'));

%merge_result_higher_total(factor_table=score_pool, suffix=0_stat_e, 
			merge_var=accum_ret, output_table=top_stat_e, exclude_list=('VOL','VALUE'));


