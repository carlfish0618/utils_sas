/*** ���Ӳ��Է�����IC���� **/
%LET utils_dir = D:\Research\GIT-BACKUP\utils\SAS\�޸İ汾; 
LIBNAME database "D:\Research\���ݿ�\ͨ��";
%LET excel_path = D:\test.xls;


/** Step1: ���������ļ�(����Ϲ����������ļ���ͬ) */
%LET pfolio_env_start_date = 15dec2012;
%INCLUDE "&utils_dir.\������Ч��_�����ļ�_���ذ汾.sas";

/*** Step2: �����������׼�� */
%LET adjust_start_date_pre = 15dec2012;
%LET adjust_end_date_suf = 31dec2100;
%LET start_intval = 0;
%LET end_intval = 12;



/** ��Step3: �����������ӡ� */
/* Ҫ���ֶΣ�end_date, stock_code, ����*/
%LET fname = vol;
PROC SQL;
	CREATE TABLE score_pool AS
	SELECT end_date, stock_code, vol,value
	FROM hqinfo
	WHERE substr(stock_code,1,3) = "300";
QUIT;



/*********************************** ִ�н׶� ********************/
/*** ��Step1��: �����¼����ܵ����ڴ��� **/
/** ���ú����б�
(1) ����_ͨ�ú���/get_month_date: ������ĩ���� --> �ɸ���Ϊget_daily_date�ȴ�����ͬƵ�ʵ�����
(2) ����_ͨ�ú���/get_date_windows���������������ڴ���
*/
%get_month_date(busday_table=busday, start_date=&adjust_start_date_pre., end_date=&adjust_end_date_suf., 
	rename=end_date, output_table=adjust_busdate, type=1);

%get_date_windows(raw_table=adjust_busdate, colname=end_date, output_table = adjust_busdate2, 
						start_intval = &start_intval., end_intval = &end_intval.);

/** ��Step2��: ���������ӱ� */
PROC SQL;
	CREATE TABLE hqinfo_subset AS
	SELECT end_date, stock_code, close*factor AS price
	FROM hqinfo
	WHERE end_date in (
		SELECT end_date FROM adjust_busdate)
	AND stock_code IN (
		SELECT stock_code FROM score_pool)
	ORDER BY end_date, stock_code;
QUIT;

/*** ��Step3��: ���㵥������ۼ����������� */
/* ���ú����б�
(1) ������Ч��_ͨ�ú���/cal_intval_return���趨is_single���Բ��õ�������ۼ�����
**/
%cal_intval_return(raw_table=hqinfo_subset, group_name=stock_code, price_name=price, date_table=adjust_busdate2, output_table=ot2, is_single = 1);


/***** ��Step4��: ���㵥���ӵ�IC�͸��ǳ̶� */
/** �����
(1) &fname._cover: ���Ƕ�
(2) &fname._ic : IC
(3) &fname._dist: distribution
**/
/*** ���ú����б�
(1) ������Ч��_ͨ�ú���/single_factor_ic: �趨type,�����ͬ�Ľ����1- ȫ����� 2- p_ic 3- s_ic 4- n_obs
(2) ������Ч��_ͨ�ú���/test_single_factor_ic: ��һ��������ͬ����ɸ��Ƕȡ�ic�ͷֲ���ͳ��
**/

/*****��ѡ��1��:�ֲ�ִ��*****/
/** Step4A: IC*/
%single_factor_ic(factor_table=score_pool, return_table=ot2, group_name=stock_code, 
		fname=&fname., output_table=&fname._ic, type=3);
/** Step4B: cover */
/** ���Ƕ� */
PROC SQL;
	CREATE TABLE &fname._cover AS
	SELECT end_date, sum(not missing(&fname.))/count(1) AS pct
	FROM score_pool
	GROUP BY end_date;
QUIT;
/** Step4C: �ֲ���� */
%cal_dist(input_table=score_pool, by_var=end_date, cal_var=&fname., out_table=stat);
PROC SQL;
	CREATE TABLE &fname._dist AS
	SELECT sum(obs) AS nobs,
		mean(mean) AS mean,
		mean(std) AS std,
		mean(p100) AS p100,
		mean(p90) AS p90,
		mean(p75) AS p75,
		mean(p50) AS p50,
		mean(p25) AS p25,
		mean(p10) AS p10,
		mean(p0) AS p0
	FROM stat;
QUIT;

/** �б�Ҫ����������ⲿ�ļ� */
/*%output_to_csv(csv_path=&output_dir.\&fname._cover.csv, input_table=&fname._cover);*/
/*%output_to_csv(csv_path=&output_dir.\&fname._ic.csv, input_table=&fname._ic);*/

/**** ��ѡ��2��:Ĭ�ϼ���spearman_ic����һ��������ʵ��cover/dist�ļ���***/
%test_single_factor_ic(factor_table=score_pool, return_table=ot2, group_name=stock_code, fname=&fname.);

/******** ��Step5��: ��������ӵ�IC�͸��ǳ̶� */
/*** ���ú����б�
(1) ������Ч��_ͨ�ú���/loop_factor_ic: �趨type,�����ͬ�Ľ����1- ȫ����� 2- p_ic 3- s_ic 4- n_obs
(2) ������Ч��_ͨ�ú���/test_multiple_factor_ic: ��һ��������ͬ����ɸ��Ƕȡ�ic�ͷֲ���ͳ��
**/

/*** ��ѡ��1�����ֻ����ic��� **/
%loop_factor_ic(factor_table=score_pool, return_table=ot2, group_name=stock_code, type=3, 
					exclude_list=(''));

/*** ��ѡ��2�� ����������ӷֲ������Ƕȵ� */
%test_multiple_factor_ic(factor_table=score_pool, return_table=ot2, group_name=stock_code, exclude_list=(''));


/******** ��Step6����ѡ��: ���ܶ����ӵĽ��������Excel�鿴 ***/
/** ���Ҫ��˥������������merge_var=s_ic_f1Ϊ�����ģ���s_ic_f2�ȼ��� */
%merge_multiple_factor_ic_result(factor_table=score_pool, merge_var=s_ic_f1, suffix=ic, is_hit=0,
				output_table=ic_stat, exclude_list=(''));
%merge_multiple_factor_ic_result(factor_table=score_pool, merge_var=s_ic_f1, suffix=ic, is_hit=1,
				output_table=ic_hit_stat, exclude_list=(''));
%merge_multiple_factor_ic_result(factor_table=score_pool, merge_var=pct, suffix=cover, is_hit=0,
				output_table=ic_cover, exclude_list=(''));

