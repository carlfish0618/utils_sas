/** ����_���Ӽ��� */

/** ��Step1: �����������ӡ� */
/* Ҫ���ֶΣ�end_date, stock_code, ����*/
%LET fname = vol;
PROC SQL;
	CREATE TABLE score_pool AS
	SELECT end_date, stock_code, vol,value
	FROM hqinfo
	WHERE substr(stock_code,1,3) = "300";
QUIT;
PROC SQL;
	CREATE TABLE fg_wind_indus AS
	SELECT end_date, stock_code, o_code AS indus_code, o_name AS indus_name
	FROM fg_wind_sector;
QUIT;

%get_sector_info(stock_table=score_pool, mapping_table=fg_wind_indus, output_stock_table=score_pool);

/** ��Step2: �����Ȩ���ӣ�һ��Ϊfmv_sqr */
%get_stock_size(stock_table=score_pool, info_table=hqinfo, share_table=fg_wind_freeshare,
					output_table=score_pool, colname=fmv_sqr, index = 1);
DATA score_pool;
	SET score_pool;
	fmv_sqr = sqrt(fmv_sqr);
RUN;


/** ��Step3: ���Ӽ��㡿 */
/** ������1�� ��׼�����ӣ�������winsorize��*/
%normalize_multi_score(input_table=score_pool, output_table=score_pool_z, 
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"));
%winsorize_multi_score(input_table=score_pool_z, 
	output_table=score_pool_zw, 
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"), type=1, upper=3, lower = -3);

/*** ������2����׼�������Ի���winsorize�� **/
%normalize_multi_score(input_table=score_pool, output_table=score_pool_z, 
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"));

%neutralize_multi_score(input_table=score_pool_z, 
	output_table=score_pool_zn, group_name=indus_code, 
		exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"));
%winsorize_multi_score(input_table=score_pool_zn, 
	output_table=score_pool_znw, 
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"), type=1, upper=3, lower = -3);
