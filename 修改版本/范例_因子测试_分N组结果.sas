/*** 因子测试范例：IC分析 **/
%LET utils_dir = D:\Research\GIT-BACKUP\utils\SAS\修改版本; 
LIBNAME database "D:\Research\数据库\通用";
%LET excel_path = D:\test.xls;


/** Step1: 调用配置文件(与组合构建的配置文件相同) */
%LET pfolio_env_start_date = 15dec2012;
%INCLUDE "&utils_dir.\因子有效性_配置文件_本地版本.sas";

/*** Step2: 参数和输入表准备 */
%LET adjust_start_date = 31dec2014;   
%LET adjust_end_date = 31dec2100;
%LET test_start_date = 1jan2015;   
%LET test_end_date = 31dec2015;

%LET fname = vol;

/** 【Step3: 构建虚拟因子】 */
/* 要求字段：end_date, stock_code, 因子*/
%LET fname = vol;
PROC SQL;
	CREATE TABLE score_pool AS
	SELECT end_date, stock_code, vol,value, input(substr(stock_code,1,1),8.) AS bmark
	FROM hqinfo
	WHERE substr(stock_code,1,3) in( "300","002");
QUIT;

/*** 【Step4: 构建虚拟universe】 */
/** 等权组合 */
PROC SQL;
	CREATE TABLE bm_equal_pool AS
	SELECT end_date, stock_code, 1 AS weight
	FROM score_pool;
QUIT;

/** 加权组合 */
PROC SQL;
	CREATE TABLE bm_weight_pool AS
	SELECT end_date, stock_code
	FROM score_pool;
QUIT;
%get_stock_size(stock_table=bm_weight_pool, info_table=hqinfo, share_table=fg_wind_freeshare,
	output_table=bm_weight_pool, colname=weight, index = 1);



/*********************************** 执行阶段 ********************/
/*** 【Step1】: 生成基准文件 **/
/* 调仓日期: 每个月月末 */
%get_month_date(busday_table=busday, start_date=&adjust_start_date., end_date=&adjust_end_date., 
					rename=end_date, output_table=adjust_busdate, type=1);
/* 回测日期：每日 */
%get_daily_date(busday_table=busday, start_date=&test_start_date., end_date=&test_end_date., 
					rename=date, output_table=test_busdate);
/** 构建等权基准指数*/
%construct_index_neat(test_pool=bm_equal_pool, adjust_date=adjust_busdate, test_date=test_busdate,
		output_index_table=bm_equal_index, excel_path=., is_output=0);
/** 构造加权基准组合*/
%construct_index_neat(test_pool=bm_weight_pool, adjust_date=adjust_busdate, test_date=test_busdate,
		output_index_table=bm_weight_index, excel_path=., is_output=0);


/*** 【Step2】: N组等权，对比不同组相较基准的alpha */
/** 【Example1: 连续变量，考察alpha】*/
%test_single_factor_group_ret(factor_table=score_pool, fname=&fname., bm_index=bm_equal_index, 
				index_result_type=1,
				adjust_date=adjust_busdate, test_date=test_busdate,
				start_date=&test_start_date., end_date=&test_end_date., 
				annualized_factor=12,
				ngroup =3, is_cut=1);


/** 【Example2: 连续变量，考察绝对收益】*/
%test_single_factor_group_ret(factor_table=score_pool, fname=&fname., bm_index=., 
				index_result_type=2,
				adjust_date=adjust_busdate, test_date=test_busdate,
				start_date=&test_start_date., end_date=&test_end_date., 
				annualized_factor=12,
				ngroup =3, is_cut=1);

/** 【Example3: 离散变量，考察alpha】*/
%test_single_factor_group_ret(factor_table=score_pool, fname=bmark, bm_index=bm_equal_index, 
				index_result_type=1,
				adjust_date=adjust_busdate, test_date=test_busdate,
				start_date=&test_start_date., end_date=&test_end_date., 
				annualized_factor=12,
				ngroup =1, is_cut=0);


/** 【Example5: 多个因子，连续变量，考察alpha】*/

/** 连续变量,一定要分3组 */
%test_multiple_factor_group_ret(factor_table=score_pool, bm_index=bm_equal_index, 
				index_result_type=1,
				adjust_date=adjust_busdate, test_date=test_busdate,
				start_date=&test_start_date., end_date=&test_end_date., 
				annualized_factor=12,
				ngroup =3, is_cut=1, exclude_list=('BMARK'));

%merge_factor_3group_result(factor_table=score_pool, merge_var=nstock, output_table=group_nstock, exclude_list=('BMARK'));
%merge_factor_3group_result(factor_table=score_pool, merge_var=accum_ret, output_table=group_ret, exclude_list=('BMARK'));
%merge_factor_3group_result(factor_table=score_pool, merge_var=sd, output_table=group_sd, exclude_list=('BMARK'));
%merge_factor_3group_result(factor_table=score_pool, merge_var=ir, output_table=group_ir, exclude_list=('BMARK'));
%merge_factor_3group_result(factor_table=score_pool, merge_var=hit_ratio_m, output_table=group_hit, exclude_list=('BMARK'));
%merge_factor_3group_result(factor_table=score_pool, merge_var=index_draw, output_table=group_draw, exclude_list=('BMARK'));
