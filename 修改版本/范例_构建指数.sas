/** 指数构建范例 **/
%LET utils_dir = D:\Research\GIT-BACKUP\utils\SAS\修改版本; 
LIBNAME database "D:\Research\数据库\通用";
%LET excel_path = D:\test.xls;


/** Step1: 调用配置文件 */
%LET pfolio_env_start_date = 15dec2012;
%INCLUDE "&utils_dir.\组合构建_配置文件_本地版本.sas";

/*** Step2: 参数和输入表准备 */
%LET adjust_start_date = 31dec2014;   
%LET adjust_end_date = 31dec2100;
%LET test_start_date = 1jan2015;   
%LET test_end_date = 31dec2015;


/* (1) 调仓日期: 每个月月末 */
%get_month_date(busday_table=busday, start_date=&adjust_start_date., end_date=&adjust_end_date., 
					rename=end_date, output_table=adjust_busdate, type=1);
/* (2) 回测日期：每日 */
%get_daily_date(busday_table=busday, start_date=&test_start_date., end_date=&test_end_date., 
					rename=date, output_table=test_busdate);

/** 【(3) 构建虚拟成分股文件】 */
/* 要求字段：end_date, stock_code, weight */
PROC SQL;
	CREATE TABLE stock_pool AS
	SELECT end_date AS date, stock_code, 1 AS weight
	FROM hqinfo
	WHERE substr(stock_code,1,3) = "300";
QUIT;
/**注意：这里move_date_forward =0.所以二者使用的adjust_busdate是一样的。
如果遇到move_date_forward=1则gen_adjust_pool里的adjust_date_table实际是effective_date_table*/
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



/***** Step2: 构建指数文件 */
/** 【Example1：绝对收益】 */
/** 关键：index_result_type=2, 允许bm_index_table不存在 */
%construct_index(test_pool=stock_pool, adjust_date=adjust_busdate, test_date=test_busdate,
			bm_index_table=.,   
			output_index_table=bm_index, output_stat_table=bm_stat, output_trade_table=bm_trade, 
			excel_path=., sheet_name_index=., sheet_name_stat=., sheet_name_trade=., 
			start_date=&test_start_date., end_date=&test_end_date., 
			index_result_type = 2,
			is_output=0, annualized_factor=12);

/** 增加输出 */
%construct_index(test_pool=stock_pool, adjust_date=adjust_busdate, test_date=test_busdate,
			bm_index_table=.,   
			output_index_table=bm_index, output_stat_table=bm_stat, output_trade_table=bm_trade, 
			excel_path=&excel_path., sheet_name_index=index, sheet_name_stat=stat, sheet_name_trade=trade, 
			start_date=&test_start_date., end_date=&test_end_date., 
			index_result_type = 2,
			is_output=1, annualized_factor=12);

/** 【Example2：相对收益】 */

%construct_index(test_pool=index_pool, adjust_date=adjust_busdate, test_date=test_busdate,
			bm_index_table=bm_index,   
			output_index_table=zxb_index, output_stat_table=zxb_stat, output_trade_table=zxb_trade, 
			excel_path=., sheet_name_index=., sheet_name_stat=., sheet_name_trade=., 
			start_date=&test_start_date., end_date=&test_end_date., 
			index_result_type = 1,
			is_output=0, annualized_factor=12);

/** 【Example3: 纯指数简化版】*/
%construct_index_neat(test_pool=index_pool, adjust_date=adjust_busdate, test_date=test_busdate,
		output_index_table=bm_index, excel_path=., is_output=0);
%construct_index_neat(test_pool=index_pool, adjust_date=adjust_busdate, test_date=test_busdate,
		output_index_table=bm_index, excel_path=&excel_path., is_output=1);
