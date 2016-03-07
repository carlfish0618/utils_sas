/*** 因子测试范例：IC分析 **/
%LET utils_dir = D:\Research\GIT-BACKUP\utils\SAS\修改版本; 
LIBNAME database "D:\Research\数据库\通用";
%LET excel_path = D:\test.xls;


/** Step1: 调用配置文件(与组合构建的配置文件相同) */
%LET pfolio_env_start_date = 15dec2012;
%INCLUDE "&utils_dir.\因子有效性_配置文件_本地版本.sas";

/*** Step2: 参数和输入表准备 */
%LET adjust_start_date_pre = 15dec2012;
%LET adjust_end_date_suf = 31dec2100;
%LET start_intval = 0;
%LET end_intval = 12;



/** 【Step3: 构建虚拟因子】 */
/* 要求字段：end_date, stock_code, 因子*/
%LET fname = vol;
PROC SQL;
	CREATE TABLE score_pool AS
	SELECT end_date, stock_code, vol,value
	FROM hqinfo
	WHERE substr(stock_code,1,3) = "300";
QUIT;



/*********************************** 执行阶段 ********************/
/*** 【Step1】: 创建事件可能的日期窗口 **/
/** 调用函数列表
(1) 日期_通用函数/get_month_date: 创建月末日期 --> 可更改为get_daily_date等创建不同频率的日期
(2) 日期_通用函数/get_date_windows：生成连续的日期窗口
*/
%get_month_date(busday_table=busday, start_date=&adjust_start_date_pre., end_date=&adjust_end_date_suf., 
	rename=end_date, output_table=adjust_busdate, type=1);

%get_date_windows(raw_table=adjust_busdate, colname=end_date, output_table = adjust_busdate2, 
						start_intval = &start_intval., end_intval = &end_intval.);

/** 【Step2】: 创建行情子表 */
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

/*** 【Step3】: 计算单区间或累计区间收益率 */
/* 调用函数列表：
(1) 因子有效性_通用函数/cal_intval_return：设定is_single，以采用单区间或累计区间
**/
%cal_intval_return(raw_table=hqinfo_subset, group_name=stock_code, price_name=price, date_table=adjust_busdate2, output_table=ot2, is_single = 1);


/***** 【Step4】: 计算单因子的IC和覆盖程度 */
/** 输出：
(1) &fname._cover: 覆盖度
(2) &fname._ic : IC
(3) &fname._dist: distribution
**/
/*** 调用函数列表：
(1) 因子有效性_通用函数/single_factor_ic: 设定type,输出不同的结果。1- 全部输出 2- p_ic 3- s_ic 4- n_obs
(2) 因子有效性_通用函数/test_single_factor_ic: 在一个函数内同步完成覆盖度、ic和分布的统计
**/

/*****【选择1】:分步执行*****/
/** Step4A: IC*/
%single_factor_ic(factor_table=score_pool, return_table=ot2, group_name=stock_code, 
		fname=&fname., output_table=&fname._ic, type=3);
/** Step4B: cover */
/** 覆盖度 */
PROC SQL;
	CREATE TABLE &fname._cover AS
	SELECT end_date, sum(not missing(&fname.))/count(1) AS pct
	FROM score_pool
	GROUP BY end_date;
QUIT;
/** Step4C: 分布情况 */
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

/** 有必要可以输出到外部文件 */
/*%output_to_csv(csv_path=&output_dir.\&fname._cover.csv, input_table=&fname._cover);*/
/*%output_to_csv(csv_path=&output_dir.\&fname._ic.csv, input_table=&fname._ic);*/

/**** 【选择2】:默认计算spearman_ic，及一个函数内实现cover/dist的计算***/
%test_single_factor_ic(factor_table=score_pool, return_table=ot2, group_name=stock_code, fname=&fname.);

/******** 【Step5】: 计算多因子的IC和覆盖程度 */
/*** 调用函数列表：
(1) 因子有效性_通用函数/loop_factor_ic: 设定type,输出不同的结果。1- 全部输出 2- p_ic 3- s_ic 4- n_obs
(2) 因子有效性_通用函数/test_multiple_factor_ic: 在一个函数内同步完成覆盖度、ic和分布的统计
**/

/*** 【选择1】如果只关心ic情况 **/
%loop_factor_ic(factor_table=score_pool, return_table=ot2, group_name=stock_code, type=3, 
					exclude_list=(''));

/*** 【选择2】 如果关心因子分布、覆盖度等 */
%test_multiple_factor_ic(factor_table=score_pool, return_table=ot2, group_name=stock_code, exclude_list=(''));


/******** 【Step6，可选】: 汇总多因子的结果，方便Excel查看 ***/
/** 如果要看衰退情况，则更改merge_var=s_ic_f1为其他的，如s_ic_f2等即可 */
%merge_multiple_factor_ic_result(factor_table=score_pool, merge_var=s_ic_f1, suffix=ic, is_hit=0,
				output_table=ic_stat, exclude_list=(''));
%merge_multiple_factor_ic_result(factor_table=score_pool, merge_var=s_ic_f1, suffix=ic, is_hit=1,
				output_table=ic_hit_stat, exclude_list=(''));
%merge_multiple_factor_ic_result(factor_table=score_pool, merge_var=pct, suffix=cover, is_hit=0,
				output_table=ic_cover, exclude_list=(''));

