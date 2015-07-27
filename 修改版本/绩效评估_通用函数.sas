/** ======================说明=================================================**/


/**** 函数列表:
(1) eval_pfmance


**/


/** =======================================================================**/



/***  模块0: 完善主动调仓记录，生成end_date,effective_date **/
/** 全局表: 
(1) busday **/
/** 输入: 
(1) stock_pool: date / stock_code/ weight/其他
(2) adjust_date_table: date (如果stock_pool的日期超出adjust_table的范围，将无效。 如果adjust_table中有stock_pool没有的日期，则认为当天股票池中没有股票)
(3) move_date_forward: 是否需要将date自动往前调整一个交易日，作为end_date  **/
/** 输出:
(1) output_stock_pool: end_date/effective_date/stock_code/weight/其他 **/

/** 特殊说明: 正常情况下，都认为如果股票池信号是在前一天收盘后至12:00生成的，即date是昨天的日期，则设定end_date = date, effective_date为end_date下一个交易日 
  		  特殊情况下，如果股票池信号是在今天0:00-开盘前生成的，即date是今天的日期，则在生成调仓记录的时候，应将date自动往前调整一个交易日。
		  特殊情况的处理主要是为了统一 **/


/**** 模块14： 策略评价(全样本) ***/
%MACRO eval_pfmance();

		/* 回测区间统计 */
		DATA ba.summary_day(drop = max_bm max_index);
			SET ba.summary_day;
			year = year(date);
			month = month(date);
			RETAIN max_bm .;
			RETAIN max_index .;

			/* 计算最大回撤 */
			IF bm_index >= max_bm THEN max_bm = bm_index;
			bm_draw = (bm_index - max_bm)/max_bm * 100;
			IF index >= max_index THEN max_index = index;
			index_draw = (index - max_index)/max_index *100;
		RUN;

		/* 按年度计算最大回撤，累计alpha，基准累计收益，指数累计收益 */
		DATA ba.summary_day(drop = max_bm max_index);
			SET ba.summary_day;
			BY year;
			RETAIN max_bm .;
			RETAIN max_index .;
			RETAIN accum_ret_year 0;
			RETAIN accum_bm_ret_year 0;

			IF first.year THEN DO;
				max_bm = .;
				max_index = .;
				accum_ret_year = 0;
				accum_bm_ret_year = 0;
			END;

			/* 计算最大回撤 */
			IF bm_index >= max_bm THEN max_bm = bm_index;
			bm_draw_year = (bm_index - max_bm)/max_bm * 100;
			IF index >= max_index THEN max_index = index;
			index_draw_year = (index - max_index)/max_index *100;
			
			/* 累计alpha，基准累计收益，指数累计收益 */
			accum_ret_year  = ((1+accum_ret_year/100)*(1+daily_ret_p/100)-1)*100;
			accum_bm_ret_year  = ((1+accum_bm_ret_year/100)*(1+bm_ret/100)-1)*100;
			accum_alpha_year = accum_ret_year - accum_bm_ret_year;	
		RUN;

	 /* 汇总指标 */
	/* 累计收益率 + 基准收益率 + 累积alpha + alpha波动率 + IC + 胜率 + 平均持仓比例 + 换手率 + 最大回撤(%) */
 
		
		/* 年度数据 */
		DATA stat1(rename = (accum_ret_year = accum_ret accum_bm_ret_year = accum_bm_ret accum_alpha_year = accum_alpha));
			SET ba.summary_day(keep = year accum_ret_year accum_bm_ret_year accum_alpha_year tar_nstock);
			BY year;
			IF last.year;
		RUN;
		PROC SQL;
			CREATE TABLE stat2 AS
			SELECT year, 
			sqrt(var(daily_alpha))*sqrt(252) AS sd_alpha,
			sum(daily_alpha)*sqrt(252)/(count(1)*sqrt(var(daily_alpha))) AS ir,
			mean(is_hit) AS hit_ratio,
			mean(is_select_hit) AS select_hit_ratio,
			mean(is_indus_hit) AS indus_hit_ratio,
			mean(is_inter_hit) AS inter_hit_ratio,
			sum(pos_t) AS turnover,
			min(index_draw_year) AS index_draw,
			min(bm_draw_year) AS bm_draw,
			sum(indus_daily_alpha) AS indus_alpha,
			sum(select_daily_alpha) AS select_alpha,
			sum(inter_daily_alpha) AS inter_alpha,
			mean(sub_bm_wt) AS sub_bm_wt
			FROM ba.summary_day
			GROUP BY year;
		QUIT;

		PROC SQL;
			CREATE TABLE summary_stat1 AS
			SELECT A.*, B.*
			FROM stat1 A JOIN stat2 B
			ON A.year = B.year
			ORDER BY A.year;
		QUIT;

		/* 回测区间数据 */
		DATA stat1(drop = index bm_index);
			SET ba.summary_day(keep = index bm_index accum_alpha tar_nstock) end = is_end;
			IF is_end = 1;
			accum_ret = index - 100;
			accum_bm_ret = bm_index - 100;
			year = 0;
		RUN;
		PROC SQL;
			CREATE TABLE stat2 AS
			SELECT 0 AS year, 
			sqrt(var(daily_alpha))*sqrt(252) AS sd_alpha,
			sum(daily_alpha)*sqrt(252)/(count(1)*sqrt(var(daily_alpha))) AS ir,
			sum(is_hit)/count(1) AS hit_ratio,
			mean(is_select_hit) AS select_hit_ratio,
			mean(is_indus_hit) AS indus_hit_ratio,
			mean(is_inter_hit) AS inter_hit_ratio,
			sum(pos_t) AS turnover,
			min(index_draw) AS index_draw,
			min(bm_draw) AS bm_draw,
			sum(indus_daily_alpha) AS indus_alpha,
			sum(select_daily_alpha) AS select_alpha,
			sum(inter_daily_alpha) AS inter_alpha,
			mean(sub_bm_wt) AS sub_bm_wt
			FROM ba.summary_day
		QUIT;
		PROC SQL;
			CREATE TABLE summary_stat2 AS
			SELECT A.*, B.*
			FROM stat1 A JOIN stat2 B
			ON A.year = B.year
			ORDER BY A.year;
		QUIT;
		DATA ba.summary_stat(drop = i);
			SET summary_stat1 summary_stat2;
			ARRAY var_a(12) accum_ret--ir turnover--sub_bm_wt;
			DO i = 1 TO 12;
				var_a(i) = round(var_a(i),0.01);
			END;
			hit_ratio = round(hit_ratio,0.0001);
			select_hit_ratio = round(select_hit_ratio, 0.0001);
			indus_hit_ratio = round(indus_hit_ratio, 0.0001);
			inter_hit_ratio = round(inter_hit_ratio, 0.0001);
		RUN; 

		PROC TRANSPOSE DATA =ba.summary_stat   OUT = ba.summary_stat
			prefix = Y_  name = stat1;
			id year;
		RUN;
		
		PROC SQL;
			DROP TABLE summary_stat1, summary_stat2, stat1, stat2;
		QUIT;

%MEND eval_pfmance;
