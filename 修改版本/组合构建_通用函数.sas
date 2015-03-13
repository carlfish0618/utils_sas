/** ======================说明=================================================**/

/*** 函数功能：提供与组合构建相关的函数。主要包括:
(1) 从调仓组合 --> 每日组合
(2) 每日组合的收益率和持仓情况统计
**/ 

/**** 函数列表:
(1) gen_adjust_pool: 完善主动调仓记录 --> !!需要用到外部表
(2) gen_daily_pool: 根据调仓数据生成每天的股票池 --> !!需要用到外部表
(3) cal_stock_wt_ret: 根据收益率确定组合每天个股准确的weight和单日收益（该版本无需每天迭代计算) --> !!需要用到外部表
(4) cal_stock_wt_ret_loop: 根据收益率确定组合每天个股准确的weight和单日收益（迭代版本，用于验证) --> !!需要用到外部表
(5) cal_portfolio_ret: 计算组合的收益和alpha等
****/ 

/*** 输入的全局表:
(1) busday: date
(2) 
** /

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

%MACRO gen_adjust_pool(stock_pool, adjust_date_table, move_date_forward, output_stock_pool);
	DATA tt;
		SET busday;
	RUN;
	PROC SORT DATA = tt;
		BY date;
	RUN;
	DATA tt;
		SET tt;
		pre_date = lag(date);
		FORMAT pre_date mmddyy10.;
	RUN;
	PROC SQL;
		CREATE TABLE tmp2 AS
		SELECT A.*, B.pre_date, C.date AS next_date
		FROM &stock_pool. A LEFT JOIN tt B
		ON A.date = B.date
		LEFT JOIN tt C
		ON A.date = C.pre_date
		ORDER BY A.date, A.stock_code;
	QUIT;
	DATA tmp2(drop = pre_date next_date date);
		SET tmp2;
		IF &move_date_forward. = 1 THEN DO;  /* 将end_date设定为date前一天 */
			end_date = pre_date;
			effective_date = date;
		END;
		ELSE DO;
			end_date = date;
			effective_date = next_date;
		END;
		IF missing(effective_date) THEN effective_date = end_date + 1; /** 若是最新的一天，则以明天作为effective_date */
		FORMAT effective_date end_date mmddyy10.;
	RUN;
	PROC SQL;
		CREATE TABLE &output_stock_pool. AS
		SELECT *
		FROM tmp2
		WHERE end_date IN
	   (SELECT end_date FROM &adjust_date_table.)  /* 只取调整日 */
		ORDER BY end_date;
	QUIT;
	PROC SQL;
		DROP TABLE tt, tmp2;
	QUIT;
%MEND gen_adjust_pool;


/*** 模块2: 根据调仓数据生成每天的股票池 */
/** 外部表: 
(1) busday: date **/

/** 输入: 
(1) stock_pool: end_date / stock_code/ weight /(其他)
(2) test_period_table(datasets): date (对应的是effective_date)
(3) adjust_date_table(datasets): end_date   **/

/** 输出:
(1) output_stock_pool: date/stock_code/adjust_date/adjust_weight / (其他) **/


/** 说明:注意回测期和调整日的选择。建议回测日期的第一天恰好是某个调整日的下一交易日。这时候最终输出的结果覆盖的时段
与设定的回测期将完全一致。否则覆盖时段可能只是回测期的子集，或者向前延长一段时间。 */

%MACRO gen_daily_pool(stock_pool, test_period_table, adjust_date_table, output_stock_pool );
	/* Step1: 确定相邻调整日期 */
	PROC SORT DATA = &adjust_date_table.;
		BY descending end_date;
	RUN;
	DATA tt_adjust;
		SET &adjust_date_table.;
		next_adj_date = lag(end_date);
		FORMAT next_adj_date mmddyy10.;
	RUN;
	
	/* Step2: 确定对应的调整日 */
	/** 有4种情况:
	(1): 回测期的第一天<=最早调整日 --> 只保留最早调整日之后的回测时间。
	(2): 回测期的第一天>最早调整日，但因为频率不同，与最近调整日之间的间隔超过1个交易日 --> 为避免wt计算的问题，把回测期向前延长到刚好距离最近调整日一个交易日
	(3): 回测期的第一天恰好是某个调整日的下一个交易日。  --> 正常
	(4): 回测期的最后一天早于最早调整日  --> 数据错误，这里暂不处理
	**/ 
	/** 从调整日中，取距离第一个回测日向前最近的调整日(不含)，作为回测的开头，为了后面计算weight的准确性 **/
	PROC SQL NOPRINT;
		CREATE TABLE tmp AS
		SELECT date
		FROM &test_period_table.
		WHERE date > (SELECT min(end_date) FROM &adjust_date_table.);  /* 要求是最早调整日之后 */
		
		SELECT max(end_date) INTO :nearby_adj
		FROM &adjust_date_table.
		WHERE end_date < (SELECT min(date) FROM tmp);

		CREATE TABLE tt_date_list AS
		SELECT A.date, B.end_date AS adjust_date
		FROM
		(
		SELECT date 
		FROM busday 
		WHERE &nearby_adj. <date <= (SELECT max(date) FROM &test_period_table.)
		) A  
		LEFT JOIN tt_adjust B
		ON B.end_date < A.date <= B.next_adj_date
		ORDER BY A.date;
	QUIT;


	/* Step3: 处理最后一个调整日之后的交易日期 */
	PROC SQL NOPRINT;
		SELECT max(end_date) INTO :adjust_ending
		FROM &adjust_date_table.
	QUIT;
	DATA tt_date_list;
		SET tt_date_list;
		IF missing(adjust_date) THEN adjust_date = &adjust_ending.;
	RUN;


	/* Step4: 与调仓股票池相连 */
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.date, A.adjust_date, B.*
		FROM tt_date_list A LEFT JOIN &stock_pool. B
		ON A.adjust_date = B.end_date 
		ORDER BY A.date;
	QUIT;
	DATA &output_stock_pool.;
		SET tmp(rename = (weight = adjust_weight) drop = end_date);
	RUN;
	PROC SORT DATA = &output_stock_pool.;
		BY date descending adjust_weight;
	RUN;

	PROC SQL;
		DROP TABLE tt_adjust, tt_date_list, tmp;
	QUIT;
%MEND gen_daily_pool;


/****** 模块3: 根据收益率确定组合每天个股准确的weight和单日收益（该版本无需每天迭代计算) **/
/** 外部表: 
(1) hqinfo: end_date/stock_code/pre_close/close/factor (并且只包含A股股票) 
(2) busday: date */

/** 输入: 
(1) daily_stock_pool: date / stock_code/ adjust_weight/ adjust_date / (其他)
(2) adjust_date_table: end_date 
**/

/** 输出:
(1) output_stock_pool: date/stock_code/adjust_weight/adjust_date/其他 + 新增 */

/* 新增有关wt的字段:
(a) 开盘(前一天收盘调整后的权重)：open_wt(基于复权因子计算) / open_wt_c(基于前收计算)
(b) 收盘(调整前): close_wt(基于复权因子计算) / close_wt_c(基于前收价格计算)
(c) 前一天收盘调整前的权重: pre_close_wt/pre_close_wt_c
(d) 收盘(调整后权重): after_close_wt/after_close_wt_c
*/
/* 新增收益率字段: daily_ret(daily_ret_c)/accum_ret(accum_ret_c)/pre_accum_ret(pre_accum_ret_c) **/

/** 新增辅助字段: pre_date/pre_price/price/pre_close/close/adjust_price */
/** 新增程序检验字段: mark(mark =1 表示没有出现异常点，否则需警惕用前收价格计算的收益和权重是否正确) */


%MACRO cal_stock_wt_ret(daily_stock_pool, adjust_date_table, output_stock_pool);
	/* Step1: 计算单日收益率，从调整日至今的累计收益率等 */
	DATA tt;
		SET busday;
		pre_date = lag(date);
		FORMAT pre_date mmddyy10.;
	RUN;
	/* 提高效率，提取子集*/
	PROC SQL;
		CREATE TABLE tt_hqinfo AS
		SELECT end_date, stock_code, pre_close, close, factor
		FROM hqinfo
		WHERE end_date >= (SELECT min(end_date)-20 FROM &daily_stock_pool.) 
		AND stock_code IN (SELECT stock_code FROM &daily_stock_pool.)
		ORDER BY end_date, stock_code;
	QUIT;

	PROC SQL;
		CREATE TABLE tt_summary_stock AS
		SELECT A.*, E.pre_date, (B.close*B.factor) AS price, (C.close*C.factor) AS pre_price, B.pre_close, B.close,
		(D.close*D.factor) AS adjust_price
		FROM &daily_stock_pool. A
		LEFT JOIN tt E
		ON A.date = E.date
		LEFT JOIN tt_hqinfo B
		ON A.date = B.end_date AND A.stock_code = B.stock_code
		LEFT JOIN tt_hqinfo C
		ON E.pre_date = C.end_date AND A.stock_code = C.stock_code
		LEFT JOIN tt_hqinfo D
		ON A.adjust_date = D.end_date AND A.stock_code = D.stock_code
		ORDER BY A.date, A.stock_code;
	QUIT;

	DATA tt_summary_stock;
		SET tt_summary_stock;
		IF not missing(price) THEN accum_ret = (price/adjust_price - 1)*100;  /** 调仓股票池必须先保证adust_price~=0 或者缺失 */
		ELSE accum_ret = 0;   /** 这里有一个问题：如果在某个持有期间，股票退市了，price缺失。这时候其实有一段收益率。整个区间的累计收益率不一定为0 */
		IF not missing(pre_price) THEN pre_accum_ret = (pre_price/adjust_price-1)*100;
		ELSE pre_accum_ret = 0;
		IF not missing(pre_price) AND not missing(price) THEN daily_ret = (price/pre_price - 1)*100; 
		ELSE daily_ret = 0;
	RUN;

	/** ！！ 新增：基于close和pre_close计算累计收益 */
	PROC SORT DATA = tt_summary_stock;
		BY stock_code date;
	RUN;
	DATA tt_summary_stock;
		SET tt_summary_stock;
		BY stock_code;
		mark = 1;  /** 程序测试用 */
		RETAIN r_last_date .;
		RETAIN r_last_accum_ret .;
		RETAIN r_last_stock_code .;
		IF first.stock_code OR pre_date = adjust_date THEN DO; /** 调仓生效的第一天 */
			r_last_date = pre_date;
			r_last_accum_ret = 0;
			r_last_stock_code = stock_code;
		END;
		IF r_last_date = pre_date AND r_last_stock_code = stock_code THEN DO;
			pre_accum_ret_c = r_last_accum_ret;
			accum_ret_c = ((1+pre_accum_ret_c/100)*close/pre_close-1)*100;
			daily_ret_c = (close/pre_close-1)*100;
		END;
		ELSE mark = 0;
		r_last_date = date;
		r_last_accum_ret = accum_ret_c;
		r_last_stock_code = stock_code;
	RUN;

	
	/* Step2: 计算个股权重 */
	/* Step2-1: 收盘权重（未调整前）*/
	PROC SQL;
		CREATE TABLE tt_stock_wt AS
		SELECT A.*, round((A.adjust_weight*(1+A.accum_ret/100))/B.port_accum_ret,0.00001) AS close_wt,
		round((A.adjust_weight*(1+A.accum_ret_c/100))/B.port_accum_ret_c,0.00001) AS close_wt_c
		FROM tt_summary_stock A LEFT JOIN
		(
		SELECT date, sum(adjust_weight*accum_ret/100)+1 AS port_accum_ret,
		sum(adjust_weight*accum_ret_c/100)+1 AS port_accum_ret_c 
		FROM tt_summary_stock 
		GROUP BY date
		) B
		ON A.date = B.date
		ORDER BY A.date, close_wt desc;
	QUIT;


	/* Step2-2: 开盘权重（已调整后）以及前一天收盘权重(调整前) */
	/* 判断该天的前一天是否为调整日或第一天，如果是，则开盘时为adjust_weight，否则为前一天的收盘权重 */
	PROC SQL NOPRINT;
		CREATE TABLE tmp AS
		SELECT A.*, B.end_date AS adj_date_b, C.close_wt AS pre_close_wt, C.close_wt_c AS pre_close_wt_c
		FROM tt_stock_wt A LEFT JOIN &adjust_date_table. B
		ON A.pre_date = B.end_date
		LEFT JOIN tt_stock_wt C
		ON A.pre_date = C.date AND A.stock_code = C.stock_code
		ORDER BY A.date, A.close_wt desc;
	QUIT;

	DATA tmp(drop = adj_date_b);
		SET tmp;
		IF not missing(adj_date_b) THEN DO;
			open_wt = adjust_weight; 
			open_wt_c = adjust_weight;
		END;
		ELSE DO;
			open_wt = pre_close_wt;
			open_wt_c = pre_close_wt_c;
		END;
		IF missing(pre_close_wt) THEN pre_close_wt = 0;  /* 新增的股票 */
		IF missing(pre_close_wt_c) THEN pre_close_wt_c = 0;
	RUN;
	/* Step2-3：收盘后调整了的权重 */
	/* 下一天的开盘权重 */
	PROC SQL;
		CREATE TABLE tt_stock_wt AS
		SELECT A.*, B.open_wt AS after_close_wt, 
		B.open_wt_c AS after_close_wt_c,
		B.date AS date_next
		FROM tmp A LEFT JOIN tmp B
		ON A.date = B.pre_date AND A.stock_code = B.stock_code
		ORDER BY A.date, A.close_wt desc;
	QUIT;
	DATA &output_stock_pool.(drop = date_next);
		SET tt_stock_wt;
		IF missing(after_close_wt) THEN after_close_wt = 0; /* 删除的股票。特殊情况: 最后一天认为是清盘 */
		IF missing(after_close_wt_c) THEN after_close_wt_c = 0;
	RUN;
	
	PROC SQL;
		DROP TABLE tt, tt_hqinfo, tt_summary_stock, tt_stock_wt, tmp;
	QUIT;
%MEND cal_stock_wt_ret;


/** 模块4-appendix: 比对: 该模块用于测试模块4的逻辑是正确的。采用每日迭代的方法 **/
/***目前支持，通过“复权因子”计算的权重 */
%MACRO cal_stock_wt_ret_loop(daily_stock_pool, output_stock_pool);
	/* Step1: 取距离调整日最近的下一个交易日 (即最近的生效日) */
	PROC SQL;
		CREATE TABLE effect_date_list AS
		SELECT adjust_date, min(date) AS effective_date  /* 如果调整日没有任何股票，这一天将会被忽略 */
		FROM &daily_stock_pool. 
		GROUP BY adjust_date
		ORDER BY adjust_date;
	QUIT;
	
	/* Step2: 计算单日收益率，从调整日至今的累计收益率等 */
	DATA tt;
		SET busday;
		pre_date = lag(date);
		FORMAT pre_date mmddyy10.;
	RUN;
	/* 提高效率，提取子集*/
	PROC SQL;
		CREATE TABLE tt_hqinfo AS
		SELECT end_date, stock_code, pre_close, close, factor
		FROM hqinfo
		WHERE end_date >= (SELECT min(end_date)-20 FROM &daily_stock_pool.) 
		AND stock_code IN (SELECT stock_code FROM &daily_stock_pool.)
		ORDER BY end_date, stock_code;
	QUIT;

	PROC SQL;
		CREATE TABLE tt_summary_stock AS
		SELECT A.*, E.pre_date, (B.close*B.factor) AS price, (C.close*C.factor) AS pre_price,
		(D.close*D.factor) AS adjust_price
		FROM &daily_stock_pool. A
		LEFT JOIN tt E
		ON A.date = E.date
		LEFT JOIN tt_hqinfo B
		ON A.date = B.end_date AND A.stock_code = B.stock_code
		LEFT JOIN tt_hqinfo C
		ON E.pre_date = C.end_date AND A.stock_code = C.stock_code
		LEFT JOIN tt_hqinfo D
		ON A.adjust_date = D.end_date AND A.stock_code = D.stock_code
		ORDER BY A.date, A.stock_code;
	QUIT;

	DATA tt_summary_stock;
		SET tt_summary_stock;
		accum_ret = (price/adjust_price - 1)*100;
		pre_accum_ret = (pre_price/adjust_price-1)*100;
		daily_ret = (price/pre_price - 1)*100;  
	RUN;
	
	/* Step3: 计算每只股票每天新的权重 */
	/** 每天循环操作 **/
	PROC SQL NOPRINT;
		CREATE TABLE tt AS
		SELECT distinct date 
		FROM &daily_stock_pool.
		ORDER BY date;

		SELECT date, count(*) 
		INTO :date_list separated by ' ',
			 :date_number
		FROM tt;
	QUIT;
	
	/* 临时股票池，保存前一天股票的close_wt */
	DATA tt_pre_pool;
		ATTRIB
			stock_code LENGTH = $ 16
			date LENGTH = 8 FORMAT = mmddyy10.
			close_wt LENGTH = 8
		;
		STOP;
	RUN;
	/* 更新股票池: 补充当天的open_wt, close_wt */
	DATA tt_append_pool;
		ATTRIB
			date LENGTH = 8 FORMAT = mmddyy10.
			stock_code LENGTH = $ 16
			close_wt LENGTH = 8
			open_wt LENGTH = 8
			pre_close_wt LENGTH = 8
		;
		STOP;
	RUN;

	%DO date_index = 1 %TO &date_number.;
		%LET curdate = %scan(&date_list, &date_index., ' ');
		/* Step3-1: 取当天和前一天股票池 */
		DATA tt_cur_pool;
			SET tt_summary_stock;
			IF date = input("&curdate.", mmddyy10.);
		RUN;
		PROC SQL;
			CREATE TABLE tt_cur_pool_2 AS
			SELECT A.*, B.close_wt AS pre_close_wt
			FROM tt_cur_pool A LEFT JOIN tt_pre_pool B
			ON A.stock_code = B.stock_code
			ORDER BY stock_code;
		QUIT;
		
		/* Step3-2: 判断当天是否为第一个生效日 */
		 PROC SQL NOPRINT;
		 	SELECT count(*) INTO :is_effect
			FROM tt_cur_pool_2
			WHERE date IN (SELECT effective_date FROM effect_date_list);
		QUIT;
 		DATA tt_cur_pool;
			SET tt_cur_pool_2;
			IF missing(pre_close_wt) THEN pre_close_wt = 0;
			IF &is_effect. > 0 OR &date_index. = 1 THEN open_wt = adjust_weight; /* 生效日: 开盘权重为主动调仓的权重 */
			ELSE open_wt = pre_close_wt; /* 否则为前一天收盘权重 */
		RUN;
		/* 计算当天的收盘权重 */
		PROC SQL NOPRINT;
			SELECT sum(open_wt*(1+daily_ret/100)) INTO :sum_wt
			FROM tt_cur_pool;
		QUIT;
		DATA tt_cur_pool;
			SET tt_cur_pool;
			close_wt = round(open_wt*(1+daily_ret/100)/&sum_wt.,0.00001);
		RUN;
		
		/* Step3-3: 更新股票池 */
		DATA tt_append_pool;
			SET tt_append_pool tt_cur_pool(keep = date stock_code close_wt open_wt pre_close_wt);
			IF missing(pre_close_wt) THEN pre_close_wt = 0;
		RUN;
		DATA tt_pre_pool;
			SET tt_cur_pool(keep = date stock_code close_wt);
		RUN;
	%END;

	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.close_wt, B.open_wt, B.pre_close_wt
		FROM tt_summary_stock A LEFT JOIN tt_append_pool B
		ON A.date = B.date AND A.stock_code = B.stock_code
		ORDER BY A.date, B.close_wt desc;
	QUIT;
	
	/* 收盘调整后权重 */
	PROC SQL;
		CREATE TABLE &output_stock_pool. AS
		SELECT A.*, B.open_wt AS after_close_wt, B.date AS date_next
		FROM tmp A LEFT JOIN tmp B
		ON A.date = B.pre_date AND A.stock_code = B.stock_code
		ORDER BY A.date, A.close_wt desc;
	QUIT;
	DATA &output_stock_pool.(drop = date_next);
		SET &output_stock_pool.;
		IF missing(after_close_wt) THEN after_close_wt = 0; /* 删除的股票 */
	RUN;

	PROC SQL;
		DROP TABLE  effect_date_list, tt, tt_hqinfo, tt_summary_stock, tt_pre_pool, tt_append_pool, tt_cur_pool, tt_cur_pool_2, tmp;
	QUIT;
%MEND cal_stock_wt_ret_loop;


		

/** 模块4: 计算组合的收益和alpha等 */
/** 输入: 
(1) daily_stock_pool: date / stock_code/open_wt(open_wt_c)/daily_ret(daily_ret_c) / (其他)
(2) type(logical): 1- 以“基于前收盘价”计算的权重为基准计算; 0- 以“基于复权因子”计算的权重为基准计算

/** 输出:
(1) output_daily_summary: date/daily_ret/accum_ret/index/nstock **/

%MACRO cal_portfolio_ret(daily_stock_pool, output_daily_summary,type = 1);

	/* Step1: 只考虑调仓日和调仓日之后的一天 */
	%IF %SYSEVALF(&type. = 1) %THEN %DO;
		PROC SQL;
			CREATE TABLE tt_stock_pool AS
			SELECT date,stock_code, open_wt_c AS open_wt, daily_ret_c AS daily_ret
			FROM &daily_stock_pool.;
		QUIT;
	%END;
	%ELSE %DO;
		PROC SQL;
			CREATE TABLE tt_stock_pool AS
			SELECT date, stock_code, open_wt,daily_ret
			FROM &daily_stock_pool.;
		QUIT;
	%END;

	/* 按单日进行统计 */
	PROC SQL;
		CREATE TABLE tt_summary_day AS
		SELECT date, sum(open_wt>0) AS nstock,
/*		((sum(adjust_weight*accum_ret/100)+1)/(sum(adjust_weight*pre_accum_ret/100)+1)-1)*100 AS daily_ret_p,*/
		sum(open_wt*daily_ret) AS daily_ret, 
		FROM tt_stock_pool
		GROUP BY date;
	QUIT;

	DATA tt_summary_day;
		SET tt_summary_day;
		RETAIN accum_ret 0;
		accum_ret = ((accum_ret/100 + 1)*(1+daily_ret/100)-1)*100; /* 以复权因子计算 */
		index = 1000 * (1+accum_ret/100);
	RUN;
	DATA &output_daily_summary;
		SET tt_summary_day;
	RUN;

	PROC SQL;
		DROP TABLE tt_summary_day;
	QUIT;
%MEND cal_portfolio_ret;

