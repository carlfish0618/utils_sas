/* 模块0: 去除事件日停牌的股票 */
/* 同样出现在模块: impact_macro_2中*/

/* INPUT:
	(0) my_library 
	(1) event_table: datasets(put in the designated library)
	(2) stock_info_table: datasets 
	(3) a_stock_list: datasets
    (4) output_table: datasets for output
/* OUTPUT:
	(1) output_table */
/* Datasets Detail:
	(1) (input) event_table: event_id, stock_code, date
	(2) (input) stock_info_table:stock_code, stock_name, is_delist, list_date, delist_date, is_st
	(3) (input) a_stock_list: stock_code
	(4) (output) output_table: filtered event_table */

%MACRO filter_event(my_library, event_table, stock_info_table, a_stock_list, output_table);

	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.list_date, B.delist_date, B.is_st
		FROM &my_library..&event_table A LEFT JOIN &my_library..&stock_info_table B
		ON A.stock_code = B.stock_code
		WHERE A.stock_code IN
		(SELECT stock_code FROM &my_library..&a_stock_list.)
		ORDER BY A.event_id;
	QUIT;


	DATA &my_library..&output_table(drop =  list_date delist_date is_st);
		SET tmp;
		IF missing(list_date) OR (NOT missing(list_date) AND date - list_date <= 250) THEN delete;
		IF is_st = 1 THEN delete;
	RUN;

	PROC SQL;
		DROP TABLE tmp;
	QUIT;
%MEND filter_event;



/* 模块1: 去除事件日停牌的股票 */
%MACRO filter_event_halt(my_library, event_table, market_table,output_table);
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.is_halt, B.is_limit
		FROM &my_library..&event_table A LEFT JOIN &my_library..&market_table B
		ON A.stock_code = B.stock_code AND A.date = B.date;
	QUIT;
	DATA &output_table(drop = is_halt is_limit);
		SET tmp;
		IF is_halt = 1 OR is_limit IN (1,2) THEN delete;
	RUN
%MEND filter_event_halt;


/* 模块2: 生成等权配置的股票池*/
/* INPUT:
	(0) my_library 
	(1) event_table: datasets(put in the designated library)
	(2) eventName: character
	(3) weight_function: f(x) for weight, x can be duration day or other arguments
	(4) busday_table: datasets(need explicitely refer to the library)
	(5) delist_table: datasets */
/* OUTPUT: 
	(1) &eventName._pool: datasets(put in the designated library) */
/* Datasets Detail:
	(1) (input) event_table: event_id, date, stock_code, max_day, min_day, ineffective_date, is_to_end
	(2) (input) busday_table: date 
	(3) (input) delist_table: date, stock_code, is_delist_at_close
	(3) (output) &eventName._pool: date, stock_code, event_id, is_buy, is_sell, day, weight, event_day, is_to_end, is_bm */


%MACRO equally_weighted_stock_pool(my_library, eventName, event_table, weight_function, busday_table, stock_info_table);

	DATA event1;
		SET &my_library..&event_table(rename = (date = event_date));
	RUN;


	%map_date_to_index(busday_table = &busday_table , raw_table = event1, date_col_name = event_date, raw_table_edit = &my_library..event1);
	%map_date_to_index(busday_table = &busday_table, raw_table = &busday_table, date_col_name = date, raw_table_edit = &my_library..busday2);

	

/*	PROC SQL;*/
/*		CREATE TABLE &my_library..event2 AS*/
/*			SELECT A.*,  B.date AS expand_date*/
/*			FROM &my_library..event1 A LEFT JOIN &my_library..busday2 B*/
/*			ON 0<= B.date_index - A.date_index <= A.max_day AND B.date <= A.ineffective_date */
/*			ORDER BY  A.stock_code, B.date;*/
/*	QUIT;*/

	PROC SQL;
		CREATE TABLE &my_library..event2 AS
			SELECT A.*,  B.date AS expand_date
			FROM &my_library..event1 A LEFT JOIN &my_library..busday2 B
			ON A.event_date <= B.date <= A.ineffective_date   /* 暂时不用max_date这个指标 */
			ORDER BY  A.stock_code, B.date;
	QUIT;

	
	/* delete those delisting stocks or not in required stock pool*/ 
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.is_delist, B.delist_date
		FROM &my_library..event2 A LEFT JOIN &my_library..&stock_info_table B
		ON A.stock_code = B.stock_code
		ORDER BY A.event_id, A.expand_date;
	QUIT;

	DATA &my_library..event2;
		SET tmp;
	RUN;

	PROC SQL;
		DROP TABLE tmp;
	QUIT;


	DATA &my_library..event2;
		SET &my_library..event2;
		BY event_id;
		RETAIN is_valid  1;
		IF first.event_id THEN is_valid = 1;
		IF is_delist = 1 AND expand_date >= delist_date THEN is_valid = 0;
	RUN;


	DATA &my_library..event2(drop = is_valid is_delist delist_date);
		SET &my_library..event2;
		IF is_valid = 1;
	RUN;

	
	PROC SQL;
		CREATE TABLE &my_library..sell_day AS
			SELECT event_id, max(expand_date) AS sell_date, max(is_to_end) AS f_is_to_end  /* f_is_to_end: 标注卖出日期是否为可得的最后一个交易日 */
			FROM &my_library..event2
			GROUP BY event_id;
		
		CREATE TABLE &my_library..event1 AS
			SELECT A.*, B.sell_date, B.f_is_to_end
			FROM &my_library..event2 A LEFT JOIN &my_library..sell_day B
			ON A.event_id = B.event_id
			ORDER BY A.event_id, A.expand_date;
	QUIT;





	/* based on the most recent events */
	PROC SQL;
		CREATE TABLE &my_library..event2 AS
			SELECT stock_code, expand_date, max(event_date) AS event_date
			FROM &my_library..event1
			GROUP BY stock_code, expand_date;
		
		CREATE TABLE &my_library..&eventName._pool AS
			SELECT A.expand_date AS date, A.stock_code, B.event_id, B.sell_date, B.score, B.f_is_to_end AS is_to_end
			FROM &my_library..event2 A LEFT JOIN &my_library..event1 B
			ON A.event_date = B.event_date AND A.expand_date = B.expand_date AND A.stock_code = B.stock_code
			ORDER BY A.stock_code, A.expand_date;
	QUIT;



	
	DATA &my_library..&eventName._pool(drop= r_last_sell_date r_last_event_id);
		SET &my_library..&eventName._pool;
		is_bm = 0;
		BY stock_code date;
		RETAIN r_last_sell_date  .;
		RETAIN r_last_event_id .;
		RETAIN day .;
		RETAIN event_day .;
	

		last_sell_date = r_last_sell_date;
		last_event_id = r_last_event_id;


		IF first.stock_code THEN DO
			r_last_sell_date = .;
			r_last_event_id = .;
			day = 0;
			event_day = 0;
			is_buy = 1;
			is_sell = 0;
			weight = &weight_function.;
		END;
		ELSE DO;
			IF event_id ~= last_event_id AND date > last_sell_date THEN DO;  /* new event coming after the last event's selling day */
				day = 0;
				event_day = 0;
				is_buy = 1;
				is_sell = 0;
				weight = &weight_function.;
			END;
			ELSE IF event_id ~= last_event_id AND date <= last_sell_date THEN DO;  /* new event coming within the last event's holding periods(including the selling day) */
				day + 1;
				event_day = 0;
				is_buy = 0;
				is_sell = 0;
				weight = &weight_function;
			END;
			ELSE IF event_id = last_event_id AND date ~= sell_date THEN DO; /* within the same event and not sell */
				day + 1;
				event_day + 1;
				is_buy = 0;
				is_sell = 0;
				weight = &weight_function.;
			END;
			ELSE DO; /* within the same event and sell */
				day + 1;
				event_day + 1;
				is_buy = 0;
				is_sell = 1;
				weight = 0;
				IF is_to_end = 1 THEN DO;
					is_sell = 0; /* 如果打算卖，但是sell_date是因为可得日期而截断，则不卖 */
					weight = &weight_function.;
				END;

			END;
		END;

		r_last_sell_date = sell_date;
		r_last_event_id = event_id;

		FORMAT last_sell_date sell_date mmddyy10.;

	RUN;

	PROC SQL;
		DROP TABLE &my_library..event1, &my_library..event2, &my_library..sell_day, &my_library..busday2;
	QUIT;  

%MEND equally_weighted_stock_pool; 


/* 模块3: 行业中性 */
%MACRO adjust_to_sector_neutral(my_library, stock_pool, stock_sector_mapping, start_date, ind_max_weight, edit_stock_pool);
	/* Step1: 计算个股权重 */
	PROC SQL NOPRINT;
		CREATE TABLE tmp1 AS
		SELECT A.*, B.o_code, B.sector_weight/100 AS sector_weight    /* 弄成以1为单位*/
		FROM &my_library..&stock_pool A LEFT JOIN &my_library..&stock_sector_mapping. B
		ON A.stock_code = B.stock_code AND A.date = B.date
		ORDER BY A.stock_code, A.date;
	QUIT;
	
	PROC SQL NOPRINT;
		CREATE TABLE tmp2 AS
		SELECT date, o_code, sum(weight) AS t_indus_weight
		FROM tmp1
		GROUP BY date, o_code;
		
		CREATE TABLE tmp3 AS
		SELECT A.*, B.t_indus_weight
		FROM tmp1 A LEFT JOIN tmp2 B
		ON A.date = B.date AND A.o_code = B.o_code
		ORDER BY date, stock_code;
	QUIT;

	DATA ind_weight(drop = t_indus_weight);
		SET tmp3;
		IF missing(sector_weight) OR round(sector_weight,0.0001) = 0 THEN weight = 0; /* 如果当日基准中没有该行业，则设定该行业的所有个股权重为0 */
		ELSE IF t_indus_weight ~= 0 THEN weight = round(sector_weight*round(weight/t_indus_weight,0.0001),0.000001);
		ELSE weight = 0;
		IF weight > &ind_max_weight AND is_bm = 0 THEN weight = &ind_max_weight;  /* 有个股上限 */
	RUN;
 

	/* Step2: 增加相应的行业指数 */
	/* 调整完个股权重后，重新计算行业加总权重 */
	PROC SQL;
		CREATE TABLE tmp2 AS
		SELECT date, o_code, sum(weight) AS t_indus_weight
		FROM ind_weight
		WHERE is_bm = 0
		GROUP BY date, o_code;
	QUIT;
	
	/* 获取基准的行业权重 */
	PROC SQL;
		CREATE TABLE t_index_info AS
		SELECT date, o_code, mean(sector_weight/100) AS sector_weight
		FROM &my_library..&stock_sector_mapping.
		WHERE not missing(o_code)
		GROUP BY date, o_code
		ORDER BY date, o_code;
	QUIT;
	
	PROC SQL;
		CREATE TABLE tmp1 AS
		SELECT B.*, A.t_indus_weight
		FROM tmp2 A RIGHT JOIN t_index_info B
		ON A.date = B.date AND A.o_code = B.o_code
		WHERE B.date >= &start_date.
		ORDER BY A.date, A.o_code;
	QUIT;

	DATA indus_weight(keep = date o_code add_indus rename = (o_code = stock_code add_indus = weight));
		SET tmp1;
		IF missing(t_indus_weight) OR round(t_indus_weight, 0.000001) = 0 THEN DO;
			add_indus = sector_weight;
		END;
		ELSE IF abs(sector_weight-t_indus_weight)> 0.001 AND round(sector_weight-t_indus_weight,0.001)>0 THEN DO;
			add_indus = round(sector_weight - t_indus_weight,0.000001);
		END;
		ELSE DO;
			add_indus = 0;
		END;
	RUN;

	/* 完善行业指数的股票池数据，新增列: score/is_to_end/day/is_buy/is_sell/is_bm */
	PROC SORT DATA = indus_weight;
		BY stock_code date;
	RUN;

	DATA indus_weight;
		SET indus_weight;
		is_bm = 2;
		BY stock_code;
		RETAIN day 0;

		is_buy = .;
		is_sell = .;
       last_weight = lag(weight);

		IF first.stock_code THEN DO;
			last_weight = 0;
			is_buy = .;
			is_sell = .;
			day = 0;
		END;

		IF weight >0 AND last_weight > 0 THEN DO;  /* 继续持有 */
			is_buy = 0;
			is_sell = 0;
			day + 1;
			is_to_end = 0;
			score = .;
		END;
		ELSE IF weight>0 AND last_weight =0 THEN DO ; /*买*/
			is_buy = 1;
			is_sell = 0;
			day = 0;
			is_to_end = 0;
			score = .;
		END;
		ELSE IF weight = 0 AND last_weight > 0 THEN DO; /* 卖 */
			is_buy = 0;
			is_sell = 1;
			day + 1;
			is_to_end = 0;
			score = .;
		END;
		ELSE IF weight = 0 AND last_weight = 0 THEN DO; /* 没有相关操作 */
			day = 0; /* 调整 */
		END;
	RUN;

	DATA indus_weight;
		SET indus_weight;
		IF not missing(is_buy) AND not missing(is_sell);
	RUN;

	DATA &my_library..&edit_stock_pool.;
		SET ind_weight(keep = date stock_code score is_to_end day is_buy is_sell weight is_bm) indus_weight(drop = last_weight);
	RUN;
	
	PROC SORT DATA = &my_library..&edit_stock_pool.;
		BY date is_bm stock_code;
	RUN;

	PROC SQL;
		DROP TABLE tmp1, tmp2, tmp3, indus_weight, ind_weight, t_index_info;
	QUIT;
%MEND adjust_to_sector_neutral;


/* 模块4: 根据仓位要求配置指数 */
%MACRO fill_in_index(my_library, stock_pool, busday_table, start_date, all_max_weight, ind_max_weight, benchmark_code, edit_stock_pool);
	/* Step1: 计算个股权重 */
	
	PROC SQL NOPRINT;
		CREATE TABLE tmp1 AS
		SELECT date, sum(weight) AS t_weight
		FROM &my_library..&stock_pool.
		GROUP BY date;
		
		CREATE TABLE tmp2 AS
		SELECT A.*, B.t_weight
		FROM &my_library..&stock_pool A LEFT JOIN tmp1 B
		ON A.date = B.date
		ORDER BY A.date;
	QUIT;

	DATA ind_pool(drop = t_weight);
		SET tmp2;
		IF t_weight ~= 0 THEN weight = round(weight/t_weight,0.000001);   /* 如果是已经经过行业中性调整后的，这里weight不会发生变化 */
		ELSE weight = 0;
		IF weight > &ind_max_weight AND is_bm = 0 THEN weight = &ind_max_weight;  /* 有个股上限 */
	RUN;
	
	/* Step2: 增加相应的行业指数 */
	/* 调整完个股权重后，重新计算行业加总权重 */
	PROC SQL;
		CREATE TABLE tmp2 AS
		SELECT date, sum(weight) AS t_weight
		FROM ind_pool
		GROUP BY date;
	QUIT;
	/* 补全所有的时间 */
	PROC SQL;
		CREATE TABLE t_info AS
		SELECT A.date, B.t_weight
		FROM &busday_table. A LEFT JOIN tmp2 B
		ON A.date = B.date
		WHERE A.date >= &start_date.
		ORDER BY date;
	QUIT;

	DATA t_info;
		SET t_info;
		IF missing(t_weight) OR round(t_weight,0.001) = 0 THEN DO;
			add_bm_weight = 1;  /* 仓位全为指数 */
			multiplier = 0;
		END;
		ELSE IF abs(t_weight-&all_max_weight.)>0.001 AND round(t_weight-&all_max_weight.,0.001)>0 THEN DO;  /* 超过设定的权重 */
			add_bm_weight = &all_max_weight.;
			multiplier = round( (1-&all_max_weight.)/t_weight,0.001);
		END;
		ELSE DO;
			add_bm_weight = 1-t_weight;
			multiplier = 1;
		END;
		/* 精度问题 */
		IF abs(add_bm_weight)<=0.001 THEN DO;
			add_bm_weight = 0;
			multiplier = 1;
		END;
	RUN;

	/* 重新调整个股权重 */
	PROC SQL;
		CREATE TABLE tmp1 AS
		SELECT A.*, B.multiplier, B.t_weight
		FROM ind_pool A LEFT JOIN t_info B
		ON A.date = B.date;
	QUIT;

	DATA ind_pool;
		SET tmp1;
		weight = weight * multiplier;
	RUN;

	/* 完善指数的股票池，新增列: score/is_to_end/day/is_buy/is_sell/is_bm */
	DATA t_info;
		SET t_info(keep = date add_bm_weight rename = (add_bm_weight = weight));
		stock_code = "&benchmark_code.";
		is_bm = 1; 
		RETAIN day 0;
		is_buy = .;
		is_sell = .;
		last_weight = lag(weight);

		IF _N_ = 1 THEN last_weight = 0;

		IF weight >0 AND last_weight > 0 THEN DO;  /* 继续持有 */
			is_buy = 0;
			is_sell = 0;
			day + 1;
			is_to_end = 0;
			score = .;
		END;
		ELSE IF weight>0 AND last_weight =0 THEN DO ; /*买*/
			is_buy = 1;
			is_sell = 0;
			day = 0;
			is_to_end = 0;
			score = .;
		END;
		ELSE IF weight = 0 AND last_weight > 0 THEN DO; /* 卖 */
			is_buy = 0;
			is_sell = 1;
			day + 1;
			is_to_end = 0;
			score = .;
		END;
		ELSE IF weight = 0 AND last_weight = 0 THEN DO; /* 没有相关操作 */
			day = 0; /* 调整 */
		END;
	RUN;

	DATA t_info;
		SET t_info;
		IF not missing(is_buy) AND not missing(is_sell);
	RUN;

	DATA &my_library..&edit_stock_pool.;
		SET ind_pool(keep = date stock_code score is_to_end day is_buy is_sell weight is_bm) t_info(drop = last_weight);
	RUN;
	
	PROC SORT DATA = &my_library..&edit_stock_pool.;
		BY date is_bm stock_code;
	RUN;

	PROC SQL;
		DROP TABLE tmp1, tmp2,t_info, ind_pool;
	QUIT;

%MEND fill_in_index;


/* 模块5: 策略回溯-生成交易清单*/

%MACRO cal_trading_list(my_library, stock_pool, benchmark_hqinfo, my_hqinfo,
				sector_hqinfo,benchmark_code, trading_list);
		/* 生成每天，每只股票的交易数据 */
		/* 这里要区分是指数还是个股，因为可能共享同个代码*/
		DATA t1;
			SET sector_hqinfo(keep = stock_code date price last_price next_price);
			is_bm = 2;
		RUN;
		DATA t2;
			SET my_hqinfo(keep = stock_code date price last_price next_price);
			is_bm = 0;
		RUN;
		DATA t3;
			SET benchmark_hqinfo(keep = stock_code date price last_price next_price);
			is_bm = 1;
		RUN;

		DATA all_hqinfo;
			SET t1 t2 t3;
		RUN;

		/* fetch current price */	
		PROC SQL NOPRINT;
			CREATE TABLE cal_holdings AS
			SELECT A.*, B.last_price, B.price, B.next_price
			FROM &my_library..&stock_pool A LEFT JOIN all_hqinfo B
			ON A.date = B.date AND A.stock_code = B.stock_code AND A.is_bm = B.is_bm
			ORDER BY stock_code,date;
		QUIT;
		
		/*　fetch last weight for stocks (including index) */
		DATA cal_holdings;
			SET cal_holdings;
			is_trade_day = 0;
			BY stock_code;
			last_weight = lag(weight);
			IF first.stock_code THEN DO;
				last_weight = 0;
			END;
			IF is_buy = 1 THEN last_weight = 0; /* not in the same holding period */
		RUN;

		
		/* filter those trading day (包括个股和指数，只要有涉及首次买或者卖，都认为是交易日）*/
		PROC SQL;
			CREATE TABLE trade_day AS
			SELECT DISTINCT date FROM cal_holdings WHERE is_buy + is_sell > 0 ;
			

			UPDATE cal_holdings
			SET is_trade_day = 1 WHERE date IN
			(SELECT DISTINCT date FROM trade_day);
		QUIT;

		PROC SORT DATA = cal_holdings;
			BY stock_code date;
		RUN;
		
		/* save the lattest price for the calculation of the last holding shares */ 
		DATA cal_holdings_2;
			SET cal_holdings;
			IF is_trade_day = 1;
		RUN;

		DATA cal_holdings_2;
			SET cal_holdings_2;
			BY stock_code;
			trade_last_price = lag(price);
			trade_last_weight = lag(weight);
			IF first.stock_code OR is_buy = 1 THEN DO;
				trade_last_price = .;
				trade_last_weight = .;
			END;
		RUN;

		PROC SORT DATA = cal_holdings_2;
			BY date DESCENDING day is_bm stock_code;
		RUN;

				
		DATA &my_library..&trading_list;	
			SET cal_holdings_2;
			BY date;
			/* calculate last_capital and capital (last capital -> the capital for the lattest trading day) */
			RETAIN last_capital 10000;  /* last_capital for inividual day */
			RETAIN increase_capital 0;   /* capital for individual day (increasing stock by stock within days)*/
			
			/* increase_capital = 0 => no stocks holding ahead of day,  no need to adjust last capital */
			IF first.date THEN DO;
				IF increase_capital ~= 0 THEN DO; 
					last_capital = increase_capital;
					increase_capital = 0;
				END;
			END;

			IF is_buy ~= 1 THEN DO;
				last_value = round(last_capital* trade_last_weight,0.0001);
				last_holding = round(last_value / trade_last_price,0.0001);   /* holding -> already buy -> have last price */
				ori_value = last_holding * price;
				increase_capital = increase_capital + ori_value;
			END;
			ELSE DO;  /* wanna buy */
				last_holding = 0;
				last_value = 0;
				ori_value = 0;
			END;	
		RUN;


		PROC SQL;
			CREATE TABLE tmp AS
			SELECT date, max(increase_capital) as capital 
			FROM &my_library..&trading_list
			GROUP BY date;
	
			CREATE TABLE tmp2 AS
			SELECT A.*, B.capital
			FROM &my_library..&trading_list A LEFT JOIN tmp B
			ON A.date = B.date
			ORDER BY date, day desc, stock_code;
		QUIT;

		DATA &my_library..&trading_list;
			SET tmp2;
			IF capital = 0 THEN capital = last_capital; /* at the beginning  */
			/* in case: wanna sell all the stocks */
			IF is_sell = 1 AND is_buy = 0 THEN DO; /* not adjust value or holding for stocks to sell*/
				adjust_value = 0;
				holding = 0;
			END;
			ELSE DO;  /* is_sell = 0 or is_buy = 1 gurantee that t_weight_edit ~=0 */
				adjust_value = round(capital* weight,0.0001);
				holding = round(adjust_value/price,0.0001);
			END;

		RUN;


		/* append the stock pool for non-trading day */
		DATA not_trade_day;
			SET cal_holdings;
			IF is_trade_day = 0;
		RUN;


		PROC DATASETS;
			APPEND BASE = &my_library..&trading_list DATA = not_trade_day;
		RUN;

		PROC SORT DATA = &my_library..&trading_list;
			BY stock_code date;
		QUIT;

		/* update the value for the individual stocks on the non-trading day */

		DATA &my_library..&trading_list(drop = retain_holding);
			SET &my_library..&trading_list(drop = last_value capital last_capital increase_capital trade_last_price trade_last_weight);
			BY stock_code;
			RETAIN retain_holding .;
			IF first.stock_code THEN DO;
				retain_holding = .;
			END;

			IF is_trade_day = 1 THEN DO;  /* update on trading day */
				retain_holding = holding;
			END;
			ELSE DO;
				holding = retain_holding;
				last_holding = retain_holding;
				ori_value = last_holding * price; /* the difference between the ori_value and adjust_value is the turnover amount */
				adjust_value = holding * price; /* the same with ori_value */
			END;
		RUN;


		PROC SORT DATA =&my_library..&trading_list;
			BY date descending day stock_code;
		RUN;

	
		PROC SQL;
			DROP TABLE tmp, tmp2,cal_holdings, 
			cal_holdings_2, not_trade_day, trade_day, all_hqinfo, t1,t2,t3;
		QUIT;

%MEND cal_trading_list;

/* 模块6: 策略回溯-策略评价 */
%MACRO eval_pfmance(my_library, benchmark_hqinfo,trading_list, busday_table,day_detail);

	/* 整理当日的分析数据 */
		PROC SQL;
			CREATE TABLE total_info_2 AS
			SELECT date, sum(adjust_value) AS capital, sum(abs(ori_value - adjust_value)) AS turnover, 
				mean(is_trade_day) AS is_trade_day
			FROM &my_library..&trading_list
			GROUP BY date
			ORDER BY date;
		QUIT;

		DATA total_info_2;
			SET total_info_2;
			last_capital = lag(capital);
		RUN;
		DATA total_info_2;
			SET total_info_2;
			IF _N_ = 1 THEN last_capital = 10000;
		RUN;

		/* calculate number of stocks */
		PROC SQL NOPRINT;
			CREATE TABLE tmp1 AS
			SELECT date, count(1) as n_stock, round(sum(weight),0.0001) as t_weight
			FROM &my_library..&trading_list
			WHERE is_sell ~= 1 AND is_bm = 0 /* number of stocks after selling and buying*/
			GROUP BY date;
		
			CREATE TABLE tmp2 AS
			SELECT date, count(1) as last_n_stock, round(sum(last_weight),0.0001) as last_t_weight
			FROM &my_library..&trading_list
			WHERE is_buy ~= 1 AND is_bm = 0
			GROUP BY date;
	
			CREATE TABLE total_info AS
				SELECT A.date, B.n_stock, B.t_weight, C.last_n_stock, C.last_t_weight
				FROM &busday_table A LEFT JOIN tmp1 B
				ON A.date = B.date
				LEFT JOIN tmp2 C
				ON A.date = C.date
				ORDER BY A.date;
		QUIT;

		DATA total_info;
			SET total_info;
			IF n_stock = . THEN n_stock = 0;
			IF last_n_stock = . THEN last_n_stock = 0;
			IF last_t_weight = . THEN last_t_weight = 0;
			IF t_weight = . THEN t_weight = 0;
		RUN;
		
		PROC SQL;
			CREATE TABLE &my_library..&day_detail  AS
			SELECT A.*, B.capital, B.last_capital,B.turnover, B.is_trade_day
			FROM total_info A LEFT JOIN total_info_2 B
			ON A.date = B.date
			ORDER BY date;
		QUIT;
	
		
	/* 与指数基准作比较 */
		PROC SQL;
			CREATE TABLE tmp AS
			SELECT A.*, B.ret AS bench_ret 
			FROM &my_library..&day_detail A LEFT JOIN &my_library..&benchmark_hqinfo B
			ON A.date = B.date
			ORDER BY A.date;
		QUIT;

		DATA &my_library..&day_detail;
			SET tmp;
			RETAIN accum_alpha 0;
			RETAIN accum_ret 0;
			RETAIN accum_bench_ret 0;

			bench_index = 100;
		    res_index = 100;
			IF _N_ = 1 THEN DO;
				ret = 0;
				alpha = 0;
				bench_ret = 0;
			END;
			ELSE DO;
				ret = (capital - last_capital)/last_capital * 100;
				alpha = round(ret - bench_ret,0.000001);
				accum_ret + ret;
				accum_bench_ret + bench_ret;
				accum_alpha + alpha;
			END;

			bench_index = 100 * (1+accum_bench_ret/100);
			res_index = 100 *(1+accum_ret/100);		
			relative = res_index/bench_index;
		RUN;

		/* 计算其他分析指标 */
		DATA &my_library..&day_detail(drop = max_bench max_res);
			SET &my_library..&day_detail(drop = last_t_weight last_n_stock);
			year = year(date);
			month = month(date);
			RETAIN accum_alpha_tc 0;
			RETAIN max_bench .;
			RETAIN max_res .;
			
			/* 胜率标记 */
			IF alpha>0 THEN is_hit = 1;
			ELSE is_hit = 0;
			/* alpha_tc */
			alpha_tc = alpha - turnover/last_capital*0.5;
			accum_alpha_tc + alpha_tc;
			IF alpha>0 THEN is_hit_tc = 1;
			ELSE is_hit_tc = 0;

			/* 计算最大回撤 */
			IF bench_index >= max_bench THEN max_bench = bench_index;
			bench_draw = (bench_index - max_bench)/max_bench * 100;
			IF res_index >= max_res THEN max_res = res_index;
			res_draw = (res_index - max_res)/max_res *100;
		RUN;

		/* 按年度计算最大回撤 */
		DATA &my_library..&day_detail(drop = last_capital max_bench max_res);
			SET &my_library..&day_detail;
			BY year;
			RETAIN max_bench .;
			RETAIN max_res .;
			IF first.year THEN DO;
				max_bench = .;
				max_res = .;
			END;
			IF bench_index >= max_bench THEN max_bench = bench_index;
			bench_draw_year = (bench_index - max_bench)/max_bench * 100;
			IF res_index >= max_res THEN max_res = res_index;
			res_draw_year = (res_index - max_res)/max_res *100;
		RUN;

		PROC SQL;
			DROP TABLE tmp, tmp1, tmp2, total_info, total_info_2;
		QUIT;
%MEND eval_pfmance;



%MACRO eval_pfmance_summary(my_library, day_detail, summary_result);
	/* 累计收益率 + 基准收益率 + 累积alpha + alpha波动率 + IC + 胜率 + 平均持仓比例 + 换手率
       + 调仓天数 + 总交易天数 + 最大回撤(%)
    */
	/* 考虑换手率的: 累积alpha_tc + alpha_tc波动率 + IC + 胜率 */
	PROC SQL;
		CREATE TABLE stat1 AS
		SELECT 0 AS year,
		sum(ret)/count(1)*252 AS accum_ret,
		sum(bench_ret)/count(1)*252 AS accum_bm_ret,
		sum(alpha)/count(1)*252 AS accum_alpha,
		sum(alpha_tc)/count(1)*252 AS accum_alpha_tc,
		sqrt(var(alpha))*sqrt(252) AS sd_alpha,
		sqrt(var(alpha_tc))*sqrt(252) AS sd_alpha_tc,
		sum(alpha)*sqrt(252)/(count(1)*sqrt(var(alpha))) AS ic,
		sum(alpha_tc)*sqrt(252)/(count(1)*sqrt(var(alpha_tc))) AS ic_tc,
		sum(is_hit)/count(1) AS hit_ratio,
		sum(is_hit_tc)/count(1) AS hit_ratio_tc,
		min(res_draw) AS res_draw,
		min(bench_draw) AS bm_draw,
		mean(t_weight)*100 AS weight,
		sum(turnover)/mean(capital) AS turnover,
		mean(n_stock) AS n_stock,
		sum(is_trade_day) AS n_trade_day,
		count(1) AS n_day,
		sum(ret) AS realized_ret,
		sum(bench_ret) AS realized_bm_ret,
		sum(alpha) AS realized_alpha,
		sum(alpha_tc) AS realized_alpha_tc
		FROM &my_library..&day_detail.;
	QUIT;

	/* 按年度统计 */
	PROC SQL;
		CREATE TABLE stat2 AS
		SELECT year,
		sum(ret)/count(1)*252 AS accum_ret,
		sum(bench_ret)/count(1)*252 AS accum_bm_ret,
		sum(alpha)/count(1)*252 AS accum_alpha,
		sum(alpha_tc)/count(1)*252 AS accum_alpha_tc,
		sqrt(var(alpha))*sqrt(252) AS sd_alpha,
		sqrt(var(alpha_tc))*sqrt(252) AS sd_alpha_tc,
		sum(alpha)*sqrt(252)/(count(1)*sqrt(var(alpha))) AS ic,
		sum(alpha_tc)*sqrt(252)/(count(1)*sqrt(var(alpha_tc))) AS ic_tc,
		sum(is_hit)/count(1) AS hit_ratio,
		sum(is_hit_tc)/count(1) AS hit_ratio_tc,
		min(res_draw_year) AS res_draw,
		min(bench_draw_year) AS bm_draw,
		mean(t_weight)*100 AS weight,
		sum(turnover)/mean(capital) AS turnover,
		mean(n_stock) AS n_stock,
		sum(is_trade_day) AS n_trade_day,
		count(1) AS n_day,
		sum(ret) AS realized_ret,
		sum(bench_ret) AS realized_bm_ret,
		sum(alpha) AS realized_alpha,
		sum(alpha_tc) AS realized_alpha_tc
		FROM &my_library..&day_detail.
		GROUP BY year;
	QUIT;

	DATA &my_library..&summary_result.(drop = i);
		SET stat2 stat1;
		ARRAY var_a(16) accum_ret--ic_tc res_draw--turnover realized_ret--realized_alpha_tc;
		DO i = 1 TO 16;
			var_a(i) = round(var_a(i),0.01);
		END;
		hit_ratio = round(hit_ratio,0.0001);
		hit_ratio_tc = round(hit_ratio,0.0001);
	RUN;

	PROC TRANSPOSE DATA = &my_library..&summary_result. OUT = &my_library..&summary_result. 
		prefix = Y_  name = stat1;
		id year;
	RUN


%MEND eval_pfmance_summary;
		









