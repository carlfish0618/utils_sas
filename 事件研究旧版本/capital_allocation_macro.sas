

%MACRO cal_holdings_for_all_equally(my_library, stock_pool, busday_table, benchmark_code, benchmark_hqinfo, my_hqinfo, trading_list, day_detail);
		/* fetch current price */	
		PROC SQL NOPRINT;
			CREATE TABLE &my_library..&trading_list AS
			SELECT A.*, B.last_price, B.price, B.next_price
			FROM &my_library..&stock_pool A LEFT JOIN &my_library..&my_hqinfo B
			ON A.date = B.date AND A.stock_code = B.stock_code
			ORDER BY stock_code,date;
		QUIT;
		

		/*¡¡fetch last weight for individual stock */
		DATA &my_library..&trading_list;
			SET &my_library..&trading_list;
			BY stock_code;
			last_weight = lag(weight);
			IF first.stock_code THEN DO;
				last_weight = 0;
			END;
			IF day = 0 THEN last_weight = 0; /* not in the same holding period */
		RUN;

		/* calculate number of stocks */
		PROC SQL NOPRINT;
			CREATE TABLE tmp1 AS
			SELECT date, count(1) as n_stock, round(sum(weight),0.000001) as t_weight
			FROM &my_library..&trading_list
			WHERE is_sell ~= 1 /* number of stocks after selling and buying*/
			GROUP BY date;
		
			CREATE TABLE tmp2 AS
			SELECT date, count(1) as last_n_stock, round(sum(last_weight),0.000001) as last_t_weight
			FROM &my_library..&trading_list
			WHERE is_buy ~= 1
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
	
		/* edit: including benchmark or cash */
			RETAIN is_holding_benchmark 0;
			is_buy_benchmark = . ;
			is_sell_benchmark = .; /* default: no benchmark considered */
			n_stock_edit = n_stock;
			last_n_stock_edit = last_n_stock;
			t_weight_edit = t_weight;
			last_t_weight_edit = last_t_weight;

			last_is_holding_benchmark = is_holding_benchmark;

			IF t_weight < 0.999 AND last_t_weight < 0.999 THEN DO;   
			/* cash -> cash, buy at the first time or continue holding benchmark(may adjust weight, but not sell or buy at the first time) */
			/* continue holding */
				IF last_is_holding_benchmark = 1 THEN DO;  
					is_holding_benchmark = 1;
					is_buy_benchmark = 0;
					is_sell_benchmark = 0;
					benchmark_weight = 1 - t_weight;
					last_benchmark_weight = 1 - last_t_weight;
					n_stock_edit = n_stock + 1;
					last_n_stock_edit = last_n_stock + 1;
					t_weight_edit = t_weight + benchmark_weight;
					last_t_weight_edit = last_t_weight + last_benchmark_weight;
				END;
				ELSE DO;  /*firt buy */
					is_holding_benchmark = 1;
					is_buy_benchmark = 1;
					is_sell_benchmark = 0;
					benchmark_weight = 1- t_weight;
					last_benchmark_weight = 0;
					n_stock_edit = n_stock + 1;
					t_weight_edit = t_weight + benchmark_weight;
				END;
			END;

			ELSE IF t_weight < 0.999 AND last_t_weight >=0.999  THEN DO;  /* stocks -> cash, buy benchmark */
				is_holding_benchmark = 1;  /* initial: 0 */
				is_buy_benchmark = 1;
				is_sell_benchmark = 0;
				benchmark_weight = 1 - t_weight;
				last_benchmark_weight = 0;
				n_stock_edit = n_stock + 1;
				t_weight_edit = t_weight + benchmark_weight;
			END;

			ELSE IF t_weight >= 0.999 AND last_t_weight < 0.999 THEN DO; /* cash -> stocks, sell benchmark; do nothing (at the begining of the strategy) */
				IF last_is_holding_benchmark = 1 THEN DO;
					is_holding_benchmark = 0;  /* initial: 1 */
					is_buy_benchmark = 0;
					is_sell_benchmark = 1;
					benchmark_weight = 0;
					last_benchmark_weight = 1 - last_t_weight;
					last_n_stock_edit = last_n_stock + 1;
					last_t_weight_edit = last_t_weight + last_benchmark_weight;
				END;
			END;
	
		RUN;

		
		PROC SQL NOPRINT;
			CREATE TABLE benchmark_holding AS
			SELECT date, is_buy_benchmark AS is_buy, is_sell_benchmark AS is_sell, 
				benchmark_weight AS weight, last_benchmark_weight AS last_weight
			FROM total_info
			WHERE is_sell_benchmark ~=. OR is_buy_benchmark ~= .
			ORDER BY date;
		QUIT;


		DATA benchmark_holding;
			SET benchmark_holding;
			stock_code = "&benchmark_code";  /* default: hs300 */
			day = .;  /* missing -> smallest */
		RUN;
		


		PROC SQL;
			CREATE TABLE benchmark_holding_2 AS
			SELECT A.*, B.price, B.last_price, B.next_price
			FROM benchmark_holding A LEFT JOIN &my_library..&benchmark_hqinfo B
			ON A.date = B.date
			ORDER BY date;
		QUIT;

	
		
	   DATA &my_library..&trading_list;
			SET &my_library..&trading_list benchmark_holding_2;
		RUN; 

			
		PROC SQL;
			CREATE TABLE cal_holdings AS   
				SELECT A.*, B.n_stock,B.last_n_stock, B.t_weight, B.last_t_weight,
						B.n_stock_edit, B.last_n_stock_edit, B.t_weight_edit, B.last_t_weight_edit, 0 AS is_trade_day
				FROM &my_library..&trading_list A LEFT JOIN total_info B
				ON A.date = B.date
				ORDER BY date, day desc, stock_code;
		QUIT;

		
		/* filter those trading day */
		PROC SQL;
			CREATE TABLE trade_day AS
			SELECT DISTINCT date FROM cal_holdings WHERE is_buy = 1 OR is_sell = 1;
			

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
			BY stock_code;
			IF is_trade_day = 1;
			trade_last_price = lag(price);
			IF first.stock_code OR (is_buy = 1 AND is_sell = 0) THEN DO;
				trade_last_price = .;
			END;
		RUN;

		PROC SORT DATA = cal_holdings_2;
			BY date DESCENDING day stock_code;
		RUN;

				
		DATA &my_library..&trading_list;	
			SET cal_holdings_2;
			BY date;
			/* calculate last_capital and capital (last capital -> the capital for the lattest trading day) */
			RETAIN last_capital 1000000;  /* last_capital for inividual day */
			RETAIN increase_capital 0;   /* capital for individual day (increasing stock by stock within days)*/
			
			/* increase_capital = 0 => no stocks holding ahead of day,  no need to adjust last capital */
			IF first.date THEN DO;
				IF increase_capital ~= 0 THEN DO; 
					last_capital = increase_capital;
					increase_capital = 0;
				END;
			END;

			IF is_buy ~= 1 THEN DO;
				last_value = last_capital/last_t_weight_edit * last_weight;
				last_holding = last_value / trade_last_price;   /* holding -> already buy -> have last price */
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
				adjust_value = capital/t_weight_edit * weight;
				holding = adjust_value/price;
			END;

		RUN;


		/* append the stock pool for non-trading day */
		DATA not_trade_day;
			SET cal_holdings;
			IF is_trade_day = 0;
			trade_last_price = .;
			last_capital = .;
			increase_capital = .;
			last_holding = .;
			last_value = .;
			ori_value = .;
		RUN;

		PROC DATASETS;
			APPEND BASE = &my_library..&trading_list DATA = not_trade_day;
		RUN;

		PROC SORT DATA = &my_library..&trading_list;
			BY stock_code date;
		QUIT;

		/* update the value for the individual stocks on the non-trading day */

		DATA &my_library..&trading_list;
			SET &my_library..&trading_list;
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
				/* not adjust last_value (this field together with last_capital, capital, increase_capital applies for for trading day only */
				ori_value = last_holding * price; /* the difference between the ori_value and adjust_value is the turnover amount */
				adjust_value = holding * price; /* the same with ori_value */
			END;
		RUN;


		
		PROC SORT DATA =&my_library..&trading_list;
			BY date descending day stock_code;
		RUN;
		
		PROC SQL;
			CREATE TABLE total_info_2 AS
			SELECT date, sum(adjust_value) AS capital, sum(abs(ori_value - adjust_value)) AS turnover, mean(is_trade_day) AS is_trade_day
			FROM &my_library..&trading_list
			GROUP BY date
			ORDER BY date;
		QUIT;

		DATA total_info_2;
			SET total_info_2;
			last_capital = lag(capital);
		RUN;

		PROC SQL;
			CREATE TABLE &my_library..&day_detail  AS
			SELECT A.*, B.capital, B.last_capital,B.turnover, B.is_trade_day
			FROM total_info A LEFT JOIN total_info_2 B
			ON A.date = B.date
			ORDER BY date;
		QUIT;
	
		/* merge with benchmark */

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

			IF _N_ = 1 THEN DO;
				ret = 0;
				alpha = 0;
				accum_ret = 0;
				accum_bench_ret = 0;
				bench_index = 100;
				res_index = 100;
			END;
			ELSE DO;
				ret = (capital - last_capital)/last_capital * 100;
				alpha = round(ret - bench_ret,0.000001);
				accum_ret + ret;
				accum_bench_ret + bench_ret;
				bench_index = 100 * (1+accum_bench_ret/100);
				res_index = 100 *(1+accum_ret/100);
			END;
			accum_alpha + alpha;
			relative = res_index/bench_index;
		RUN;


		PROC SQL;
			DROP TABLE tmp, tmp1, tmp2, benchmark_holding, benchmark_holding_2, cal_holdings, cal_holdings_2, total_info_2, total_info, not_trade_day, trade_day;
		QUIT;

%MEND cal_holdings_for_all_equally;












		
/* module 1: allocation of the capital */
/* Input: 
	(1) my_library
	(2) stock_pool: datasets
	(3) busday_table: datasets(not designated library) 
	(4) is_fixed_size: logical 
	(5) size: numeric (only valid when is_fixed_size = 1)
	(6) is_weight_limit: logical 
	(7) max_weight: numeric (only valid when is_weight_limit = 1)
	(8) is_cash: logical( 1-> : holding cash for available capital / 0 -> holding benchmark for available capital)
	(9) benchmark_code: character (only valid when is_cash = 0)
	(10) benchmark_hqinfo: datasets (only valid when is_cash = 0)
	(11) my_hqinfo: datasets
	(12) is_rebalance_set: logical
	(13) rebalance_date: datasets(only valid when is_rebalance_set = 1)
	(14) initial_capital: numeric(default: 1000000) */

/* Output: 
	(1) trading_list: datasets
	(2) day_detail: datasets */

/* Datasets Detail:
	(1) (input) stock_pool: date, stock_code, weight, day, is_buy, is_sell (and other columns)
	(2) (input) benchmark_hqinfo: stock_code, date, price, last_price, next_price, ret
	(3) (input) my_hqinfo: the same columns with those of the benchmark_hqinfo
	(4) (input) rebalance_date: date	
	(5) (input) busday_table: date
	(6) (output) trading_list: date, stock_code, is_buy, is_sell, weight, holding, last_holding, last_value, ori_value, adjust_value, trading_amount, is_rebalance_day, 
	(7) (output) day_detail: date, is_rebalance_day, n_stock, last_n_stock, n_stock_bm, last_n_stock_bm, t_weight, last_t_weight, t_weight_bm, last_t_weight_bm, turnover*/

	
