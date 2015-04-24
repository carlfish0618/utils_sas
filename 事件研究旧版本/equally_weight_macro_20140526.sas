

/* 去除事件日停牌的股票 */
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


/* module 1: 生成股票池*/
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
	(3) (output) &eventName._pool: date, stock_code, event_id, is_buy, is_sell, day, weight, event_day, is_to_end */

%LET weight_function = score;
%LET busday_table = busday;
%LET stock_info_table = stock_info_table;

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



	
	
