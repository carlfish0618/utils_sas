/* 3种策略：
(1) 持有至退出信号
(2) 固定持有天数
(3) 卖出信号+最大持有期
(4) 财报发布后退出
*/

/*%INCLUDE  "D:\Research\DATA\yjyg_link\code_20140827\全业绩链事件整理_2.sas";*/

%map_date_to_index(busday_table=busday, raw_table=merge_signal, date_col_name=date, raw_table_edit=merge_signal);
%map_date_to_index(busday_table=busday, raw_table=merge_stock_pool, date_col_name=date, raw_table_edit=merge_stock_pool);

PROC SQL;
	CREATE TABLE merge_stock_pool_2 AS
	SELECT A.*, B.signal, B.tar_cmp, B.date AS signal_date, B.date_index AS index_signal_date
	FROM merge_stock_pool A LEFT JOIN merge_signal B
	ON A.stock_code = B.stock_code AND A.date = B.date
	ORDER BY A.stock_code, A.date;
QUIT;


/* !!!!!!!!!!!!!!! 需要把所有的信号都调整为日末调整后的，不要设定为日初的。(该步骤尚未进行） ****/
DATA merge_stock_pool_2(drop= r_hold r_hold2 r_hold3 r_hold4 r_signal_date r_index_signal_date r_hold3 r_signal_date3 r_index_signal_date3 dif_day dif_day3);
	SET merge_stock_pool_2;
	BY stock_code;
	RETAIN r_hold 0; 
	RETAIN r_hold2 0;  /* 持有到期 */
	RETAIN r_hold3 0; 
	RETAIN r_hold4 0;
	RETAIN r_signal_date .;
	RETAIN r_index_signal_date .;
	RETAIN r_signal_date3 .;
	RETAIN r_index_signal_date3 .;


	IF first.stock_code THEN DO;
		r_hold = 0;
		r_hold2 = 0;
		r_hold3 = 0;
		r_hold4 = 0;
		r_signal_date = .;
		r_index_signal_date = .;
		r_signal_date3 = .;
		r_index_signal_date3 = .;
	END;
	
	/** 第一种策略：持有到卖出信号 */
	/* 生成明天的信号 */
	hold = r_hold;  /* 用昨天的信号构造买入与否信号 */
	IF  tar_cmp = 1 AND signal = 1 THEN r_hold = 1;  /* 买入信号发生，待下一个交易日开盘后买入 */
*	ELSE IF tar_cmp = 1 AND signal = 0 THEN r_hold = 0;
*	ELSE IF tar_cmp = 0 AND signal = 0 THEN r_hold = 0;  /* tar_cmp = 0 and signal = 1 信号不变 */
	ELSE IF r_hold = 1 AND signal = 0 THEN r_hold = 0;  /* 卖出信号 */
	

	/* 第二种策略：持有固定天数 */
	hold2 = r_hold2;
	IF  tar_cmp = 1 AND signal = 1 THEN DO;
		r_hold2 = 1;
		r_signal_date = signal_date;
		r_index_signal_date = index_signal_date;
	END;
	ELSE DO;
*		IF NOT missing(r_index_signal_date) AND date_index - r_index_signal_date < 60 THEN r_hold2 = 1;
		IF r_hold2 = 1 AND date_index - r_index_signal_date >= 60 THEN  r_hold2 = 0;
	END;
	dif_day = date_index - r_index_signal_date;
	FORMAT r_signal_date mmddyy10.;
	

	/* 第三种策略: 卖出信号+最长持有时间 */
	hold3 = r_hold3;
	IF  tar_cmp = 1 AND signal = 1 THEN DO;
		r_hold3 = 1;
		r_signal_date3 = signal_date;
		r_index_signal_date3 = index_signal_date;
	END;
	ELSE DO;
		IF r_hold3 = 1 AND signal = 0 THEN r_hold3 = 0;
		ELSE IF r_hold3 = 1 AND date_index - r_index_signal_date3 >= 60 THEN  r_hold3 = 0;
	END;
	dif_day3 = date_index - r_index_signal_date3;
	FORMAT r_signal_date3 mmddyy10.;

	/* 第四种策略: 财报发布后，卖出 */
	hold4 = r_hold4;
	IF tar_cmp = 1 AND signal = 1 THEN r_hold4 = 1;  /* 买入信号发生，待下一个交易日开盘后买入 */
	ELSE IF r_hold4 = 1 AND (signal = 0 OR tar_cmp = 0) THEN r_hold4 = 0;  /* 卖出信号 */
	
RUN;

DATA merge_stock_pool;
	SET merge_stock_pool_2;
RUN;


/* 暂时不予处理 */
/* 考虑无法买入/卖出的情况 */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.close, B.open, B.pre_close, B.high, B.low, B.vol
	FROM merge_stock_pool_2 A LEFT JOIN hqinfo B
	ON A.stock_code = B.stock_code AND A.date = B.end_date
	ORDER BY A.stock_code, A.date;
QUIT;

DATA merge_stock_pool(drop = close open pre_close high low vol);
	SET tmp;
	IF date = list_date THEN not_trade = 1; /* 上市首日，不可买入 */
	IF missing(vol) OR vol = 0 THEN not_trade = 1;
	ELSE IF close = high AND close = low AND close = open AND close > pre_close THEN not_trade = 1;  /* 涨停 */
	ELSE IF close = high AND close = low AND close = open AND close < pre_close THEN not_trade = 1; /* 跌停 */
	ELSE not_trade = 0;
RUN;



DATA merge_stock_pool(drop = rr_hold rr_hold2 rr_hold3 rr_hold4 list_date delist_date start_date end_date signal_date index_signal_date);
	SET merge_stock_pool;
	BY stock_code;
	RETAIN rr_hold .;
	RETAIN rr_hold2 .;
	RETAIN rr_hold3 .;
	RETAIN rr_hold4 .;
	IF first.stock_code THEN DO;
		rr_hold = .;
		rr_hold2 = .;
		rr_hold3 = .;
		rr_hold4 = .;
	END;
	IF (rr_hold = 0 OR missing(rr_hold)) AND hold = 1 THEN DO ;   /* 首次买入 */
		IF not_trade = 1 THEN DO;
			f_hold = 0;  /* 无法买入 */
		END;
		ELSE DO;
			f_hold = 1;
		END;
	END;
	/* 需要卖出 */
	ELSE IF rr_hold = 1 AND hold = 0 THEN DO; 
		IF not_trade = 1 THEN DO; 
			f_hold = 1;  
		END;
		ELSE DO;
			f_hold = 0;
		END;
	END;
	ELSE DO;
		f_hold = hold;
	END;
	IF last.stock_code AND date = delist_date THEN f_hold = 0; /* 如果已经是最后一个交易日，且恰好是退市日，则强制卖出 */
	rr_hold = f_hold;

	/* 持有到期策略 */
	IF (rr_hold2 = 0 OR missing(rr_hold2)) AND hold2 = 1 THEN DO ;   /* 首次买入 */
		IF not_trade = 1 THEN DO;
			f_hold2 = 0;  /* 无法买入 */
		END;
		ELSE DO;
			f_hold2 = 1;
		END;
	END;
	/* 需要卖出 */
	ELSE IF rr_hold2 = 1 AND hold2 = 0 THEN DO; 
		IF not_trade = 1 THEN DO; 
			f_hold2 = 1;  
		END;
		ELSE DO;
			f_hold2 = 0;
		END;
	END;
	ELSE DO;
		f_hold2 = hold2;
	END;
	IF last.stock_code AND date = delist_date THEN f_hold2 = 0; /* 如果已经是最后一个交易日，且恰好是退市日，则强制卖出 */
	rr_hold2 = f_hold2;

	/* 卖出信号+最大持有期 */
	IF (rr_hold3 = 0 OR missing(rr_hold3)) AND hold3 = 1 THEN DO ;   /* 首次买入 */
		IF not_trade = 1 THEN DO;
			f_hold3 = 0;  /* 无法买入 */
		END;
		ELSE DO;
			f_hold3 = 1;
		END;
	END;
	/* 需要卖出 */
	ELSE IF rr_hold3 = 1 AND hold3 = 0 THEN DO; 
		IF not_trade = 1 THEN DO; 
			f_hold3 = 1;  
		END;
		ELSE DO;
			f_hold3 = 0;
		END;
	END;
	ELSE DO;
		f_hold3 = hold3;
	END;
	IF last.stock_code AND date = delist_date THEN f_hold3 = 0; /* 如果已经是最后一个交易日，且恰好是退市日，则强制卖出 */
	rr_hold3 = f_hold3;

	/* 正是财报卖出 */
	IF (rr_hold4 = 0 OR missing(rr_hold4)) AND hold4 = 1 THEN DO ;   /* 首次买入 */
		IF not_trade = 1 THEN DO;
			f_hold4 = 0;  /* 无法买入 */
		END;
		ELSE DO;
			f_hold4 = 1;
		END;
	END;
	/* 需要卖出 */
	ELSE IF rr_hold4 = 1 AND hold4 = 0 THEN DO; 
		IF not_trade = 1 THEN DO; 
			f_hold4 = 1;  
		END;
		ELSE DO;
			f_hold4 = 0;
		END;
	END;
	ELSE DO;
		f_hold4 = hold4;
	END;
	IF last.stock_code AND date = delist_date THEN f_hold4 = 0; /* 如果已经是最后一个交易日，且恰好是退市日，则强制卖出 */
	rr_hold4 = f_hold4;
RUN;
