/*** 事件研究：通用函数版本2 **/
/** 说明：
	可省略的步骤包括：
	(3)(7)(8)
**/

/*** 函数列表：
	(1A) event_gen_windows: 根据窗口长度，生成事件窗口 
	(1B) event_gen_fixed_windows:根据窗口起始结束日期，生成事件窗口
	(1C) event_gen_trade_date:根据事件买入和卖出日期，确定可交易区间
	(2) event_get_marketdata:标注事件窗口计算收益的基准日，并获取基本信息
	(3*) event_mark_win:标注事件窗口是否超出上市、退市，以及是否涨跌超过10%+(一般是复牌后行为) 
	(4) event_cal_accum_alpha:计算累计收益率(用单日累乘的方法)
	(5) event_smooth_rtn:在考虑特殊样本点的情况下，对累计收益率进行平滑
	(6) event_cal_stat: 根据提供的分组变量，汇总每个窗口的变量取值情况 
	(7*) event_addindex_stat:为event_cal_stat的结果增加index变量，便于画图
	(8*) event_stack_stat: 将event_cal_stat或者event_addindex_stat的结果进行stack，便于查看
	(9) event_mdf_stat: 将event_cal_stat或者event_addindex_stat的结果进行简化，并转置win.方便输出到报告。输出特定窗口区间的rtn和prob 
********/




/* 模块1A: 根据窗口长度，生成事件窗口*/
/* 输入
	(1) event_table: event_id / event_date(交易日) / stock_code /其他
	(2) start_win: 窗口起始
	(3) end_win：窗口结束
	(4) busday_table: date

/** 输出:
       (1) output_table: 在event_table基础上新增：event_date/win
/** 外部函数:
	(1) ~/日期_通用函数/map_date_to_index
***/
%MACRO event_gen_windows(event_table, start_win, end_win, busday_table, output_table);
	%map_date_to_index(&busday_table., &event_table., event_date, tt_event);
	%map_date_to_index(&busday_table.,&busday_table., date, tt_busday);
	PROC SQL;
		CREATE TABLE &output_table. AS
		SELECT A.*, 
			B.date AS win_date,
			B.date_index-A.date_index AS win
		FROM tt_event A LEFT JOIN tt_busday B
		ON &start_win. <=B.date_index-A.date_index<= &end_win.
		ORDER BY A.event_id, win_date;

		ALTER TABLE &output_table.
		DROP COLUMN date_index;

		DROP TABLE tt_event, tt_busday;
	QUIT;
	
%MEND event_gen_windows;

/* 模块1B: 根据窗口起始结束日期，生成事件窗口*/
/* 输入
	(1) event_table: event_id / event_date(交易日) / stock_code / &start_col. / &end_col.
	(2) start_col: 窗口起始
	(3) end_col：窗口结束
	(4) busday_table: date

/** 输出:
       (1) output_table: 在event_table基础上新增：event_date/win
***/
%MACRO event_gen_fixed_windows(event_table, start_col, end_col, busday_table, output_table);
	%map_date_to_index(&busday_table., &event_table., event_date, tt_event);
	%map_date_to_index(&busday_table.,&busday_table., date, tt_busday);

	PROC SQL;
		CREATE TABLE &output_table. AS
		SELECT A.*, 
			B.date AS win_date,
			B.date_index-A.date_index AS win
		FROM tt_event A LEFT JOIN tt_busday B
		ON &start_col. <=B.date<= &end_col.
		ORDER BY A.event_id, win_date;

		ALTER TABLE &output_table.
		DROP COLUMN date_index;

		DROP TABLE tt_event, tt_busday;
	QUIT;
	
%MEND event_gen_fixed_windows;


/* 模块1C: 根据事件买入和卖出日期，确定可交易区间*/
/** !!!! 注：这里的日期认为是信息可得，且可交易的日期。如果信息是在X日收盘后才可得到，则buy_col应该设定为X+1 */
/* 输入
	(1) event_table: event_id / event_date(交易日) / stock_code /&buy_col. / &sell_col.
	(2) buy_col: 买入日期
	(3) sell_col：卖出日期
	(4) busday_table: date
	(5) hq_table: 个股行情数据，包括end_date/stock_code/close/factor/vol/high/low/open等

/** 输出:
       (1) output_table: 在event_table基础上新增：trade_buy_date和trade_sell_date
**/

%MACRO event_gen_trade_date(event_table, buy_col, sell_col, busday_table, hq_table, output_table, threshold_rtn=0.095);
	PROC SQL;
		CREATE TABLE tt_hq_table AS
		SELECT *
		FROM &hq_table.
		WHERE stock_code IN (SELECT stock_code FROM &event_table.)
		AND end_date >= (SELECT min(&buy_col.) FROM &event_table.)
		AND end_date <= (SELECT max(date) FROM &busday_table.);
	QUIT;
	
	/** 延后买入时间 */
	PROC SQL;
		CREATE TABLE tt_event_table AS
		SELECT A.event_id, min(end_date) AS trade_buy_date FORMAT yymmdd10.
		FROM &event_table. A LEFT JOIN tt_hq_table B
		ON B.stock_code = A.stock_code 
		AND B.end_date >= A.&buy_col. 
		AND B.end_date < A.&sell_col.
		AND B.vol >0 AND (B.low < B.high ) AND round(close/pre_close-1,0.01)<=&threshold_rtn.  /* 涨幅不超过限定 */
		GROUP BY event_id;
	QUIT;
	
	/** 延后卖出时间 */
	PROC SQL;
		CREATE TABLE tt_event_table2 AS
		SELECT A.event_id, min(end_date) AS trade_sell_date FORMAT yymmdd10.
		FROM &event_table. A LEFT JOIN tt_hq_table B
		ON A.stock_code = B.stock_code
		AND B.end_date >= A.&sell_col.
		AND B.vol >0 AND (B.low < B.high ) AND round(close/pre_close-1,0.01) >= -&threshold_rtn.  
		 /** 只考虑跌停不能卖，不考虑停牌不能卖 */ 
		AND round(close/pre_close-1,0.01) >= -&threshold_rtn.   
		GROUP BY A.event_id;
	QUIT;
	PROC SQL;
		CREATE TABLE tt_event_table3 AS
		SELECT A.*, B.trade_buy_date, C.trade_sell_date
		FROM &event_table. A LEFT JOIN tt_event_table B
		ON A.event_id = B.event_id
		LEFT JOIN tt_event_table2 C
		ON A.event_id = C.event_id
		ORDEr BY A.event_id;
	QUIT;
	 DATA &output_table.;
		SET tt_event_table3;
	RUN;
	PROC SQL;
		DROP TABLE tt_hq_table, tt_event_table, tt_event_table2, tt_event_table3;
	QUIT;
%MEND event_gen_trade_date;




/** 模块2：标注事件窗口计算收益的基准日，并获取基本信息和收益率 */
/* 输入
	(1) win_table: event_gen_windows的输出
	(2) buy_win: 买入日期(当日收盘买入，0-表示事件日，为常用设定)
	(3) hq_table: 个股行情数据，包括end_date/stock_code/close/factor/vol/high/low/open等
	(4) bm_table: 基准行情数据（支持基准随着日期和股票而变化): end_date/stock_code/close/factor
	(5) busday_table: date

/** 输出:
       (1) output_table: 在win_table基础上新增：
			(a) date_1_mdf：计算累计收益的起始日(收益日不含当天)
			(b) date_2_mdf：计算累计收益的结束日（收益日含当天）
			(c) rtn: 单日收益率
			(d) accum_rtn/bm_accum_rtn:累计收益率/基准累计收益率
			(e) alpha: 单日alpha
			(f) rel_rtn: 累计的相对收益率(个股和基准累计收益率之比)
			(g) filter: 标注窗口特征(0-无，1-因为一字涨跌停无法操作 2-停牌)
/** 外部函数:
	(1) ~/日期_通用函数/map_date_to_index
***/


%MACRO event_get_marketdata(win_table, buy_win, hq_table, bm_table, busday_table, output_table);
	/** Step1- 取基准日(当天收盘买入) */
	PROC SQL;
		CREATE TABLE tt_win_table AS
		SELECT A.*, 
			B.win_date AS buy_date
		FROM &win_table. A LEFT JOIN (
			SELECT event_id, win_date
			FROM &win_table.
			WHERE win = &buy_win.) B
		ON A.event_id = B.event_id;
	QUIT;

	/* Step2- 标注计算收益的基准日期 */
	/* date_2: 持有结束日 */
	/* date_1: 持有开始日 */
	/* num_1和num_2：在计算(date_1,date_2]之间的收益率时，是否需要对时间进行处理。主要针对：基准日及之前日期的处理 */
	DATA tt_win_table;
		SET tt_win_table;
		/* 买入日之前 */
		/* 例：假设T=0为买入日，则T=-1关心的是[-1]当天的收益率，不含T=0 */
		IF win < &buy_win. THEN DO;
			date_1 = win_date;
			date_2 = buy_date;
			num_1 = -1;  
			num_2 = -1;
		END;
		ELSE IF win = &buy_win. THEN DO;
			date_1 = win_date;
			date_2 = buy_date;
			num_1 = -1;
			num_2 = 0;
		END;
		ELSE IF win > &buy_win. THEN DO;
			date_1 = buy_date;
			date_2 = win_date;
			num_1 = 0;
			num_2 = 0;
		END;
		FORMAT date_1 date_2 yymmdd10.;
	RUN;

	/** Step3: 取date_1(date_2)经过num_1(num_2)平移过后的日期 */
	%map_date_to_index(&busday_table., tt_win_table, date_1, tt_win_table, index_name=date_1_index);
	%map_date_to_index(&busday_table., tt_win_table, date_2, tt_win_table, index_name=date_2_index);
	%map_date_to_index(&busday_table.,&busday_table., date, tt_busday);
	PROC SQL;
		CREATE TABLE tmp AS
		SELECt A.*, 
			B.date AS date_1_mdf,
			C.date AS date_2_mdf
		FROM tt_win_table A LEFT JOIN tt_busday B
		ON A.date_1_index + num_1 = B.date_index
		LEFT JOIN tt_busday C
		ON A.date_2_index + num_2 = C.date_index
		ORDER BY event_id, win_date;

		ALTER TABLE tmp
		DROP COLUMN date_1, date_2, num_1, num_2, date_1_index, date_2_index;
	QUIT;

	/* Step4: 取对应的个股行情数据 */
	PROC SQL;
		CREATE TABLE tt_hq_table AS
		SELECT *
		FROM &hq_table.
		WHERE stock_code IN (SELECT stock_code FROM tmp) 
			AND end_date >= (SELECT min(date_1_mdf) FROM tmp) 
			AND end_date <= (SELECT max(date_2_mdf) FROM tmp);
	QUIT;

	PROC SQL;
		CREATE TABLE tt_win_table AS
		SELECT A.*, 
			B.vol, B.close/B.pre_close-1 AS rtn,  /* 窗口日单日收益率*/
			(C.close*C.factor)/(D.close*D.factor)-1 AS accum_rtn,  /* 累计绝对收益 */
			D.vol AS date_1_vol,   /* 用于判断收益起始日(date_1)能否买入 */
			D.close AS date_1_close,
			D.open AS date_1_open,
			D.high AS date_1_high,
			D.low AS date_1_low,
			D.pre_close AS date_1_pre_close
		FROM tmp A LEFT JOIN tt_hq_table B
		ON A.win_date = B.end_date AND A.stock_code = B.stock_code
		LEFT JOIN tt_hq_table C
		ON A.date_2_mdf = C.end_date AND A.stock_code = C.stock_code
		LEFT JOIN tt_hq_table D
		ON A.date_1_mdf = D.end_date AND A.stock_code = D.stock_code
		ORDER BY event_id, win_date;
	QUIT;

	/** Step5: 取对应的基准行情 */
	PROC SQL;
		CREATE TABLE tt_bm_table AS
		SELECT *
		FROM &bm_table.
		WHERE stock_code IN (SELECT stock_code FROM tmp) 
			AND end_date >= (SELECT min(date_1_mdf) FROM tmp) 
			AND end_date <= (SELECT max(date_2_mdf) FROM tmp);
	QUIT;

	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, 
			(rtn+1)-B.close/B.pre_close AS alpha, /* 单日alpha */
			(C.close*C.factor)/(D.close*D.factor)-1 AS bm_accum_rtn,  /* 基准累计绝对收益 */
			(accum_rtn+1)/((C.close*C.factor)/(D.close*D.factor))-1 AS rel_rtn /* 相对基准收益*/
		FROM tt_win_table A LEFT JOIN tt_bm_table B
		ON A.win_date = B.end_date AND A.stock_code = B.stock_code
		LEFT JOIN tt_bm_table C
		ON A.date_2_mdf = C.end_date AND A.stock_code = C.stock_code
		LEFT JOIN tt_bm_table D
		ON A.date_1_mdf = D.end_date AND A.stock_code = D.stock_code
		ORDER BY event_id, win_date;
	QUIT;
	
	/** Step6: 标注某些不符合要求的记录 */
	DATA tt_win_table;
		SET tmp;
		filter = 0;
		IF vol = 0 OR missing(vol) THEN DO;  /* 停牌日 */
			filter = 10;
		END;
		/** date_1无法买入 */
		ELSE IF date_1_vol = 0 OR missing(date_1_vol) THEN DO;
			filter = 20;
		END;
		ELSE IF date_1_close=date_1_open AND date_1_close = date_1_high AND date_1_close = date_1_low 
			AND (date_1_close > date_1_pre_close) THEN DO;
			filter = 21;
		END;
		ELSE IF date_1_close=date_1_open AND date_1_close = date_1_high AND date_1_close = date_1_low 
			AND (date_1_close < date_1_pre_close) THEN DO;
			filter = 22;
		END;
		DROP date_1_open date_1_close date_1_high date_1_low date_1_pre_close date_1_vol vol;
	RUN;
	/** Step7: 计算累计alpha */
	DATA &output_table.;
		SET tt_win_table;
	RUN;
	PROC SQL;
		DROP TABLE tmp,tt_hq_table, tt_bm_table, tt_busday, tt_win_table;
	QUIT;
%MEND event_get_marketdata;

/** 模块3：标注单日事件窗口是否超出上市、退市，以及是否涨跌超过10%+(一般是复牌后行为) */
/* 输入
	(1) rtn_table: event_get_marketdata的输出
	(2) stock_info_table: 个股基本信息(stock_code/list_date/delist_date/is_delist/is_st)

/** 输出:
       (1) output_table: 在rtn_table基础上对filter字段进行更新。新增情况
			(a) filter: 标注窗口特征
***/
%MACRO event_mark_win(rtn_table, stock_info_table, output_table);	
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*,  
			coalesce(C.list_date, "31dec2100"d) AS list_date FORMAT yymmdd10., 
			coalesce(C.delist_date, "31dec2100"d) AS delist_date FORMAT yymmdd10.,
			coalesce(C.is_st,0) AS is_st
		FROM &rtn_table. A LEFT JOIN &stock_info_table. C
		ON A.stock_code = C.stock_code
		ORDER BY event_id, win_date;
	QUIT;
	/** 顺序决定了优先级 */
	PROC SQL;
		UPDATE tmp 
		SET filter = CASE
			WHEN win_date <= list_date OR win_date >= delist_date THEN 30  /** 超出上市-退市记录 */
			WHEN missing(rtn) THEN 50  /* 数据本身缺失 */
			WHEN is_st = 1 AND abs(rtn)>=0.045 AND rtn>0 THEN 60
			WHEN is_st = 1 AND abs(rtn)>=0.045 AND rtn<0 THEN 61
			WHEN is_st = 0 AND abs(rtn)>=0.095 AND rtn >0 THEN 62
			WHEN is_st = 0 AND abs(rtn)>=0.095 AND rtn <0 THEN 63
			WHEN is_st = 1 AND abs(rtn)>=0.09 AND rtn >0 THEN 40
			WHEN is_st = 1 AND abs(rtn)>= 0.09 AND rtn <0 THEN 41
			WHEN is_st = 0 AND abs(rtn)>=0.15 AND rtn >0  THEN 42
			WHEN is_st = 0 AND abs(rtn)>=0.15 AND rtn <0  THEN 43
			ELSE filter
		END;
		ALTER TABLE tmp
		DROP COLUMN list_date, delist_date, is_st;
	QUIT;
	DATA &output_table.;
		SET tmp;
	RUN;
	PROC SQL;
		DROP TABLE tmp;
	QUIT;

%MEND event_mark_win;

/** 模块3B：标识连续出现单日某种状态(如连续停牌，或者涨停)的情况*/
/** 主要用于预先分析，也可知道二次平滑的影响 */
/* 输入
	(1) rtn_table: event_mark_win()的输出(需要具备filter字段)
	(2) buy_win: 买入日期(当日收盘买入，0-表示事件日，为常用设定)
	(3) filter_set: 当filter落入该集合时需要对结果进行统计

/** 输出:
       (1) output_table: 在rtn_table基础上新增字段&invalid_start_win.和&ninvalid.：

***/
%MACRO event_mark_successive_win(rtn_table, buy_win, output_table, 
			invalid_start_win=invalid_start_win, ninvalid = ninvalid, filter_set=(.));
	/** 买入日之后 */
	PROC SORT DATA = &rtn_table. OUT = tmp;
		BY event_id win_date;
	RUN;
	DATA tmp(drop = invalid ss);
		SET tmp;
		BY event_id;
		RETAIN invalid 0;
		RETAIN ss .;
		IF first.event_id THEN DO;
			invalid = 0;
			ss = .;
		END;
		IF win >= &buy_win. + 1 AND filter in &filter_set. THEN DO;
			invalid + 1;
			IF missing(ss) THEN ss = win;
		END;
		ELSE DO;
			invalid = 0;  /* 重新复位 */
			ss = .;
		END;
		&ninvalid. = invalid;
		&invalid_start_win. = ss;
	RUN;

	/** 买入之前 */
	PROC SORT DATA = tmp OUT = tmp;
		BY event_id descending win_date;
	RUN;
	DATA tmp(drop = ss invalid);
		SET tmp;
		BY event_id;
		RETAIN invalid 0;
		RETAIN ss .;
		IF first.event_id THEN DO;
			invalid = 0;
			ss = .;
		END;
		/* 这里改为win<=&buy_win.而不用&buy_win-1是为了更好地统计连续性*/
		/* 这是跟二次平滑中所使用的不同。因为那里buy_win这一天不影响任何一段累计收益 */
		IF win <= &buy_win. AND filter in &filter_set. THEN DO;	
			invalid + 1;  
			IF missing(ss) THEN ss = win;
		END;
		ELSE DO;
			invalid = 0;
			ss = .;
		END;
		/* 更新字段 */
		IF win <= &buy_win. THEN DO;
			&ninvalid. = invalid;
			&invalid_start_win. = ss;
		END;
	RUN;
	
	PROC SORT DATA = tmp OUT = &output_table.;
		BY event_id win_date;
	RUN;
	PROC SQL;
		DROP TABLE tmp;
	QUIT;
%MEND event_mark_successive_win;




/** 模块4：计算累计收益率(用单日累乘的方法)*/
/* 输入
	(1) rtn_table: event_get_marketdata的输出
	(2) buy_win: 买入日期(当日收盘买入，0-表示事件日，为常用设定)

/** 输出:
       (1) output_table: 在rtn_table基础上新增：
			(a) accum_alpha: 累计alpha
***/
%MACRO event_cal_accum_alpha(rtn_table,buy_win, output_table);
	/** 事件日之后 */
	PROC SORT DATA = &rtn_table. OUT = tmp;
		BY event_id win_date;
	RUN;
	DATA tmp;
		SET tmp;
		BY event_id;
		RETAIN accum_alpha_f .;
		IF first.event_id THEN accum_alpha_f = .;
		IF win < &buy_win. THEN accum_alpha_f = .; /* 此句可以省略 */
		IF win = &buy_win. THEN accum_alpha_f = 0;
		IF win > &buy_win. THEN accum_alpha_f = (1+accum_alpha_f)*(1+coalesce(alpha,0))-1;
	RUN;
	/** 事件日之后 */
	PROC SORT DATA = tmp OUT = tmp;
		BY event_id descending win_date;
	RUN;
	DATA tmp;
		SET tmp;
		BY event_id;
		RETAIN accum_alpha_b .;
		IF first.event_id THEN accum_alpha_b = .;
		IF win > &buy_win. THEN accum_alpha_b = .; /* 此句可以省略 */
		IF win = &buy_win. THEN accum_alpha_b = 0;
		IF win < &buy_win. THEN accum_alpha_b = (1+accum_alpha_b)*(1+coalesce(alpha,0))-1;
		accum_alpha = sum(accum_alpha_f, accum_alpha_b);
	RUN;
	PROC SQL;
		UPDATE tmp
		SET accum_alpha = coalesce(alpha,0) WHERE win = &buy_win.;  /* 这里将accum_alpha设置为0,避免alpha缺失 */
		ALTER TABLE tmp
		DROP COLUMN accum_alpha_b, accum_alpha_f;
	QUIT;
	DATA &output_table.;
		SET tmp;
	RUN;
	PROC SQL;
		DROP TABLE tmp;
	QUIT;
%MEND event_cal_accum_alpha;


/** 模块5：在考虑特殊样本点的情况下，对累计收益率进行平滑*/
/* 输入
	(1) rtn_table: event_cal_accum_alpha的输出(需要包含accum_alpha字段)
	(2) buy_win: 买入日期(当日收盘买入，0-表示事件日，为常用设定)
	(3) suffix: 在累计收益率(包含rel_rtn/accum_rtn/accum_alpha)之后新增的后缀(_suffix)
	(4) filter_set: 当filter落入该集合时需要对结果进行平滑(即：将其替换为前值)


/** 输出:
       (1) output_table: 在rtn_table基础上新增：
			(a) rel_rtn_&suffix.
			(b) accum_rtn_&suffix.
			(c) accum_alpha_&suffix.
***/

%MACRO event_smooth_rtn(rtn_table, buy_win, suffix, output_table, filter_set=(.));
	/** 买入日之后 */
	PROC SORT DATA = &rtn_table. OUT = tmp;
		BY event_id win_date;
	RUN;
	DATA tmp;
		SET tmp;
		BY event_id;
		RETAIN rel_rtn_&suffix. accum_rtn_&suffix. accum_alpha_&suffix. .;
		IF first.event_id THEN DO;
			rel_rtn_&suffix. = .;
			accum_rtn_&suffix. = .;
			accum_alpha_&suffix. = .;
		END;
		IF win = &buy_win. + 1 AND filter in &filter_set. THEN DO; /* 买入的第一天，需要将不符合的设置为缺失 */
			rel_rtn_&suffix. = .;
			accum_rtn_&suffix. = .;
			accum_alpha_&suffix. = .;
		END;

		IF (win > &buy_win.+ 1 AND filter not in &filter_set.) OR (win <= &buy_win.) THEN DO; /* 保留符合条件的 */
			rel_rtn_&suffix. = rel_rtn;
			accum_rtn_&suffix. = accum_rtn;
			accum_alpha_&suffix. = accum_alpha;
		END;
	RUN;
	
	/** 买入之前 */
	PROC SORT DATA = tmp OUT = tmp;
		BY event_id descending win_date;
	RUN;
	DATA tmp;
		SET tmp;
		BY event_id;
		RETAIN rel_rtn_tmp accum_rtn_tmp accum_alpha_tmp .;
		IF first.event_id THEN DO;
			rel_rtn_tmp = .;
			accum_rtn_tmp = .;
			accum_alpha_tmp = .;
		END;
		IF win = &buy_win.-1 AND filter in &filter_set. THEN DO;  /* 买入日前，收益计算的最后一天 */
			rel_rtn_&suffix. = .;
			accum_rtn_&suffix. = .;
			accum_alpha_&suffix. = .;
		END;
			
		IF (win < &buy_win.-1 AND filter in &filter_set.) THEN DO; /* 替换不符合条件的 */
			rel_rtn_&suffix. = rel_rtn_tmp;
			accum_rtn_&suffix. = accum_rtn_tmp;
			accum_alpha_&suffix. = accum_alpha_tmp;
		END;
		rel_rtn_tmp = rel_rtn_&suffix.;
		accum_rtn_tmp = accum_rtn_&suffix.;
		accum_alpha_tmp = accum_alpha_&suffix.;
		DROP rel_rtn_tmp accum_rtn_tmp accum_alpha_tmp;
	RUN;
	PROC SQL;
		UPDATE tmp
		SET rel_rtn_&suffix. = . , 
			accum_rtn_&suffix. = .,
			accum_alpha_&suffix. = .
			WHERE win = &buy_win. AND (filter in &filter_set. AND filter NOT IN (20,21,22)); /* 买入日不应该影响事件当天 */
	QUIT;

	PROC SORT DATA = tmp OUT = &output_table.;
		BY event_id win_date;
	RUN;
	PROC SQL;
		DROP TABLE tmp;
	QUIT;
%MEND event_smooth_rtn;

/** 模块5B：在考虑特殊样本点的情况下，对累计收益率进行二次平滑(主要考虑“连续”停牌超过N天或者涨跌停超过N天)*/
/* 输入
	(1) rtn_table: event_cal_accum_alpha的输出(需要包含accum_alpha字段)
	(2) buy_win: 买入日期(当日收盘买入，0-表示事件日，为常用设定)
	(3) suffix: 在累计收益率(包含rel_rtn/accum_rtn/accum_alpha)之后新增的后缀(_suffix)
	(4) filter_set: 当filter落入该集合时需要对结果进行平滑(即：将其替换为前值)


/** 输出:
       (1) output_table: 在rtn_table基础上替换以下字段的值：
			(a) rel_rtn_&suffix.
			(b) accum_rtn_&suffix.
			(c) accum_alpha_&suffix.
***/

%MACRO event_second_smooth_rtn(rtn_table, buy_win, suffix, output_table, threshold = 5, filter_set=(.));
	/** 买入日之后 */
	PROC SORT DATA = &rtn_table. OUT = tmp;
		BY event_id win_date;
	RUN;
	DATA tmp(drop = invalid mark);
		SET tmp;
		BY event_id;
		RETAIN invalid 0;
		RETAIN mark 0;
		IF first.event_id THEN DO;
			invalid = 0;
			mark = 0;
		END;
		IF win >= &buy_win. + 1 AND filter in &filter_set. THEN invalid + 1;
		ELSE invalid = 0;  /* 重新复位 */
		IF invalid >= &threshold. THEN DO;
			mark = 1;   /* 此后都标注为无效样本 */
		END;
		IF mark = 1 THEN DO;
			rel_rtn_&suffix. = .;
			accum_rtn_&suffix. = .;
			accum_alpha_&suffix. = .;
		END;
	RUN;

	/** 买入之前 */
	PROC SORT DATA = tmp OUT = tmp;
		BY event_id descending win_date;
	RUN;
	DATA tmp(drop = mark invalid);
		SET tmp;
		BY event_id;
		RETAIN invalid 0;
		RETAIN mark 0;
		IF first.event_id THEN DO;
			invalid = 0;
			mark = 0;
		END;
		IF win <= &buy_win.-1 AND filter in &filter_set. THEN invalid + 1;
		ELSE invalid = 0;
		IF invalid >= &threshold. THEN DO;
			mark = 1;
		END;
		IF mark = 1 THEN DO;
			rel_rtn_&suffix. = .;
			accum_rtn_&suffix. = .;
			accum_alpha_&suffix. = .;
		END;
	RUN;

	PROC SORT DATA = tmp OUT = &output_table.;
		BY event_id win_date;
	RUN;
	PROC SQL;
		DROP TABLE tmp;
	QUIT;
%MEND event_second_smooth_rtn;



/** 模块6： 根据提供的分组变量，汇总每个窗口的变量取值情况 */
/* 输入:
	(1) rtn_table: event_smooth_rtn的输出
	(2) rtn_var: 字符，可以选择alpha, rtn, accum_rtn(可带后缀), rel_rtn(可带后缀), accum_alpha(可带后缀)等
	(3) filter_set：剔除掉filter落入该集合的记录
	(4) group_var: 分组变量，以空格间隔
	
/** 输出：
	(1) output_table:
			(a) win
			(b) obs/&rtn_var./prob:样本数，均值和胜率
			(c) std_&rtn_var.: 波动率
			(d) pct100-0共有7档分位数
**/

%MACRO event_cal_stat(rtn_table, rtn_var, output_table,group_var=, filter_set=(.));

	%LET group_str = win &group_var.;
	
	DATA tt_rtn_table(keep=  event_id &group_str. &rtn_var. is_hit);
		SET &rtn_table.;
		IF filter not in &filter_set.;
		IF not missing(&rtn_var.) AND &rtn_var. > 0 THEN is_hit = 1;
		ELSE IF not missing(&rtn_var.) THEN is_hit = 0;
		ELSE is_hit = .;
	RUN;
	PROC SORT DATA = tt_rtn_table;
		BY &group_str.
		;
	RUN;
	/** 分组汇总 */
	PROC UNIVARIATE DATA = tt_rtn_table NOPRINT;
		BY &group_str.;
		VAR &rtn_var. is_hit;
		OUTPUT OUT = tt_stat N = obs mean = &rtn_var. prob std = std_&rtn_var. 
			pctlpts = 100 90 75 50 25 10 0  pctlpre = pct;
	QUIT;
	DATA &output_table.;
		SET tt_stat;
	RUN;
	PROC SQL;
		DROP TABLE tt_rtn_table, tt_stat;
	QUIT;
%MEND event_cal_stat;


/** 模块7： 为event_cal_stat的结果增加index变量，便于画图 */
/* 输入:
	(1) stat_table: event_cal_stat的输出
	(2) rtn_var: 字符，!!只支持累计变量，包括accum_rtn(可带后缀), rel_rtn(可带后缀), accum_alpha(可带后缀)等
	(3) buy_win: 买入日期(当日收盘买入，0-表示事件日，为常用设定)，这是因为当天的index需要设定为100。注意保持与事件研究中的buy_win一致
	(4) group_var: 分组变量，以空格间隔
	
/** 输出：
	(1) output_table:在stat_table中增加
			(a) &ret_var._index
**/


%MACRO event_addindex_stat(stat_table, rtn_var, buy_win, output_table, group_var=);
	%LET group_str = &group_var. descending win;  /* win逆序 */
	PROC SORT DATA = &stat_table. OUT = tt_stat_table;
		BY &group_str.
		;
	RUN;
	DATA tt_stat_table;
		SET tt_stat_table;
		RETAIN rtn_0 .;
		&rtn_var._tmp = lag(&rtn_var.);  /* 只有在win<&buy_win.-1时会用到，所以不需要用first进行重新设定了*/
		IF win = &buy_win. THEN DO;
			&rtn_var._index = 100;
			rtn_0 = &rtn_var.;
		END;
		IF win > &buy_win. THEN &rtn_var._index = 100*(1+&rtn_var.);
		IF win = &buy_win.-1 THEN &rtn_var._index = 100/(1+rtn_0);
		IF win < &buy_win.-1 THEN &rtn_var._index = 100/(1+rtn_0)*(1+&rtn_var._tmp);
	RUN;
	DATA &output_table.;
		SET tt_stat_table;
		DROP &rtn_var._tmp rtn_0;
	RUN;
	%LET group_str = &group_var. win;  /* win升序 */
	PROC SORT DATA = &output_table.;
		BY &group_str.
		;
	RUN;
	PROC SQL;
		DROP TABLE tt_stat_table;
	QUIT;
%MEND event_addindex_stat;

/** 模块8： 将event_cal_stat或者event_addindex_stat的结果进行stack，便于查看*/
/* 输入:
	(1) stat_table: event_cal_stat或者event_addindex_stat的输出
	(2) group_var: 分组变量，以空格间隔.(注意与stat_table生成过程的参数保持一致)
	
/** 输出：
	(1) output_table:
			(a) win
			(b) variable: 统计变量的名称，如nobs/prob等
			(c) data: 对应变量的取值
**/
%MACRO event_stack_stat(stat_table, output_table, group_var=);
	%LET group_str = win &group_var.;
	PROC SORT DATA = &stat_table. OUT = tt;
		BY &group_str.;
	RUN;
	PROC TRANSPOSE DATA = tt OUT = tt;
		BY &group_str.;
	RUN;
	DATA &output_table.;
		SET tt;
		variable = trim(left(_NAME_));
		data =col1;
		KEEP &group_str. variable data;
	RUN;
	PROC SORT DATA = &output_table.;
		BY win variable &group_var.;
	RUN;
%MEND event_stack_stat;

/** 模块9： 将event_cal_stat或者event_addindex_stat的结果进行简化，并转置win.方便输出到报告。
			输出特定窗口区间的rtn和prob **/
		
/* 输入:
	(1) stat_table: event_cal_stat或者event_addindex_stat的输出
	(2) rtn_var: 字符，可以选择alpha, rtn, accum_rtn(可带后缀), rel_rtn(可带后缀), accum_alpha(可带后缀)等
	(3) group_var: 分组变量，以空格间隔.(注意与stat_table生成过程的参数保持一致)
	(4) win_set: 只输出该集合中的统计变量
	
/** 输出：
	(1) output_table:
			(a) _NAME_: 统计变量的名称
			(b) 各个窗口对应一个变量
**/
%MACRO event_mdf_stat(stat_table, rtn_var, output_table,group_var=, win_set=(-60,-40,-20,-10,-5,0,5,10,20,40,60,120));
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT *
		FROM &stat_table.
		WHERE win in &win_set.
		;
	QUIT;
	%IF %SYSEVALF(&group_var.~=) %THEN %DO;
		PROC SORT DATA = tmp;
			BY &group_var.;
		RUN;

		PROC TRANSPOSE DATA = tmp OUT = &output_table.;
			VAR &rtn_var. prob obs;
			BY &group_var.;
			ID win;
		RUN;
		PROC SORT DATA = &output_table.;
			BY _NAME_ &group_var.;
		RUN;
	%END;
	%ELSE %DO;
		PROC TRANSPOSE DATA = tmp OUT = &output_table.;
			VAR &rtn_var. prob obs;
			ID win;
		RUN;
		PROC SORT DATA =  &output_table.;
			BY _NAME_;
		RUN;
	%END;
	PROC SQL;
		ALTER TABLE &output_table.
		DROP COLUMN _LABEL_;
		DROP TABLE tmp;
	QUIT;
%MEND event_mdf_stat;

	

	
		





