/** ======================说明=================================================**/

/*** 函数功能：提供与事件研究相关的函数。主要包括:
(1) 过滤不符合条件的事件
(2) 生成事件窗口（包含有重叠和无重叠版本）
(2) 过滤事件窗口，并标注事件窗口的有效性
(3) 计算事件窗口的收益率
**/ 

/**** 函数列表:
(1) filter_event: 过滤不符合条件的事件
(2) mark_event_win: 过滤事件窗口，并标注事件窗口的有效性
(3) gen_overlapped_win: 生成事件窗口(允许重叠）
(4) gen_no_overlapped_win：生成事件窗口(不允许重叠）
(5) cal_access_ret: 计算事件窗口的收益率
****/ 



/** =======================================================================**/




/* 模块1: 过滤事件。剔除以下事件：
(1) 上市时间未超过N个自然日（默认为365天，即一年)
(2) 非ST股票 
(3) 非A股
(4) 已退市
(5) 停牌时间超过N个交易日(默认为20个交易日)  ---> 可以事先统计一下：停牌的事件占比，并且已经停牌了多少天。
***/

/* INPUT:
	(1) event_table: event_id/event_date/stock_code/其他
	(2) stock_info_table(仅限于所有的A股): stock_code/stock_name(可选)/list_date/delist_date/is_st
	(3) market_table: stock_code/end_date/is_halt/halt_days(只有在is_halt=1的时候才计算)/is_in_pool
	(4) ndays: 上市之后的N个自然日。要求ndays>=0 (即上市当天予以剔除。避免新股的影响)
	(5) halt_days: 停牌时间超过N个交易日。要求halt_days>=0 
	(6) is_filter_mark: 1-给出不符合要求的标志位filter(>0表示需要过滤)，0-直接予以剔除。

/* OUTPUT:
	(3) output_table: event_id/date/stock_code/其他

**/

%MACRO filter_event(event_table, stock_info_table, market_table, output_table, ndays = 365, halt_days = 20, is_filter_mark = 0);
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.list_date, B.delist_date, B.is_st, C.halt_days, C.is_halt
		FROM &event_table A LEFT JOIN &stock_info_table B
		ON A.stock_code = B.stock_code
		LEFT JOIN &market_table. C
		ON A.event_date = C.end_date AND A.stock_code = C.stock_code
		WHERE A.stock_code IN
		(SELECT stock_code FROM &stock_info_table.)
		ORDER BY A.event_id;
	QUIT;

	DATA &output_table.(drop =  list_date delist_date is_st halt_days is_halt);
		SET tmp;
		%IF %SYSEVALF(&is_filter_mark. =1) %THEN %DO;
			filter = 0;
			IF missing(list_date) OR (NOT missing(list_date) AND event_date - list_date <= &ndays.) THEN filter = 1;
			IF is_st = 1 THEN filter = 2;
			IF not missing(delist_date) AND event_date >= delist_date THEN filter = 3;
			IF missing(is_halt) THEN filter = 4;   /** 必须要有是否停牌的信息 */
			IF is_halt = 1 AND halt_days >= &halt_days. THEN filter = 5;
		%END; 
		%ELSE %DO;
			IF missing(list_date) OR (NOT missing(list_date) AND event_date - list_date <= &ndays.) THEN delete;
			IF is_st = 1 THEN delete;
			IF not missing(delist_date) AND event_date >= delist_date THEN delete;
			IF missing(is_halt) THEN delete;   /** 必须要有是否停牌的信息 */
			IF is_halt = 1 AND halt_days >= &halt_days. THEN delete;
		%END; 
	RUN;

	PROC SQL;
		DROP TABLE tmp;
	QUIT;
%MEND filter_event;

/*** 模块2：过滤事件窗口，并标注事件窗口的有效性 ***/
/* (0) 直接剔除：窗口日尚未上市，或者窗口日已经退市的记录
(1) 停牌日标注为1。在计算当日收益时，可以考虑纳入。也可以考虑不纳入（目前不纳入)。停牌日不影响后续窗口的计算（统计占比)
(2) 复牌日，涨跌停超过10%限制的，标注为2。为避免极端值影响，当日收益不予以计算。但不影响后续窗口的计算(统计占比)
(3) 复牌日，一字涨跌停的，标注为3。为剔除复牌日影响，当日收益不予以纳入。但不影响后续窗口的占比（统计占比）
(4) 非复牌日，涨跌停超过10%限制的，标注为4。为避免极端值影响，当日收益不予以计算。但不影响后续窗口的计算(统计占比)
(5) 非复牌日，一字涨跌停的，标注为5。照常计算当日收益。且不影响后续窗口的计算（统计占比）
*****/ 

/*** 注意：在计算累计收益率的时候，分两种情况统计，一种是将出现mark=2/3/4的事件收益，都统一不予计算。另外一种是全部计算。这样的收益是包含异常点的 **/

/* INPUT:
	(1) event_win_table: event_id/event_date/stock_code/win_date/win/其他
	(2) stock_info_table(仅限于所有的A股): stock_code/stock_name(可选)/list_date/delist_date/is_st
	(3) market_table: stock_code/end_date/is_limit(其中/is_halt/is_resumption
/* OUTPUT:
	(1) output_table: event_id/event_date/stock_code/win_date/win/mark/其他
***/
	


%MACRO mark_event_win(event_win_table, stock_info_table, market_table,output_table);	
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*,  C.list_date, C.delist_date, C.stock_code AS stock_code_c
		FROM &event_win_table. A LEFT JOIN &stock_info_table. C
		ON A.stock_code = C.stock_code
		ORDER BY event_id, stock_code;
	QUIT;
	DATA tmp(drop = stock_code_c list_date delist_date);
		SET tmp;
		IF missing(stock_code_c) THEN delete;
		IF win_date <= list_date OR (not missing(delist_date) AND win_date >= delist_date) THEN delete;
	RUN;
	
	PROC SQL;
		CREATE TABLE tmp2 AS
		SELECT A.*, B.is_halt, B.is_limit, B.is_resumption, B.stock_code AS stock_code_b
		FROM tmp A LEFT JOIN &market_table. B
		ON A.win_date = B.end_date AND A.stock_code = B.stock_code
		ORDER BY A.event_id, A.stock_code;
	QUIT;
	DATA &output_table.(drop = is_halt is_limit is_resumption stock_code_b);
		SET tmp2;
		IF missing(stock_code_b) THEN delete;
		IF is_halt = 1 THEN mark = 1;
		ELSE IF is_resumption = 1 AND is_limit IN (3,4) THEN mark = 2;
		ELSE IF is_resumption = 1 AND is_limit IN (1,2) THEN mark = 3;
		ELSE IF is_resumption = 0 AND is_limit IN (3,4) THEN mark = 4;
		ELSE IF is_resumption = 0 AND is_limit IN (1,2) THEN mark = 5;
		ELSE mark = 0;
	RUN;
		
	PROC SQL;
		DROP TABLE tmp, tmp2;
	QUIT;
%MEND mark_event_win;






/* 模块1: 生成事件窗口，允许不同事件的窗口之间有重叠*/
/* 输入
	(0) eventName: 字符类型
	(1) event_table: event_id / event_date(交易日) / stock_code /其他
	(2) stock_hqinfo_table: end_date/stock_code/price/last_price
	(3) bm_hqinfo_table: end_date/stock_code/price/last_price (这里每个stock可以指定不同的基准。因此price就是指定基准的价格)
	(4) start_win: 窗口起始
	(5) end_win：窗口结束
	(6) busday_table: date (这个避免使用外部表）

/** 输出:
       (1) &eventName._hq: event_id/event_date/stock_code/win_date/win/price/last_price/bm_price/last_bm_price

/** 外部函数:
	(1) ~/日期_通用函数/map_date_to_index

***/
%MACRO gen_overlapped_win(eventName, event_table, stock_hqinfo_table, bm_hqinfo_table, start_win, end_win, busday_table = busday);
	
	PROC SQL;
		CREATE TABLE tt_hqinfo_with_bm AS
		SELECT A.end_date, A.stock_code, A.price, A.last_price,
			B.price AS bm_price LABEL "bm_price", B.last_price AS bm_last_price LABEL "bm_last_price"
		FROM &stock_hqinfo_table. A LEFT JOIN &bm_hqinfo_table. B
		ON A.end_date = B.end_Date AND A.stock_code = B.stock_code
		WHERE A.stock_code IN
		(SELECT stock_code FROM &event_table.)
		ORDER BY A.end_date, A.stock_code;
	QUIT;

	%map_date_to_index(&busday_table., &event_table., event_date, busday_copy)
	%map_date_to_index(&busday_table.,tt_hqinfo_with_bm, end_date, tt_hqinfo_with_bm)

	PROC SQL;
		CREATE TABLE &eventName._hq AS
		SELECT A.*, B.stock_code AS stock_code_b, B.price, B.bm_price,B.last_price, B.bm_last_price, 
				B.end_date AS win_date LABEL "win_date", B.date_index-A.date_index AS win
		FROM busday_copy A LEFT JOIN  tt_hqinfo_with_bm B
		ON A.stock_code = B.stock_code AND &start_win. <=B.date_index-A.date_index<= &end_win.
		ORDER BY A.event_id, B.end_date;
	QUIT;

	DATA &eventName._hq(drop = stock_code_b date_index);
		SET &eventName._hq;
		IF NOT MISSING(stock_code_b);
		FORMAT event_date win_date yymmdd10.;
	RUN;

	PROC SQL;
		DROP TABLE busday_copy, tt_hqinfo_with_bm;
	QUIT;
%MEND gen_overlapped_win;


/* 模块2: 生成事件窗口，不允许不同事件的窗口之间有重叠，会有窗口的截断*/

/* 输入
	(0) eventName: 字符类型
	(1) event_table: event_id / date(交易日) / stock_code /其他
	(2) stock_hqinfo_table: end_date/stock_code/price/last_price
	(3) bm_hqinfo_table: end_date/stock_code/price/last_price (这里每个stock可以指定不同的基准。因此price就是指定基准的价格)
	(4) start_win: 窗口起始
	(5) end_win：窗口结束

/** 输出:
    (1) &eventName._hq: event_id/event_date/stock_code/win_date/win/price/last_price/bm_price/last_bm_price


/** 外部函数:
	(1) ~/日期_通用函数/map_date_to_index

***/

%MACRO gen_no_overlapped_win(eventName, event_table, stock_hqinfo_table, bm_hqinfo_table, start_win, end_win);
	PROC SQL;
		CREATE TABLE tt_hqinfo_with_bm AS
		SELECT A.end_date, A.stock_code, A.price, A.last_price,
				B.price AS bm_price LABEL "bm_price", B.last_price AS bm_last_price LABEL "bm_last_price"
			FROM &stock_hqinfo_table. A LEFT JOIN &bm_hqinfo_table. B
			ON A.end_date = B.end_Date AND A.stock_code = B.stock_code
			WHERE A.stock_code IN
			(SELECT stock_code FROM &event_table.)
			ORDER BY A.end_date, A.stock_code;

		CREATE TABLE &eventName._hq AS
			SELECT A.end_date AS win_date, A.stock_code AS stock_code_a,
				A.price, A.last_price, A.bm_price, A.bm_last_price, B.*
			FROM tt_hqinfo_with_bm A LEFT JOIN &event_table. B   /* 只限于所有的A股 */
			ON A.stock_code = B.stock_code and A.end_date = B.event_date
			ORDER BY A.stock_code, A.end_date;

		DROP TABLE tt_hqinfo_with_bm;
	QUIT;
	
	/* mark event windows (next/previous) */
	DATA &eventName._hq_next(drop = cur_required);
		SET	&eventName._hq;
		BY stock_code_a;
		RETAIN next_win .;
		RETAIN cur_required 0;
		RETAIN next_event_id .;
		RETAIN next_event_date .;
		IF first.stock_code_a THEN DO;
			cur_required = 0;
			next_win = .;
			next_event_id = .;
			next_event_date = .;
		END;
		IF event_id ~=. THEN DO;
			next_win = -1;
			cur_required = 1;
			next_event_id = event_id;
			next_event_date = event_date;
		END;
		IF cur_required = 1 THEN next_win + 1;
	RUN;
	
	PROC SORT DATA = &eventName._hq;
		BY stock_code_a descending win_date;
	RUN;

	DATA &eventName._hq_pre(drop = cur_required);
		SET	&eventName._hq;
		BY stock_code_a;
		RETAIN pre_win .;
		RETAIN cur_required 0;
		RETAIN pre_event_id .;
		RETAIN pre_event_date .;
		IF first.stock_code_a THEN DO;
			cur_required = 0;
			pre_win = .;
			pre_event_id = .;
			pre_event_date = .;
		END;	
		IF event_id ~=. THEN DO;
			pre_win = 1;
			cur_required = 1;
			pre_event_id = event_id;
			pre_event_date = event_date;
		END;
		IF cur_required = 1 THEN pre_win = pre_win -1 ;		
	RUN;

	PROC SORT DATA = &eventName._hq_pre;
		BY stock_code_a win_date;
	RUN;


	DATA &eventName._hq;
		SET &eventName._hq_pre(drop = event_id event_date rename = (pre_event_id = event_id pre_win = win pre_event_date = event_date)) 
			&eventName._hq_next(drop = event_id event_date rename = (next_event_id = event_id next_win = win next_event_date = event_date));
		IF event_id ~=.;
		FORMAT event_date yymmdd10.;
	RUN;

	PROC SORT DATA = &eventName._hq NODUP;  /* event day( win = 0) has two records */
		BY _ALL_;
	RUN; 

	DATA &eventName._hq(drop = stock_code_b stock_code_a);
		SET &eventName._hq(rename = (stock_code = stock_code_b));
		IF &start_win <= win <= &end_win;
		stock_code = stock_code_a;
		FORMAT event_date win_date yymmdd10.;
	RUN;
	PROC SORT DATA =  &eventName._hq;
		BY event_id win_date;
	RUN;
	
	PROC SQL;
		DROP TABLE &eventName._hq_pre, &eventName._hq_next;
	QUIT;

%MEND gen_no_overlapped_win;



/* 模块3: 计算超额收益率 */


/* 输入
	(0) eventName: 字符类型
	(1) event_hq_table: 由gen_no_overlapped_win或者gen_overlapped_win生成的表格。
		包括: event_id/event_date/win_date/win/stock_code/price/last_price/bm_price/bm_last_price/mark
	(2) buy_win：买入窗口点(在收盘买入。如: buy_win = 0 表示在事件日收盘买入)
	(3) start_win: 窗口起始
	(4) end_win：窗口结束
	(5) filter_invalid ： 1-在计算累计收益率的时候，将出现mark=2/3/4的事件收益，都统一不予计算。0-全部计算。这样的收益是包含异常点的 **/

/** 输出:
    (1) &eventName._%eval(-&start_win)_&end_win:  event_id/event_date/win_date/win/stock_code +
		alpha 
		accum_alpha(accumulative alpha from the start_win) 
		event_valid(valid if no return exceeds 10%)
		realized_alpha(realized alpha before the events)
 		is_d_alpha_pos (daily alpha is positive?)
		is_a_alpha_pos (accumulative alpha is positive?)
**/


%MACRO cal_access_ret(eventName, event_hq_table, buy_win, start_win, end_win, filter_invalid);
	/* 计算单日alpha */
	PROC SQL;
		CREATE TABLE &eventName._alpha AS 
			SELECT *,
			(price/last_price-1)*100 AS abs_ret,
			(price/last_price - bm_price/bm_last_price)*100 AS alpha 
			FROM &event_hq_table.
			ORDER BY event_id, win;
	QUIT;
	
	/* 计算起始日开始的累积alpha并标注事件是否有效 */
	DATA &eventName._alpha(drop = start_price start_bm_price);
		SET &my_library..&eventName._alpha;
		BY event_id;
		RETAIN accum_alpha_after 0;
		REATAIN accum_ret_after 0;
		RETAIN valid 1;
		
		IF first.event_id THEN DO;
			accum_alpha_after = 0;
			accum_ret_after = 0;
			valid = 1;
		E
		






		RETAIN accum_alpha_after 0;
		RETAIN valid 1;
		RETAIN start_price .;
		RETAIN start_bm_price .;
		accum_after_direct_f = .;
		accum_after_direct = .;
		ret_after_direct = .;

		IF first.event_id THEN DO;
			accum_alpha_after = 0;
			start_price = .;
			start_bm_price =.;
			valid = 1;
		END;
		IF win = &buy_win THEN DO;
			start_price = price;
			start_bm_price = bm_price;
		END;
	

		IF win > &buy_win AND mark = -1 THEN valid = 0; 
		
		IF win > &buy_win  THEN DO;                             /* 从buy_win末开始买进 */
			IF NOT missing(alpha) AND mark = 1 THEN DO; /* 当天不是停牌或者超过涨跌幅限制才计算alpha */
				accum_alpha_after = accum_alpha_after + alpha;	
			END;
 
			accum_after_direct =  (price/start_price - bm_price/start_bm_price)*100;
			ret_after_direct = (price/start_price-1)*100;
			/* 如果出现超过涨跌停限制的交易日，则之后的都不在计算alpha*/
			IF valid = 0 THEN accum_after_direct_f = .;
			ELSE accum_after_direct_f = (price/start_price - bm_price/start_bm_price)*100;
		END;

	RUN;

	/* 计算起始日以前的累积alpha，以起始日为截止点往前倒推 */
	/* 如：起始日为0，则win=-2的值表示[-2,0]之间的收益率 */
	PROC SORT DATA = &my_library..&eventName._alpha;
		BY event_id descending win;
	RUN;

	DATA &my_library..&eventName._alpha(drop = start_price start_bm_price);
		SET &my_library..&eventName._alpha;
		BY event_id;
		accum_before_direct_f = .;
		accum_before_direct = .;
		ret_before_direct = .;
		RETAIN accum_alpha_before 0;
		RETAIN before_valid 1;
		RETAIN start_price .;
		RETAIN start_bm_price .;

		IF first.event_id THEN DO;
			accum_alpha_before = 0;
			before_valid = 1;
			start_price = .;
			start_bm_price =.;
		END;
		IF win = &buy_win THEN DO;
			start_price = price;
			start_bm_price = bm_price;
		END;

		IF win <= &buy_win AND mark = -1 THEN before_valid = 0;
			
		IF win <= &buy_win THEN DO;
			IF NOT missing(alpha) AND mark = 1 THEN DO;
				accum_alpha_before = accum_alpha_before + alpha;
			END;
			accum_before_direct = (start_price/last_price - start_bm_price/bm_last_price)*100;
			ret_before_direct = (start_price/last_price-1) * 100;
			IF before_valid = 0 THEN accum_before_direct_f = .;
			ELSE accum_before_direct_f = (start_price/last_price - start_bm_price/bm_last_price)*100;
		END;
	RUN;

	DATA &my_library..&eventName._alpha(drop = i);
		SET  &my_library..&eventName._alpha;
		IF win <= &buy_win. THEN DO;
			valid = .;
			accum_alpha_after = .;
		END;
		IF win > &buy_win. THEN DO;
			before_valid = .;
			accum_alpha_before = .;
		END;


		ARRAY alpha_array(9) alpha accum_alpha_after accum_alpha_before accum_after_direct accum_before_direct accum_after_direct_f  accum_before_direct_f
							ret_after_direct ret_before_direct;
		ARRAY mark_alpha_array(9) is_d_pos is_a_pos is_b_a_pos is_a2_pos is_b_a2_pos is_a3_pos is_b_a3_pos is_ret_pos is_b_ret_pos;

		DO i = 1 TO 9;
			IF alpha_array(i) > 0 THEN mark_alpha_array(i) = 1;
			ELSE IF NOT MISSING(alpha_array(i)) THEN mark_alpha_array(i) = 0; /* 单日alpha或者累积alpha都可能为0 */
			ELSE mark_alpha_array(i) = .;
		END;
		
		m_valid = max(valid,before_valid);  /* 算法保证针对某个特定的win,必然其中一个取值是缺失的*/
		is_m_a_pos = max(is_a_pos,is_b_a_pos); 
		is_m_a2_pos = max(is_a2_pos,is_b_a2_pos);
		is_m_a3_pos = max(is_a3_pos, is_b_a3_pos);
		is_m_ret_pos = max(is_ret_pos, is_b_ret_pos);

		accum_alpha = max(accum_alpha_before ,accum_alpha_after);
		accum_direct = max(accum_before_direct, accum_after_direct);
		accum_direct_f = max(accum_before_direct_f,accum_after_direct_f);
		ret_direct = max(ret_before_direct, ret_after_direct);
	RUN; 

	PROC SORT DATA = &my_library..&eventName._alpha;
		BY event_id win;
	RUN;
	
	DATA  &my_library..&eventName._alpha;
		SET  &my_library..&eventName._alpha(drop = accum_alpha_after accum_alpha_before accum_after_direct accum_before_direct accum_after_direct_f  accum_before_direct_f
				is_a_pos is_b_a_pos is_a2_pos is_b_a2_pos is_a3_pos is_b_a3_pos valid before_valid is_ret_pos is_b_ret_pos ret_after_direct ret_before_direct);
	RUN;

%MEND cal_access_ret;

