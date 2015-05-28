/** ======================说明=================================================**/

/*** 函数功能：提供与事件研究相关的函数。主要包括:
(1) 过滤不符合条件的事件
(2) 生成事件窗口（包含有重叠和无重叠版本）
(3) 过滤事件窗口，并标注事件窗口的有效性
(3-2) 给出每个窗口中，不同有效性的统计信息
(4) 计算事件窗口的收益率
(5) 计算事件窗口的收益率
(6) 补充信息，事件日前[N,0]的alpha和return，用于分组使用
(7) 赋予事件属性
(8) 根据提供的分组变量，汇总每个窗口的alpha/ret等的分析结果
(9) 输出某个特定窗口下，所有事件accum_alpha/accum_ret的情况


**/ 

/**** 函数列表:
(1) filter_event: 过滤不符合条件的事件
(2) mark_event_win: 过滤事件窗口，并标注事件窗口的有效性
(2-2) mark_stat: 根据过滤结果，给出每个窗口中，不同有效性的统计信息
(3) gen_overlapped_win: 生成事件窗口(允许重叠）
(4) gen_no_overlapped_win：生成事件窗口(不允许重叠）
(5) cal_win_ret: 计算事件窗口的收益率
(6) append_ahead_effect: 补充信息，事件日前[N,0]的alpha和return，用于分组使用
(7) attribute_to_event：赋予事件属性
(8) alpha_collect_by_group: 根据提供的分组变量，汇总每个窗口的alpha/ret等的分析结果
(9) alpha_detail_output: 输出某个特定窗口下，所有事件accum_alpha/accum_ret的情况
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
(1*) 停牌日标注为1。在计算当日收益时，可以考虑纳入。也可以考虑不纳入。停牌日不影响后续窗口的计算（统计占比)
(2**) 复牌日，涨跌停超过10%限制的，标注为2。为避免极端值影响，当日收益不予以计算。且可能影响后续窗口的计算(统计占比)
(3*) 复牌日，一字涨跌停的，标注为3。为剔除复牌日影响，当日收益可考虑纳入，也可考虑不纳入。但不影响后续窗口的占比（统计占比）
(4**) 非复牌日，涨跌停超过10%限制的，标注为4。为避免极端值影响，当日收益不予以计算。且可能影响后续窗口的计算(统计占比)
(5) 非复牌日，一字涨跌停的，标注为5。照常计算当日收益。且不影响后续窗口的计算（统计占比）
*****/ 

/*** 注意：在计算累计收益率的时候，分两种情况统计，一种是将出现mark=2/4的事件收益，都统一不予计算。另外一种是全部计算。这样的收益是包含异常点的 **/

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

/*** 模块2-2：根据过滤结果，给出每个窗口中，不同有效性的统计信息 ***/

/* INPUT:
	(1) event_win_table: mark_event_win的输出结果(mark = 0-5)
/* OUTPUT:
	(1) output_table: win/n_events/mark_i（有几个事件出现了mark)
***/

%MACRO mark_stat(event_win_table, output_table);
	PROC SQL NOPRINT;
		SELECT distinct mark, count(distinct mark)
		INTO :mark_group SEPARATED BY ' ',
			 :n_mark
		FROM &event_win_table.
		ORDEr BY mark;
	QUIT;
	PROC SQL;
		CREATE TABLE tt_result AS
		SELECT win, count(1) AS n_events
		FROM &event_win_table.
		GROUP BY win;
	QUIT;
	%DO i = 1 %TO &n_mark.;
		%LET cur_mark = %SCAN(&mark_group.,&i., ' ');
		PROC SQL;
			CREATE TABLE tt_stat AS
			SELECT A.*, B.nobs AS mark_&cur_mark.
			FROM tt_result A LEFT JOIN
			(SELECT win, count(1) AS nobs
			 FROM &event_win_table.
			 WHERE mark = &cur_mark.
			 GROUP BY win) B
			 ON A.win = B.win
			 ORDER BY A.win;
		QUIT;
		DATA tt_result;
			SET tt_stat;
		RUN;
		PROC SQL;
			DROP TABLE tt_stat;
		QUIT;
	%END;

	PROC CONTENTS DATA = tt_result OUT = tt_result_name(keep = name) NOPRINT;
    RUN;
    PROC SQL NOPRINT;
          SELECT name, count(1)
          INTO :name_list SEPARATED BY ' ',
               :n_name
          FROM tt_result_name
		  WHERE upcase(name) not IN ("WIN", "N_EVENTS");
     QUIT;

	DATA &output_table.(drop = i);
		SET tt_result;
		ARRAY var_list(&n_name.) &name_list.;
		DO i = 1 TO &n_name.;
			IF missing(var_list(i)) THEN var_list(i) = 0;
		END;
	RUN;
	PROC SQL;
		DROP TABLE tt_result;
	QUIT;
%MEND mark_stat;
	
	


/* 模块3: 生成事件窗口，允许不同事件的窗口之间有重叠*/
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


/* 模块4: 生成事件窗口，不允许不同事件的窗口之间有重叠，会有窗口的截断*/

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



/* 模块5: 计算每个窗口的绝对收益和超额收益率 */
/* 输入
	(0) eventName: 字符类型
	(1) event_hq_table: 由gen_no_overlapped_win或者gen_overlapped_win生成的表格。
		包括: event_id/event_date/win_date/win/stock_code/price/last_price/bm_price/bm_last_price/mark
	(2) buy_win：买入窗口点(在收盘买入。如: buy_win = 0 表示在事件日收盘买入)
	(3) filter_invalid_after： 1-在计算事件后累计收益率的时候，将出现mark=2/4的事件收益，都统一不予计算。0-全部计算。这样的收益是包含异常点的 
	(4) filter_invalid_before: 1-在计算事件前累计收益率的时候，将出现mark=2/4的事件收益，都统一不予计算。0-全部计算。这样的收益是包含异常点的。
	(5) filter_limit_after： 1-在计算事件后单日收益时，将出现mark =1/3的事件收益（停牌或者复牌后一字涨停)。0-全部计算。
	(6) filter_limit_before: 1-在计算事件前单日收益时，将出现mark =1/3的事件收益（停牌或者复牌后一字涨停)。0-全部计算。

注：在计算单日收益时（无论事件前/后)，默认都是剔除mark=2/4的情况的（否则会带来很大的干扰)。
注2：在分析alpha时，停牌如果纳入容易造成alpha低估（如果是上涨市），而复牌后的涨停又可能造成alpha高估。所以建议是将filter_limit_after/before都设定为1。
**/

/** 输出:
    (1) &eventName._%eval(-&start_win)_&end_win:  原event_hq_table中的字段+
		abs_ret: 绝对收益
		alpha：相对收益
		abs_ret_mdf: 绝对收益（如果filter_limit_after=1时，出现mark=1/3，则自动设置为缺失)
		alpha_mdf: 相对收益（如果filter_limit_after=1时，出现mark=1/3，则自动设置为缺失)
		is_d_alpha_pos: 单日alpha是否为正
		is_d_ret_pos: 单日绝对收益是否为正
		accum_alpha：累计alpha
		acccum_ret: 累计绝对收益
		accum_valid：计算的累计alpha/ret是否包含了涨停超过10%限制的情况（1- 未包含 0-包含)
		is_accum_alpha_pos：累计alpha是否为正
		is_accum_ret_pos:累计ret是否为正
		test_mark: 是否出现单日abs_ret/alpha其中之一为0 (1-未出现 0-出现) ---> 在计算累计值时，如果出现该情况，都认定当天alpha或者ret为0。
				事后需要核对该检验字段的取值
**/

%MACRO cal_win_ret(eventName, event_hq_table, buy_win, filter_invalid_after=1, filter_invalid_before = 1, 
									filter_limit_after=1, filter_limit_before=1);
	/* 计算单日alpha */
	PROC SQL;
		CREATE TABLE &eventName._alpha AS 
			SELECT *,
			(price/last_price-1)*100 AS abs_ret,
			(price/last_price - bm_price/bm_last_price)*100 AS alpha,
			&filter_invalid_after. AS filter_invalid_after,
			&filter_invalid_before. AS filter_invalid_before,
			&filter_limit_after. AS filter_limit_after,
			&filter_limit_before. AS filter_limit_before
			FROM &event_hq_table.
			ORDER BY event_id, win;
	QUIT;
	
	/* 计算起始日开始的累积alpha并标注事件是否有效 */
	DATA &eventName._alpha;
		SET &eventName._alpha;
		BY event_id;
		RETAIN accum_alpha_after 0;
		RETAIN accum_ret_after 0;
		RETAIN after_valid 1;
		test_mark = 1; /** 测试是否出现alpha或者abs_ret缺失的情况 */

		IF first.event_id THEN DO;
			accum_alpha_after = 0;
			accum_ret_after = 0;
			after_valid = 1;
		END;
		IF win > &buy_win AND mark IN (2,4) THEN after_valid = 0; 
		IF filter_invalid_after = 1 THEN DO;
			IF win > &buy_win  THEN DO;    
				IF not missing(alpha) AND not missing(abs_ret) AND after_valid = 1 THEN DO;
					accum_alpha_after = ((1+alpha/100)*(1+accum_alpha_after/100)-1)*100;
					accum_ret_after = ((1+accum_ret_after/100)*(1+abs_ret/100)-1)*100;
				END;
				ELSE IF after_valid = 0 THEN DO;
					accum_alpha_after = .;
					accum_ret_after = .;
				END;
				ELSE test_mark = 0;
			END;
		END;
		ELSE DO;
			IF win > &buy_win  THEN DO;
				IF not missing(alpha) AND not missing(abs_ret) THEN DO;
					accum_alpha_after = ((1+alpha/100)*(1+accum_alpha_after/100)-1)*100;
					accum_ret_after = ((1+accum_ret_after/100)*(1+abs_ret/100)-1)*100;
				END;
				ELSE test_mark = 0;
			END;
		END;
	RUN;

	/* 计算起始日以前的累积alpha，以起始日为截止点往前倒推 */
	/* 如：起始日为0，则win=-2的值表示[-2,0]之间的收益率 */
	PROC SORT DATA = &eventName._alpha;
		BY event_id descending win;
	RUN;

	DATA &eventName._alpha;
		SET &eventName._alpha;
		BY event_id;
		RETAIN accum_alpha_before 0;
		RETAIN accum_ret_before 0;
		RETAIN before_valid 1;

		IF first.event_id THEN DO;
			accum_alpha_before = 0;
			accum_ret_before = 0;
			before_valid = 1;
		END;

		IF win <= &buy_win AND mark IN (2,4) THEN before_valid = 0;	
		IF filter_invalid_before =1 THEN DO;	
			IF win <= &buy_win  THEN DO;    
				IF not missing(alpha) AND not missing(abs_ret) AND before_valid = 1 THEN DO;
					accum_alpha_before = ((1+alpha/100)*(1+accum_alpha_before/100)-1)*100;
					accum_ret_before = ((1+accum_ret_before/100)*(1+abs_ret/100)-1)*100;
				END;
				ELSE IF before_valid = 0 THEN DO;
					accum_alpha_before = .;
					accum_ret_before = .;
				END;
				ELSE test_mark = 0;
			END;
		END;
		ELSE DO;
			IF win <= &buy_win  THEN DO; 
				IF not missing(alpha) AND not missing(abs_ret) THEN DO;
					accum_alpha_before = ((1+alpha/100)*(1+accum_alpha_before/100)-1)*100;
					accum_ret_before = ((1+accum_ret_before/100)*(1+abs_ret/100)-1)*100;
				END;
				ELSE test_mark = 0;
			END;
		END;
	RUN;

	DATA &eventName._alpha(drop = i filter_invalid_after filter_invalid_before filter_limit_after filter_limit_before);
		SET &eventName._alpha;
		IF win <= &buy_win. THEN DO;
			after_valid = .;
			accum_alpha_after = .;
			accum_ret_after = .;
		END;
		IF win > &buy_win. THEN DO;
			before_valid = .;
			accum_alpha_before = .;
			accum_ret_before = .;
		END;
		/** 考虑是否需要调整单日收益 */
		IF filter_limit_before = 1 AND win <= &buy_win. AND mark IN (1) THEN DO;
			abs_ret_mdf = .;
			alpha_mdf = .;
		END;
		ELSE IF filter_limit_after = 1 AND win > &buy_win. AND mark IN (1) THEN DO;
			abs_ret_mdf = .;
			alpha_mdf = .;
		END;
		ELSE DO;
			abs_ret_mdf = abs_ret;
			alpha_mdf = alpha;
		END;

		ARRAY alpha_array(6) alpha_mdf abs_ret_mdf accum_alpha_after accum_ret_after accum_alpha_before accum_ret_before;
		ARRAY mark_alpha_array(6) is_d_alpha_pos is_d_ret_pos is_aft_alpha_pos is_aft_ret_pos is_bef_alpha_pos is_bef_ret_pos;

		DO i = 1 TO 6;
			IF alpha_array(i) > 0 THEN mark_alpha_array(i) = 1;
			ELSE IF NOT MISSING(alpha_array(i)) THEN mark_alpha_array(i) = 0; /* 单日alpha或者累积alpha都可能缺失 */
			ELSE mark_alpha_array(i) = .;
		END;
		/* 算法保证针对某个特定的win,必然其中一个取值是缺失的*/
		accum_valid = coalesce(after_valid,before_valid);  
		accum_alpha = coalesce(accum_alpha_before ,accum_alpha_after);
		accum_ret = coalesce(accum_ret_before, accum_ret_after);
		is_accum_alpha_pos = coalesce(is_aft_alpha_pos, is_bef_alpha_pos);
		is_accum_ret_pos = coalesce(is_aft_ret_pos, is_bef_alpha_pos);
	RUN; 

	PROC SORT DATA = &eventName._alpha;
		BY event_id win;
	RUN;
	
	DATA  &eventName._alpha;
		SET &eventName._alpha;
		drop is_aft_alpha_pos is_aft_ret_pos is_bef_alpha_pos is_bef_ret_pos;
		drop accum_alpha_after accum_ret_after accum_alpha_before accum_ret_before;
		drop after_valid before_valid;
	RUN;

%MEND cal_win_ret;


/* 模块6: 补充信息：事件日前[N,0]的alpha和return，用于分组使用 */
/* 输入
	(1) alpha_table: 由cal_win_ret生成的表格。
	(2) win_ahead：事件日前的N的取值。如果超过start_win的最大值，则取start_win
**/
/** 输出:
    (1) alpha_table_edit（建议仍为alpha_table，方便下一步分析）:原alpha_table +
			accum_alpha_ahead：[N,0]之间的累计alpha
			accum_ret_ahead：[N,0]之间的累计ret
			is_ret_pos_ahead:
			is_alpha_pos_ahead:
			win_ahead：最终确定的事件前窗口（因为有的会因为截尾，并非刚好是win_ahead)

注：当在计算cal_win_ret时选择filter_invalid_before=1，则可能会有一些事件accum_ret或accum_alpha是缺失的。
	这时候只选择，最远且满足条件的未缺失的值，作为win_ahead。因此在使用该函数时，建议设定filter_invalid_before = 0。
**/

%MACRO append_ahead_effect(alpha_table, win_ahead, alpha_table_edit);
	PROC SQL;
		CREATE TABLE tmp2 AS
		SELECT A.event_id, A.accum_ret AS accum_ret_ahead,
			A.accum_alpha AS accum_alpha_ahead,
			A.is_accum_alpha_pos AS is_alpha_pos_ahead,
			A.is_accum_ret_pos AS is_ret_pos_ahead
		FROM &alpha_table. A JOIN 
		(
			SELECT event_id,min(win) AS win
			FROM &alpha_table.
			WHERE win >= &win_ahead. AND not missing(accum_ret) AND not missing(accum_alpha)
			GROUP event_id
		) B
		ON A.event_id = B.event_id AND A.win = B.win
		ORDER BY A.event_id;
	QUIT;
	PROC SQL;
		CREATE TABLE tmp3 AS
		SELECT A.*, B.accum_alpha_ahead,
			B.accum_ret_ahead,
			B.is_alpha_pos_ahead,
			B.is_ret_pos_ahead
		FROM &alpha_table. A LEFT JOIN tmp2 B
		ON A.event_id = B.event_id
		ORDER BY event_id, win;
	QUIT;
	DATA &alpha_table_edit.;
		SET tmp3;
	RUN;
	PROC SQL;
		DROP TABLE tmp2, tmp3;
	QUIT;
%MEND append_ahead_effect;


/* 模块7：赋予事件属性 */
/* Input:
	(1) alpha_table: cal_win_ret或append_ahead_effect的输出。
	(2) attribute_table: stock_code, end_date, 相关属性列。（除stock_code和end_date外的其余所有信息都认为是属性)

注：以event_date=end_date作为连接条件。即认为属性归属限定在事件日，而不是窗口日。
如果alpha_table中本身也包含了可以作为分组的信息，如is_ret_pos_ahead等，则无需调用该函数，可直接调用alpha_collect_by_group进行分组分析

/* Output: 
	(1) alpha_table_edit： 输出(alpha_file + 所有attribute_table中出现的属性列),建议为alpha_file，方便之后进行下一步分析 */


%MACRO attribute_to_event(alpha_table, attribute_table, alpha_table_edit);
	/* 为避免重名，进行更名 */
	DATA tt;
		SET &attribute_table.;
		RENAME end_date = end_date_bb stock_code = stock_code_bb;
	RUN;

	PROC SQL;
		CREATE TABLE tmp2 AS
		SELECT A.*, B.* 
		FROM &alpha_table. A LEFT JOIN tt B
		ON A.stock_code = B.stock_code_bb AND A.event_date = B.end_date_bb
		ORDER BY A.event_id, A.win_date;
	QUIT;

	DATA &alpha_table_edit.(drop = end_date_bb stock_code_bb);
		SET tmp2;
	RUN;

	PROC SQL;
		DROP TABLE tt, tmp2;
	QUIT;
%MEND attribute_to_event;


/** 模块8： 根据提供的分组变量，汇总每个窗口的alpha/ret等的分析结果 */
/* 输入:
	(0) eventName: 事件名
	(1) alpha_table: cal_win_ret或append_ahead_effect或attribute_to_event的输出结果
	(2) is_group: 是否有其他除了win之外的分组变量，0-no, 1-yeas, group_var is valid only when this takes value 1。
	(3) group_var: 分组变量，以空格间隔。
	(4) alpha_var: 字符，可以选择alpha, abs_ret, accum_alpha, accum_ret这四个变量之一
	(5) is_output： 是否输出到外部文件夹：1- 输出（这时候filename和sheetname才有效)，0-不输出
	(6) file_name: 字符串
	(7) sheet_name: 字符串

**/

/* output: 
	(1) filename(all_&sheetname.和group_&sheetname.)：包含分组分析以及全样本的结果。
	(2) &eventName._&alpha_var._g: 包含分组分析的结果。
	(3) &eventName._&alpha_var._all: 包含全样本分析的结果。
***/


%MACRO alpha_collect_by_group(eventName, alpha_table, alpha_var, is_group, group_var, is_output = 0, file_name=., sheet_name=.);
	
	%LET alpha_var_mdf = &alpha_var.;
	%LET group_sentence = win; 
	%IF %sysevalf(&is_group. = 1) %THEN  %LET group_sentence = win &group_var;
	/** 不同alpha变量，对应不同的hit_ratio变量*/
	%IF &alpha_var. = alpha %THEN %DO;
		%LET hitratio = is_d_alpha_pos;
		%LET alpha_var_mdf = alpha_mdf;  /* 在实际中，用的是调整后的alpha */
	%END;
	%ELSE %IF &alpha_var. = abs_ret %THEN %DO;
		%LET hitratio = is_d_ret_pos;
		%LET alpha_var_mdf = abs_ret_mdf;  /* 在实际中，用的是调整后的abs_ret */
	%END;
	%ELSE %IF &alpha_var. = accum_alpha %THEN %LET hitratio = is_accum_alpha_pos;
	%ELSE %IF &alpha_var. = accum_ret %THEN %LET hitratio = is_accum_ret_pos;

	DATA tt_alpha_table;
		SET &alpha_table.;
	RUN;
	PROC SORT DATA = tt_alpha_table;
		BY win;
	RUN;
	/** 全样本分析 */
	PROC UNIVARIATE DATA = tt_alpha_table NOPRINT;
		BY win;
		VAR &alpha_var_mdf. &hitratio.;
		OUTPUT OUT = &eventName._&alpha_var._all N = obs mean = mean_&alpha_var. mean_&hitratio. std = std_&alpha_var. 
			pctlpts = 100 90 75 50 25 10 0  pctlpre = &alpha_var.;
	QUIT;
	 
	/* 按照属性分组分析 */
	%IF %sysevalf(&is_group. = 1) %THEN %DO;
		PROC SORT DATA = tt_alpha_table;
			BY &group_sentence
			;
		RUN;
		PROC UNIVARIATE DATA = tt_alpha_table NOPRINT;
			BY &group_sentence;
			VAR &alpha_var_mdf. &hitratio.;
			OUTPUT OUT =  &eventName._&alpha_var._g N = obs mean = mean_&alpha_var. mean_&hitratio. std = std_&alpha_var. 
				pctlpts = 100 90 75 50 25 10 0  pctlpre = &alpha_var.;
		QUIT;
		PROC SORT DATA =  &eventName._&alpha_var._g;
			BY &group_var. win;
		RUN;
	%END;
	/* 输出到文件 */
	%IF %sysevalf(&is_output. = 1) %THEN %DO;
		%output_to_excel(excel_path = &file_name., input_table = &eventName._&alpha_var._all, sheet_name = all_&sheet_name.);
		%IF %sysevalf(&is_group. = 1) %THEN %DO;
			%output_to_excel(excel_path = &file_name., input_table = &eventName._&alpha_var._g, sheet_name = group_&sheet_name.);
			/** 转置输出。但目前只支持单一的alpha_var */
			PROC SORT DATA = &eventName._&alpha_var._g;
				BY win;
			RUN;
			PROC TRANSPOSE  DATA = &eventName._&alpha_var._g prefix = g_ OUT = &eventName._&alpha_var._g2 ;
				BY win; 
				ID &group_var.;
				VAR mean_&alpha_var.;
			RUN;
			%output_to_excel(excel_path=&file_name., input_table=&eventName._&alpha_var._g2, sheet_name = group_&sheet_name._mean);
			PROC SQL;
				DROP TABLE &eventName._&alpha_var._g2;
			QUIT;
		%END;
	%END;
	PROC SQL;
		DROP TABLE tt_alpha_table ;
	QUIT;
	/** 如果已输出到文件，则不保留数据表 */
	%IF %sysevalf(&is_output. = 1) %THEN %DO;
		PROC SQL;
			DROP TABLE &eventName._&alpha_var._all, &eventName._&alpha_var._g;
		QUIT;
	%END;
%MEND alpha_collect_by_group;



/** 模块9： 输出某个特定窗口下，所有事件accum_alpha/accum_ret的情况 */
/* 输入:
	(0) eventName: 事件名
	(1) event_table: 最初的股票信息表
	(1) alpha_table: cal_win_ret或append_ahead_effect或attribute_to_event的输出结果
	(2) output_win: 特定的某个win下的结果
	(2) is_output： 是否输出到外部文件夹：1- 输出（这时候filename和sheetname才有效)，0-不输出
	(3) file_name: 字符串
	(4) sheet_name: 字符串
	(5) group_var: 感兴趣的分组信息，以空格隔开
**/

/* output: 
	(1) filename(all_&sheetname.和group_&sheetname.)：包含detail结果。
	(2) &eventName._alpha_detail: 包含分组分析的结果。
***/


%MACRO alpha_detail_output(eventName, event_table, alpha_table, output_win, is_output = 0, file_name=., sheet_name=detail, group_var=);
	DATA &eventName._alpha_detail(keep = event_id win accum_alpha accum_ret &group_var.);
		SET &alpha_table.;
		IF win = &output_win.;
	RUN;
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.*
		FROM &event_table. A LEFT JOIN &eventName._alpha_detail B
		ON A.event_id = B.event_id
		ORDER BY accum_alpha desc, accum_ret desc;
	QUIT;
	DATA &eventName._alpha_detail;
		SET tmp;
	RUN;

	/* 输出到文件 */
	%IF %sysevalf(&is_output. = 1) %THEN %DO;
		%output_to_excel(excel_path = &file_name., input_table = &eventName._alpha_detail, sheet_name = &sheet_name.);
		PROC SQL;
			DROP TABLE &eventName._alpha_detail;
		QUIT;
	%END;
	PROC SQL;
		DROP TABLE tmp ;
	QUIT;
%MEND alpha_detail_output;
	
