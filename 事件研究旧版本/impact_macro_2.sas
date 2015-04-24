/* environment setting is required */
/* macro: my_library, input_dir, output_dir */


/* 模块0: 过滤事件 */
/* 上市时间未超过1年
   非ST股票 
   A股*/

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


/* 模块0-0: 删除事件窗口很近的样本，认为是重复样本 */
/* 要求event_id是唯一的 */
%MACRO delete_event(my_library, event_table, busday_table, intval, output_table);
	PROC SQL;
		CREATE TABLE r1 AS
		SELECT event_id, stock_code, date
		FROM &my_library..&event_table
		ORDER BY stock_code, date;
	QUIT;

	%map_date_to_index(busday_table=&busday_table, raw_table= r1, date_col_name=date, raw_table_edit=r1);
	PROC SORT DATA = r1;
		BY stock_code date;
	RUN;

	DATA kk1 dd1;
		SET r1;
		BY stock_code;
		RETAIN start_index .;
		IF first.stock_code THEN DO;
			start_index = date_index;
			OUTPUT kk1;
		END;
		ELSE DO;
			IF date_index - start_index <= &intval. THEN OUTPUT dd1;
			ELSE DO;
				start_index = date_index;
				OUTPUT kk1;
			END;
		END;
	RUN;

	PROC SQL;
		CREATE TABLE &my_library..&output_table AS
		SELECT *
		FROM &my_library..&event_table
		WHERE event_id IN
		(SELECT event_id FROM kk1)
		ORDER BY stock_code, date;
	QUIT;
%MEND delete_event;
	


/* 模块1: 标注事件窗口日 */
/* 若: 股票当日停牌，或者存在涨跌幅限制，或者尚未上市，或者已经退市，则标注为0，否则标注为1 */

/* INPUT:
	(0) my_library 
	(1) event_win_table: datasets(put in the designated library)
	(2) stock_info_table: datasets 
	(3) market_table: mark whether halt
    (3) output_table: datasets for output
/* OUTPUT:
	(1) output_table */
/* Datasets Detail:
	(1) (input) event_win_table: event_id, date, stock_code, date_b (date of event windows), win (allow other columns)
	(2) (input) market_table: stock_code date is_halt is_limit is_in_pool
	(3) (input) stock_info_table:stock_code, stock_name, is_delist, list_date, delist_date, is_st
	(4) (output) output_table: event_win_table with mark added column (mark) */
	
%MACRO mark_event_win(my_library, event_win_table, stock_info_table, market_table,output_table);
	
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.is_halt, B.is_limit, B.is_resumption, B.stock_code AS stock_code_b, C.list_date, C.delist_date, C.stock_code AS stock_code_c
		FROM &my_library..&event_win_table A LEFT JOIN &my_library..&market_table B
		ON A.stock_code = B.stock_code AND A.date_b = B.date
		LEFT JOIN &my_library..&stock_info_table C
		ON A.stock_code = C.stock_code
		ORDER BY event_id, stock_code, date;
	QUIT;

	DATA &my_library..&output_table(drop = is_halt is_limit is_resumption stock_code_b list_date delist_date stock_code_c);
		SET tmp;
		IF  MISSING(stock_code_b) OR MISSING(stock_code_c) OR is_halt = 1 OR is_limit IN (1,2)  /* 停牌当天收益率不计算 */
				OR list_date>=date_b OR (NOT MISSING(delist_date) AND delist_date<=date_b) THEN mark = 0;
		IF (is_resumption = 1 AND is_limit > 0) OR is_limit IN (3,4) THEN mark = -1; /* 复牌且当天超过涨跌幅 */
		ELSE mark = 1;
	RUN;


	PROC SQL;
		DROP TABLE tmp;
	QUIT;
%MEND mark_event_win;




/* 模块2: 生成事件窗口，允许不同事件的窗口之间有重叠*/

/* Input: 
	(1) my_library: designated library
	(2) eventName: the event name or its abbreviation (character)
	(3) eventName_strategy: datasets
	(4) my_hqinfo_with_bm: datasets (bm: benchmark) 
	(5) start_win
	(6) end_win

/* Output: (1) &eventName._hq: datasets after merging event with hqinfo
		(2) &eventName._%eval(-&start_win)_&end_win*/

/* Dataset Detail:
(1) (input)eventName_strategy: event_id, date(business day adjusted), 
stock_code (允许其他属性变量)
(2) (input) my_hqinfo_with_bm: stock_code, date, price, ret, bm_price, bm_ret
(3) (output)&eventName._hq: event_id, date, stock_code, price, ret, bm_price, bm_ret, last_price, bm_last_price,  win, date_b (date of event table) */



%MACRO gen_overlapped_win(my_library, eventName, eventName_strategy, my_hqinfo_with_bm, start_win, end_win);
	%map_date_to_index(busday_table=&my_library..busday, raw_table=&eventName_strategy., date_col_name=date, raw_table_edit=busday_copy)
	%map_date_to_index(busday_table=&my_library..busday, raw_table=&my_hqinfo_with_bm., date_col_name=date, raw_table_edit=my_hqinfo_with_bm_copy)
	PROC SQL;
		CREATE TABLE &my_library..&eventName._hq AS
		SELECT A.*, B.stock_code AS stock_code_b, B.price, B.bm_price, B.ret, B.bm_ret, B.last_price, B.bm_last_price, 
				B.date AS date_b, B.date_index AS date_index_b, B.date_index-A.date_index AS win
		FROM busday_copy A LEFT JOIN my_hqinfo_with_bm_copy B
		ON A.stock_code = B.stock_code AND &start_win. <=B.date_index-A.date_index<= &end_win.
		ORDER BY A.event_id, B.date;
	QUIT;

	DATA &my_library..&eventName._hq(drop = stock_code_b);
		SET &my_library..&eventName._hq(drop = date_index_b);
		IF NOT MISSING(stock_code_b) AND date_b <= &max_busday.;
	RUN;

	PROC SQL;
		DROP TABLE busday_copy, my_hqinfo_with_bm_copy;
	QUIT;

%MEND gen_overlapped_win;

		
		
/* 模块3: 生成事件窗口，不允许不同事件的窗口之间有重叠，会有窗口的截断*/

/* Input: 
	(1) my_library: designated library
	(2) eventName: the event name or its abbreviation (character)
	(3) eventName_strategy: datasets
	(4) my_hqinfo_with_bm: datasets (bm: benchmark) *
	(5) start_win
	(6) end_win */

/* Output: (1) &eventName._hq: datasets after merging event with hqinfo */

/* Dataset Detail:
(1) (input)eventName_strategy: event_id, date(business day adjusted), stock_code (允许其他属性变量)
(2) (input) my_hqinfo_with_bm: stock_code, date, price, ret, bm_price, bm_ret
(3) (output)&eventName._hq: event_id, date, price, ret, bm_price, bm_ret, win  */



%MACRO gen_no_overlapped_win(my_library, eventName, eventName_strategy, my_hqinfo_with_bm, start_win, end_win);
	

	PROC SQL;
		CREATE TABLE tmp AS
		SELECT *
		FROM &my_library..&my_hqinfo_with_bm
		WHERE stock_code IN
		(SELECT DISTINCT stock_code FROM &my_library..&eventName_strategy);

		CREATE TABLE &my_library..&eventName._hq AS
			SELECT A.*, B.event_id
			FROM tmp A LEFT JOIN &my_library..&eventName_strategy B   /* 只限于所有的A股 */
			ON A.stock_code = B.stock_code and A.date = B.date
			ORDER BY stock_code, A.date;
		
		DROP TABLE tmp;
	QUIT;
	
	
	/* mark event windows (next/previous) */
	DATA &eventName._hq_next(drop = cur_required);
		SET	&my_library..&eventName._hq;
		BY stock_code;
		RETAIN next_win .;
		RETAIN cur_required 0;
		RETAIN next_event_id .;
		IF first.stock_code THEN DO;
			cur_required = 0;
			next_win = .;
			next_event_id = .;
		END;
		IF event_id ~=. THEN DO;
			next_win = -1;
			cur_required = 1;
			next_event_id = event_id;
		END;
		IF cur_required = 1 THEN next_win + 1;
	RUN;
	
	PROC SORT DATA = &my_library..&eventName._hq;
		BY stock_code descending date;
	RUN;

	DATA &eventName._hq_pre(drop = cur_required);
		SET	&my_library..&eventName._hq;
		BY stock_code;
		RETAIN pre_win .;
		RETAIN cur_required 0;
		RETAIN pre_event_id .;
		IF first.stock_code THEN DO;
			cur_required = 0;
			pre_win = .;
			pre_event_id = .;
		END;	
		IF event_id ~=. THEN DO;
			pre_win = 1;
			cur_required = 1;
			pre_event_id = event_id;
		END;
		IF cur_required = 1 THEN pre_win = pre_win -1 ;		
	RUN;

	PROC SORT DATA = &eventName._hq_pre;
		BY stock_code date;
	RUN;


	DATA &my_library..&eventName._hq;
		SET &eventName._hq_pre(drop = event_id rename = (pre_event_id = event_id pre_win = win)) 
			&eventName._hq_next(drop = event_id rename = (next_event_id = event_id next_win = win));
		IF event_id ~=.;
	RUN;

	PROC SORT DATA = &my_library..&eventName._hq NODUP;  /* event day( win = 0) has two records */
		BY _ALL_;
	RUN; 

	DATA &my_library..&eventName._hq;
		SET &my_library..&eventName._hq;
		IF &start_win <= win <= &end_win;
	RUN;
	
	PROC SQL;
		DROP TABLE &eventName._hq_pre, &eventName._hq_next;
	QUIT;

%MEND gen_no_overlapped_win;


/* 模块4: 计算超额收益率 */

/* Input: 
	(0) my_library
	(1) eventName: the event name or its abbreviation (character)
	(2) eventName_hq: datasets after merging event with hqinfo (the output generated after module: merge_event_hq)
	(3) start_win: start window (negative for days ahead, postive for days after, 0 for event day) (numeric)
	(4) end_win: end window (numeric)
	(5) buy_win: buy day (at the end of the day, eg: 0 -> buy at the end of event day) 
	(6) stock_info_table: datasets 
	(7) market_table: mark whether haltt

/* Output: (1) &eventName._%eval(-&start_win)_&end_win */
/* Datasets Detail:
	(1) (input)  eventName_hq: event_id, date, price, ret, bm_price, bm_ret, win
		(input) market_table: stock_code date is_halt is_limit is_in_pool
	    (input) stock_info_table:stock_code, stock_name, is_delist, list_date, delist_date, is_st
	(2) (output) &eventName._alpha: event_id, date, win, alpha, 
			accum_alpha(accumulative alpha from the start_win)
			accum_alpha_after(accumulative alpha from the buy_win), 
			event_valid(valid if no return exceeds 10%), 
			realized_alpha(realized alpha before the events),
			is_d_alpha_pos (daily alpha is positive?),
			is_a_alpha_pos (accumulative alpha is positive?)*/


%MACRO cal_access_ret(my_library, eventName, eventName_hq, buy_win, stock_info_table, market_table);
	/* 计算单日alpha */
	PROC SQL;
		CREATE TABLE &my_library..&eventName._alpha AS 
			SELECT *,
			ret - bm_ret AS alpha 
			FROM &my_library..&eventName_hq
			ORDER BY event_id, win;
	QUIT;
	
	/* 标注事件窗口是否有效 */
	%mark_event_win(my_library=&my_library, event_win_table=&eventName._alpha, 
			stock_info_table=&stock_info_table, market_table=&market_table,output_table=&eventName._alpha);
	PROC SORT DATA = &my_library..&eventName._alpha;
		BY event_id win;
	RUN;
	
	/* 计算起始日开始的累积alpha并标注事件是否有效 */
	DATA &my_library..&eventName._alpha(drop = start_price start_bm_price);
		SET &my_library..&eventName._alpha;
		BY event_id;
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


/* 模块4-1: 补充事件日前的alpha和return */
%MACRO append_before_effect(my_library, alpha_file, before_win, output_file);
	PROC SQL;
		CREATE TABLE tmp1 AS
		SELECT event_id,min(win) AS win
		FROM &my_library..&alpha_file
		WHERE win >= &before_win AND not missing(ret_direct)
		GROUP event_id;
	QUIT;

	PROC SQL;
		CREATE TABLE tmp2 AS
		SELECT A.event_id, A.ret_direct AS ret_direct_b, A.is_m_ret_pos AS is_m_ret_pos_b, 
				A.accum_direct AS accum_direct_b , A.is_m_a2_pos AS is_m_a2_pos_b, B.win AS before_win
		FROM &my_library..&alpha_file A JOIN tmp1 B
		ON A.event_id = B.event_id AND A.win = B.win
		ORDER BY A.event_id;
	QUIT;


	PROC SQL;
		CREATE TABLE tmp3 AS
		SELECT A.*, B.ret_direct_b, B.is_m_ret_pos_b, B.accum_direct_b, B.is_m_a2_pos_b, B.before_win
		FROM &my_library..&alpha_file A LEFT JOIN tmp2 B
		ON A.event_id = B.event_id
		ORDER BY event_id, win;
	QUIT;

	DATA &my_library..&output_file;
		SET tmp3;
	RUN;

	PROC SQL;
		DROP TABLE tmp1, tmp2, tmp3;
	QUIT;
%MEND append_before_effect;
	


/* 模块5：赋予事件属性 */
/* Input:
	(0) my_library
	(1) alpha_file: 模块4的输出
	(2) eventName_strategy: event_id, stock_code, date及其他附加属性(除年份，行业，板块外)
	(3) stock_sector_mapping: stock_code, date, o_code, o_name
	(4) stock_info_table: stock_code, bk
	(3) output_file: 输出(alpha_file + 属性列),建议为alpha_file */

/* Output: output_file */

%MACRO attribute_to_event(my_library, alpha_file, eventName_strategy, output_file, stock_sector_mapping, stock_info_table);
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.o_name, C.bk, year(A.date) AS year
		FROM &my_library..&eventName_strategy. A LEFT JOIN &my_library..&stock_sector_mapping. B
		ON A.stock_code = B.stock_code AND A.date = B.date
		LEFT JOIN &my_library..&stock_info_table. C
		ON A.stock_code = C.stock_code
		ORDER BY A.event_id;
	QUIT;

	PROC SQL;
		CREATE TABLE tmp2 AS
		SELECT A.*, B.o_name, B.bk, B.year
		FROM &my_library..&alpha_file. A LEFT JOIN tmp B
		ON A.event_id = B.event_id
		ORDER BY A.event_id, A.date;
	QUIT;

	DATA &my_library..&output_file;
		SET tmp2;
	RUN;

	PROC SQL;
		DROP TABLE tmp, tmp2;
	QUIT;
%MEND attribute_to_event;

	


/* module 3: analyze alpha based on optional variable */
/* Input:
	(0) my_library
	(1) is_group: whether by different group except windows(0 -> no, 1 -> yes, group_var is valid only when this takes value 1)
	(2) group_var: group variable seperated by space
	(3) alpha_var: charcter (alpha, accum_alpha accum_direct accum_direct_f ret_direct) 
	(4) filename: character
	(5) sheetname: character 
	(6) alpha_file (the output of module: cal_access_ret) 

/* output: &output_dir.\&filename */
/* Datasets Detail:
	(1) (input) 
	alpha_file: event_id, date, win, 
			alpha, is_d_pos,
			accum_alpha, is_m_a_pos,
			accum_direct, is_m_a2_pos,
			accum_direct_f, is_m_a3_pos
			<other group variable> */

%MACRO alpha_collect(my_library, alpha_file, alpha_var, is_group, group_var,filename, sheetname);
	
	%LET group_sentence = win; 
	%IF &is_group = 1 %THEN  %LET group_sentence = win &group_var;


	%IF &alpha_var = alpha %THEN %LET hitratio = is_d_pos;
	%ELSE %IF &alpha_var = accum_alpha %THEN %LET hitratio = is_m_a_pos;
	%ELSE %IF &alpha_var = accum_direct %THEN %LET hitratio = is_m_a2_pos;
	%ELSE %IF &alpha_var = accum_direct_f %THEN %LET hitratio = is_m_a3_pos;
	%ELSE %IF &alpha_var = ret_direct %THEN %LET hitratio = is_m_ret_pos;

	PROC SORT DATA = &my_library..&alpha_file.;
		BY &group_sentence
		;
	RUN;

	DATA stat_file;
		SET &my_library..&alpha_file.;
		IF mark = 1;
	RUN;
	
	PROC UNIVARIATE DATA = stat_file NOPRINT;
		BY &group_sentence;
		VAR &alpha_var. &hitratio.;
		OUTPUT OUT = &alpha_var. N = obs mean = mean_&alpha_var. mean_&hitratio. std = std_&alpha_var. 
			pctlpts = 100 90 75 50 25 10 0  pctlpre = &alpha_var.;
	QUIT;

	DATA &alpha_var.;
		SET &alpha_var.;
		IF NOT MISSING(&group_var);  /* 去除未分组的样本 */
	RUN;
	
	/* 按照属性排序 */
	PROC SORT DATA = &alpha_var.;
		BY &group_var. win;
	RUN;
		
	
	LIBNAME myxls "&output_dir.\&filename";  /* external file */
		DATA myxls.&sheetname.;
			SET &alpha_var;
		RUN;
	LIBNAME myxls CLEAR;

	/* 分组分析的，补上全样本的结果 */
	%IF &is_group = 1 %THEN %DO;
		PROC UNIVARIATE DATA = stat_file NOPRINT;
			BY win;
			VAR &alpha_var. &hitratio.;
			OUTPUT OUT = tmp2 N = obs mean = mean_&alpha_var. mean_&hitratio. std = std_&alpha_var. 
				pctlpts = 100 90 75 50 25 10 0  pctlpre = &alpha_var.;
		QUIT;

		LIBNAME myxls "&output_dir.\&filename";  /* external file */
			DATA myxls.all_&sheetname.;
				SET tmp2;
			RUN;
		LIBNAME myxls CLEAR;
	%END;


	PROC SQL;
		DROP TABLE stat_file, &alpha_var. ;
	QUIT;

%MEND alpha_collect;


