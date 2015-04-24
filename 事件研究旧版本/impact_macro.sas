/* module 1: merge with hqinfo */

/* Input: 
	(1) my_library: designated library
	(2) eventName: the event name or its abbreviation (character)
	(3) eventName_strategy: datasets
	(4) my_hqinfo_with_bm: datasets (bm: benchmark) */

/* Output: (1) &eventName._hq: datasets after merging event with hqinfo  */

/* Dataset Detail:
(1) (input)eventName_strategy: event_id, date(business day adjusted), stock_code
(2) (input) my_hqinfo_with_bm: stock_code, date, price, ret, bm_price, bm_ret
(3) (output)my_hqinfo_with_bm: event_id, date, price, ret, bm_price, bm_ret, win  */

* options mprint;

%MACRO merge_event_hq(my_library, eventName, eventName_strategy, my_hqinfo_with_bm);

	PROC SQL;
		CREATE TABLE tmp AS
		SELECT *
		FROM &my_library..&my_hqinfo_with_bm
		WHERE stock_code IN
		(SELECT DISTINCT stock_code FROM &my_library..&eventName_strategy);

		CREATE TABLE &my_library..&eventName._hq AS
			SELECT A.*, B.event_id
			FROM tmp A LEFT JOIN &my_library..&eventName_strategy B
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

	PROC SORT DATA = &my_library..&eventName._hq;
		BY event_id date;
	RUN;

	PROC SQL;
		DROP TABLE &eventName._hq_pre, &eventName._hq_next;
	QUIT;

%MEND merge_event_hq;


/* module 2: calculate access return */

/* Input: 
	(0) my_library
	(1) eventName: the event name or its abbreviation (character)
	(2) eventName_hq: datasets after merging event with hqinfo (the output generated after module: merge_event_hq)
	(3) start_win: start window (negative for days ahead, postive for days after, 0 for event day) (numeric)
	(4) end_win: end window (numeric)
	(5) buy_win: buy day (at the end of the day, eg: 0 -> buy at the end of event day) */

/* Output: (1) &eventName._%eval(-&start_win)_&end_win */
/* Datasets Detail:
	(1) (input)  eventName_hq: event_id, date, price, ret, bm_price, bm_ret, win
	(2) (output) &eventName._%eval(-&start_win)_&end_win: event_id, date, win, alpha, 
			accum_alpha(accumulative alpha from the start_win)
			accum_alpha_after(accumulative alpha from the buy_win), 
			event_valid(valid if no return exceeds 10%), 
			realized_alpha(realized alpha before the events),
			is_d_alpha_pos (daily alpha is positive?),
			is_a_alpha_pos (accumulative alpha is positive?)*/


%MACRO cal_access_ret(my_library, eventName, eventName_hq, start_win, end_win, buy_win);
	PROC SQL;
		CREATE TABLE &my_library..&eventName._%eval(-&start_win)_&end_win AS 
			SELECT *,
			ret - bm_ret AS alpha 
			FROM &my_library..&eventName_hq
			WHERE &start_win <= win <= &end_win 
			ORDER BY event_id, win;
	QUIT;

	DATA &my_library..&eventName._%eval(-&start_win)_&end_win;
		SET &my_library..&eventName._%eval(-&start_win)_&end_win;
		BY 	event_id;
		RETAIN accum_alpha 0;
		RETAIN accum_alpha_after 0;
		RETAIN valid 1;

		IF first.event_id THEN accum_alpha = 0;
		accum_alpha = accum_alpha + alpha;

		IF first.event_id THEN valid = 1;
		IF abs(ret) > 11 OR ret = .  THEN valid = 0;   
		
		IF first.event_id THEN accum_alpha_after = 0;
		IF win > &buy_win  THEN accum_alpha_after = accum_alpha_after + alpha;	
	RUN;

	DATA tmp_valid;
		SET &my_library..&eventName._%eval(-&start_win)_&end_win(keep = event_id valid);
		BY event_id;
		IF last.event_id;   /* if there is any outlier within the event window(pre or next), then mark it */
	RUN;
	 
	PROC SQL;
		CREATE TABLE new_table AS
		SELECT A.*, B.valid AS event_valid
		FROM &my_library..&eventName._%eval(-&start_win)_&end_win A LEFT JOIN tmp_valid B
		ON A.event_id = B.event_id
		ORDER BY A.event_id, A.date;
	QUIT;


	PROC SQL;
		CREATE TABLE tmp AS
		SELECT event_id, accum_alpha AS realized_alpha
		FROM new_table
		WHERE win = -1
		ORDER BY event_id;
	QUIT;

	PROC SQL;
		CREATE TABLE tmp2 AS
		SELECT A.*, B.realized_alpha
		FROM new_table A LEFT JOIN tmp B
		ON A.event_id = B.event_id
		ORDER BY event_id, date;
	QUIT;
	
	DATA &my_library..&eventName._%eval(-&start_win)_&end_win(drop = valid);
		SET tmp2;
		IF alpha > 0 THEN is_d_alpha_pos = 1;
		ELSE is_d_alpha_pos = 0;
		IF accum_alpha_after > 0 THEN is_a_alpha_pos = 1;
		ELSE is_a_alpha_pos = 0;
	RUN;

	PROC SQL;
		DROP TABLE tmp, tmp2, new_table, tmp_valid;
	QUIT;

%MEND cal_access_ret;


/* module 3: analyze alpha based on optional variable */
/* Input:
	(0) my_library
	(1) is_group: whether by different group except windows(0 -> no, 1 -> yes, group_var is valid only when this takes value 1)
	(2) group_var: group variable seperated by space
	(3) alpha_var: charcter (alpha, accum_alpha or accum_alpha_after) 
	(4) filename: character
	(5) sheetname: character 
	(6) alpha_file (the output of module: cal_access_ret) 

/* output: &output_dir.\&filename */
/* Datasets Detail:
	(1) (input) 
	alpha_file: event_id, date, win, alpha, 
			accum_alpha(accumulative alpha from the start_win)
			accum_alpha_after(accumulative alpha from the buy_win), 
			event_valid(valid if no return exceeds 10%), 
			realized_alpha(realized alpha before the events),
			is_d_alpha_pos (daily alpha is positive?),
			is_a_alpha_pos (accumulative alpha is positive?)
			<other group variable> */

%MACRO alpha_collect(my_library, alpha_file, alpha_var, is_group, group_var,filename, sheetname);
	
	%LET group_sentence = win; 
	%IF &is_group = 1 %THEN  %LET group_sentence = win &group_var;

	%LET hitratio = is_d_alpha_pos;
	%IF &alpha_var = accum_alpha OR &alpha_var = accum_alpha_after %THEN %LET hitratio = is_a_alpha_pos;

	PROC SORT DATA = &my_library..&alpha_file;
		BY &group_sentence
		;
	RUN;
	
	PROC UNIVARIATE DATA = &my_library..&alpha_file.(where = (event_valid = 1 AND NOT MISSING(&alpha_var.))) NOPRINT;
		BY &group_sentence;
		VAR &alpha_var. &hitratio.;
		OUTPUT OUT = &alpha_var. N = obs mean = mean_&alpha_var. mean_&hitratio. std = std_&alpha_var. 
			pctlpts = 100 90 75 50 25 10 0  pctlpre = &alpha_var.;
	QUIT;
	
	LIBNAME myxls "&output_dir.\&filename";  /* external file */
		DATA myxls.&sheetname;
			SET &alpha_var;
		RUN;
	LIBNAME myxls CLEAR;

%MEND alpha_collect;


