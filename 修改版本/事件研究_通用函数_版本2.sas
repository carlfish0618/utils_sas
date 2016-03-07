/*** �¼��о���ͨ�ú����汾2 **/
/** ˵����
	��ʡ�ԵĲ��������
	(3)(7)(8)
**/

/*** �����б�
	(1A) event_gen_windows: ���ݴ��ڳ��ȣ������¼����� 
	(1B) event_gen_fixed_windows:���ݴ�����ʼ�������ڣ������¼�����
	(1C) event_gen_trade_date:�����¼�������������ڣ�ȷ���ɽ�������
	(2) event_get_marketdata:��ע�¼����ڼ�������Ļ�׼�գ�����ȡ������Ϣ
	(3*) event_mark_win:��ע�¼������Ƿ񳬳����С����У��Լ��Ƿ��ǵ�����10%+(һ���Ǹ��ƺ���Ϊ) 
	(4) event_cal_accum_alpha:�����ۼ�������(�õ����۳˵ķ���)
	(5) event_smooth_rtn:�ڿ������������������£����ۼ������ʽ���ƽ��
	(6) event_cal_stat: �����ṩ�ķ������������ÿ�����ڵı���ȡֵ��� 
	(7*) event_addindex_stat:Ϊevent_cal_stat�Ľ������index���������ڻ�ͼ
	(8*) event_stack_stat: ��event_cal_stat����event_addindex_stat�Ľ������stack�����ڲ鿴
	(9) event_mdf_stat: ��event_cal_stat����event_addindex_stat�Ľ�����м򻯣���ת��win.������������档����ض����������rtn��prob 
********/




/* ģ��1A: ���ݴ��ڳ��ȣ������¼�����*/
/* ����
	(1) event_table: event_id / event_date(������) / stock_code /����
	(2) start_win: ������ʼ
	(3) end_win�����ڽ���
	(4) busday_table: date

/** ���:
       (1) output_table: ��event_table������������event_date/win
/** �ⲿ����:
	(1) ~/����_ͨ�ú���/map_date_to_index
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

/* ģ��1B: ���ݴ�����ʼ�������ڣ������¼�����*/
/* ����
	(1) event_table: event_id / event_date(������) / stock_code / &start_col. / &end_col.
	(2) start_col: ������ʼ
	(3) end_col�����ڽ���
	(4) busday_table: date

/** ���:
       (1) output_table: ��event_table������������event_date/win
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


/* ģ��1C: �����¼�������������ڣ�ȷ���ɽ�������*/
/** !!!! ע�������������Ϊ����Ϣ�ɵã��ҿɽ��׵����ڡ������Ϣ����X�����̺�ſɵõ�����buy_colӦ���趨ΪX+1 */
/* ����
	(1) event_table: event_id / event_date(������) / stock_code /&buy_col. / &sell_col.
	(2) buy_col: ��������
	(3) sell_col����������
	(4) busday_table: date
	(5) hq_table: �����������ݣ�����end_date/stock_code/close/factor/vol/high/low/open��

/** ���:
       (1) output_table: ��event_table������������trade_buy_date��trade_sell_date
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
	
	/** �Ӻ�����ʱ�� */
	PROC SQL;
		CREATE TABLE tt_event_table AS
		SELECT A.event_id, min(end_date) AS trade_buy_date FORMAT yymmdd10.
		FROM &event_table. A LEFT JOIN tt_hq_table B
		ON B.stock_code = A.stock_code 
		AND B.end_date >= A.&buy_col. 
		AND B.end_date < A.&sell_col.
		AND B.vol >0 AND (B.low < B.high ) AND round(close/pre_close-1,0.01)<=&threshold_rtn.  /* �Ƿ��������޶� */
		GROUP BY event_id;
	QUIT;
	
	/** �Ӻ�����ʱ�� */
	PROC SQL;
		CREATE TABLE tt_event_table2 AS
		SELECT A.event_id, min(end_date) AS trade_sell_date FORMAT yymmdd10.
		FROM &event_table. A LEFT JOIN tt_hq_table B
		ON A.stock_code = B.stock_code
		AND B.end_date >= A.&sell_col.
		AND B.vol >0 AND (B.low < B.high ) AND round(close/pre_close-1,0.01) >= -&threshold_rtn.  
		 /** ֻ���ǵ�ͣ��������������ͣ�Ʋ����� */ 
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




/** ģ��2����ע�¼����ڼ�������Ļ�׼�գ�����ȡ������Ϣ�������� */
/* ����
	(1) win_table: event_gen_windows�����
	(2) buy_win: ��������(�����������룬0-��ʾ�¼��գ�Ϊ�����趨)
	(3) hq_table: �����������ݣ�����end_date/stock_code/close/factor/vol/high/low/open��
	(4) bm_table: ��׼�������ݣ�֧�ֻ�׼�������ں͹�Ʊ���仯): end_date/stock_code/close/factor
	(5) busday_table: date

/** ���:
       (1) output_table: ��win_table������������
			(a) date_1_mdf�������ۼ��������ʼ��(�����ղ�������)
			(b) date_2_mdf�������ۼ�����Ľ����գ������պ����죩
			(c) rtn: ����������
			(d) accum_rtn/bm_accum_rtn:�ۼ�������/��׼�ۼ�������
			(e) alpha: ����alpha
			(f) rel_rtn: �ۼƵ����������(���ɺͻ�׼�ۼ�������֮��)
			(g) filter: ��ע��������(0-�ޣ�1-��Ϊһ���ǵ�ͣ�޷����� 2-ͣ��)
/** �ⲿ����:
	(1) ~/����_ͨ�ú���/map_date_to_index
***/


%MACRO event_get_marketdata(win_table, buy_win, hq_table, bm_table, busday_table, output_table);
	/** Step1- ȡ��׼��(������������) */
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

	/* Step2- ��ע��������Ļ�׼���� */
	/* date_2: ���н����� */
	/* date_1: ���п�ʼ�� */
	/* num_1��num_2���ڼ���(date_1,date_2]֮���������ʱ���Ƿ���Ҫ��ʱ����д�����Ҫ��ԣ���׼�ռ�֮ǰ���ڵĴ��� */
	DATA tt_win_table;
		SET tt_win_table;
		/* ������֮ǰ */
		/* ��������T=0Ϊ�����գ���T=-1���ĵ���[-1]����������ʣ�����T=0 */
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

	/** Step3: ȡdate_1(date_2)����num_1(num_2)ƽ�ƹ�������� */
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

	/* Step4: ȡ��Ӧ�ĸ����������� */
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
			B.vol, B.close/B.pre_close-1 AS rtn,  /* �����յ���������*/
			(C.close*C.factor)/(D.close*D.factor)-1 AS accum_rtn,  /* �ۼƾ������� */
			D.vol AS date_1_vol,   /* �����ж�������ʼ��(date_1)�ܷ����� */
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

	/** Step5: ȡ��Ӧ�Ļ�׼���� */
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
			(rtn+1)-B.close/B.pre_close AS alpha, /* ����alpha */
			(C.close*C.factor)/(D.close*D.factor)-1 AS bm_accum_rtn,  /* ��׼�ۼƾ������� */
			(accum_rtn+1)/((C.close*C.factor)/(D.close*D.factor))-1 AS rel_rtn /* ��Ի�׼����*/
		FROM tt_win_table A LEFT JOIN tt_bm_table B
		ON A.win_date = B.end_date AND A.stock_code = B.stock_code
		LEFT JOIN tt_bm_table C
		ON A.date_2_mdf = C.end_date AND A.stock_code = C.stock_code
		LEFT JOIN tt_bm_table D
		ON A.date_1_mdf = D.end_date AND A.stock_code = D.stock_code
		ORDER BY event_id, win_date;
	QUIT;
	
	/** Step6: ��עĳЩ������Ҫ��ļ�¼ */
	DATA tt_win_table;
		SET tmp;
		filter = 0;
		IF vol = 0 OR missing(vol) THEN DO;  /* ͣ���� */
			filter = 10;
		END;
		/** date_1�޷����� */
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
	/** Step7: �����ۼ�alpha */
	DATA &output_table.;
		SET tt_win_table;
	RUN;
	PROC SQL;
		DROP TABLE tmp,tt_hq_table, tt_bm_table, tt_busday, tt_win_table;
	QUIT;
%MEND event_get_marketdata;

/** ģ��3����ע�����¼������Ƿ񳬳����С����У��Լ��Ƿ��ǵ�����10%+(һ���Ǹ��ƺ���Ϊ) */
/* ����
	(1) rtn_table: event_get_marketdata�����
	(2) stock_info_table: ���ɻ�����Ϣ(stock_code/list_date/delist_date/is_delist/is_st)

/** ���:
       (1) output_table: ��rtn_table�����϶�filter�ֶν��и��¡��������
			(a) filter: ��ע��������
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
	/** ˳����������ȼ� */
	PROC SQL;
		UPDATE tmp 
		SET filter = CASE
			WHEN win_date <= list_date OR win_date >= delist_date THEN 30  /** ��������-���м�¼ */
			WHEN missing(rtn) THEN 50  /* ���ݱ���ȱʧ */
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

/** ģ��3B����ʶ�������ֵ���ĳ��״̬(������ͣ�ƣ�������ͣ)�����*/
/** ��Ҫ����Ԥ�ȷ�����Ҳ��֪������ƽ����Ӱ�� */
/* ����
	(1) rtn_table: event_mark_win()�����(��Ҫ�߱�filter�ֶ�)
	(2) buy_win: ��������(�����������룬0-��ʾ�¼��գ�Ϊ�����趨)
	(3) filter_set: ��filter����ü���ʱ��Ҫ�Խ������ͳ��

/** ���:
       (1) output_table: ��rtn_table�����������ֶ�&invalid_start_win.��&ninvalid.��

***/
%MACRO event_mark_successive_win(rtn_table, buy_win, output_table, 
			invalid_start_win=invalid_start_win, ninvalid = ninvalid, filter_set=(.));
	/** ������֮�� */
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
			invalid = 0;  /* ���¸�λ */
			ss = .;
		END;
		&ninvalid. = invalid;
		&invalid_start_win. = ss;
	RUN;

	/** ����֮ǰ */
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
		/* �����Ϊwin<=&buy_win.������&buy_win-1��Ϊ�˸��õ�ͳ��������*/
		/* ���Ǹ�����ƽ������ʹ�õĲ�ͬ����Ϊ����buy_win��һ�첻Ӱ���κ�һ���ۼ����� */
		IF win <= &buy_win. AND filter in &filter_set. THEN DO;	
			invalid + 1;  
			IF missing(ss) THEN ss = win;
		END;
		ELSE DO;
			invalid = 0;
			ss = .;
		END;
		/* �����ֶ� */
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




/** ģ��4�������ۼ�������(�õ����۳˵ķ���)*/
/* ����
	(1) rtn_table: event_get_marketdata�����
	(2) buy_win: ��������(�����������룬0-��ʾ�¼��գ�Ϊ�����趨)

/** ���:
       (1) output_table: ��rtn_table������������
			(a) accum_alpha: �ۼ�alpha
***/
%MACRO event_cal_accum_alpha(rtn_table,buy_win, output_table);
	/** �¼���֮�� */
	PROC SORT DATA = &rtn_table. OUT = tmp;
		BY event_id win_date;
	RUN;
	DATA tmp;
		SET tmp;
		BY event_id;
		RETAIN accum_alpha_f .;
		IF first.event_id THEN accum_alpha_f = .;
		IF win < &buy_win. THEN accum_alpha_f = .; /* �˾����ʡ�� */
		IF win = &buy_win. THEN accum_alpha_f = 0;
		IF win > &buy_win. THEN accum_alpha_f = (1+accum_alpha_f)*(1+coalesce(alpha,0))-1;
	RUN;
	/** �¼���֮�� */
	PROC SORT DATA = tmp OUT = tmp;
		BY event_id descending win_date;
	RUN;
	DATA tmp;
		SET tmp;
		BY event_id;
		RETAIN accum_alpha_b .;
		IF first.event_id THEN accum_alpha_b = .;
		IF win > &buy_win. THEN accum_alpha_b = .; /* �˾����ʡ�� */
		IF win = &buy_win. THEN accum_alpha_b = 0;
		IF win < &buy_win. THEN accum_alpha_b = (1+accum_alpha_b)*(1+coalesce(alpha,0))-1;
		accum_alpha = sum(accum_alpha_f, accum_alpha_b);
	RUN;
	PROC SQL;
		UPDATE tmp
		SET accum_alpha = coalesce(alpha,0) WHERE win = &buy_win.;  /* ���ｫaccum_alpha����Ϊ0,����alphaȱʧ */
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


/** ģ��5���ڿ������������������£����ۼ������ʽ���ƽ��*/
/* ����
	(1) rtn_table: event_cal_accum_alpha�����(��Ҫ����accum_alpha�ֶ�)
	(2) buy_win: ��������(�����������룬0-��ʾ�¼��գ�Ϊ�����趨)
	(3) suffix: ���ۼ�������(����rel_rtn/accum_rtn/accum_alpha)֮�������ĺ�׺(_suffix)
	(4) filter_set: ��filter����ü���ʱ��Ҫ�Խ������ƽ��(���������滻Ϊǰֵ)


/** ���:
       (1) output_table: ��rtn_table������������
			(a) rel_rtn_&suffix.
			(b) accum_rtn_&suffix.
			(c) accum_alpha_&suffix.
***/

%MACRO event_smooth_rtn(rtn_table, buy_win, suffix, output_table, filter_set=(.));
	/** ������֮�� */
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
		IF win = &buy_win. + 1 AND filter in &filter_set. THEN DO; /* ����ĵ�һ�죬��Ҫ�������ϵ�����Ϊȱʧ */
			rel_rtn_&suffix. = .;
			accum_rtn_&suffix. = .;
			accum_alpha_&suffix. = .;
		END;

		IF (win > &buy_win.+ 1 AND filter not in &filter_set.) OR (win <= &buy_win.) THEN DO; /* �������������� */
			rel_rtn_&suffix. = rel_rtn;
			accum_rtn_&suffix. = accum_rtn;
			accum_alpha_&suffix. = accum_alpha;
		END;
	RUN;
	
	/** ����֮ǰ */
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
		IF win = &buy_win.-1 AND filter in &filter_set. THEN DO;  /* ������ǰ�������������һ�� */
			rel_rtn_&suffix. = .;
			accum_rtn_&suffix. = .;
			accum_alpha_&suffix. = .;
		END;
			
		IF (win < &buy_win.-1 AND filter in &filter_set.) THEN DO; /* �滻������������ */
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
			WHERE win = &buy_win. AND (filter in &filter_set. AND filter NOT IN (20,21,22)); /* �����ղ�Ӧ��Ӱ���¼����� */
	QUIT;

	PROC SORT DATA = tmp OUT = &output_table.;
		BY event_id win_date;
	RUN;
	PROC SQL;
		DROP TABLE tmp;
	QUIT;
%MEND event_smooth_rtn;

/** ģ��5B���ڿ������������������£����ۼ������ʽ��ж���ƽ��(��Ҫ���ǡ�������ͣ�Ƴ���N������ǵ�ͣ����N��)*/
/* ����
	(1) rtn_table: event_cal_accum_alpha�����(��Ҫ����accum_alpha�ֶ�)
	(2) buy_win: ��������(�����������룬0-��ʾ�¼��գ�Ϊ�����趨)
	(3) suffix: ���ۼ�������(����rel_rtn/accum_rtn/accum_alpha)֮�������ĺ�׺(_suffix)
	(4) filter_set: ��filter����ü���ʱ��Ҫ�Խ������ƽ��(���������滻Ϊǰֵ)


/** ���:
       (1) output_table: ��rtn_table�������滻�����ֶε�ֵ��
			(a) rel_rtn_&suffix.
			(b) accum_rtn_&suffix.
			(c) accum_alpha_&suffix.
***/

%MACRO event_second_smooth_rtn(rtn_table, buy_win, suffix, output_table, threshold = 5, filter_set=(.));
	/** ������֮�� */
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
		ELSE invalid = 0;  /* ���¸�λ */
		IF invalid >= &threshold. THEN DO;
			mark = 1;   /* �˺󶼱�עΪ��Ч���� */
		END;
		IF mark = 1 THEN DO;
			rel_rtn_&suffix. = .;
			accum_rtn_&suffix. = .;
			accum_alpha_&suffix. = .;
		END;
	RUN;

	/** ����֮ǰ */
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



/** ģ��6�� �����ṩ�ķ������������ÿ�����ڵı���ȡֵ��� */
/* ����:
	(1) rtn_table: event_smooth_rtn�����
	(2) rtn_var: �ַ�������ѡ��alpha, rtn, accum_rtn(�ɴ���׺), rel_rtn(�ɴ���׺), accum_alpha(�ɴ���׺)��
	(3) filter_set���޳���filter����ü��ϵļ�¼
	(4) group_var: ����������Կո���
	
/** �����
	(1) output_table:
			(a) win
			(b) obs/&rtn_var./prob:����������ֵ��ʤ��
			(c) std_&rtn_var.: ������
			(d) pct100-0����7����λ��
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
	/** ������� */
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


/** ģ��7�� Ϊevent_cal_stat�Ľ������index���������ڻ�ͼ */
/* ����:
	(1) stat_table: event_cal_stat�����
	(2) rtn_var: �ַ���!!ֻ֧���ۼƱ���������accum_rtn(�ɴ���׺), rel_rtn(�ɴ���׺), accum_alpha(�ɴ���׺)��
	(3) buy_win: ��������(�����������룬0-��ʾ�¼��գ�Ϊ�����趨)��������Ϊ�����index��Ҫ�趨Ϊ100��ע�Ᵽ�����¼��о��е�buy_winһ��
	(4) group_var: ����������Կո���
	
/** �����
	(1) output_table:��stat_table������
			(a) &ret_var._index
**/


%MACRO event_addindex_stat(stat_table, rtn_var, buy_win, output_table, group_var=);
	%LET group_str = &group_var. descending win;  /* win���� */
	PROC SORT DATA = &stat_table. OUT = tt_stat_table;
		BY &group_str.
		;
	RUN;
	DATA tt_stat_table;
		SET tt_stat_table;
		RETAIN rtn_0 .;
		&rtn_var._tmp = lag(&rtn_var.);  /* ֻ����win<&buy_win.-1ʱ���õ������Բ���Ҫ��first���������趨��*/
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
	%LET group_str = &group_var. win;  /* win���� */
	PROC SORT DATA = &output_table.;
		BY &group_str.
		;
	RUN;
	PROC SQL;
		DROP TABLE tt_stat_table;
	QUIT;
%MEND event_addindex_stat;

/** ģ��8�� ��event_cal_stat����event_addindex_stat�Ľ������stack�����ڲ鿴*/
/* ����:
	(1) stat_table: event_cal_stat����event_addindex_stat�����
	(2) group_var: ����������Կո���.(ע����stat_table���ɹ��̵Ĳ�������һ��)
	
/** �����
	(1) output_table:
			(a) win
			(b) variable: ͳ�Ʊ��������ƣ���nobs/prob��
			(c) data: ��Ӧ������ȡֵ
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

/** ģ��9�� ��event_cal_stat����event_addindex_stat�Ľ�����м򻯣���ת��win.������������档
			����ض����������rtn��prob **/
		
/* ����:
	(1) stat_table: event_cal_stat����event_addindex_stat�����
	(2) rtn_var: �ַ�������ѡ��alpha, rtn, accum_rtn(�ɴ���׺), rel_rtn(�ɴ���׺), accum_alpha(�ɴ���׺)��
	(3) group_var: ����������Կո���.(ע����stat_table���ɹ��̵Ĳ�������һ��)
	(4) win_set: ֻ����ü����е�ͳ�Ʊ���
	
/** �����
	(1) output_table:
			(a) _NAME_: ͳ�Ʊ���������
			(b) �������ڶ�Ӧһ������
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

	

	
		





