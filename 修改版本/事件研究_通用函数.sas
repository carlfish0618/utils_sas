/** ======================˵��=================================================**/

/*** �������ܣ��ṩ���¼��о���صĺ�������Ҫ����:
(1) ���˲������������¼�
(2) �����¼����ڣ��������ص������ص��汾��
(3) �����¼����ڣ�����ע�¼����ڵ���Ч��
(3-2) ����ÿ�������У���ͬ��Ч�Ե�ͳ����Ϣ
(4) �����¼����ڵ�������
(5) �����¼����ڵ�������
(6) ������Ϣ���¼���ǰ[N,0]��alpha��return�����ڷ���ʹ��
(7) �����¼�����
(8) �����ṩ�ķ������������ÿ�����ڵ�alpha/ret�ȵķ������
(9) ���ĳ���ض������£������¼�accum_alpha/accum_ret�����


**/ 

/**** �����б�:
(1) filter_event: ���˲������������¼�
(2) mark_event_win: �����¼����ڣ�����ע�¼����ڵ���Ч��
(2-2) mark_stat: ���ݹ��˽��������ÿ�������У���ͬ��Ч�Ե�ͳ����Ϣ
(3) gen_overlapped_win: �����¼�����(�����ص���
(4) gen_no_overlapped_win�������¼�����(�������ص���
(5) cal_win_ret: �����¼����ڵ�������
(6) append_ahead_effect: ������Ϣ���¼���ǰ[N,0]��alpha��return�����ڷ���ʹ��
(7) attribute_to_event�������¼�����
(8) alpha_collect_by_group: �����ṩ�ķ������������ÿ�����ڵ�alpha/ret�ȵķ������
(9) alpha_detail_output: ���ĳ���ض������£������¼�accum_alpha/accum_ret�����
****/ 



/** =======================================================================**/




/* ģ��1: �����¼����޳������¼���
(1) ����ʱ��δ����N����Ȼ�գ�Ĭ��Ϊ365�죬��һ��)
(2) ��ST��Ʊ 
(3) ��A��
(4) ������
(5) ͣ��ʱ�䳬��N��������(Ĭ��Ϊ20��������)  ---> ��������ͳ��һ�£�ͣ�Ƶ��¼�ռ�ȣ������Ѿ�ͣ���˶����졣
***/

/* INPUT:
	(1) event_table: event_id/event_date/stock_code/����
	(2) stock_info_table(���������е�A��): stock_code/stock_name(��ѡ)/list_date/delist_date/is_st
	(3) market_table: stock_code/end_date/is_halt/halt_days(ֻ����is_halt=1��ʱ��ż���)/is_in_pool
	(4) ndays: ����֮���N����Ȼ�ա�Ҫ��ndays>=0 (�����е��������޳��������¹ɵ�Ӱ��)
	(5) halt_days: ͣ��ʱ�䳬��N�������ա�Ҫ��halt_days>=0 
	(6) is_filter_mark: 1-����������Ҫ��ı�־λfilter(>0��ʾ��Ҫ����)��0-ֱ�������޳���

/* OUTPUT:
	(3) output_table: event_id/date/stock_code/����

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
			IF missing(is_halt) THEN filter = 4;   /** ����Ҫ���Ƿ�ͣ�Ƶ���Ϣ */
			IF is_halt = 1 AND halt_days >= &halt_days. THEN filter = 5;
		%END; 
		%ELSE %DO;
			IF missing(list_date) OR (NOT missing(list_date) AND event_date - list_date <= &ndays.) THEN delete;
			IF is_st = 1 THEN delete;
			IF not missing(delist_date) AND event_date >= delist_date THEN delete;
			IF missing(is_halt) THEN delete;   /** ����Ҫ���Ƿ�ͣ�Ƶ���Ϣ */
			IF is_halt = 1 AND halt_days >= &halt_days. THEN delete;
		%END; 
	RUN;

	PROC SQL;
		DROP TABLE tmp;
	QUIT;
%MEND filter_event;

/*** ģ��2�������¼����ڣ�����ע�¼����ڵ���Ч�� ***/
/* (0) ֱ���޳�����������δ���У����ߴ������Ѿ����еļ�¼
(1*) ͣ���ձ�עΪ1���ڼ��㵱������ʱ�����Կ������롣Ҳ���Կ��ǲ����롣ͣ���ղ�Ӱ��������ڵļ��㣨ͳ��ռ��)
(2**) �����գ��ǵ�ͣ����10%���Ƶģ���עΪ2��Ϊ���⼫��ֵӰ�죬�������治���Լ��㡣�ҿ���Ӱ��������ڵļ���(ͳ��ռ��)
(3*) �����գ�һ���ǵ�ͣ�ģ���עΪ3��Ϊ�޳�������Ӱ�죬��������ɿ������룬Ҳ�ɿ��ǲ����롣����Ӱ��������ڵ�ռ�ȣ�ͳ��ռ�ȣ�
(4**) �Ǹ����գ��ǵ�ͣ����10%���Ƶģ���עΪ4��Ϊ���⼫��ֵӰ�죬�������治���Լ��㡣�ҿ���Ӱ��������ڵļ���(ͳ��ռ��)
(5) �Ǹ����գ�һ���ǵ�ͣ�ģ���עΪ5���ճ����㵱�����档�Ҳ�Ӱ��������ڵļ��㣨ͳ��ռ�ȣ�
*****/ 

/*** ע�⣺�ڼ����ۼ������ʵ�ʱ�򣬷��������ͳ�ƣ�һ���ǽ�����mark=2/4���¼����棬��ͳһ������㡣����һ����ȫ�����㡣�����������ǰ����쳣��� **/

/* INPUT:
	(1) event_win_table: event_id/event_date/stock_code/win_date/win/����
	(2) stock_info_table(���������е�A��): stock_code/stock_name(��ѡ)/list_date/delist_date/is_st
	(3) market_table: stock_code/end_date/is_limit(����/is_halt/is_resumption
/* OUTPUT:
	(1) output_table: event_id/event_date/stock_code/win_date/win/mark/����
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

/*** ģ��2-2�����ݹ��˽��������ÿ�������У���ͬ��Ч�Ե�ͳ����Ϣ ***/

/* INPUT:
	(1) event_win_table: mark_event_win��������(mark = 0-5)
/* OUTPUT:
	(1) output_table: win/n_events/mark_i���м����¼�������mark)
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
	
	


/* ģ��3: �����¼����ڣ�����ͬ�¼��Ĵ���֮�����ص�*/
/* ����
	(0) eventName: �ַ�����
	(1) event_table: event_id / event_date(������) / stock_code /����
	(2) stock_hqinfo_table: end_date/stock_code/price/last_price
	(3) bm_hqinfo_table: end_date/stock_code/price/last_price (����ÿ��stock����ָ����ͬ�Ļ�׼�����price����ָ����׼�ļ۸�)
	(4) start_win: ������ʼ
	(5) end_win�����ڽ���
	(6) busday_table: date (�������ʹ���ⲿ��

/** ���:
       (1) &eventName._hq: event_id/event_date/stock_code/win_date/win/price/last_price/bm_price/last_bm_price

/** �ⲿ����:
	(1) ~/����_ͨ�ú���/map_date_to_index

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


/* ģ��4: �����¼����ڣ�������ͬ�¼��Ĵ���֮�����ص������д��ڵĽض�*/

/* ����
	(0) eventName: �ַ�����
	(1) event_table: event_id / date(������) / stock_code /����
	(2) stock_hqinfo_table: end_date/stock_code/price/last_price
	(3) bm_hqinfo_table: end_date/stock_code/price/last_price (����ÿ��stock����ָ����ͬ�Ļ�׼�����price����ָ����׼�ļ۸�)
	(4) start_win: ������ʼ
	(5) end_win�����ڽ���

/** ���:
    (1) &eventName._hq: event_id/event_date/stock_code/win_date/win/price/last_price/bm_price/last_bm_price


/** �ⲿ����:
	(1) ~/����_ͨ�ú���/map_date_to_index

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
			FROM tt_hqinfo_with_bm A LEFT JOIN &event_table. B   /* ֻ�������е�A�� */
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



/* ģ��5: ����ÿ�����ڵľ�������ͳ��������� */
/* ����
	(0) eventName: �ַ�����
	(1) event_hq_table: ��gen_no_overlapped_win����gen_overlapped_win���ɵı��
		����: event_id/event_date/win_date/win/stock_code/price/last_price/bm_price/bm_last_price/mark
	(2) buy_win�����봰�ڵ�(���������롣��: buy_win = 0 ��ʾ���¼�����������)
	(3) filter_invalid_after�� 1-�ڼ����¼����ۼ������ʵ�ʱ�򣬽�����mark=2/4���¼����棬��ͳһ������㡣0-ȫ�����㡣�����������ǰ����쳣��� 
	(4) filter_invalid_before: 1-�ڼ����¼�ǰ�ۼ������ʵ�ʱ�򣬽�����mark=2/4���¼����棬��ͳһ������㡣0-ȫ�����㡣�����������ǰ����쳣��ġ�
	(5) filter_limit_after�� 1-�ڼ����¼���������ʱ��������mark =1/3���¼����棨ͣ�ƻ��߸��ƺ�һ����ͣ)��0-ȫ�����㡣
	(6) filter_limit_before: 1-�ڼ����¼�ǰ��������ʱ��������mark =1/3���¼����棨ͣ�ƻ��߸��ƺ�һ����ͣ)��0-ȫ�����㡣

ע���ڼ��㵥������ʱ�������¼�ǰ/��)��Ĭ�϶����޳�mark=2/4������ģ����������ܴ�ĸ���)��
ע2���ڷ���alphaʱ��ͣ����������������alpha�͹�������������У��������ƺ����ͣ�ֿ������alpha�߹������Խ����ǽ�filter_limit_after/before���趨Ϊ1��
**/

/** ���:
    (1) &eventName._%eval(-&start_win)_&end_win:  ԭevent_hq_table�е��ֶ�+
		abs_ret: ��������
		alpha���������
		abs_ret_mdf: �������棨���filter_limit_after=1ʱ������mark=1/3�����Զ�����Ϊȱʧ)
		alpha_mdf: ������棨���filter_limit_after=1ʱ������mark=1/3�����Զ�����Ϊȱʧ)
		is_d_alpha_pos: ����alpha�Ƿ�Ϊ��
		is_d_ret_pos: ���վ��������Ƿ�Ϊ��
		accum_alpha���ۼ�alpha
		acccum_ret: �ۼƾ�������
		accum_valid��������ۼ�alpha/ret�Ƿ��������ͣ����10%���Ƶ������1- δ���� 0-����)
		is_accum_alpha_pos���ۼ�alpha�Ƿ�Ϊ��
		is_accum_ret_pos:�ۼ�ret�Ƿ�Ϊ��
		test_mark: �Ƿ���ֵ���abs_ret/alpha����֮һΪ0 (1-δ���� 0-����) ---> �ڼ����ۼ�ֵʱ��������ָ���������϶�����alpha����retΪ0��
				�º���Ҫ�˶Ըü����ֶε�ȡֵ
**/

%MACRO cal_win_ret(eventName, event_hq_table, buy_win, filter_invalid_after=1, filter_invalid_before = 1, 
									filter_limit_after=1, filter_limit_before=1);
	/* ���㵥��alpha */
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
	
	/* ������ʼ�տ�ʼ���ۻ�alpha����ע�¼��Ƿ���Ч */
	DATA &eventName._alpha;
		SET &eventName._alpha;
		BY event_id;
		RETAIN accum_alpha_after 0;
		RETAIN accum_ret_after 0;
		RETAIN after_valid 1;
		test_mark = 1; /** �����Ƿ����alpha����abs_retȱʧ����� */

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

	/* ������ʼ����ǰ���ۻ�alpha������ʼ��Ϊ��ֹ����ǰ���� */
	/* �磺��ʼ��Ϊ0����win=-2��ֵ��ʾ[-2,0]֮��������� */
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
		/** �����Ƿ���Ҫ������������ */
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
			ELSE IF NOT MISSING(alpha_array(i)) THEN mark_alpha_array(i) = 0; /* ����alpha�����ۻ�alpha������ȱʧ */
			ELSE mark_alpha_array(i) = .;
		END;
		/* �㷨��֤���ĳ���ض���win,��Ȼ����һ��ȡֵ��ȱʧ��*/
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


/* ģ��6: ������Ϣ���¼���ǰ[N,0]��alpha��return�����ڷ���ʹ�� */
/* ����
	(1) alpha_table: ��cal_win_ret���ɵı��
	(2) win_ahead���¼���ǰ��N��ȡֵ���������start_win�����ֵ����ȡstart_win
**/
/** ���:
    (1) alpha_table_edit��������Ϊalpha_table��������һ��������:ԭalpha_table +
			accum_alpha_ahead��[N,0]֮����ۼ�alpha
			accum_ret_ahead��[N,0]֮����ۼ�ret
			is_ret_pos_ahead:
			is_alpha_pos_ahead:
			win_ahead������ȷ�����¼�ǰ���ڣ���Ϊ�еĻ���Ϊ��β�����Ǹպ���win_ahead)

ע�����ڼ���cal_win_retʱѡ��filter_invalid_before=1������ܻ���һЩ�¼�accum_ret��accum_alpha��ȱʧ�ġ�
	��ʱ��ֻѡ����Զ������������δȱʧ��ֵ����Ϊwin_ahead�������ʹ�øú���ʱ�������趨filter_invalid_before = 0��
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


/* ģ��7�������¼����� */
/* Input:
	(1) alpha_table: cal_win_ret��append_ahead_effect�������
	(2) attribute_table: stock_code, end_date, ��������С�����stock_code��end_date�������������Ϣ����Ϊ������)

ע����event_date=end_date��Ϊ��������������Ϊ���Թ����޶����¼��գ������Ǵ����ա�
���alpha_table�б���Ҳ�����˿�����Ϊ�������Ϣ����is_ret_pos_ahead�ȣ���������øú�������ֱ�ӵ���alpha_collect_by_group���з������

/* Output: 
	(1) alpha_table_edit�� ���(alpha_file + ����attribute_table�г��ֵ�������),����Ϊalpha_file������֮�������һ������ */


%MACRO attribute_to_event(alpha_table, attribute_table, alpha_table_edit);
	/* Ϊ�������������и��� */
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


/** ģ��8�� �����ṩ�ķ������������ÿ�����ڵ�alpha/ret�ȵķ������ */
/* ����:
	(0) eventName: �¼���
	(1) alpha_table: cal_win_ret��append_ahead_effect��attribute_to_event��������
	(2) is_group: �Ƿ�����������win֮��ķ��������0-no, 1-yeas, group_var is valid only when this takes value 1��
	(3) group_var: ����������Կո�����
	(4) alpha_var: �ַ�������ѡ��alpha, abs_ret, accum_alpha, accum_ret���ĸ�����֮һ
	(5) is_output�� �Ƿ�������ⲿ�ļ��У�1- �������ʱ��filename��sheetname����Ч)��0-�����
	(6) file_name: �ַ���
	(7) sheet_name: �ַ���

**/

/* output: 
	(1) filename(all_&sheetname.��group_&sheetname.)��������������Լ�ȫ�����Ľ����
	(2) &eventName._&alpha_var._g: ������������Ľ����
	(3) &eventName._&alpha_var._all: ����ȫ���������Ľ����
***/


%MACRO alpha_collect_by_group(eventName, alpha_table, alpha_var, is_group, group_var, is_output = 0, file_name=., sheet_name=.);
	
	%LET alpha_var_mdf = &alpha_var.;
	%LET group_sentence = win; 
	%IF %sysevalf(&is_group. = 1) %THEN  %LET group_sentence = win &group_var;
	/** ��ͬalpha��������Ӧ��ͬ��hit_ratio����*/
	%IF &alpha_var. = alpha %THEN %DO;
		%LET hitratio = is_d_alpha_pos;
		%LET alpha_var_mdf = alpha_mdf;  /* ��ʵ���У��õ��ǵ������alpha */
	%END;
	%ELSE %IF &alpha_var. = abs_ret %THEN %DO;
		%LET hitratio = is_d_ret_pos;
		%LET alpha_var_mdf = abs_ret_mdf;  /* ��ʵ���У��õ��ǵ������abs_ret */
	%END;
	%ELSE %IF &alpha_var. = accum_alpha %THEN %LET hitratio = is_accum_alpha_pos;
	%ELSE %IF &alpha_var. = accum_ret %THEN %LET hitratio = is_accum_ret_pos;

	DATA tt_alpha_table;
		SET &alpha_table.;
	RUN;
	PROC SORT DATA = tt_alpha_table;
		BY win;
	RUN;
	/** ȫ�������� */
	PROC UNIVARIATE DATA = tt_alpha_table NOPRINT;
		BY win;
		VAR &alpha_var_mdf. &hitratio.;
		OUTPUT OUT = &eventName._&alpha_var._all N = obs mean = mean_&alpha_var. mean_&hitratio. std = std_&alpha_var. 
			pctlpts = 100 90 75 50 25 10 0  pctlpre = &alpha_var.;
	QUIT;
	 
	/* �������Է������ */
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
	/* ������ļ� */
	%IF %sysevalf(&is_output. = 1) %THEN %DO;
		%output_to_excel(excel_path = &file_name., input_table = &eventName._&alpha_var._all, sheet_name = all_&sheet_name.);
		%IF %sysevalf(&is_group. = 1) %THEN %DO;
			%output_to_excel(excel_path = &file_name., input_table = &eventName._&alpha_var._g, sheet_name = group_&sheet_name.);
			/** ת���������Ŀǰֻ֧�ֵ�һ��alpha_var */
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
	/** �����������ļ����򲻱������ݱ� */
	%IF %sysevalf(&is_output. = 1) %THEN %DO;
		PROC SQL;
			DROP TABLE &eventName._&alpha_var._all, &eventName._&alpha_var._g;
		QUIT;
	%END;
%MEND alpha_collect_by_group;



/** ģ��9�� ���ĳ���ض������£������¼�accum_alpha/accum_ret����� */
/* ����:
	(0) eventName: �¼���
	(1) event_table: ����Ĺ�Ʊ��Ϣ��
	(1) alpha_table: cal_win_ret��append_ahead_effect��attribute_to_event��������
	(2) output_win: �ض���ĳ��win�µĽ��
	(2) is_output�� �Ƿ�������ⲿ�ļ��У�1- �������ʱ��filename��sheetname����Ч)��0-�����
	(3) file_name: �ַ���
	(4) sheet_name: �ַ���
	(5) group_var: ����Ȥ�ķ�����Ϣ���Կո����
**/

/* output: 
	(1) filename(all_&sheetname.��group_&sheetname.)������detail�����
	(2) &eventName._alpha_detail: ������������Ľ����
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

	/* ������ļ� */
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
	
