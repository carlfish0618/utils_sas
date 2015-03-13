/** ======================˵��=================================================**/

/*** �������ܣ��ṩ����Ϲ�����صĺ�������Ҫ����:
(1) �ӵ������ --> ÿ�����
(2) ÿ����ϵ������ʺͳֲ����ͳ��
**/ 

/**** �����б�:
(1) gen_adjust_pool: �����������ּ�¼ --> !!��Ҫ�õ��ⲿ��
(2) gen_daily_pool: ���ݵ�����������ÿ��Ĺ�Ʊ�� --> !!��Ҫ�õ��ⲿ��
(3) cal_stock_wt_ret: ����������ȷ�����ÿ�����׼ȷ��weight�͵������棨�ð汾����ÿ���������) --> !!��Ҫ�õ��ⲿ��
(4) cal_stock_wt_ret_loop: ����������ȷ�����ÿ�����׼ȷ��weight�͵������棨�����汾��������֤) --> !!��Ҫ�õ��ⲿ��
(5) cal_portfolio_ret: ������ϵ������alpha��
****/ 

/*** �����ȫ�ֱ�:
(1) busday: date
(2) 
** /

/** =======================================================================**/



/***  ģ��0: �����������ּ�¼������end_date,effective_date **/
/** ȫ�ֱ�: 
(1) busday **/
/** ����: 
(1) stock_pool: date / stock_code/ weight/����
(2) adjust_date_table: date (���stock_pool�����ڳ���adjust_table�ķ�Χ������Ч�� ���adjust_table����stock_poolû�е����ڣ�����Ϊ�����Ʊ����û�й�Ʊ)
(3) move_date_forward: �Ƿ���Ҫ��date�Զ���ǰ����һ�������գ���Ϊend_date  **/
/** ���:
(1) output_stock_pool: end_date/effective_date/stock_code/weight/���� **/

/** ����˵��: ��������£�����Ϊ�����Ʊ���ź�����ǰһ�����̺���12:00���ɵģ���date����������ڣ����趨end_date = date, effective_dateΪend_date��һ�������� 
  		  ��������£������Ʊ���ź����ڽ���0:00-����ǰ���ɵģ���date�ǽ�������ڣ��������ɵ��ּ�¼��ʱ��Ӧ��date�Զ���ǰ����һ�������ա�
		  ��������Ĵ�����Ҫ��Ϊ��ͳһ **/

%MACRO gen_adjust_pool(stock_pool, adjust_date_table, move_date_forward, output_stock_pool);
	DATA tt;
		SET busday;
	RUN;
	PROC SORT DATA = tt;
		BY date;
	RUN;
	DATA tt;
		SET tt;
		pre_date = lag(date);
		FORMAT pre_date mmddyy10.;
	RUN;
	PROC SQL;
		CREATE TABLE tmp2 AS
		SELECT A.*, B.pre_date, C.date AS next_date
		FROM &stock_pool. A LEFT JOIN tt B
		ON A.date = B.date
		LEFT JOIN tt C
		ON A.date = C.pre_date
		ORDER BY A.date, A.stock_code;
	QUIT;
	DATA tmp2(drop = pre_date next_date date);
		SET tmp2;
		IF &move_date_forward. = 1 THEN DO;  /* ��end_date�趨Ϊdateǰһ�� */
			end_date = pre_date;
			effective_date = date;
		END;
		ELSE DO;
			end_date = date;
			effective_date = next_date;
		END;
		IF missing(effective_date) THEN effective_date = end_date + 1; /** �������µ�һ�죬����������Ϊeffective_date */
		FORMAT effective_date end_date mmddyy10.;
	RUN;
	PROC SQL;
		CREATE TABLE &output_stock_pool. AS
		SELECT *
		FROM tmp2
		WHERE end_date IN
	   (SELECT end_date FROM &adjust_date_table.)  /* ֻȡ������ */
		ORDER BY end_date;
	QUIT;
	PROC SQL;
		DROP TABLE tt, tmp2;
	QUIT;
%MEND gen_adjust_pool;


/*** ģ��2: ���ݵ�����������ÿ��Ĺ�Ʊ�� */
/** �ⲿ��: 
(1) busday: date **/

/** ����: 
(1) stock_pool: end_date / stock_code/ weight /(����)
(2) test_period_table(datasets): date (��Ӧ����effective_date)
(3) adjust_date_table(datasets): end_date   **/

/** ���:
(1) output_stock_pool: date/stock_code/adjust_date/adjust_weight / (����) **/


/** ˵��:ע��ز��ں͵����յ�ѡ�񡣽���ز����ڵĵ�һ��ǡ����ĳ�������յ���һ�����ա���ʱ����������Ľ�����ǵ�ʱ��
���趨�Ļز��ڽ���ȫһ�¡����򸲸�ʱ�ο���ֻ�ǻز��ڵ��Ӽ���������ǰ�ӳ�һ��ʱ�䡣 */

%MACRO gen_daily_pool(stock_pool, test_period_table, adjust_date_table, output_stock_pool );
	/* Step1: ȷ�����ڵ������� */
	PROC SORT DATA = &adjust_date_table.;
		BY descending end_date;
	RUN;
	DATA tt_adjust;
		SET &adjust_date_table.;
		next_adj_date = lag(end_date);
		FORMAT next_adj_date mmddyy10.;
	RUN;
	
	/* Step2: ȷ����Ӧ�ĵ����� */
	/** ��4�����:
	(1): �ز��ڵĵ�һ��<=��������� --> ֻ�������������֮��Ļز�ʱ�䡣
	(2): �ز��ڵĵ�һ��>��������գ�����ΪƵ�ʲ�ͬ�������������֮��ļ������1�������� --> Ϊ����wt��������⣬�ѻز�����ǰ�ӳ����պþ������������һ��������
	(3): �ز��ڵĵ�һ��ǡ����ĳ�������յ���һ�������ա�  --> ����
	(4): �ز��ڵ����һ���������������  --> ���ݴ��������ݲ�����
	**/ 
	/** �ӵ������У�ȡ�����һ���ز�����ǰ����ĵ�����(����)����Ϊ�ز�Ŀ�ͷ��Ϊ�˺������weight��׼ȷ�� **/
	PROC SQL NOPRINT;
		CREATE TABLE tmp AS
		SELECT date
		FROM &test_period_table.
		WHERE date > (SELECT min(end_date) FROM &adjust_date_table.);  /* Ҫ�������������֮�� */
		
		SELECT max(end_date) INTO :nearby_adj
		FROM &adjust_date_table.
		WHERE end_date < (SELECT min(date) FROM tmp);

		CREATE TABLE tt_date_list AS
		SELECT A.date, B.end_date AS adjust_date
		FROM
		(
		SELECT date 
		FROM busday 
		WHERE &nearby_adj. <date <= (SELECT max(date) FROM &test_period_table.)
		) A  
		LEFT JOIN tt_adjust B
		ON B.end_date < A.date <= B.next_adj_date
		ORDER BY A.date;
	QUIT;


	/* Step3: �������һ��������֮��Ľ������� */
	PROC SQL NOPRINT;
		SELECT max(end_date) INTO :adjust_ending
		FROM &adjust_date_table.
	QUIT;
	DATA tt_date_list;
		SET tt_date_list;
		IF missing(adjust_date) THEN adjust_date = &adjust_ending.;
	RUN;


	/* Step4: ����ֹ�Ʊ������ */
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.date, A.adjust_date, B.*
		FROM tt_date_list A LEFT JOIN &stock_pool. B
		ON A.adjust_date = B.end_date 
		ORDER BY A.date;
	QUIT;
	DATA &output_stock_pool.;
		SET tmp(rename = (weight = adjust_weight) drop = end_date);
	RUN;
	PROC SORT DATA = &output_stock_pool.;
		BY date descending adjust_weight;
	RUN;

	PROC SQL;
		DROP TABLE tt_adjust, tt_date_list, tmp;
	QUIT;
%MEND gen_daily_pool;


/****** ģ��3: ����������ȷ�����ÿ�����׼ȷ��weight�͵������棨�ð汾����ÿ���������) **/
/** �ⲿ��: 
(1) hqinfo: end_date/stock_code/pre_close/close/factor (����ֻ����A�ɹ�Ʊ) 
(2) busday: date */

/** ����: 
(1) daily_stock_pool: date / stock_code/ adjust_weight/ adjust_date / (����)
(2) adjust_date_table: end_date 
**/

/** ���:
(1) output_stock_pool: date/stock_code/adjust_weight/adjust_date/���� + ���� */

/* �����й�wt���ֶ�:
(a) ����(ǰһ�����̵������Ȩ��)��open_wt(���ڸ�Ȩ���Ӽ���) / open_wt_c(����ǰ�ռ���)
(b) ����(����ǰ): close_wt(���ڸ�Ȩ���Ӽ���) / close_wt_c(����ǰ�ռ۸����)
(c) ǰһ�����̵���ǰ��Ȩ��: pre_close_wt/pre_close_wt_c
(d) ����(������Ȩ��): after_close_wt/after_close_wt_c
*/
/* �����������ֶ�: daily_ret(daily_ret_c)/accum_ret(accum_ret_c)/pre_accum_ret(pre_accum_ret_c) **/

/** ���������ֶ�: pre_date/pre_price/price/pre_close/close/adjust_price */
/** ������������ֶ�: mark(mark =1 ��ʾû�г����쳣�㣬�����辯����ǰ�ռ۸����������Ȩ���Ƿ���ȷ) */


%MACRO cal_stock_wt_ret(daily_stock_pool, adjust_date_table, output_stock_pool);
	/* Step1: ���㵥�������ʣ��ӵ�����������ۼ������ʵ� */
	DATA tt;
		SET busday;
		pre_date = lag(date);
		FORMAT pre_date mmddyy10.;
	RUN;
	/* ���Ч�ʣ���ȡ�Ӽ�*/
	PROC SQL;
		CREATE TABLE tt_hqinfo AS
		SELECT end_date, stock_code, pre_close, close, factor
		FROM hqinfo
		WHERE end_date >= (SELECT min(end_date)-20 FROM &daily_stock_pool.) 
		AND stock_code IN (SELECT stock_code FROM &daily_stock_pool.)
		ORDER BY end_date, stock_code;
	QUIT;

	PROC SQL;
		CREATE TABLE tt_summary_stock AS
		SELECT A.*, E.pre_date, (B.close*B.factor) AS price, (C.close*C.factor) AS pre_price, B.pre_close, B.close,
		(D.close*D.factor) AS adjust_price
		FROM &daily_stock_pool. A
		LEFT JOIN tt E
		ON A.date = E.date
		LEFT JOIN tt_hqinfo B
		ON A.date = B.end_date AND A.stock_code = B.stock_code
		LEFT JOIN tt_hqinfo C
		ON E.pre_date = C.end_date AND A.stock_code = C.stock_code
		LEFT JOIN tt_hqinfo D
		ON A.adjust_date = D.end_date AND A.stock_code = D.stock_code
		ORDER BY A.date, A.stock_code;
	QUIT;

	DATA tt_summary_stock;
		SET tt_summary_stock;
		IF not missing(price) THEN accum_ret = (price/adjust_price - 1)*100;  /** ���ֹ�Ʊ�ر����ȱ�֤adust_price~=0 ����ȱʧ */
		ELSE accum_ret = 0;   /** ������һ�����⣺�����ĳ�������ڼ䣬��Ʊ�����ˣ�priceȱʧ����ʱ����ʵ��һ�������ʡ�����������ۼ������ʲ�һ��Ϊ0 */
		IF not missing(pre_price) THEN pre_accum_ret = (pre_price/adjust_price-1)*100;
		ELSE pre_accum_ret = 0;
		IF not missing(pre_price) AND not missing(price) THEN daily_ret = (price/pre_price - 1)*100; 
		ELSE daily_ret = 0;
	RUN;

	/** ���� ����������close��pre_close�����ۼ����� */
	PROC SORT DATA = tt_summary_stock;
		BY stock_code date;
	RUN;
	DATA tt_summary_stock;
		SET tt_summary_stock;
		BY stock_code;
		mark = 1;  /** ��������� */
		RETAIN r_last_date .;
		RETAIN r_last_accum_ret .;
		RETAIN r_last_stock_code .;
		IF first.stock_code OR pre_date = adjust_date THEN DO; /** ������Ч�ĵ�һ�� */
			r_last_date = pre_date;
			r_last_accum_ret = 0;
			r_last_stock_code = stock_code;
		END;
		IF r_last_date = pre_date AND r_last_stock_code = stock_code THEN DO;
			pre_accum_ret_c = r_last_accum_ret;
			accum_ret_c = ((1+pre_accum_ret_c/100)*close/pre_close-1)*100;
			daily_ret_c = (close/pre_close-1)*100;
		END;
		ELSE mark = 0;
		r_last_date = date;
		r_last_accum_ret = accum_ret_c;
		r_last_stock_code = stock_code;
	RUN;

	
	/* Step2: �������Ȩ�� */
	/* Step2-1: ����Ȩ�أ�δ����ǰ��*/
	PROC SQL;
		CREATE TABLE tt_stock_wt AS
		SELECT A.*, round((A.adjust_weight*(1+A.accum_ret/100))/B.port_accum_ret,0.00001) AS close_wt,
		round((A.adjust_weight*(1+A.accum_ret_c/100))/B.port_accum_ret_c,0.00001) AS close_wt_c
		FROM tt_summary_stock A LEFT JOIN
		(
		SELECT date, sum(adjust_weight*accum_ret/100)+1 AS port_accum_ret,
		sum(adjust_weight*accum_ret_c/100)+1 AS port_accum_ret_c 
		FROM tt_summary_stock 
		GROUP BY date
		) B
		ON A.date = B.date
		ORDER BY A.date, close_wt desc;
	QUIT;


	/* Step2-2: ����Ȩ�أ��ѵ������Լ�ǰһ������Ȩ��(����ǰ) */
	/* �жϸ����ǰһ���Ƿ�Ϊ�����ջ��һ�죬����ǣ�����ʱΪadjust_weight������Ϊǰһ�������Ȩ�� */
	PROC SQL NOPRINT;
		CREATE TABLE tmp AS
		SELECT A.*, B.end_date AS adj_date_b, C.close_wt AS pre_close_wt, C.close_wt_c AS pre_close_wt_c
		FROM tt_stock_wt A LEFT JOIN &adjust_date_table. B
		ON A.pre_date = B.end_date
		LEFT JOIN tt_stock_wt C
		ON A.pre_date = C.date AND A.stock_code = C.stock_code
		ORDER BY A.date, A.close_wt desc;
	QUIT;

	DATA tmp(drop = adj_date_b);
		SET tmp;
		IF not missing(adj_date_b) THEN DO;
			open_wt = adjust_weight; 
			open_wt_c = adjust_weight;
		END;
		ELSE DO;
			open_wt = pre_close_wt;
			open_wt_c = pre_close_wt_c;
		END;
		IF missing(pre_close_wt) THEN pre_close_wt = 0;  /* �����Ĺ�Ʊ */
		IF missing(pre_close_wt_c) THEN pre_close_wt_c = 0;
	RUN;
	/* Step2-3�����̺�����˵�Ȩ�� */
	/* ��һ��Ŀ���Ȩ�� */
	PROC SQL;
		CREATE TABLE tt_stock_wt AS
		SELECT A.*, B.open_wt AS after_close_wt, 
		B.open_wt_c AS after_close_wt_c,
		B.date AS date_next
		FROM tmp A LEFT JOIN tmp B
		ON A.date = B.pre_date AND A.stock_code = B.stock_code
		ORDER BY A.date, A.close_wt desc;
	QUIT;
	DATA &output_stock_pool.(drop = date_next);
		SET tt_stock_wt;
		IF missing(after_close_wt) THEN after_close_wt = 0; /* ɾ���Ĺ�Ʊ���������: ���һ����Ϊ������ */
		IF missing(after_close_wt_c) THEN after_close_wt_c = 0;
	RUN;
	
	PROC SQL;
		DROP TABLE tt, tt_hqinfo, tt_summary_stock, tt_stock_wt, tmp;
	QUIT;
%MEND cal_stock_wt_ret;


/** ģ��4-appendix: �ȶ�: ��ģ�����ڲ���ģ��4���߼�����ȷ�ġ�����ÿ�յ����ķ��� **/
/***Ŀǰ֧�֣�ͨ������Ȩ���ӡ������Ȩ�� */
%MACRO cal_stock_wt_ret_loop(daily_stock_pool, output_stock_pool);
	/* Step1: ȡ����������������һ�������� (���������Ч��) */
	PROC SQL;
		CREATE TABLE effect_date_list AS
		SELECT adjust_date, min(date) AS effective_date  /* ���������û���κι�Ʊ����һ�콫�ᱻ���� */
		FROM &daily_stock_pool. 
		GROUP BY adjust_date
		ORDER BY adjust_date;
	QUIT;
	
	/* Step2: ���㵥�������ʣ��ӵ�����������ۼ������ʵ� */
	DATA tt;
		SET busday;
		pre_date = lag(date);
		FORMAT pre_date mmddyy10.;
	RUN;
	/* ���Ч�ʣ���ȡ�Ӽ�*/
	PROC SQL;
		CREATE TABLE tt_hqinfo AS
		SELECT end_date, stock_code, pre_close, close, factor
		FROM hqinfo
		WHERE end_date >= (SELECT min(end_date)-20 FROM &daily_stock_pool.) 
		AND stock_code IN (SELECT stock_code FROM &daily_stock_pool.)
		ORDER BY end_date, stock_code;
	QUIT;

	PROC SQL;
		CREATE TABLE tt_summary_stock AS
		SELECT A.*, E.pre_date, (B.close*B.factor) AS price, (C.close*C.factor) AS pre_price,
		(D.close*D.factor) AS adjust_price
		FROM &daily_stock_pool. A
		LEFT JOIN tt E
		ON A.date = E.date
		LEFT JOIN tt_hqinfo B
		ON A.date = B.end_date AND A.stock_code = B.stock_code
		LEFT JOIN tt_hqinfo C
		ON E.pre_date = C.end_date AND A.stock_code = C.stock_code
		LEFT JOIN tt_hqinfo D
		ON A.adjust_date = D.end_date AND A.stock_code = D.stock_code
		ORDER BY A.date, A.stock_code;
	QUIT;

	DATA tt_summary_stock;
		SET tt_summary_stock;
		accum_ret = (price/adjust_price - 1)*100;
		pre_accum_ret = (pre_price/adjust_price-1)*100;
		daily_ret = (price/pre_price - 1)*100;  
	RUN;
	
	/* Step3: ����ÿֻ��Ʊÿ���µ�Ȩ�� */
	/** ÿ��ѭ������ **/
	PROC SQL NOPRINT;
		CREATE TABLE tt AS
		SELECT distinct date 
		FROM &daily_stock_pool.
		ORDER BY date;

		SELECT date, count(*) 
		INTO :date_list separated by ' ',
			 :date_number
		FROM tt;
	QUIT;
	
	/* ��ʱ��Ʊ�أ�����ǰһ���Ʊ��close_wt */
	DATA tt_pre_pool;
		ATTRIB
			stock_code LENGTH = $ 16
			date LENGTH = 8 FORMAT = mmddyy10.
			close_wt LENGTH = 8
		;
		STOP;
	RUN;
	/* ���¹�Ʊ��: ���䵱���open_wt, close_wt */
	DATA tt_append_pool;
		ATTRIB
			date LENGTH = 8 FORMAT = mmddyy10.
			stock_code LENGTH = $ 16
			close_wt LENGTH = 8
			open_wt LENGTH = 8
			pre_close_wt LENGTH = 8
		;
		STOP;
	RUN;

	%DO date_index = 1 %TO &date_number.;
		%LET curdate = %scan(&date_list, &date_index., ' ');
		/* Step3-1: ȡ�����ǰһ���Ʊ�� */
		DATA tt_cur_pool;
			SET tt_summary_stock;
			IF date = input("&curdate.", mmddyy10.);
		RUN;
		PROC SQL;
			CREATE TABLE tt_cur_pool_2 AS
			SELECT A.*, B.close_wt AS pre_close_wt
			FROM tt_cur_pool A LEFT JOIN tt_pre_pool B
			ON A.stock_code = B.stock_code
			ORDER BY stock_code;
		QUIT;
		
		/* Step3-2: �жϵ����Ƿ�Ϊ��һ����Ч�� */
		 PROC SQL NOPRINT;
		 	SELECT count(*) INTO :is_effect
			FROM tt_cur_pool_2
			WHERE date IN (SELECT effective_date FROM effect_date_list);
		QUIT;
 		DATA tt_cur_pool;
			SET tt_cur_pool_2;
			IF missing(pre_close_wt) THEN pre_close_wt = 0;
			IF &is_effect. > 0 OR &date_index. = 1 THEN open_wt = adjust_weight; /* ��Ч��: ����Ȩ��Ϊ�������ֵ�Ȩ�� */
			ELSE open_wt = pre_close_wt; /* ����Ϊǰһ������Ȩ�� */
		RUN;
		/* ���㵱�������Ȩ�� */
		PROC SQL NOPRINT;
			SELECT sum(open_wt*(1+daily_ret/100)) INTO :sum_wt
			FROM tt_cur_pool;
		QUIT;
		DATA tt_cur_pool;
			SET tt_cur_pool;
			close_wt = round(open_wt*(1+daily_ret/100)/&sum_wt.,0.00001);
		RUN;
		
		/* Step3-3: ���¹�Ʊ�� */
		DATA tt_append_pool;
			SET tt_append_pool tt_cur_pool(keep = date stock_code close_wt open_wt pre_close_wt);
			IF missing(pre_close_wt) THEN pre_close_wt = 0;
		RUN;
		DATA tt_pre_pool;
			SET tt_cur_pool(keep = date stock_code close_wt);
		RUN;
	%END;

	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.close_wt, B.open_wt, B.pre_close_wt
		FROM tt_summary_stock A LEFT JOIN tt_append_pool B
		ON A.date = B.date AND A.stock_code = B.stock_code
		ORDER BY A.date, B.close_wt desc;
	QUIT;
	
	/* ���̵�����Ȩ�� */
	PROC SQL;
		CREATE TABLE &output_stock_pool. AS
		SELECT A.*, B.open_wt AS after_close_wt, B.date AS date_next
		FROM tmp A LEFT JOIN tmp B
		ON A.date = B.pre_date AND A.stock_code = B.stock_code
		ORDER BY A.date, A.close_wt desc;
	QUIT;
	DATA &output_stock_pool.(drop = date_next);
		SET &output_stock_pool.;
		IF missing(after_close_wt) THEN after_close_wt = 0; /* ɾ���Ĺ�Ʊ */
	RUN;

	PROC SQL;
		DROP TABLE  effect_date_list, tt, tt_hqinfo, tt_summary_stock, tt_pre_pool, tt_append_pool, tt_cur_pool, tt_cur_pool_2, tmp;
	QUIT;
%MEND cal_stock_wt_ret_loop;


		

/** ģ��4: ������ϵ������alpha�� */
/** ����: 
(1) daily_stock_pool: date / stock_code/open_wt(open_wt_c)/daily_ret(daily_ret_c) / (����)
(2) type(logical): 1- �ԡ�����ǰ���̼ۡ������Ȩ��Ϊ��׼����; 0- �ԡ����ڸ�Ȩ���ӡ������Ȩ��Ϊ��׼����

/** ���:
(1) output_daily_summary: date/daily_ret/accum_ret/index/nstock **/

%MACRO cal_portfolio_ret(daily_stock_pool, output_daily_summary,type = 1);

	/* Step1: ֻ���ǵ����պ͵�����֮���һ�� */
	%IF %SYSEVALF(&type. = 1) %THEN %DO;
		PROC SQL;
			CREATE TABLE tt_stock_pool AS
			SELECT date,stock_code, open_wt_c AS open_wt, daily_ret_c AS daily_ret
			FROM &daily_stock_pool.;
		QUIT;
	%END;
	%ELSE %DO;
		PROC SQL;
			CREATE TABLE tt_stock_pool AS
			SELECT date, stock_code, open_wt,daily_ret
			FROM &daily_stock_pool.;
		QUIT;
	%END;

	/* �����ս���ͳ�� */
	PROC SQL;
		CREATE TABLE tt_summary_day AS
		SELECT date, sum(open_wt>0) AS nstock,
/*		((sum(adjust_weight*accum_ret/100)+1)/(sum(adjust_weight*pre_accum_ret/100)+1)-1)*100 AS daily_ret_p,*/
		sum(open_wt*daily_ret) AS daily_ret, 
		FROM tt_stock_pool
		GROUP BY date;
	QUIT;

	DATA tt_summary_day;
		SET tt_summary_day;
		RETAIN accum_ret 0;
		accum_ret = ((accum_ret/100 + 1)*(1+daily_ret/100)-1)*100; /* �Ը�Ȩ���Ӽ��� */
		index = 1000 * (1+accum_ret/100);
	RUN;
	DATA &output_daily_summary;
		SET tt_summary_day;
	RUN;

	PROC SQL;
		DROP TABLE tt_summary_day;
	QUIT;
%MEND cal_portfolio_ret;

