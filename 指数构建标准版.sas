/** ��Ϲ���_��Ч����_1.0 **/

%LET trans_cost = 0.35;  /** ���� **/

/** ����: 
(1) �������µĹ�Ʊ�� stock_pool: date/stock_code/weight
(2) ���еĵ��������б� adjust_date_table: date 
(3) ����ȫ�ֱ���: index_code/stock_sector_mapping(table)/index_component(table)/hqinfo(table)/benchmark_hqinfo(table)/busday(table)
**/



/*** ģ��0: �����������ּ�¼������end_date,effective_date **/
/** ȫ�ֱ�: 
(1) busday **/
/** ����: 
(1) stock_pool: date / stock_code/ weight
(2) adjust_date_table: date (���stock_pool�����ڳ���adjust_table�ķ�Χ������Ч�� ���adjust_table����stock_poolû�е����ڣ�����Ϊ�����Ʊ����û�й�Ʊ)
(3) move_date_forward: �Ƿ���Ҫ��date�Զ���ǰ����һ�������գ���Ϊend_date  **/
/** ���:
(1) output_stock_pool: end_date/effective_date/stock_code/weight  **/

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


/*** ģ��2: ��weight���б�׼������ **/
/** ����: 
(1) stock_pool: end_date / effective_date(��ѡ) / stock_code/ weight
/** ���:
(1) output_stock_pool: end_date/effective_date(��ѡ)/stock_code/weight(������)  **/

%MACRO neutralize_weight(stock_pool, output_stock_pool);
	PROC SQL NOPRINT;	
		CREATE TABLE tmp AS
		SELECT A.*, B.t_weight
		FROM &stock_pool. A LEFT JOIN 
		(
		SELECT end_date, sum(weight) AS t_weight
		FROM &stock_pool.
		GROUP BY end_date
		)B
		ON A.end_date = B.end_date;
	QUIT;

	DATA &output_stock_pool.(drop = t_weight);
		SET tmp;
		IF t_weight ~= 0 THEN weight = round(weight/t_weight,0.00001);  
		ELSE weight = 0;
	RUN;
	PROC SQL;
		DROP TABLE tmp;
	QUIT;
%MEND neutralize_weight;

/** ģ��3: �趨�����⼰����ƫ������ **/
/** ����: 
(1) stock_pool: end_date / effective_date(��ѡ) / stock_code/ weight /indus_code/indus_name
(2) indus_limit: ������ҵ������е����Ȩ��
(3)  stock_limit: �������������ҵ�е����Ȩ��
/** ���:
(1) output_stock_pool: end_date/effective_date(��ѡ)/stock_code/weight(������)/indus_code/indus_name  **/

%MACRO limit_adjust(stock_pool, indus_upper, indus_lower,  stock_limit, output_stock_pool);
	/** �޶���ҵȨ�� **/
	PROC SQL;
		CREATE TABLE indus_info AS
		SELECT end_date, indus_code, sum(weight) AS indus_weight
		FROM &stock_pool.
		GROUP BY end_date, indus_code;
	QUIT;

	PROC SQL NOPRINT;
		SELECT distinct end_date, count(distinct end_date)
		INTO :date_list separated by ' ',
			 :date_nobs
		FROM indus_info;
	QUIT;
	
	/** ÿ�촦�� */
	%DO date_index = 1 %TO &date_nobs;
		%LET curdate = %SCAN(&date_list., &date_index., ' ');
		
		/* Step1�����ʹ�������ҵ */
		/** �������޵���ҵ **/
		%LET big_nobs = 0;
		%LET big_wt = 0;
		PROC SQL NOPRINT;
			SELECT end_date, sum(indus_weight), count(*)
			INTO :end_date,
				 :big_wt,
				 :big_nobs
			FROM indus_info
			WHERE end_date = input("&curdate.", mmddyy10.) AND indus_weight > &indus_upper.
			GROUP BY end_date;
		QUIT;

		/** �������޵���ҵ **/ 
		%LET small_nobs = 0;
		%LET small_wt = 0;
		PROC SQL NOPRINT;
				SELECT end_date, sum(indus_weight),count(*)
				INTO :end_date,
				 	 :small_wt,
					 :small_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND indus_weight < &indus_upper.
				GROUP BY end_date;
			QUIT;

	
		%DO %WHILE (%SYSEVALF( &big_nobs. AND &small_nobs.));
			DATA indus_info;
				MODIFY indus_info;
				IF indus_weight > &indus_upper. AND end_date = input("&curdate.", mmddyy10.) THEN DO;
					indus_weight = &indus_upper.;
				END;
				ELSE IF indus_weight < &indus_upper. AND end_date =input("&curdate.", mmddyy10.) THEN DO;
					%LET large_part = %SYSEVALF(&big_wt. - &indus_upper. * &big_nobs.);
					indus_weight = indus_weight + (indus_weight / &small_wt.) * &large_part.;
				END;
			RUN;

			/** �������޵���ҵ **/
			%LET big_nobs = 0;
			%LET big_wt = 0;
			PROC SQL NOPRINT;
				SELECT end_date, sum(indus_weight), count(*)
				INTO :end_date,
				 :big_wt,
				 :big_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND indus_weight > &indus_upper.
				GROUP BY end_date;
			QUIT;

			/** �������޵���ҵ **/ 
			%LET small_nobs = 0;
			%LET small_wt = 0;
			PROC SQL NOPRINT;
				/* �������Ƶ� */
				SELECT end_date, sum(indus_weight),count(*)
				INTO :end_date,
				 	 :small_wt,
					 :small_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND indus_weight < &indus_upper.
				GROUP BY end_date;
			QUIT;
		%END;


		/** Step2: ���ӣ���������ҵ */
		/** �������޵���ҵ **/
		%LET big_nobs = 0;
		%LET big_wt = 0;
		PROC SQL NOPRINT;
			SELECT end_date, sum(indus_weight), count(*)
			INTO :end_date,
				 :big_wt,
				 :big_nobs
			FROM indus_info
			WHERE end_date = input("&curdate.", mmddyy10.) AND indus_weight > &indus_lower.
			GROUP BY end_date;
		QUIT;

		/** �������޵���ҵ **/ 
		%LET small_nobs = 0;
		%LET small_wt = 0;
		PROC SQL NOPRINT;
				/* �������Ƶ� */
				SELECT end_date, sum(indus_weight),count(*)
				INTO :end_date,
				 	 :small_wt,
					 :small_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND indus_weight < &indus_lower.
				GROUP BY end_date;
			QUIT;

	
		%DO %WHILE (%SYSEVALF( &big_nobs. AND &small_nobs.));
			DATA indus_info;
				MODIFY indus_info;
				IF indus_weight < &indus_lower. AND end_date = input("&curdate.", mmddyy10.) THEN DO;
					indus_weight = &indus_lower.;
				END;
				ELSE IF indus_weight > &indus_lower. AND end_date =input("&curdate.", mmddyy10.) THEN DO;
					%LET small_part = %SYSEVALF(&indus_lower. * &small_nobs. - &small_wt.);
					indus_weight = indus_weight - (indus_weight / &big_wt.) * &small_part.;
				END;
			RUN;

			/** �������޵���ҵ **/
			%LET big_nobs = 0;
			%LET big_wt = 0;
			PROC SQL NOPRINT;
				SELECT end_date, sum(indus_weight), count(*)
				INTO :end_date,
				 :big_wt,
				 :big_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND indus_weight > &indus_lower.
				GROUP BY end_date;
			QUIT;

			/** �������޵���ҵ **/ 
			%LET small_nobs = 0;
			%LET small_wt = 0;
			PROC SQL NOPRINT;
				SELECT end_date, sum(indus_weight),count(*)
				INTO :end_date,
				 	 :small_wt,
					 :small_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND indus_weight < &indus_lower.
				GROUP BY end_date;
			QUIT;
		%END;

	%END;
	
	/** ��ÿ����ҵ��Ȩ�أ�ƥ�䵽���ɡ�Ҫ�����Ȩ���������趨Ȩ�أ��������������ҵ�ڲ����� **/
	PROC SQL;
		CREATE TABLE stock_info AS
		SELECT A.*, B.indus_weight, C.indus_weight_raw
		FROM &stock_pool. A LEFT JOIN indus_info B
		ON A.end_date = B.end_date AND A.indus_code = B.indus_code
		LEFT JOIN 
		(SELECT end_date, indus_code, sum(weight) AS indus_weight_raw
		FROM &stock_pool.
		GROUP BY end_date, indus_code ) C
		ON A.end_date = C.end_date AND A.indus_code = C.indus_code
		ORDER BY A.end_date, A.indus_code;
	QUIT;
	/* ���յ��������ҵȨ�أ����·�����ҵ�ڸ���Ȩ�� */
	DATA stock_info(drop = indus_weight_raw);
		SET stock_info;
		weight = weight * indus_weight / indus_weight_raw;
	RUN;
	


	/** Ҫ����ɵ����Ȩ�ز��ܳ�����������ҵȨ�ص�һ������ */
	/** ÿ�죬����ҵ���� */

	%DO date_index = 1 %TO &date_nobs;
		%LET curdate = %SCAN(&date_list., &date_index., ' ');
		/* ȡ��ҵ���ֱ��� */

		PROC SQL NOPRINT;
			SELECT distinct indus_name, count(distinct indus_name)
				INTO :indus_list separated by ' ',
					: indus_nobs
			FROM stock_info
			WHERE end_date = input("&curdate.", mmddyy10.);
		QUIT;


		%DO indus_index = 1 %TO &indus_nobs.;
			%LET cur_indus = %SCAN(&indus_list, &indus_index., ' ');

			%LET big_nobs = 0;
			/** �������Ƶĸ��� **/
			PROC SQL NOPRINT;
				SELECT end_date, indus_code, sum(weight), count(*), mean(indus_weight)
				INTO :end_date,
					 :indus_code,
					 :big_wt,
				 	 :big_nobs,
					 :indus_wt
				FROM stock_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND indus_name = "&cur_indus." 
					AND weight > &stock_limit.
				GROUP BY end_date, indus_code;
			QUIT;

			%LET small_wt = 0;
			%LET small_nobs = 0;
			PROC SQL NOPRINT;
				SELECT end_date, indus_code, sum(weight), count(*)
				INTO :end_date,
						 :indus_code,
					 	:small_wt,
				 	 	:small_nobs
				FROM stock_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND indus_name = "&cur_indus." 
					AND weight < &stock_limit.
				GROUP BY end_date, indus_code;
			QUIT;

		
			%DO %WHILE (%SYSEVALF(&big_nobs. AND &small_nobs.));
				/** �������Ƶĸ��� **/
				DATA stock_info;
					MODIFY stock_info;
					IF end_date = input("&curdate.", mmddyy10.) AND indus_name = "&cur_indus." 
						AND weight > &stock_limit. THEN DO;
						weight = &stock_limit.;
					END;
					ELSE IF end_date = input("&curdate.", mmddyy10.) AND indus_name = "&cur_indus." 
						AND weight < &stock_limit. THEN DO;
							%LET large_part = %SYSEVALF(&big_wt. - &stock_limit. * &big_nobs.);
							weight = weight + (weight / &small_wt.) * &large_part.;
					END;
				RUN;

				/** �����Ƿ��г������Ƶ���ҵ */
				%LET big_nobs = 0;
				PROC SQL NOPRINT;
					SELECT end_date, indus_code, sum(weight), count(*), mean(indus_weight)
					INTO :end_date,
						 :indus_code,
					 	:big_wt,
				 	 	:big_nobs,
					 	:indus_wt
					FROM stock_info
					WHERE end_date = &curdate. AND indus_name = "&cur_indus." 
						AND weight >  &stock_limit.
					GROUP BY end_date, indus_code;
				QUIT;

				%LET small_nobs = 0;
				%LET small_wt = 0;
				PROC SQL NOPRINT;
					SELECT end_date, indus_code, sum(weight), count(*)
					INTO :end_date,
						 :indus_code,
					 	:small_wt,
				 	 	:small_nobs
					FROM stock_info
					WHERE end_date = input("&curdate.", mmddyy10.) AND indus_name = "&cur_indus." 
						AND weight < &stock_limit.
					GROUP BY end_date, indus_code;
				QUIT;

			%END;
		%END;
	%END;

	DATA &output_stock_pool;
		SET stock_info(drop = indus_weight);
	RUN;
	PROC SQL;
		DROP TABLE stock_info, indus_info;
	QUIT;

%MEND limit_adjust;

/** ģ��3-2: �趨����Ȩ�� **/
/** ����: 
(1) stock_pool: end_date / effective_date(��ѡ) / stock_code/ weight 
(2)  stock_limit: ����Ȩ��
/** ���:
(1) output_stock_pool: end_date/effective_date(��ѡ)/stock_code/weight(������)/indus_code/indus_name  **/

%MACRO limit_adjust_stock_only(stock_pool, stock_upper, stock_lower, output_stock_pool);
	/** �޶�����Ȩ�� **/
	PROC SQL;
		CREATE TABLE indus_info AS
		SELECT *
		FROM &stock_pool.;
	QUIT;

	PROC SQL NOPRINT;
		SELECT distinct end_date, count(distinct end_date)
		INTO :date_list separated by ' ',
			 :date_nobs
		FROM indus_info;
	QUIT;
	
	/** ÿ�촦�� */
	%DO date_index = 1 %TO &date_nobs;
		%LET curdate = %SCAN(&date_list., &date_index., ' ');
		
		/* Step1�����ʹ����޸��� */
		/** �������޵ĸ��� **/
		%LET big_nobs = 0;
		%LET big_wt = 0;
		PROC SQL NOPRINT;
			SELECT end_date, sum(weight), count(*)
			INTO :end_date,
				 :big_wt,
				 :big_nobs
			FROM indus_info
			WHERE end_date = input("&curdate.", mmddyy10.) AND weight > &stock_upper.
			GROUP BY end_date;
		QUIT;

		/** �������޵ĸ��� **/ 
		%LET small_nobs = 0;
		%LET small_wt = 0;
		PROC SQL NOPRINT;
				SELECT end_date, sum(weight),count(*)
				INTO :end_date,
				 	 :small_wt,
					 :small_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND weight < &stock_upper.
				GROUP BY end_date;
			QUIT;
	
		%DO %WHILE (%SYSEVALF( &big_nobs. AND &small_nobs.));
			DATA indus_info;
				MODIFY indus_info;
				IF weight > &stock_upper. AND end_date = input("&curdate.", mmddyy10.) THEN DO;
					weight = &stock_upper.;
				END;
				ELSE IF weight < &stock_upper. AND end_date =input("&curdate.", mmddyy10.) THEN DO;
					%LET large_part = %SYSEVALF(&big_wt. - &stock_upper. * &big_nobs.);
					weight = weight + (weight / &small_wt.) * &large_part.;
				END;
			RUN;

			/** �������޵ĸ��� **/
			%LET big_nobs = 0;
			%LET big_wt = 0;
			PROC SQL NOPRINT;
				SELECT end_date, sum(weight), count(*)
				INTO :end_date,
				 :big_wt,
				 :big_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND weight > &stock_upper.
				GROUP BY end_date;
			QUIT;

			/** �������޵ĸ��� **/ 
			%LET small_nobs = 0;
			%LET small_wt = 0;
			PROC SQL NOPRINT;
				/* �������Ƶ� */
				SELECT end_date, sum(weight),count(*)
				INTO :end_date,
				 	 :small_wt,
					 :small_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND weight < &stock_upper.
				GROUP BY end_date;
			QUIT;
		%END;


		/** Step2: ���ӣ������޸��� */
		/** �������޵ĸ��� **/
		%LET big_nobs = 0;
		%LET big_wt = 0;
		PROC SQL NOPRINT;
			SELECT end_date, sum(weight), count(*)
			INTO :end_date,
				 :big_wt,
				 :big_nobs
			FROM indus_info
			WHERE end_date = input("&curdate.", mmddyy10.) AND weight > &stock_lower.
			GROUP BY end_date;
		QUIT;

		/** �������޵���ҵ **/ 
		%LET small_nobs = 0;
		%LET small_wt = 0;
		PROC SQL NOPRINT;
				/* �������Ƶ� */
				SELECT end_date, sum(weight),count(*)
				INTO :end_date,
				 	 :small_wt,
					 :small_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND weight < &stock_lower.
				GROUP BY end_date;
			QUIT;

	
		%DO %WHILE (%SYSEVALF( &big_nobs. AND &small_nobs.));
			DATA indus_info;
				MODIFY indus_info;
				IF weight < &stock_lower. AND end_date = input("&curdate.", mmddyy10.) THEN DO;
					weight = &stock_lower.;
				END;
				ELSE IF weight > &stock_lower. AND end_date =input("&curdate.", mmddyy10.) THEN DO;
					%LET small_part = %SYSEVALF(&stock_lower. * &small_nobs. - &small_wt.);
					weight = weight - (weight / &big_wt.) * &small_part.;
				END;
			RUN;

			/** �������޵ĸ��� **/
			%LET big_nobs = 0;
			%LET big_wt = 0;
			PROC SQL NOPRINT;
				SELECT end_date, sum(weight), count(*)
				INTO :end_date,
				 :big_wt,
				 :big_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND weight > &stock_lower.
				GROUP BY end_date;
			QUIT;

			/** �������޵���ҵ **/ 
			%LET small_nobs = 0;
			%LET small_wt = 0;
			PROC SQL NOPRINT;
				SELECT end_date, sum(weight),count(*)
				INTO :end_date,
				 	 :small_wt,
					 :small_nobs
				FROM indus_info
				WHERE end_date = input("&curdate.", mmddyy10.) AND weight < &stock_lower.
				GROUP BY end_date;
			QUIT;
		%END;

	%END;

	DATA &output_stock_pool;
		SET indus_info;
	RUN;
	PROC SQL;
		DROP TABLE indus_info;
	QUIT;

%MEND limit_adjust_stock_only;



/*** ģ��4: ���ݵ�����������ÿ��Ĺ�Ʊ�� */
/** �ⲿ��: 
(1) busday: date **/

/** ����: 
(1) stock_pool: end_date / effective_date(��ѡ) / stock_code/ weight /(����)
(2) test_period_table: date (��Ӧ����effective_date)
(3) adjust_date_table: end_date   **/

/** ���:
(1) output_stock_pool: end_date/effective_date(��ѡ)/stock_code/weight(��׼���ҵ�����)/adjust_date/adjust_weight / (����) **/

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
	(3): �ز��ڵĵ�һ��ǡ����ĳ�������յ���һ�졣  --> ����
	(4): �ز��ڵ����һ���������������  --> ���ݴ��������ݲ�����
	**/ 
	/** ȡ��ǰ�����һ���ز�������ĵ�����(����)����Ϊ�ز�Ŀ�ͷ��Ϊ�˺������weight��׼ȷ�� **/
	PROC SQL NOPRINT;
		CREATE TABLE tmp AS
		SELECT date
		FROM &test_period_table.
		WHERE today() > date > (SELECT min(end_date) FROM &adjust_date_table.);  /* ��ֻ�������������֮�� */
		
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


	/* Step4: ���Ʊ������ */
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


/** ģ��5: ����������ȷ�����ÿ�����׼ȷ��weight�͵������棨�ð汾����ÿ���������) **/
/** �ⲿ��: hqinfo
(1) end_date/stock_code/pre_close/close/factor (����ֻ����A�ɹ�Ʊ) */

/** ����: 
(1) daily_stock_pool: date / stock_code/ adjust_weight/ adjust_date / (����)
(2) adjust_date_table: end_date   **/

/** ���:
(1) output_stock_pool: date/stock_code/adjust_weight/adjust_date/���� + ���� */

/* �����й�wt���ֶ�:
(a) ����: open_wt (ǰһ�����̵������Ȩ��)
(b) ����: close_wt 
(c) pre_close_wt
(d) after_close_wt
*/
/* �����������ֶ�: daily_ret/accum_ret/pre_accum_ret(��adjust_date��ʼ) **/
/** ���������ֶ�: pre_date/pre_price/price **/


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
		IF not missing(price) THEN accum_ret = (price/adjust_price - 1)*100;
		ELSE accum_ret = 0;
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
		mark = 1;
		RETAIN r_last_date .;
		RETAIN r_last_accum_ret .;
		RETAIN r_last_stock_code .;
		IF first.stock_code OR pre_date = adjust_date THEN DO;
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


	/* Step2-2: ����Ȩ�أ��ѵ������Լ�ǰ����Ȩ�� */
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
		
	


/** ģ��6: ������ϵ������alpha�� */

/** ����: 
(1) daily_stock_pool: date / stock_code/open_wt/daily_ret / (����)

/** ���:
(1) output_daily_summary: date/daily_ret/accum_ret/nstock **/


%MACRO cal_portfolio_ret(daily_stock_pool, output_daily_summary);
	/* �����ս���ͳ�� */
	PROC SQL;
		CREATE TABLE tt_summary_day AS
		SELECT date, sum(open_wt*daily_ret) AS daily_ret, sum(open_wt>0) AS nstock,
		((sum(adjust_weight*accum_ret/100)+1)/(sum(adjust_weight*pre_accum_ret/100)+1)-1)*100 AS daily_ret_p,
		sum(open_wt_c * daily_ret_c) AS daily_ret_c,
		((sum(adjust_weight*accum_ret_c/100)+1)/(sum(adjust_weight*pre_accum_ret_c/100)+1)-1)*100 AS daily_ret_c_p
		FROM &daily_stock_pool.
		GROUP BY date;
	QUIT;

	DATA tt_summary_day;
		SET tt_summary_day;
		RETAIN accum_ret 0;
		accum_ret = ((accum_ret/100 + 1)*(1+daily_ret_c/100)-1)*100;  /* ������ȫ����ָ������������ʣ�Ϊ׼*/
		index = 1000 * (1+accum_ret/100);
	RUN;
	DATA &output_daily_summary;
		SET tt_summary_day;
	RUN;

	PROC SQL;
		DROP TABLE tt_summary_day;
	QUIT;
%MEND cal_portfolio_ret;


/** ģ��7: ��ҵ��Ȩ������ά����ԭ������ҵ�ڵ�Ȩ�� */
/** ����: 
(1) stock_pool: end_date / stock_code/ weight /indus_code/indus_name

/** ���:
(1) output_stock_pool: end_date/stock_code/weight/indus_code/indus_name */

%MACRO indus_netrual(stock_pool, output_stock_pool);
	PROC SQL;
		CREATE TABLE tt_pool AS 
		SELECT A.*, B.indus_nobs, C.indus_wt
		FROM &stock_pool. A LEFT JOIN
		(SELECT end_date, count(distinct indus_code) AS indus_nobs
		FROM &stock_pool.
		GROUP BY end_date) B 
		ON A.end_date = B.end_date
		LEFT JOIN
		(SELECT end_date, indus_code, sum(weight) AS indus_wt
		FROM &stock_pool.
		GROUP BY end_date, indus_code) C
		ON A.end_date = C.end_date AND A.indus_code = C.indus_code
		ORDER BY A.end_date, A.stock_code;
	QUIT;
	DATA &output_stock_pool.(drop = indus_wt indus_nobs);
		SET tt_pool;
		weight = (1/indus_nobs)*(weight/indus_wt);
	RUN;
%MEND indus_netrual;

/** ģ��8: ���ɽ����嵥 */
/** �������ű�: ��Ϊ���׷�����date�����̺�
(1) ��Ʊ�����嵥; ����date/stock_code/initial_wt/traded_wt/final_wt/status/trade_type/trans_cost(�ݶ�Ϊ����0.35%)
(2) ÿ�콻���嵥: ����date/delete_assets/added_assets/sell_wt/buy_wt/buy_cost/sell_cost/turnover(˫�ߺ�)
**/
%MACRO trading_summary(daily_stock_pool, adjust_date_table, output_stock_trading, output_daily_trading);
	
	/* Step1: ֻ���ǵ����պ͵�����֮��ļ�¼ */
	/** ������ȫ����������Ȩ��Ϊ׼ */
	PROC SQL;
		CREATE TABLE tt_stock_pool AS
		SELECT date, pre_date, stock_code, 
		close_wt_c AS close_wt, 
		open_wt_c AS open_wt,
		pre_close_wt_c AS pre_close_wt, 
		after_close_wt_c AS after_close_wt
		FROM &daily_stock_pool.
		WHERE date IN  (SELECT end_date FROM &adjust_date_table.) 
		OR pre_date IN (SELECT end_date FROM &adjust_date_table.);
	QUIT;
	/* Step2: ȷ����Ʊ�䶯*/
	DATA tt_stock_pool;
		SET tt_stock_pool;
		/* ���� */
		IF after_close_wt = 0 THEN status = -1;  /* ��ʾ�޳� */
		ELSE status = 0; /* ���� */
		initial_wt = close_wt;
		traded_wt = after_close_wt - close_wt;
		final_wt = after_close_wt;
		IF after_close_wt - close_wt > 0 THEN trade_type = 1;
		ELSE IF after_close_wt = close_wt THEN trade_type = 0;
		ELSE trade_type = -1;
		/* ǰһ�� */
		IF pre_close_wt = 0 THEN status_p = 1;
		ELSE status_p = 0;
		initial_wt_p = pre_close_wt;
		traded_wt_p = open_wt - pre_close_wt;
		final_wt_p = open_wt;
		IF open_wt - pre_close_wt > 0 THEN trade_type_p = 1;
		ELSE IF open_wt = pre_close_wt  THEN trade_type_p = 0;
		ELSE trade_type_p = -1;
	RUN;

	/* Step3�������еĵ��ֶ���Ϊ���������̺� */
	/* ȷ�������������еĵ����� */
	PROC SQL;
		CREATE TABLE tt_adjust AS
		SELECT distinct adjust_date AS date
		FROM &daily_stock_pool.;
	QUIT;
		
	PROC SQL;
		CREATE TABLE tmp1 AS
		SELECT A.date, B.stock_code,
		B.status, B.trade_type, B.initial_wt, B.traded_wt, B.final_wt
		FROM tt_adjust A LEFT JOIN tt_stock_pool B
		ON A.date = B.date /* �ڵ�һ�ε���ʱ, stock_code_b����Ϊ�� */
		WHERE not missing(stock_code)
		ORDER BY A.date;

		CREATE TABLE tmp2 AS
		SELECT A.date, B.stock_code, B.status_p, B.trade_type_p, 
		B.initial_wt_p, B.traded_wt_p, B.final_wt_p
		FROM tt_adjust A LEFT JOIN tt_stock_pool B
		ON A.date = B.pre_date
		WHERE not missing(stock_code)
		ORDER BY A.date;

		CREATE TABLE tt_stock_pool AS  /* ȡ��������Ʊ���û�е�������ģ���tmp1��tmp2����һ���� */
		SELECT *
		FROM tmp1 UNION
		(SELECT date, stock_code, status_p AS status, trade_type_p AS trade_type,
		initial_wt_p AS initial_wt, traded_wt_p AS traded_wt, final_wt_p AS final_wt
		FROM tmp2) 
		ORDER BY date, stock_code;
	QUIT;
	DATA &output_stock_trading.;
		SET tt_stock_pool;
		trans_cost = abs(traded_wt) * 0.0035;
	RUN;

	/* Step4: ͳ��ÿ������ */
	PROC SQL;
		CREATE TABLE &output_daily_trading. AS
		SELECT date, sum(status=1) AS added_assets, sum(status=-1) AS deleted_assets,
			sum(traded_wt *(traded_wt>0)) AS buy_wt, 
			- sum(traded_wt * (traded_wt<0)) AS sell_wt,
			sum(traded_wt *(traded_wt>0)) * 0.0035 AS buy_cost,
			- sum(traded_wt * (traded_wt<0)) * 0.0035 AS sell_cost,
			sum(traded_wt *(traded_wt>0)) - sum(traded_wt * (traded_wt<0)) AS turnover
		FROM &output_stock_trading.
		GROUP BY date;
	QUIT;
	
	PROC SQL;
		DROP TABLE tt_stock_pool, tmp1, tmp2, tt_adjust;
	QUIT;
%MEND trading_summary;
