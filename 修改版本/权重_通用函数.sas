/** ======================˵��=================================================**/

/*** �������ܣ��ṩ�����Ȩ����ص�ͨ�ú��� **** /

/**** �����б�:
(1) neutralize_weight:  ��weight���б�׼������
(2) limit_adjust_stock_only: ���ݸ���Ȩ��(��/����)���е���
(3) limit_adjust: ��������ҵ(��/����)�͸���Ȩ��(����)������ҵȨ�أ�����������ҵ�е�Ȩ��
(4) indus_netrual: ��ҵ��Ȩ������ά����ԭ������ҵ�ڵ�Ȩ��
(5) adjust_to_sector_neutral: ���ĳһָ����������ҵ���Բ��ԡ���ҵ�ڸ��ɱ��֣������ԭ�е�Ȩ��
(6) fill_in_index: ���ݲ�λҪ�����ù�Ʊ
****/ 
/** =======================================================================**/




/*** ģ��1: ��weight���б�׼������ **/
/** ����: 
(1) stock_pool: end_date /stock_code/ weight
/** ���:
(1) output_stock_pool: end_date/stock_code/weight(������) /����
**/

%MACRO neutralize_weight(stock_pool, output_stock_pool, col=weight);
	PROC SQL NOPRINT;	
		CREATE TABLE tmp AS
		SELECT A.*, B.t_&col.
		FROM &stock_pool. A LEFT JOIN 
		(
		SELECT end_date, sum(&col.) AS t_&col.
		FROM &stock_pool.
		GROUP BY end_date
		)B
		ON A.end_date = B.end_date;
	QUIT;

	DATA &output_stock_pool.(drop = t_&col.);
		SET tmp;
		IF t_&col. ~= 0 THEN &col. = round(&col./t_&col.,0.00001);  
		ELSE &col. = 0;
	RUN;
	PROC SQL;
		DROP TABLE tmp;
	QUIT;
%MEND neutralize_weight;


/***** ģ��2: �趨����Ȩ�� **/
/** ����: 
(1) stock_pool: end_date / stock_code/ weight 
(2)  stock_upper(numeric): ��������
(3) stock_lower(numeric): ��������
/** ���:
(1) output_stock_pool: end_date/stock_code/weight(������)/����
**/

/** ע��: ������޺������趨��������ʼ���޷��������������������������������������ǵ�����weight�Ǵ���� */
/** �������ȱ�֤���޲��ᱻ�������������ȱ�֤���޲��ᱻ�����Ľ���ǲ�ͬ�� */

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


/****** ģ��3: �趨����ҵ������ƫ�����ֵ **/
/** ����: 
(1) stock_pool: end_date / stock_code/ weight /indus_code/����
(2) indus_upper(numeric): ������ҵ������е����Ȩ��
(3) indus_lower(numeric): ������ҵ���������СȨ��
(4) stock_limit(numeric): �������������ҵ�е����Ȩ��
/** ���:
(1) output_stock_pool: end_date/stock_code/weight(������)/indus_code/����  **/

%MACRO limit_adjust(stock_pool, indus_upper, indus_lower, stock_limit, output_stock_pool);
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
			SELECT distinct indus_code, count(distinct indus_code)
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
				WHERE end_date = input("&curdate.", mmddyy10.) AND indus_code = "&cur_indus." 
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
				WHERE end_date = input("&curdate.", mmddyy10.) AND indus_code = "&cur_indus." 
					AND weight < &stock_limit.
				GROUP BY end_date, indus_code;
			QUIT;

		
			%DO %WHILE (%SYSEVALF(&big_nobs. AND &small_nobs.));
				/** �������Ƶĸ��� **/
				DATA stock_info;
					MODIFY stock_info;
					IF end_date = input("&curdate.", mmddyy10.) AND indus_code = "&cur_indus." 
						AND weight > &stock_limit. THEN DO;
						weight = &stock_limit.;
					END;
					ELSE IF end_date = input("&curdate.", mmddyy10.) AND indus_code = "&cur_indus." 
						AND weight < &stock_limit. THEN DO;
							%LET large_part = %SYSEVALF(&big_wt. - &stock_limit. * &big_nobs.);
							weight = weight + (weight / &small_wt.) * &large_part.;
					END;
				RUN;

				/** �����Ƿ��г������Ƶĸ��� */
				%LET big_nobs = 0;
				PROC SQL NOPRINT;
					SELECT end_date, indus_code, sum(weight), count(*), mean(indus_weight)
					INTO :end_date,
						 :indus_code,
					 	:big_wt,
				 	 	:big_nobs,
					 	:indus_wt
					FROM stock_info
					WHERE end_date = &curdate. AND indus_code = "&cur_indus." 
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
					WHERE end_date = input("&curdate.", mmddyy10.) AND indus_code = "&cur_indus." 
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



/** ģ��4: ��ҵ��Ȩ������ά����ԭ������ҵ�ڵ�Ȩ�� */
/** ����: 
(1) stock_pool: end_date / stock_code/ weight /indus_code/����

/** ���:
(1) output_stock_pool: end_date/stock_code/weight(������)/indus_code/���� */

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


/*** ģ��5: ��ҵ���Բ��� **/

/** ����: 
(1) stock_pool: end_date / stock_code/ weight /indus_code/����
(2) upper_limit(numeric): ��������ҵ�����Ȩ��(����޷����㣬��Ҫ������Ȩ�أ���"��ҵ"Ȩ�ػ���"��ҵ"�еĸ���Ȩ�ز���)
(3) index_component_table: end_date/stock_code/o_code/����

/** ���:
(1) output_stock_pool: end_date/stock_code/weight(������)/indus_code/type/���� 
����:
type: ��עλ���Ƿ�Ϊָ������(0- ����/1-��׼ָ��/2-��ҵָ��)
**/

%MACRO adjust_to_sector_neutral(stock_pool, upper_limit, index_component_table, output_stock_pool);
	/* Step1:��������գ���׼����ҵȨ�� */
	PROC SQL;
		CREATE TABLE tt_index_info AS
		SELECT end_date, indus_code, sum(weight)/100 AS sector_weight
		FROM &index_component_table.
		WHERE end_date IN
		(SELECT end_date FROM &stock_pool.)
		GROUP BY end_date, indus_code;
	QUIT;

	/* Step2: �������Ȩ�� */
	PROC SQL NOPRINT;
		CREATE TABLE tt_stock_pool AS
		SELECT A.*, B.sector_weight, C.t_indus_weight  
		FROM &stock_pool. A 
		LEFT JOIN 
		(
			SELECT end_date, indus_code, sum(weight)/100 AS sector_weight /* ȡ��ҵ�ڻ�׼�е�Ȩ��*/
			FROM tt_index_info
			GROUP BY end_date, indus_code;
		) B
		ON A.end_date = B.end_date AND A.indus_code = B.indus_code 
		LEFT JOIN
		(
			SELECT end_date, indus_code, sum(weight)/100 AS t_indus_weight
			FROM &stock_pool.
			GROUP BY end_date, indus_code
		) C 
		ON A.end_date = C.end_date AND A.indus_code = C.indus_code 
		ORDER BY A.end_date, A.indus_code;
	QUIT;
	
	
	DATA tt_stock_pool(drop = t_indus_weight);
		SET  tt_stock_pool;
		IF missing(sector_weight) OR round(sector_weight,0.0001) = 0 THEN weight = 0; /* ������ջ�׼��û�и���ҵ�����趨����ҵ�����и���Ȩ��Ϊ0 */
		ELSE IF t_indus_weight ~= 0 THEN weight = round(sector_weight*round(weight/t_indus_weight,0.0001),0.00001);
		ELSE weight = 0;
		IF weight > &upper_limit. * sector_weight THEN weight = &upper_limit. * sector_weight;  /* ��ҵ���и������� */
		type = 0 ; /* ��ע���� */
	RUN;
 

	/* Step3: ���������Ȩ�غ����¼�����ҵ����Ȩ�ء���Ҫʱ��������Ӧ����ҵָ�� */
	PROC SQL;
		CREATE TABLE tt_indus_weight AS
		SELECT A.*, B.t_indus_weight
		FROM tt_index_info A LEFT JOIN
		(
			SELECT end_date, indus_code, sum(weight) AS t_indus_weight
			FROM tt_stock_pool
			GROUP BY end_date, indus_code
		) B
		ON A.end_date = B.end_date AND A.indus_code = B.indus_code
		WHERE NOT missing(sector_weight)  /* ��ҵȨ�ؿ���ȱʧ */
		ORDER BY A.end_date, A.o_code;
	QUIT;
 
	DATA tt_indus_weight(keep = end_date indus_code add_indus type rename = (indus_code = stock_code add_indus = weight));
		SET tt_indus_weight;
		IF missing(t_indus_weight) OR round(t_indus_weight, 0.0001) = 0 THEN DO;
			add_indus = sector_weight;
		END;
		ELSE IF abs(sector_weight-t_indus_weight)> 0.001 AND round(sector_weight-t_indus_weight,0.001)>0 THEN DO;
			add_indus = round(sector_weight - t_indus_weight,0.0001);
		END;
		ELSE DO;
			add_indus = 0;
		END;
		type = 2;
		/* ������ */
		IF abs(add_indus) <= 0.001 THEN add_indus = 0;
	RUN;
	
	/* Step4: ����ָ���Ĺ�Ʊ�� */
	DATA &output_stock_pool.;
		SET tt_ind_weight tt_indus_weight;
		IF weight > 0;
	RUN;
	
	PROC SORT DATA = &output_stock_pool.;
		BY end_date descending type stock_code;
	RUN;

	PROC SQL;
		DROP TABLE  tt_indus_weight, tt_index_info, tt_stock_pool;
	QUIT;
%MEND adjust_to_sector_neutral;


/*** ģ��5: ���ݲ�λҪ�����ù�Ʊ **/

/** ����: 
(1) stock_pool: end_date / stock_code/ weight /����
(2) index_code(characters): ָ������
(3) ind_max_weight(numeric): ����������������Ȩ��(����޷����㣬��Ҫ������Ȩ�أ���ָ���еĸ��ɻ�ָ������Ȩ�ز���)
(4) all_max_weight(numeric): ��λ���ֵ


/** ���:
(1) output_stock_pool: end_date/stock_code/weight(������)/type/���� 
����:
&mark.: ��עλ���Ƿ�Ϊָ������(0- ����/1-��׼ָ��/2-��ҵָ��)
**/

/** Ҫ��ԭʼȨ���Ѿ���׼���� */

%MACRO fill_in_index(stock_pool,all_max_weight, ind_max_weight, output_stock_pool,index_code, mark=type);
	/* Step1: �������Ȩ�� */
	DATA tt_ind_pool;
		SET &stock_pool.;
		IF weight > &ind_max_weight. THEN weight = &ind_max_weight;  /* �и������� */
		&mark. = 0;
	RUN;
	
	/* Step2: ���������Ȩ�غ����¼�����ϼ���Ȩ�ء�������е����գ���Ҫʱ��������Ӧ�Ļ�׼ָ�� */
	PROC SQL;
		CREATE TABLE tt_info AS
		SELECT end_date, sum(weight) AS t_weight
		FROM tt_ind_pool
		GROUP BY end_date
		ORDER BY end_date;
	QUIT;

	DATA tt_info;
		SET tt_info;
		IF missing(t_weight) OR round(t_weight,0.0001) = 0 THEN DO;
			add_bm_weight = 1;  /* ��λȫΪָ�� */
			multiplier = 0;
		END;
		ELSE IF abs(t_weight-&all_max_weight.)>=0.0001 AND round(t_weight-&all_max_weight.,0.0001)>=0 THEN DO;  /* �����趨��Ȩ�� */
			add_bm_weight = 1-&all_max_weight.;
			multiplier = round(&all_max_weight./t_weight,0.001);  /* ��λ�ȱ������� */
		END;
		ELSE DO;
			add_bm_weight = 1-t_weight;
			multiplier = 1;
		END;
		/* �������� */
		IF abs(add_bm_weight)<=0.001 THEN DO;
			add_bm_weight = 0;
			multiplier = 1;
		END;
	RUN;

	/* Step3: ���µ�������Ȩ�� */
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.multiplier
		FROM tt_ind_pool A LEFT JOIN tt_info B
		ON A.end_date = B.end_date;
	QUIT;

	DATA tt_ind_pool;
		SET tmp;
		weight = weight * multiplier;
	RUN;

	/* Step4: ����ָ���Ĺ�Ʊ�أ�������: type+stock_code*/
	DATA tt_info;
		SET tt_info(keep = end_date add_bm_weight rename = (add_bm_weight = weight));
		stock_code = "&index_code.";
		&mark. = 1; 
		IF weight > 0;
	RUN;

	DATA &output_stock_pool.;
		SET tt_ind_pool tt_info;
/*		IF weight > 0;*/
	RUN;
	
	PROC SORT DATA = &output_stock_pool.;
		BY end_date descending &mark. stock_code;
	RUN;

	PROC SQL;
		DROP TABLE tmp,tt_info, tt_ind_pool;
	QUIT;

%MEND fill_in_index;


/** ģ��6: ��ָ���滻Ϊ��Ӧ�ɷֹ� (ԭʼ������ "��Ϲ���_��Ч����_2.0.sas"�С���ʱ���ø�ģ��)**/

