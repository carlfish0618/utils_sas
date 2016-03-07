/*** ����������Ч�� **/

/**** �����б�:
	(1) cal_intval_return: ���㵥������ۼ���������
	(2) single_factor_ic: ���㵥���ӵ�IC��ʤ��
	(3) loop_factor_ic: ���������ӵ�IC��ʤ��
	(4) single_factor_score: �������ӵ÷֣����򲢴��
	(5) single_score_ret�� ���ݷ���ͳ������
	(6) test_single_factor_ic: ��single_factor_ic����������cover��ͬʱ������spearman_ic����Ĭ�����
	(7) test_multiple_factor_ic: ��loog_factor_ic���ƣ����˴�����test_single_factor_ic 
	****/ 


/*** ģ��1: ���㵥������ۼ���������**/
/** Ҫ��start_intval <= end_intval */
/** ����:
(1) raw_table(�������б�): ����end_date, &group_name, &price_name������
(2) group_name(character): ������stock_code/indus_code�ȣ��κα�ʶ���������
(3) price_name: ���̵�λ
(4) is_single: ��������ۼ�����(1-������ / 0-�ۼ�����)
(5) date_table: end_date/date_i(f/b)����������������У� --> ����get_date_windows�����н��
**/

/**�������
(1) output_table: end_date/group_name/ret_(f/b)_i
���⣺���end_dateδ������date_table�У���ƥ����ȱʧ��
**/

/** ע��Ҫ������date_table����Ҫ����date_f0!!! ��get_date_windows���趨��start_win <=0����end_win>=0 **/

%MACRO cal_intval_return(raw_table, group_name, price_name, date_table, output_table, is_single = 1);

	/** Step1: ȡ����ĵ�λ���� */
	PROC SQL;
		CREATE TABLE indus_close_sub AS
		SELECT end_date, &group_name., &price_name.
		FROM &raw_table.
		WHERE end_date IN
		(SELECT end_date FROM &date_table.)
		ORDER BY &group_name., end_date;
	QUIT;

	/** Step2: ��ȡdate_table������ */
	PROC CONTENTS DATA = &date_table. OUT = var_list(keep = name) NOPRINT;
	RUN;
	DATA var_list;
		SET var_list;
		IF upcase(name) NOT IN ("END_DATE");
	RUN;
	PROC SQL NOPRINT;
		SELECT name, count(1) 
		INTO :name_list SEPARATED BY ' ',
			 :nvars
		FROM var_list
		ORDER BY name;
	QUIT;
	
	/* ѭ�������ۼ������� */
	%IF %SYSEVALF(&is_single. = 0) %THEN %DO;
		%DO index = 1 %TO &nvars.;
			%LET var_i = %scan(&name_list., &index., ' ');
			%LET var_i = %substr(&var_i,%length(date_)+1,%length(&var_i)-%length(date_));
			%IF %sysevalf(&var_i. ~= f0)  %THEN %DO;
			PROC SQL;
				CREATE TABLE tt_accum AS
				SELECT A.end_date, A.&group_name., A.&price_name., C.&price_name. AS &price_name._&var_i. LABEL "&price_name._&var_i."
				FROM indus_close_sub A LEFT JOIN &date_table. B
				ON A.end_date = B.end_date
				LEFT JOIN 
				(SELECT end_date, &group_name., &price_name. FROM indus_close_sub) C
				ON A.&group_name. = C.&group_name. AND B.date_&var_i. = C.end_date
				ORDER BY A.&group_name., A.end_date;
			QUIT;
		
			/* �����ۼ������� */
			DATA tt_accum(drop = &price_name._&var_i.);
				SET tt_accum;
				IF not missing(&price_name.) AND not missing(&price_name._&var_i.) THEN DO; /* ��ͬ��˳�� */
					IF substr("&var_i.",1,1) = 'f' AND &price_name. ~= 0 THEN DO;
						ret_&var_i. = (&price_name._&var_i.-&price_name.)/&price_name. * 100; 
					END;
					ELSE IF substr("&var_i.",1,1) = 'f' THEN ret_&var_i. = .;
					ELSE IF &price_name._&var_i. ~= 0 THEN ret_&var_i. = (&price_name.-&price_name._&var_i.)/&price_name._&var_i. * 100;
					ELSE ret_&var_i. = .;
				END;
				ELSE ret_&var_i. = .;
			RUN;
			PROC SQL;
				CREATE TABLE tmp AS
				SELECT A.*, B.ret_&var_i.
				FROM indus_close_sub A LEFT JOIN tt_accum B
				ON A.&group_name. = B.&group_name. AND A.end_date = B.end_date;
			QUIT;
			DATA indus_close_sub;
				SET tmp;
			RUN;
			PROC SQL;
				DROP TABLE tt_accum, tmp;
			QUIT;
		%END;
		%END;
	%END;
	
	%IF %SYSEVALF(&is_single. = 1) %THEN %DO;
		DATA tt_accum;
			SET indus_close_sub;
		RUN;
		%DO index = 1 %TO &nvars.;
			%LET var_i = %scan(&name_list., &index., ' ');
			%LET var_i = %substr(&var_i., %length(date_)+1,%length(&var_i.)-%length(date_));
			PROC SQL;
				CREATE TABLE tmp AS
				SELECT A.*, C.&price_name. AS &price_name._&var_i. LABEL "&price_name._&var_i."
				FROM tt_accum A LEFT JOIN &date_table. B
				ON A.end_date = B.end_date
				LEFT JOIN 
				(SELECT end_date, &group_name., &price_name. FROM indus_close_sub) C
				ON A.&group_name. = C.&group_name. AND B.date_&var_i. = C.end_date
				ORDER BY A.&group_name., A.end_date;
			QUIT;
			DATA tt_accum;
				SET tmp;
			RUN;
		%END;

		/* ���㵥�������� */
		%DO index = 1 %TO &nvars.;
			%LET var_i = %scan(&name_list., &index., ' ');
			%LET var_i = %substr(&var_i., %length(date_)+1,%length(&var_i.)-%length(date_));

			%IF &var_i. = b1 %THEN %DO;
				%put 1!!;
				%LET single_name = ret_b1;
				%put &single_name.;
				DATA tt_accum;
					SET tt_accum;
					IF &price_name._b1 ~=0 AND not missing(&price_name._b1) AND not missing(&price_name._f0) THEN DO;
						&single_name. = (&price_name._f0-&price_name._b1)/&price_name._b1 * 100;
					END;
					ELSE &single_name. = .;
				RUN;
			%END;
			%ELSE %IF %substr(&var_i.,1,1) = b %THEN %DO; 
				%put 2!!;
				%LET single_name =ret_&var_i; 
			    %put &single_name.;

				%LET number = %substr(&var_i.,2,%length(&var_i.)-1);
				%LET number_b = %sysevalf(&number.-1);
				DATA tt_accum;
					SET tt_accum;
					IF &price_name._&var_i.~=0 AND not missing(&price_name._&var_i.) AND not missing(&price_name._b&number_b.) THEN DO;
						&single_name. = (&price_name._b&number_b.-&price_name._&var_i.)/&price_name._&var_i. * 100;
					END;
					ELSE &single_name. = .;
				RUN;
			%END;
			%ELSE %IF %substr(&var_i.,1,1) = f AND &var_i. ~= f0 %THEN %DO;
				%put 3!!;
				%LET single_name =ret_&var_i; 
				%put &single_name.;
				%LET number = %substr(&var_i.,2,%length(&var_i.)-1);
				%LET number_b = %sysevalf(&number.-1);
				DATA tt_accum;
					SET tt_accum;
					IF &price_name._f&number_b.~=0 AND not missing(&price_name._&var_i.) AND not missing(&price_name._f&number_b.) THEN DO; 
						&single_name. = (&price_name._&var_i.-&price_name._f&number_b.)/&price_name._f&number_b. * 100; 
					END;
					ELSE &single_name. = .;
				RUN;
			%END;
			%IF &var_i. ~= f0 %THEN %DO; /** f0ʱ��Ӧ�����ѭ�� */
				PROC SQL;
					CREATE TABLE tmp AS
					SELECT A.*, B.&single_name.
					FROM indus_close_sub A LEFT JOIN tt_accum B
					ON A.&group_name. = B.&group_name. AND A.end_date = B.end_date;
				QUIT;
				DATA indus_close_sub;
					SET tmp;
				RUN;
			%END;
		%END;
		PROC SQL;
			DROP TABLE tt_accum, tmp;
		QUIT;

	%END;
	DATA &output_table.;
		SET indus_close_sub;
	RUN;
	PROC SQL;
		DROP TABLE indus_close_sub, var_list;
	QUIT;

%MEND;



/*** ģ��2: ���㵥���ӵ�IC��ʤ��**/
/** ����:
(1) factor_table(�������б�): ����end_date, group_name, factor_name
(2) return_table(�����ʱ�): ����end_date, group_name���������� ---> cal_intval_return()�Ľ��
(3) group_name(character): ������stock_code/indus_code�ȣ��κα�ʶ���������
(4) fname: ��������
(5) type������������ݣ�1- ȫ����� 2- p_ic 3- s_ic 4- n_obs
**/

/**�������
(1) output_table: end_date/n_obs_f1/p_ic_f1/s_ic_f1 (�����б���factor_table�е�end_dateΪ׼)
**/

%MACRO single_factor_ic(factor_table, return_table, group_name, fname, output_table, type=1);
	DATA &fname._t_raw;
		SET &factor_table.(keep = end_date &group_name. &fname.);
		IF not missing(&fname.);
	RUN;
	/** �޸ģ�2016-1-15��Ҫ��end_dateҪ��return_table��end_date�С������Ͳ���Ҫ��factor_table��return_tableƵ����Ҫһ��*/
	PROC SQL;
		CREATE TABLE &output_table. AS
		SELECT distinct end_date 
		FROM &fname._t_raw
		WHERE end_date IN (SELECT end_date FROM &return_table.)
		ORDER BY end_date;
	QUIT;

	/* ѭ�����δ��N���µ��ۼƻ��������� */
	PROC CONTENTS DATA = &return_table. OUT = var_list(keep = name) NOPRINT;
	RUN;
	DATA var_list;
		SET var_list;
		IF index(name, "ret") = 1;
	RUN;
	PROC SQL NOPRINT;
		SELECT name, count(1) 
		INTO :name_list SEPARATED BY ' ',
			 :nvars
		FROM var_list;
	QUIT;
	
	%DO index = 1 %TO &nvars.;
		%LET var_i = %scan(&name_list., &index., ' ');
		%LET var_i = %substr(&var_i.,%length(ret_)+1,%length(&var_i.)-%length(ret_));
		PROC SQL;
			CREATE TABLE tmp AS
			SELECT A.*, B.ret_&var_i. AS &var_i.
			FROM &fname._t_raw A JOIN &return_table. B  /** ȡ������ֻ������Щͬʱ���������ӱ��������еļ�¼��������������ȱʧ */
			ON A.&group_name. = B.&group_name. AND A.end_date = B.end_date
			ORDER BY A.end_date, A.&fname.;
		QUIT;

		/* �������ϵ����ȥ��ȱʧֵ */
		DATA &fname._t;
			SET tmp;
			IF not missing(&var_i.) AND not missing(&fname.);
		RUN;
		PROC CORR DATA = &fname._t pearson spearman OUTP = corr_p OUTS = corr_s NOPRINT;
			BY end_date;
			VAR &fname. &var_i.;
		RUN;
		/** pearson */
		DATA corr_p(keep = end_date _TYPE_ &var_i.);
			SET corr_p;
			IF upcase(_NAME_) = upcase("&fname.") OR _TYPE_ = "N";
		RUN;
		PROC TRANSPOSE DATA = corr_p OUT = corr_p(keep = end_date corr N rename = (N = nobs_&var_i. corr = p_ic_&var_i.));
			BY end_date;
			VAR &var_i.;
			ID  _TYPE_;
		RUN;
		/* spearman */
		DATA corr_s(keep = end_date _TYPE_ &var_i.);
			SET corr_s;
			IF upcase(_NAME_) = upcase("&fname.") OR _TYPE_ = "N";
		RUN;
		PROC TRANSPOSE DATA = corr_s OUT = corr_s(keep = end_date corr N rename = (N = nobs_&var_i. corr = s_ic_&var_i.));
			BY end_date;
			VAR &var_i.;
			ID  _TYPE_;
		RUN;

		/* ���л��� */
		%IF %SYSEVALF(&type. = 1) %THEN %DO;
			PROC SQL;
				CREATE TABLE tmp AS
				SELECT A.*, B.nobs_&var_i., B.p_ic_&var_i., C.s_ic_&var_i.
				FROM  &output_table. A 
				LEFT JOIN corr_p B
				ON A.end_date = B.end_date
				LEFT JOIN corr_s C
				ON A.end_date = C.end_date
				ORDER BY A.end_date;
			QUIT;
			DATA &output_table.;
				SET tmp;
			RUN;
		%END;
		%IF %SYSEVALF(&type. = 2) %THEN %DO;
			PROC SQL;
				CREATE TABLE tmp AS
				SELECT A.*,  B.p_ic_&var_i.
				FROM  &output_table. A 
				LEFT JOIN corr_p B
				ON A.end_date = B.end_date
				ORDER BY A.end_date;
			QUIT;
			DATA &output_table.;
				SET tmp;
			RUN;
		%END;
		%IF %SYSEVALF(&type. = 3) %THEN %DO;
			PROC SQL;
				CREATE TABLE tmp AS
				SELECT A.*, C.s_ic_&var_i.
				FROM  &output_table. A 
				LEFT JOIN corr_s C
				ON A.end_date = C.end_date
				ORDER BY A.end_date;
			QUIT;
			DATA &output_table.;
				SET tmp;
			RUN;
		%END;
		%IF %SYSEVALF(&type. = 4) %THEN %DO;
			PROC SQL;
				CREATE TABLE tmp AS
				SELECT A.*, B.nobs_&var_i.
				FROM  &output_table. A 
				LEFT JOIN corr_p B
				ON A.end_date = B.end_date
				ORDER BY A.end_date;
			QUIT;
			DATA &output_table.;
				SET tmp;
			RUN;
		%END;
	%END;
	PROC SQL;
		DROP TABLE tmp, corr_p, corr_s, &fname._t, var_list,&fname._t_raw;
	QUIT;
%MEND single_factor_ic;


/*** ģ��3: ���������ӵ�IC��ʤ��**/
/** ����ã�single_factor_ic() ���� */

/** ����:
(1) factor_table(�������б�): ����end_date, group_name, factor_name
(2) return_table(�����ʱ�): ����end_date, group_name���������� ---> cal_intval_return()�Ľ��
(3) group_name(character): ������stock_code/indus_code�ȣ��κα�ʶ���������
(4) fname: ��������
(5) type: ����������ݣ�1- ȫ����� 2- p_ic 3- s_ic 4- n_obs
(6) exclude_list: Ҫ���д�����޳������Ե���
**/

/**�����(ÿ������)��
(1) &fname._ic: end_date/n_obs_f1/p_ic_f1/s_ic_f1
**/


%MACRO loop_factor_ic(factor_table, return_table, group_name, type=1, exclude_list=(''));
	/** ���������б� */
	PROC CONTENTS DATA = &factor_table. OUT = factor_list(keep = name) NOPRINT;
	RUN;
	%LET group_name = %upcase(&group_name.);
	DATA factor_list;
		SET factor_list;
		IF upcase(name) not IN ("END_DATE", "&group_name." );  /* ע���Сд */
		IF upcase(name) NOT IN &exclude_list.;
	RUN;
	PROC SQL NOPRINT;   /** �໥���õĹ�ϵ����Ҫ��single_factor_ic�еĺ����ظ� */
		SELECT name, count(1) 
		INTO :factor_list SEPARATED BY ' ',
			 :nfactors
		FROM factor_list;
	QUIT;
			
	%DO i = 1 %TO &nfactors.;
		%LET fname =  %scan(&factor_list.,&i., ' ');
		%single_factor_ic(&factor_table., &return_table., &group_name., &fname.,&fname._ic, type=&type.);
	%END;
	PROC SQL;
		DROP TABLE factor_list;
	QUIT;
		
%MEND;


/*** ģ��4���������ӵ÷֣����򲢴�� */
/** ����:
(1) raw_table(�������б�): ����end_date, &identity, &factor_name������
(2) identity(character): ������stock_code/indus_code�ȣ��κα�ʶ���������
(3) factor_name: ����
(4) is_increase: 1-���� 0-����
(5) group_num: ����ĸ���

**/

/**�������
(1) output_table: ԭ�е�raw_table�м�һ�� &factor_name._score
**/

%MACRO single_factor_score(raw_table, identity, factor_name,output_table, is_increase = 1, group_num = 5);
	
	/** ͳ��ÿ��������� */
	PROC SQL;
		CREATE TABLE tt_rank AS
		SELECT end_date, count(1) AS nobs
		FROM &raw_table.
		WHERE not missing(&factor_name.)   /** ��û������ֵ���޳� */
		GROUP BY end_date;
	QUIT;
	DATA tt_rank;
		SET tt_rank;
		part = floor(nobs/&group_num.);
		res = nobs - part * &group_num.;
	RUN;
	
	%IF %SYSEVALF(&is_increase.=1) %THEN %DO;
		PROC SQL;
		CREATE TABLE tt_raw_table AS
			SELECT A.end_date, A.&identity., A.&factor_name., B.part, B.res
			FROM &raw_table. A LEFT JOIN tt_rank B
		 	ON A.end_date = B.end_date
			WHERE not missing(&factor_name.)
			ORDER BY A.end_date, A.&factor_name.;
		QUIT;
	%END;
	%ELSE %DO;
		PROC SQL;
		CREATE TABLE tt_raw_table AS
			SELECT A.end_date, A.&identity., A.&factor_name., B.part, B.res
			FROM &raw_table. A LEFT JOIN tt_rank B
		 	ON A.end_date = B.end_date
			WHERE not missing(&factor_name.)
			ORDER BY A.end_date, A.&factor_name. desc;
		QUIT;
	%END;
			
	DATA tt_raw_table;
		SET tt_raw_table;
		BY end_date;
		RETAIN group 1;
		RETAIN within_rank 1;
		IF first.end_date THEN DO;
			group = 1;
			within_rank = 1;
		END;
		rank = within_rank;
		score = group;
		/** �ж����������� */
		IF score <= res THEN DO;  /* ��ǰN�� */
			IF rank + 1 <= part + 1 THEN DO;
				within_rank + 1;
			END;
			ELSE DO; /* �µ�һ�� */
				group + 1;  
				within_rank = 1;
			END;
		END;
		ELSE DO;
			IF rank + 1 <= part THEN DO;
				within_rank + 1;
			END;
			ELSE DO;
				group + 1;
				within_rank = 1;
			END;
		END;
	RUN;
	
	%IF %SYSEVALF(&is_increase.=1) %THEN %DO;
		PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.score AS &factor_name._score
		FROM &raw_table. A LEFT JOIN tt_raw_table B
		ON A.end_date = B.end_date AND A.&identity. = B.&identity.
		ORDER BY A.end_date, A.&factor_name.;
	QUIT;
	%END;
	%ELSE %DO;
		PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.score AS &factor_name._score
		FROM &raw_table. A LEFT JOIN tt_raw_table B
		ON A.end_date = B.end_date AND A.&identity. = B.&identity.
		ORDER BY A.end_date, A.&factor_name. desc;
	QUIT;
	%END;
			

	DATA &output_table.; /** ��������ֵȱʧ�Ĺ�Ʊ����û�ж�Ӧ���ӵĵ÷� */
		SET tmp;
	RUN;
	PROC SQL;
		DROP TABLE tmp, tt_rank, tt_raw_table;
	QUIT;
%MEND single_factor_score;


/*** ģ��5: ����ͳ��������**/
/** ����:
(1) score_table(�������б�): ����end_date, identity, score_name
(2) return_table(�����ʱ�): ����end_date, identity���������� ---> cal_intval_return()�Ľ��
(3) identity(character): ������stock_code/indus_code�ȣ��κα�ʶ���������
(4) score_name: ����ĵ÷�����
(5) type������������ݣ�1- ȫ����� 2- ret only 3- n_obs only 4-std only
(6) ret_column: ȱʧ(.)-������е�return table�е�ret�С�Ҳ����ָ��Ϊĳһ�С�
(7) is_transpose: �Ƿ��շ��飬���Ž������ֻ�е�ret_columnΪ�ض�һ�е�ʱ�򣬸ò�������Ч)
	
**/

/**�������
(1) &score_name._ret: end_date/n_obs_f1/ret_f1(�����б���score_table�е�end_dateΪ׼)
**/

%MACRO single_score_ret(score_table, return_table, identity, score_name, ret_column =., is_transpose = 0, type=2);
	DATA &score_name._t;
		SET &score_table.(keep = end_date &identity. &score_name.);
		IF not missing(&score_name.);  /* û�����ӵ÷ֵģ�������֮����������ʵļ��� */
	RUN;


	/* ѭ�����δ��N���µ��ۼƻ��������� */
	PROC CONTENTS DATA = &return_table. OUT = var_list(keep = name) NOPRINT;
	RUN;
	%IF %SYSEVALF(&ret_column. = .) %THEN %DO;
		DATA var_list;
			SET var_list;
			IF index(name, "ret") = 1;
		RUN;
	%END;
	%ELSE %DO;
		DATA var_list;
			SET var_list;
			IF upcase(name) = upcase("&ret_column.");
		RUN;
	%END;
		
	PROC SQL NOPRINT;
		SELECT name, count(1) 
		INTO :name_list SEPARATED BY ' ',
			 :nvars
		FROM var_list;
	QUIT;

	
	%DO index = 1 %TO &nvars.;
		%LET var_i = %scan(&name_list., &index., ' ');
		%LET var_i = %substr(&var_i.,%length(ret_)+1,%length(&var_i.)-%length(ret_));
		PROC SQL;
			CREATE TABLE tt_result AS
			SELECT A.end_date, A.&score_name., mean(B.ret_&var_i.) AS &var_i., 
				std(B.ret_&var_i.) AS std_&var_i., 
				count(1) AS nobs_&var_i.
			FROM &score_name._t A JOIN &return_table. B  /** ȡ������ֻ������Щͬʱ�����ڵ÷ֱ��������еļ�¼��������������ȱʧ */
			ON A.&identity. = B.&identity. AND A.end_date = B.end_date
			GROUP BY A.end_date, A.&score_name.
			ORDER BY A.end_date, A.&score_name.;
		QUIT;
		%IF %SYSEVALF(&index. = 1) %THEN %DO;
				PROC SQL;
					CREATE TABLE &score_name._stat AS
					SELECT distinct end_date, &score_name.
					FROM &score_name._t
					ORDER BY end_date, &score_name.;
				QUIT;
		%END;

		/* ���л��� */
		%IF %SYSEVALF(&type. = 1) %THEN %DO;
			PROC SQL;
				CREATE TABLE tmp AS
				SELECT A.*, B.nobs_&var_i., B.&var_i., B.std_&var_i.
				FROM  &score_name._stat A
				LEFT JOIN tt_result B
				ON A.end_date = B.end_date AND A.&score_name. = B.&score_name.
				ORDER BY A.end_date;
			QUIT;
			DATA &score_name._stat;
				SET tmp;
			RUN;
		%END;
		%IF %SYSEVALF(&type. = 2) %THEN %DO;
			PROC SQL;
				CREATE TABLE tmp AS
				SELECT A.*, B.&var_i.
				FROM  &score_name._stat A
				LEFT JOIN tt_result B
				ON A.end_date = B.end_date AND A.&score_name. = B.&score_name.
				ORDER BY A.end_date;
			QUIT;
			DATA &score_name._stat;
				SET tmp;
			RUN;
		%END;
		%IF %SYSEVALF(&type. = 3) %THEN %DO;
			PROC SQL;
				CREATE TABLE tmp AS
				SELECT A.*, B.nobs_&var_i.
				FROM  &score_name._stat A
				LEFT JOIN tt_result B
				ON A.end_date = B.end_date AND A.&score_name. = B.&score_name.
				ORDER BY A.end_date;
			QUIT;
			DATA &score_name._stat;
				SET tmp;
			RUN;
		%END;
		%IF %SYSEVALF(&type. = 4) %THEN %DO;
			PROC SQL;
				CREATE TABLE tmp AS
				SELECT A.*, B.std_&var_i.
				FROM  &score_name._stat A
				LEFT JOIN tt_result B
				ON A.end_date = B.end_date AND A.&score_name. = B.&score_name.
				ORDER BY A.end_date;
			QUIT;
			DATA &score_name._stat;
				SET tmp;
			RUN;
		%END;
	%END;
	%IF %SYSEVALF(&ret_column. ~= . AND &is_transpose. = 1) %THEN %DO;
		PROC TRANSPOSE DATA = &score_name._stat prefix = g OUT =  &score_name._stat;
			BY end_date;
			ID &score_name.;
		RUN;
	%END;

	PROC SQL;
		DROP TABLE tmp,&score_name._t, tt_result, var_list;
	QUIT;
%MEND single_score_ret;


/*** ģ��6������single_factor_ic����spearman_ic����ͬʱ����cover */
/** Ĭ�������
(1) &fname._cover
(2) &fname._ic
��3�� &fname._dist
**/



%MACRO test_single_factor_ic(factor_table, return_table, group_name, fname);
	/** 1- ���Ӹ��Ƕ� */
	DATA tt_test_pool(keep = end_date stock_code &fname.);
		SET &factor_table.;
	RUN;
	PROC SQL;
		CREATE TABLE &fname._cover AS
		SELECT end_date, sum(not missing(&fname.))/count(1) AS pct
		FROM tt_test_pool
		GROUP BY end_date;
	QUIT;

	/** Step2: ����IC **/
	%single_factor_ic(factor_table=tt_test_pool, return_table=&return_table., group_name=&group_name., 
			fname=&fname., output_table=&fname._ic, type=3);

	/** Step3: ���ӷֲ���� */
	%cal_dist(input_table=tt_test_pool, by_var=end_date, cal_var=&fname., out_table=stat);
	PROC SQL;
		CREATE TABLE &fname._dist AS
		SELECT sum(obs) AS nobs,
		mean(mean) AS mean,
		mean(std) AS std,
		mean(p100) AS p100,
		mean(p90) AS p90,
		mean(p75) AS p75,
		mean(p50) AS p50,
		mean(p25) AS p25,
		mean(p10) AS p10,
		mean(p0) AS p0
	FROM stat;
	QUIT;

	PROC SQL;
		DROP TABLE tt_test_pool, stat;
	QUIT;
%MEND test_single_factor_ic;

/*** ģ��7������test_factor_ic������single_factor_ic�����������*/
%MACRO 	test_multiple_factor_ic(factor_table, return_table, group_name, exclude_list=(''));
	PROC CONTENTS DATA = &factor_table. OUT = tt_varlist2(keep = name) NOPRINT;
	RUN;
	DATA tt_varlist2;
		SET tt_varlist2;
		IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE");
		IF upcase(name) NOT IN &exclude_list.;
	RUN;
	
	PROC SQL NOPRINT;
		SELECT name, count(1)
		 INTO :name_list2 SEPARATED BY ' ',
               :nfactors2
          FROM tt_varlist2;
     QUIT;
           
     %DO i = 1 %TO &nfactors2.;
          %LET fname =  %scan(&name_list2.,&i., ' ');
		  %test_single_factor_ic(factor_table=&factor_table., return_table=&return_table., 
								group_name=&group_name., fname=&fname.);
     %END;
	PROC SQL;
		DROP TABLE tt_varlist2;
	QUIT;
%MEND test_multiple_factor_ic;

