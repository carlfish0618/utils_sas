/*** ����������Ч�� **/

/**** �����б�:
	(1) cal_intval_return: ���㵥������ۼ���������
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
					IF substr("&var_i.",1,1) = 'f' THEN ret_&var_i. = (&price_name._&var_i.-&price_name.)/&price_name. * 100; 
					ELSE ret_&var_i. = (&price_name.-&price_name._&var_i.)/&price_name._&var_i. * 100;
				END;
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
			%LET var_i = %substr(&var_i,%length(date_)+1,%length(&var_i)-%length(date_));
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
		%DO index = 2 %TO &nvars.;  /* �ӵڶ�����ʼ */
			%LET var_i = %scan(&name_list., &index., ' ');
			%LET var_i_b = %scan(&name_list., &index.-1, ' ');
			%LET var_i = %substr(&var_i,%length(date_)+1,%length(&var_i.)-%length(date_));
			%LET var_i_b = %substr(&var_i_b,%length(date_)+1,%length(&var_i_b.)-%length(date_));
/*			%put &var_i.;*/
/*			%put &var_i_b.;*/
			%IF %substr(&var_i.,1,1) = f AND %substr(&var_i_b.,1,1) = f %THEN %DO;
				%LET single_name =ret_&var_i; 
				DATA tt_accum;
					SET tt_accum;
					ret_&var_i = (&price_name._&var_i.-&price_name._&var_i_b.)/&price_name._&var_i_b. * 100; 
				RUN;
			%END;
			%ELSE %IF %substr(&var_i.,1,1) = b AND %substr(&var_i_b.,1,1) = b %THEN %DO;  /** ��Ӧ���磺date_b2,date_b1 */
				%LET single_name =ret_&var_i; 
				DATA tt_accum;
					SET tt_accum;
					ret_&var_i. = (&price_name._&var_i_b.-&price_name._&var_i.)/&price_name._&var_i. * 100;
				RUN;
			%END;
			%ELSE %IF %substr(&var_i.,1,1) = f AND %substr(&var_i_b.,1,1) = b %THEN %DO;/** ��Ӧ����: date_f0, date_b(*) */
				%LET single_name = ret_b1; 
				DATA tt_accum;
					SET tt_accum;
					ret_b1 = (&price_name._f0-&price_name._b1)/&price_name._b1 * 100;
				RUN;
			%END;
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
(4) fname: ���̵�λ
**/

/**�������
(1) &fname._stat: end_date/n_obs_f1/p_ic_f1/s_ic_f1 (�����б���factor_table�е�end_dateΪ׼)
**/

%MACRO single_factor_ic(factor_table, return_table, group_name, fname, output_table);
	DATA &fname._t;
		SET &factor_table.(keep = end_date &group_name. &fname.);
		IF not missing(&fname.);
	RUN;
	PROC SQL;
		CREATE TABLE &fname._stat AS
		SELECT distinct end_date 
		FROM &fname._t
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
			FROM &fname._t A JOIN &return_table. B  /** ȡ������ֻ������Щͬʱ�����ڵ÷ֱ�����ӱ��еļ�¼��������������ȱʧ */
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
		PROC SQL;
			CREATE TABLE tmp AS
			SELECT A.*, B.nobs_&var_i., B.p_ic_&var_i., C.s_ic_&var_i.
			FROM  &fname._stat A 
			LEFT JOIN corr_p B
			ON A.end_date = B.end_date
			LEFT JOIN corr_s C
			ON A.end_date = C.end_date
			ORDER BY A.end_date;
		QUIT;
		DATA &fname._stat;
			SET tmp;
		RUN;
	%END;

	PROC SQL;
		DROP TABLE tmp, corr_p, corr_s, &fname._t, var_list;
	QUIT;
%MEND single_factor_ic;


/*** ģ��3: ���������ӵ�IC��ʤ��**/
/** ����ã�single_factor_ic() ���� */

/** ����:
(1) factor_table(�������б�): ����end_date, group_name, factor_name
(2) return_table(�����ʱ�): ����end_date, group_name���������� ---> cal_intval_return()�Ľ��
(3) group_name(character): ������stock_code/indus_code�ȣ��κα�ʶ���������
(4) fname: ���̵�λ
**/

/**�����(ÿ������)��
(1) &fname._stat: end_date/n_obs_f1/p_ic_f1/s_ic_f1
**/


%MACRO loop_factor_ic(factor_table, return_table, group_name);
	/** ���������б� */
	PROC CONTENTS DATA = &factor_table. OUT = factor_list(keep = name) NOPRINT;
	RUN;
	%LET group_name = %upcase(&group_name.);
	DATA factor_list;
		SET factor_list;
		IF upcase(name) not IN ("END_DATE", "&group_name." );  /* ע���Сд */
	RUN;
	PROC SQL NOPRINT;   /** �໥���õĹ�ϵ����Ҫ��single_factor_ic�еĺ����ظ� */
		SELECT name, count(1) 
		INTO :factor_list SEPARATED BY ' ',
			 :nfactors
		FROM factor_list;
	QUIT;
			
	%DO i = 1 %TO &nfactors.;
		%LET fname =  %scan(&factor_list.,&i., ' ');
		%single_factor_ic(&factor_table., &return_table., &group_name., &fname.);
	%END;
	PROC SQL;
		DROP TABLE factor_list;
	QUIT;
		
%MEND;

