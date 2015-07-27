/*** ���Ӽ��� **/

/**** �����б�:
	(1) cal_percentitle: ����ĳһ�е÷ֵķ�λ��
	(2) normalize_single_score: ��׼�������ӵ÷�
	(3) normalize_multi_score: ��׼��������ӵ÷�
	(4) winsorize_single_score: winsorize�����ӵ÷�
	(5) winsorize_multi_score: winsorize������ӵ÷�
	(6) weighted_single_factor: ���㵥�����Ӽ�Ȩ�÷�
	(7) weighted_multi_factor: ���������Ӽ�Ȩ�÷�
	(8) neutralize_single_score�����Ի����ӵ÷�
	(9) neutralize_multi_score: ���Ի�������ӵ÷�
	(10) cut_subset: �������ӵ÷֣�ȡĳ����λ������ֵ�µķ���
	****/ 

/** ע�⣺���Ӽ���һ�������ಽ��
(1) normalize --��winsorize
(2) normalize --> neutralize --> winsorize (���ڽ�����ҵ���Ի�ʱ���Ȳ��ö�normaliz�Ľ������winsorize������������� 
���⣬normalize��neutralizeһ�㶼ѡ��fmv_sqr��ΪȨ�ؼ���sample average����ǰ�߼���z-score,�ұ�׼����"��Ȩ"��׼�����normalize֮��ĺͲ�����Ϊ0)��
���߼������deviation(������ҵ��ֵ��ƫ��)��������z-score�����Ƶģ���Ϊ��ֵ���㷽����ԭ����ߵĺ�Ҳ����Ϊ0��
*****************/


/*** ģ��1: ����ĳһ�е÷ֵķ�λ�� **/
/** ����:
(1) input_table: end_date / stock_code / &colname.
(2) colname(character): ����
(3) pct: ��Ҫ����İٷ�λ��(��: 50/90��)

/**�������
(1) output_table: end_date, pct&pct.
**/

/** ˵��: ����˵����: http://en.wikipedia.org/wiki/Percentile ��Excel�ķ��� **/

%MACRO cal_percentitle(input_table, colname, pct, output_table);
	PROC SORT DATA = &input_table.;
		BY end_date &colname.;
	RUN;
	/** Step1: ����ÿ��ǿյ������� */
	PROC SQL;
		CREATE TABLE tt_intval AS
		SELECT end_date, count(1) AS nobs
		FROM &input_table.
		WHERE not missing(&colname.)
		GROUP BY end_date;
	QUIT;
	DATA tt_intval;
		SET tt_intval;
		nn = &pct./100 * (nobs-1)+1;
		nk = floor(nn);
		nd = nn - nk;
	RUN;
	/** Step2: ��ԭʼ�������� */
	DATA tt_nomissing;
		SET &input_table.(keep = end_date stock_code &colname.);
		IF not missing(&colname.);
	RUN;
	PROC SORT DATA = tt_nomissing;
		BY end_Date &colname.;
	RUN;
	DATA tt_nomissing;
		SET tt_nomissing;
		BY end_Date;
		RETAIN rank 0;
		IF first.end_date THEN rank = 0;
		rank + 1;
	RUN;
	PROC SQL;
		CREATE TABLE tt_intval2 AS
		SELECT A.*, B.&colname. AS v_1, C.&colname. AS v_n, D.&colname. AS v_k, E.&colname. AS v_k1
		FROM tt_intval A LEFT JOIN tt_nomissing B
		ON A.end_date = B.end_date AND B.rank = 1
		LEFT JOIN tt_nomissing C
		ON A.end_Date = C.end_date AND C.rank = A.nobs
		LEFT JOIN tt_nomissing D
		ON A.end_Date = D.end_Date AND D.rank = A.nk
		LEFT JOIN tt_nomissing E
		ON A.end_date = E.end_date AND E.rank = A.nk+1;
	QUIT;
	DATA tt_intval(keep = end_date pct&pct.);
		SET tt_intval2;
		IF nk = 0 THEN pct&pct. = v_1;
		ELSE IF nk = nobs THEN pct&pct. = v_n;
		ELSE pct&pct. = v_k + nd*(v_k1-v_k);
	RUN;
	DATA &output_table.;
		SET tt_intval;
	RUN;
	PROC SQL;
		DROP TABLE tt_intval, tt_intval2, tt_nomissing;
	QUIT;

%MEND cal_percentitle;


/*** ģ��2: ��׼�������ӵ÷� **/
/** ����:
(1) input_table: end_date / stock_code / &colname./fmv_sqr (��ͨ��ֵ��ƽ����)
(2) colname(character): ������
(3) is_replace: �Ƿ�ԭʼ�����滻Ϊ��׼����Ľ����1-�滻 0- ��������:&colname._mdf

/**�������
(1) output_table: input_table + &colname._mdf(is_replace=0)
**/

%MACRO normalize_single_score(input_table, colname, output_table, is_replace = 1);
	%cal_percentitle(&input_table., &colname., 1, pct1);
	%cal_percentitle(&input_table., &colname., 99, pct99);
	PROC SQL;
		CREATE TABLE tt_raw AS
		SELECT A.end_date, A.stock_code, A.&colname., A.fmv_sqr, B.pct1, C.pct99
		FROM &input_table. A LEFT JOIN pct1 B
		ON A.end_date = B.end_date
		LEFT JOIN pct99 C
		ON A.end_Date = C.end_date
		ORDER BY A.end_date;
	QUIT;
	/* winsorize */
	DATA tt_raw;
		SET tt_raw;
		IF not missing(&colname.) AND &colname. > pct99 THEN &colname._mdf = pct99;
		ELSE IF not missing(&colname.) AND &colname. < pct1 THEN &colname._mdf = pct1;
		ELSE IF not missing(&colname.) THEN &colname._mdf = &colname.;
		ELSE &colname._mdf = .;
	RUN;
	/* normalized */
	PROC SQL;
		CREATE TABLE tmp_std AS
		SELECT end_date, std(&colname._mdf) AS std, sum(&colname._mdf * fmv_sqr) AS sum_wt
		FROM tt_raw
		GROUP BY end_date;
	QUIT;
	PROC SQL;
		CREATE TABLE tmp_fmv AS
		SELECT end_date, sum(fmv_sqr) AS sum_fmv
		FROM tt_raw
		WHERE not missing(&colname._mdf) AND not missing(fmv_sqr)
		GROUP BY end_Date;
	QUIT;
	PROC SQL;
		CREATE TABLE tmp_normal AS
		SELECT A.end_date, A.stock_code, A.&colname._mdf, B.std, B.sum_wt, C.sum_fmv
		FROM tt_raw A LEFT JOIN tmp_std B
		ON A.end_date = B.end_date
		LEFT JOIN tmp_fmv C
		ON A.end_date = C.end_date
		ORDER BY A.end_date, A.stock_code;
	QUIT;

	DATA tmp_normal(drop = std sum_wt sum_fmv);
		SET tmp_normal;
		IF not missing(&colname._mdf) THEN &colname._mdf = (&colname._mdf - sum_wt/sum_fmv)/std;
	RUN;
	%IF %SYSEVALF(&is_replace.) %THEN %DO;   /* �滻��ʼֵ */
		DATA tmp_normal(drop = &colname._mdf);
			SET tmp_normal;
			&colname = &colname._mdf;
		RUN;
	%END;
	PROC SORT DATA = &input_table.;
		BY end_date stock_code;
	RUN;
	PROC SORT DATA = tmp_normal;
		BY end_date stock_code;
	RUN;
	DATA &output_table.;
		UPDATE &input_table. tmp_normal;
		BY end_date stock_code;
	RUN;
	PROC SQL;
		DROP TABLE tt_raw, tmp_std, tmp_fmv, tmp_normal, pct1, pct99;
	QUIT;
%MEND  normalize_single_score;


/*** ģ��3: ��׼��������ӵ÷� **/
/** ����:
(1) input_table: end_date / stock_code / fmv_sqr (��ͨ��ֵ��ƽ����) / ������ж���Ϊ���ӵ÷�
(2) exclude_list: �޳������ӵ��У���("TOT", "F1")�ȡ��ǵ���Ҫ�����š�
(3) type: 1-exclude_list��Ч��2-include_list��Ч, 3-���߶���Ч 4-���߶���Ч

/**�������
(1) output_table: �����ж��ɱ�׼����÷��滻��
**/

%MACRO 	normalize_multi_score(input_table, output_table, exclude_list=(), include_list=(), type=1);
	DATA rr_result;
		SET &input_table.;
	RUN;

	PROC CONTENTS DATA = &input_table. OUT = tt_varlist(keep = name) NOPRINT;
	RUN;
	%IF %SYSEVALF(&type.=1) %THEN %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE","FMV_SQR");
			IF upcase(name) NOT IN &exclude_list.;
		RUN;
	%END;
	%ELSE %IF %SYSEVALF(&type.=2) %THEN %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) IN &include_list.;
		RUN;
	%END;
	%ELSE %IF %SYSEVALF(&type.=3) %THEN %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) IN &include_list. AND upcase(name) NOT IN &exclude_list.;
		RUN;
	%END;
	%ELSE %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE","FMV_SQR");
		RUN;
	%END;
		

	PROC SQL NOPRINT;
		SELECT name, count(1)
		 INTO :name_list SEPARATED BY ' ',
               :nfactors
          FROM tt_varlist;
     QUIT;
              
     %DO i = 1 %TO &nfactors.;
	 	 %put total &nfactors.;
	 	 %put NO &i. factor;
          %LET fname =  %scan(&name_list.,&i., ' ');
          %normalize_single_score(rr_result, &fname., rr_result);
		  %put ending...;
     %END;
	 DATA &output_table.;
	 	SET rr_result;
	RUN;
	PROC SQL;
		DROP TABLE rr_result, tt_varlist;
	QUIT;
%MEND normalize_multi_score;


/*** ģ��4: winsorize�����ӵ÷� **/
/** ����:
(1) input_table: end_date / stock_code / &colname.
(2) colname(character): ������
(3) is_replace: �Ƿ�ԭʼ�����滻Ϊ��׼����Ľ����1-�滻 0- ��������:&colname._mdf
(4) upper(��׼���Ͻ�) 
(5) lower(��׼���½�)

/**�������
(1) output_table: input_table + &colname._mdf(is_replace=0)
**/
%MACRO winsorize_single_score(input_table, colname, output_table, upper=3, lower = -3, is_replace = 1);
	/* winsorize */
	DATA tt_raw;
		SET &input_table.;
		IF not missing(&colname.) AND &colname. > &upper. THEN &colname._mdf = &upper.;
		ELSE IF not missing(&colname.) AND &colname. < &lower. THEN &colname._mdf = &lower.;
		ELSE IF not missing(&colname.) THEN &colname._mdf = &colname.;
		ELSE &colname._mdf = .;
	RUN;

	%IF %SYSEVALF(&is_replace.) %THEN %DO;   /* �滻��ʼֵ */
		DATA tt_raw(drop = &colname._mdf);
			SET tt_raw;
			&colname = &colname._mdf;
		RUN;
	%END;
	PROC SORT DATA = &input_table.;
		BY end_date stock_code;
	RUN;
	PROC SORT DATA = tt_raw;
		BY end_date stock_code;
	RUN;
	DATA &output_table.;
		UPDATE &input_table. tt_raw;
		BY end_date stock_code;
	RUN;
	PROC SQL;
		DROP TABLE tt_raw;
	QUIT;
%MEND  winsorize_single_score;


/*** ģ��5: winsorize������ӵ÷� **/
/** ����:
(1) input_table: end_date / stock_code / &colname.
(2) upper(��׼���Ͻ�) 
(3) lower(��׼���½�)
(4) exclude_list: �޳������ӵ��У���(TOT F1)�ȡ��ǵ���Ҫ�����š�
(5) type: 1-exclude_list��Ч��2-include_list��Ч, 3-���߶���Ч 4-���߶���Ч


/**�������
(1) output_table: �����ж���winsorize��÷��滻��
**/

%MACRO 	winsorize_multi_score(input_table, output_table, exclude_list=(), include_list=(), type=1, upper=3, lower = -3);
	DATA rr_result;
		SET &input_table.;
	RUN;

	PROC CONTENTS DATA = &input_table. OUT = tt_varlist(keep = name) NOPRINT;
	RUN;
	%IF %SYSEVALF(&type.=1) %THEN %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE","FMV_SQR");
			IF upcase(name) NOT IN &exclude_list.;
		RUN;
	%END;
	%ELSE %IF %SYSEVALF(&type.=2) %THEN %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) IN &include_list.;
		RUN;
	%END;
	%ELSE %IF %SYSEVALF(&type.=3) %THEN %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) IN &include_list. AND upcase(name) NOT IN &exclude_list.;
		RUN;
	%END;
	%ELSE %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE","FMV_SQR");
		RUN;
	%END;
		
	PROC SQL NOPRINT;
		SELECT name, count(1)
		 INTO :name_list SEPARATED BY ' ',
               :nfactors
          FROM tt_varlist;
     QUIT;
              
     %DO i = 1 %TO &nfactors.;
          %LET fname =  %scan(&name_list.,&i., ' ');
          %winsorize_single_score(rr_result, &fname., rr_result, upper=&upper., lower = &lower., is_replace = 1);
     %END;
	 DATA &output_table.;
	 	SET rr_result;
	RUN;
	PROC SQL;
		DROP TABLE rr_result, tt_varlist;
	QUIT;
%MEND winsorize_multi_score;



/*** ģ��6: �������Ӽ�Ȩ�÷�(��: ����ֵ*�����ӵ�Ȩ��) **/
/** �������ɷ�Ϊ��ͬ���飬��ͬ�����ܲ�һ��������Ȩ�ء���ӵ����ͬ��Ȩ����group_idӦ��ǰ�趨Ϊ��һ��ֵ */

/** ����:
(1) input_table: end_date / stock_code / &colname. /group_id(���)
(2) colname(character): ������
(3) is_replace: �Ƿ�ԭʼ�����滻Ϊ��׼����Ľ����1-�滻 0- ��������:&colname._mdf
(4) weight_table: end_date/ factor_name / weight(���ӵ�Ȩ��) / group_id


/**�������
(1) output_table: input_table + &colname._mdf(is_replace=0)
**/
%MACRO weighted_single_factor(input_table, colname, weight_table, output_table, is_replace=1);

	PROC SQL;
		CREATE TABLE tt_date AS
		SELECT A.end_date, B.end_date AS effective_date
		FROM 
		(SELECT distinct(end_date) FROM &input_table.) A
		LEFT JOIN
		(SELECT distinct(end_date) FROM &weight_table.) B
		ON A.end_date >= B.end_date
		GROUP BY A.end_date
		HAVING B.end_date = max(B.end_date)
		ORDER BY A.end_Date;
	QUIT;
	
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.end_date, A.stock_code, A.&colname., B.weight
		FROM &input_table. A LEFT JOIN tt_date C
		ON A.end_date = C.end_date
		LEFT JOIN &weight_table. B
		ON B.end_date = C.effective_date AND upcase(A.factor_name) = upcase("&colname.") AND A.group_id = B.group_id
		ORDER BY A.end_date;
	QUIT;
	DATA tmp(drop = weight);
		SET tmp;
		IF not missing(weight) THEN &colname._mdf = weight * &colname.;
		ELSE &colname._mdf = 0;
	RUN;
	%IF %SYSEVALF(&is_replace.=1) %THEN %DO;   /* �滻��ʼֵ */
		DATA tmp(drop = &colname._mdf);
			SET tmp;
			&colname. = &colname._mdf;
		RUN;
	%END;
	PROC SORT DATA = &input_table.;
		BY end_date stock_code;
	RUN;
	PROC SORT DATA = tmp;
		BY end_date stock_code;
	RUN;
	DATA &output_table.;
		UPDATE &input_table. tmp;
		BY end_date stock_code;
	RUN;
	PROC SQL;
		DROP TABLE tmp, tt_date;
	QUIT;
%MEND weighted_single_factor;


/*** ģ��7: ���������Ӽ�Ȩ�÷�(��: ����ֵ*�����ӵ�Ȩ��) **/
/** �������ɷ�Ϊ��ͬ���飬��ͬ�����ܲ�һ��������Ȩ�ء���ӵ����ͬ��Ȩ����group_idӦ��ǰ�趨Ϊ��һ��ֵ */

/** ����:
(1) input_table: end_date / stock_code / &colname. /group_id(���)
(2) exclude_list: �޳������ӵ��У���(TOT F1)�ȡ��ǵ���Ҫ�����š�
(3) weight_table: end_date/ factor_name / weight(���ӵ�Ȩ��) / group_id
(4) type: 1-exclude_list��Ч��2-include_list��Ч, 3-���߶���Ч 4-���߶���Ч


/**�������
(1) output_table: input_table + &colname._mdf(is_replace=0)
**/
%MACRO weighted_multi_factor(input_table, weight_table, output_table, exclude_list=(), include_list=(), type=1);
	DATA rr_result;
		SET &input_table.;
	RUN;

	PROC CONTENTS DATA = &input_table. OUT = tt_varlist(keep = name) NOPRINT;
	RUN;
	%IF %SYSEVALF(&type.=1) %THEN %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE","FMV_SQR");
			IF upcase(name) NOT IN &exclude_list.;
		RUN;
	%END;
	%ELSE %IF %SYSEVALF(&type.=2) %THEN %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) IN &include_list.;
		RUN;
	%END;
	%ELSE %IF %SYSEVALF(&type.=3) %THEN %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) IN &include_list. AND upcase(name) NOT IN &exclude_list.;
		RUN;
	%END;
	%ELSE %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE","FMV_SQR");
		RUN;
	%END;
	PROC SQL NOPRINT;
		SELECT name, count(1)
		 INTO :name_list SEPARATED BY ' ',
               :nfactors
          FROM tt_varlist;
     QUIT;
	 
	 /** ȷ��weight_table��һ���ڣ�ÿ����������Ӻ�Ϊ1 */
	 PROC SQL;	
		CREATE TABLE tmp AS
		SELECT A.*, B.t_weight
		FROM &weight_table. A LEFT JOIN 
		(
		SELECT end_date, group_id, sum(weight) AS t_weight
		FROM &weight_table.
		GROUP BY end_date, group_id
		)B
		ON A.end_date = B.end_date AND A.group_id = B.group_id
	QUIT;

	DATA tt_weight(drop = t_weight);
		SET tmp;
		IF t_weight ~= 0 THEN weight = round(weight/t_weight,0.00001);  
		ELSE weight = 0;
	RUN;
              
     %DO i = 1 %TO &nfactors.;
          %LET fname =  %scan(&name_list.,&i., ' ');
          %weighted_single_factor(input_table=rr_result, colname=&fname., weight_table = tt_weight, output_table = rr_result);
     %END;
	 DATA &output_table.;
	 	SET rr_result;
	RUN;
	PROC SQL;
		DROP TABLE rr_result, tt_varlist, tt_weight;
	QUIT;
%MEND weighted_multi_factor;
	

/*** ģ��7: ���Ի����ӵ÷� **/
/** �������ɷ�Ϊ��ͬ���飬��ͬ���ڽ������Ի� */
/** �磺����������һ����ҵ�������Ի�����ֵ����һ���ڶ�����ҵ�����Ի� **/

/** ����:
(1) input_table: end_date / stock_code / &colname. /fmv_sqr / group_id(���)
(2) colname(character): ������
(3) group_name(character): ������(���Ի��ڷ����ڽ���)
(4) is_replace: �Ƿ�ԭʼ�����滻Ϊ��׼����Ľ����1-�滻 0- ��������:&colname._mdf


/**�������
(1) output_table: input_table + &colname._mdf(is_replace=0)
**/


%MACRO neutralize_single_score(input_table, colname, group_name, output_table, is_replace=1);

	PROC SQL;
		CREATE TABLE tmp_std AS
		SELECT end_date, &group_name., sum(&colname. * fmv_sqr) AS sum_wt
		FROM &input_table.
		GROUP BY end_date, &group_name.;
	QUIT;
	PROC SQL;
		CREATE TABLE tmp_fmv AS
		SELECT end_date, &group_name., sum(fmv_sqr) AS sum_fmv
		FROM &input_table.
		WHERE not missing(&colname.) AND not missing(fmv_sqr)
		GROUP BY end_date, &group_name.;
	QUIT;
	PROC SQL;
		CREATE TABLE tmp_normal AS
		SELECT A.end_date, A.stock_code, A.&colname.,B.sum_wt, C.sum_fmv
		FROM &input_table. A LEFT JOIN tmp_std B
		ON A.end_date = B.end_date AND A.&group_name. = B.&group_name.
		LEFT JOIN tmp_fmv C
		ON A.end_date = C.end_date AND A.&group_name. = C.&group_name.
		ORDER BY A.end_date, A.stock_code;
	QUIT;

	DATA tmp_normal(drop = sum_wt sum_fmv);
		SET tmp_normal;
		IF not missing(&colname.) THEN &colname._mdf = &colname. - sum_wt/sum_fmv;
		ELSE &colname._mdf = .;
	RUN;
	%IF %SYSEVALF(&is_replace.) %THEN %DO;   /* �滻��ʼֵ */
		DATA tmp_normal(drop = &colname._mdf);
			SET tmp_normal;
			&colname = &colname._mdf;
		RUN;
	%END;
	PROC SORT DATA = &input_table.;
		BY end_date stock_code;
	RUN;
	PROC SORT DATA = tmp_normal;
		BY end_date stock_code;
	RUN;
	DATA &output_table.;
		UPDATE &input_table. tmp_normal;
		BY end_date stock_code;
	RUN;
	PROC SQL;
		DROP TABLE tmp_std, tmp_fmv, tmp_normal;
	QUIT;

%MEND  neutralize_single_score;


/*** ģ��8: ���Ի�������ӵ÷� **/
/** ����:
(1) input_table: end_date / stock_code / fmv_sqr (��ͨ��ֵ��ƽ����) / ������ж���Ϊ���ӵ÷�
(2) exclude_list: �޳������ӵ��У���(TOT F1)�ȡ��ǵ���Ҫ�����š�
(3) group_name(character): ������(���Ի��ڷ����ڽ���)
(4) type: 1-exclude_list��Ч��2-include_list��Ч, 3-���߶���Ч 4-���߶���Ч


/**�������
(1) output_table: �����ж��ɱ�׼����÷��滻��
**/

/** ע: exclude_list����Ҫ��&group_name�ȷ������е��޳� */
%MACRO 	neutralize_multi_score(input_table, output_table,group_name, exclude_list=(), include_list=(),type=1);
	DATA rr_result;
		SET &input_table.;
	RUN;

	PROC CONTENTS DATA = &input_table. OUT = tt_varlist(keep = name) NOPRINT;
	RUN;
	%IF %SYSEVALF(&type.=1) %THEN %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE","FMV_SQR");
			IF upcase(name) NOT IN &exclude_list.;
		RUN;
	%END;
	%ELSE %IF %SYSEVALF(&type.=2) %THEN %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) IN &include_list.;
		RUN;
	%END;
	%ELSE %IF %SYSEVALF(&type.=3) %THEN %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) IN &include_list. AND upcase(name) NOT IN &exclude_list.;
		RUN;
	%END;
	%ELSE %DO;
		DATA tt_varlist;
			SET tt_varlist;
			IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE","FMV_SQR");
		RUN;
	%END;
	PROC SQL NOPRINT;
		SELECT name, count(1)
		 INTO :name_list SEPARATED BY ' ',
               :nfactors
          FROM tt_varlist;
     QUIT;
              
     %DO i = 1 %TO &nfactors.;
	 	 %put NO &i. factor!!!!;
          %LET fname =  %scan(&name_list.,&i., ' ');
          %neutralize_single_score(rr_result, &fname., &group_name.,  rr_result);
     %END;
	 DATA &output_table.;
	 	SET rr_result;
	RUN;
	PROC SQL;
		DROP TABLE rr_result, tt_varlist;
	QUIT;
%MEND neutralize_multi_score;


/*** ģ��9: �������ӵ÷֣�ȡĳ����λ������ֵ�µķ��� **/
/** ����

/** ����:
(1) input_table: end_date / stock_code / &colname.
(2) colname(character): ������
(3) type: 1-���ݰٷֵ���з���(�ʺ�����������) 2- ���ݾ���ֵ���з���(�ʺ���ɢ������)
(4) threshold: type=1ʱ����ʾ��Ҫ���ֵİٷ�λ��(��: 50/90��); type=2ʱ����ʾ����ֵ
(4) is_decrease: 0-ѡС,1-ѡ��,2-ǡ�����
(5) is_cut: 1-���ֻѡȡ���յ��Ӽ��� 0-����ȫ�������������ֶ�cut_mark(1-required 0-filter)


/**�������
(1) output_table: input_table(is_cut=1:ֻ��������Ҫ����Ӽ�) + cut_mark(is_cut=0)
**/

%MACRO cut_subset(input_table, colname, output_table, type=1, threshold=50, is_decrease=1, is_cut=1);
	%IF %SYSEVALF(&type.=1) %THEN %DO;	
		%cal_percentitle(&input_table., &colname., &threshold., pct_table);
		DATA pct_table;
			SET pct_table(rename = (pct&threshold = threshold));
		RUN;
	%END;
	%ELSE %DO;
		PROC SQL;
			CREATE TABLE pct_table AS  /* ����һ������ʽ */
			SELECT distinct end_date, &threshold. AS threshold
			FROM &input_table.
			ORDER BY end_Date;
		QUIT;
	%END; 
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, B.threshold 
		FROM &input_table. A LEFT JOIN pct_table B
		ON A.end_date = B.end_date
		ORDER BY A.end_date, A.&colname.;
	QUIT;
 	%IF %SYSEVALF(&is_decrease.=1) %THEN %DO;
		DATA &output_table.(drop = threshold);
			SET tmp;
			IF &colname. >= threshold THEN cut_mark = 1;
			ELSE cut_mark = 0;
		RUN;
	%END;
	%ELSE %IF %SYSEVALF(&is_decrease.=0) %THEN %DO;;
		DATA &output_table.(drop = threshold);
			SET tmp;
			IF &colname. <= threshold THEN cut_mark = 1;
			ELSE cut_mark = 0;
		RUN;
	%END;
	%ELSE %DO;;
		DATA &output_table.(drop = threshold);
			SET tmp;
			IF &colname. = threshold THEN cut_mark = 1;
			ELSE cut_mark = 0;
		RUN;
	%END;
	 %IF %SYSEVALF(&is_cut.=1) %THEN %DO;
		DATA &output_table.(drop = cut_mark);
			SET &output_table.;
			IF cut_mark = 1;
		RUN;
	%END;

	PROC SQL;
		DROP TABLE tmp, pct_table;
	QUIT;
%MEND cut_subset;

